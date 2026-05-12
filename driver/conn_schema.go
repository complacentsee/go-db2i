package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"strings"

	"github.com/complacentsee/go-db2i/hostserver"
)

// SetSchema issues `SET SCHEMA <name>` against the connection,
// changing the default schema for unqualified table / view /
// procedure references on this connection.
//
// Matches JT400's AS400JDBCConnection.setSchema wire output --
// plain SQL through the existing Exec path, no special wire CP.
//
// The name is sanitised against the IBM-i library-identifier rules
// (1-10 chars from [A-Z0-9_#@$]) before assembly; the server folds
// to uppercase on its own, but we canonicalise client-side so
// callers can pass mixed-case names. Names that contain
// quote/semicolon characters are rejected before they reach the
// wire (defence-in-depth against SQL injection through the
// schema-change path).
//
// Access pattern via database/sql:
//
//	conn, _ := db.Conn(ctx); defer conn.Close()
//	err := conn.Raw(func(driverConn any) error {
//	    return driverConn.(*Conn).SetSchema(ctx, "MYLIB")
//	})
//
// Concurrency: the database/sql pool serialises operations on a
// single sql.Conn, so SetSchema is safe to call from one goroutine
// while another holds the same conn via Raw; mixing across pool-
// returned conns is undefined (use one sql.Conn per goroutine if
// it matters).
func (c *Conn) SetSchema(ctx context.Context, name string) error {
	if c.closed {
		return driver.ErrBadConn
	}
	canon := canonPackageIdent(name)
	if err := validateLibraryName(canon); err != nil {
		return fmt.Errorf("db2i: SetSchema: invalid schema name %q: %w", name, err)
	}
	stmt := &Stmt{conn: c, query: "SET SCHEMA " + canon}
	_, err := stmt.ExecContext(ctx, nil)
	return err
}

// AddLibraries appends `libs` to the connection's library list
// (*LIBL) via one NDB ADD_LIBRARY_LIST round-trip. The first entry
// is tagged with indicator 'C' (current schema = libs[0]) and the
// rest with 'L' (append to back of *LIBL), matching the connect-
// time `libraries=A,B,C` semantics.
//
// Each library must satisfy the IBM-i object-name rules (1-10
// chars from [A-Z0-9_#@$]); names are canonicalised to uppercase
// before sending. Duplicate / already-on-list entries return
// errorClass=5 RC=1301 from the server, which the underlying
// hostserver helper tolerates as a warning (no error returned).
//
// Use SetSchema for the simpler "just change the default schema"
// case -- AddLibraries is for callers who need multi-library
// resolution (RPG-shop migrations, environment mirroring).
func (c *Conn) AddLibraries(ctx context.Context, libs []string) error {
	if c.closed {
		return driver.ErrBadConn
	}
	if len(libs) == 0 {
		return fmt.Errorf("db2i: AddLibraries: empty list (pass at least one library)")
	}
	canon := make([]string, len(libs))
	for i, lib := range libs {
		c := canonPackageIdent(lib)
		if err := validateLibraryName(c); err != nil {
			return fmt.Errorf("db2i: AddLibraries: invalid library name %q at index %d: %w", lib, i, err)
		}
		canon[i] = c
	}
	// withContextDeadline arms the underlying net.Conn so the
	// in-flight write/read unblocks on ctx cancellation -- same
	// pattern Stmt.ExecContext uses for the prepared-exec path.
	cleanup := withContextDeadline(ctx, c.conn)
	defer cleanup()
	if err := hostserver.NDBAddLibraryListMulti(c.conn, canon, c.nextCorr()); err != nil {
		return c.classifyConnErr(fmt.Errorf("db2i: AddLibraries: %w", resolveCtxErr(ctx, err)))
	}
	return nil
}

// RemoveLibraries removes each entry in `libs` from the
// connection's library list, looping `CALL QSYS2.QCMDEXC('RMVLIBLE
// LIB(X)')` once per library. JT400 doesn't expose a NDB REMOVE
// wire either -- mid-session library-list shrinking is a CL
// operation on both sides.
//
// CPF-2104 ("library not in list") is downgraded to a slog WARN
// log line and the loop continues -- callers that want strict
// behaviour should pre-check the list. Other CPF / SQL errors
// abort the loop and return the wrapped error from the failing
// library; libraries earlier in the slice were removed
// successfully and are not rolled back.
//
// CPF-9810 ("library not found") is also tolerated -- the typed-
// wrong-name case is benign for our purposes since the library
// can't have been on the list if it doesn't exist.
func (c *Conn) RemoveLibraries(ctx context.Context, libs []string) error {
	if c.closed {
		return driver.ErrBadConn
	}
	if len(libs) == 0 {
		return fmt.Errorf("db2i: RemoveLibraries: empty list (pass at least one library)")
	}
	for i, lib := range libs {
		canon := canonPackageIdent(lib)
		if err := validateLibraryName(canon); err != nil {
			return fmt.Errorf("db2i: RemoveLibraries: invalid library name %q at index %d: %w", lib, i, err)
		}
		// Use parameter binding rather than literal concat -- it's
		// the proven path for QCMDEXC and avoids any server-side
		// parsing quirks around SQL string literals in stored-proc
		// arguments.
		cmd := "RMVLIBLE LIB(" + canon + ")"
		stmt := &Stmt{conn: c, query: "CALL QSYS2.QCMDEXC(?)"}
		if _, err := stmt.ExecContext(ctx, []driver.NamedValue{{Ordinal: 1, Value: cmd}}); err != nil {
			if isBenignRmvlibleErr(err) {
				if c.log != nil {
					c.log.LogAttrs(ctx, slog.LevelWarn, "db2i: RemoveLibraries: library not on list (treating as success)",
						slog.String("library", canon),
						slog.String("err", err.Error()),
					)
				}
				continue
			}
			return fmt.Errorf("db2i: RemoveLibraries: %s: %w", canon, err)
		}
	}
	return nil
}

// isBenignRmvlibleErr returns true when the RMVLIBLE failure is
// the "library not on list" / "library not found" case. The Db2-
// for-i wrapper around QCMDEXC turns CPF messages into SQL-443
// (CPF received from external program); on V7R6M0 the error text
// contains the friendly CPF message rather than the bare CPF id,
// so the match is a union of message-id and message-text shapes.
//
// Recognised benign signals:
//   - "CPF2104" / "CPF9810" (message ID when surfaced)
//   - "not removed from"   (CPF2104 friendly text -- "Library X not
//     removed from library list" because X is not on the list)
//   - "not found"          (CPF9810 friendly text -- library
//     doesn't exist)
func isBenignRmvlibleErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "CPF2104") || strings.Contains(msg, "CPF9810") {
		return true
	}
	if strings.Contains(msg, "not removed from") {
		return true
	}
	if strings.Contains(msg, "not found") {
		return true
	}
	return false
}
