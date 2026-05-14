package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
)

// Savepoint issues `SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS`
// against the connection. Plain SQL through the existing Exec path;
// no special wire CP.
//
// IBM i auto-starts a unit of work on the first SAVEPOINT when
// autocommit is on, but the savepoint then scopes nothing useful --
// best practice is to call this from inside an explicit
// `db.BeginTx` block.
//
// Access pattern via database/sql:
//
//	conn, _ := db.Conn(ctx); defer conn.Close()
//	err := conn.Raw(func(driverConn any) error {
//	    return driverConn.(*Conn).Savepoint(ctx, "SP1")
//	})
//
// Name rules (IBM i SQL identifier syntax): 1-128 chars, must
// start with a letter, remaining chars are letters / digits /
// underscore. The server folds names to uppercase.
func (c *Conn) Savepoint(ctx context.Context, name string) error {
	return c.runSavepointSQL(ctx, "SAVEPOINT", name, " ON ROLLBACK RETAIN CURSORS")
}

// ReleaseSavepoint issues `RELEASE SAVEPOINT <name>`.
func (c *Conn) ReleaseSavepoint(ctx context.Context, name string) error {
	return c.runSavepointSQL(ctx, "RELEASE SAVEPOINT", name, "")
}

// RollbackToSavepoint issues `ROLLBACK TO SAVEPOINT <name>`.
func (c *Conn) RollbackToSavepoint(ctx context.Context, name string) error {
	return c.runSavepointSQL(ctx, "ROLLBACK TO SAVEPOINT", name, "")
}

func (c *Conn) runSavepointSQL(ctx context.Context, verb, name, suffix string) error {
	if c.closed {
		return driver.ErrBadConn
	}
	if err := validateSavepointName(name); err != nil {
		return err
	}
	var b strings.Builder
	b.Grow(len(verb) + 1 + len(name) + len(suffix))
	b.WriteString(verb)
	b.WriteByte(' ')
	b.WriteString(name)
	b.WriteString(suffix)
	stmt := &Stmt{conn: c, query: b.String()}
	_, err := stmt.ExecContext(ctx, nil)
	return err
}

// validateSavepointName enforces the IBM i SQL identifier syntax
// that the server accepts unquoted: 1-128 chars, leading letter,
// remaining letters / digits / underscore. Matches JT400's
// JDUtilities.checkSavepointName behaviour.
func validateSavepointName(name string) error {
	if name == "" {
		return fmt.Errorf("db2i: savepoint name is empty")
	}
	if len(name) > 128 {
		return fmt.Errorf("db2i: savepoint name %q exceeds 128 chars", name)
	}
	first := name[0]
	if !isASCIILetter(first) {
		return fmt.Errorf("db2i: savepoint name %q must start with a letter", name)
	}
	for i := 1; i < len(name); i++ {
		ch := name[i]
		if !isASCIILetter(ch) && !isASCIIDigit(ch) && ch != '_' {
			return fmt.Errorf("db2i: savepoint name %q: illegal character %q at position %d (allowed: letters, digits, underscore)", name, ch, i)
		}
	}
	return nil
}

func isASCIILetter(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func isASCIIDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
