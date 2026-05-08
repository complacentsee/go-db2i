package driver

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// Stmt holds the SQL string for later Exec/Query. We don't currently
// PREPARE on the server until execute time, so Stmt is essentially a
// closure over (conn, query). NumInput returns -1 (unknown) so the
// runtime won't try to count parameter markers itself; the bind
// path validates count when it builds the wire format.
type Stmt struct {
	conn  *Conn
	query string
}

// NumInput: -1 means "let the driver figure it out" -- we don't
// currently bind parameters via this path (M3 deferred work).
func (s *Stmt) NumInput() int { return -1 }

func (s *Stmt) Close() error { return nil }

// Exec runs INSERT / UPDATE / DELETE / DDL via ExecuteImmediate.
// Parameter binding via args is NOT implemented in this scaffold --
// callers must inline values into the SQL string. Lands with the
// M3-deferred prepared-bind I/U/D work.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("gojtopen: parameter binding for Exec not yet implemented (got %d args)", len(args))
	}
	if isSelect(s.query) {
		return nil, fmt.Errorf("gojtopen: Exec called with SELECT; use Query")
	}
	res, err := hostserver.ExecuteImmediate(s.conn.conn, s.query, s.conn.nextCorr())
	if err != nil {
		return nil, err
	}
	return &Result{rowsAffected: res.RowsAffected}, nil
}

// Query runs a SELECT (or VALUES / WITH) via SelectStaticSQL.
// Buffers the entire result set; lazy iteration is M6+ work.
// Parameter binding NOT implemented in this scaffold.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("gojtopen: parameter binding for Query not yet implemented (got %d args)", len(args))
	}
	if !isSelect(s.query) {
		return nil, fmt.Errorf("gojtopen: Query called with non-SELECT; use Exec")
	}
	res, err := hostserver.SelectStaticSQL(s.conn.conn, s.query, s.conn.nextCorr())
	if err != nil {
		return nil, err
	}
	return &Rows{result: res, pos: 0}, nil
}

// isSelect returns true iff the SQL begins with SELECT, VALUES, WITH,
// or any other read-only verb. Used to dispatch Exec vs Query without
// requiring the caller to pre-classify.
func isSelect(sql string) bool {
	for i, r := range sql {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		head := sql[i:]
		// 6 is the longest read-only verb we look for ("SELECT")
		if len(head) >= 6 && strings.EqualFold(head[:6], "SELECT") {
			return true
		}
		if len(head) >= 6 && strings.EqualFold(head[:6], "VALUES") {
			return true
		}
		if len(head) >= 4 && strings.EqualFold(head[:4], "WITH") {
			return true
		}
		return false
	}
	return false
}
