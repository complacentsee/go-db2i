package driver

import (
	"database/sql/driver"
	"testing"
)

// TestConnImplementsExecerContext confirms the v0.7.20 shortcut
// satisfies the database/sql driver.ExecerContext interface.
// Without this, `db.Exec` falls back to Prepare → Stmt.ExecContext
// → Stmt.Close, paying a struct allocation per call.
//
// We check the interface satisfaction at compile-time via type
// assertion -- if a future refactor accidentally drops the
// ExecContext method off *Conn, this stops compiling.
func TestConnImplementsExecerContext(t *testing.T) {
	var _ driver.ExecerContext = (*Conn)(nil)
}

// TestConnImplementsQueryerContext mirrors the above for the
// Query-side shortcut. Same compile-time guarantee.
func TestConnImplementsQueryerContext(t *testing.T) {
	var _ driver.QueryerContext = (*Conn)(nil)
}

// TestConnShortcutsRejectClosedConn confirms both shortcuts honour
// the closed flag and return driver.ErrBadConn -- same contract as
// Prepare's gate, so a dead conn surfaces consistently regardless
// of which dispatch path database/sql picks.
func TestConnShortcutsRejectClosedConn(t *testing.T) {
	c := &Conn{cfg: &Config{}, log: silentLogger, closed: true}
	if _, err := c.ExecContext(nil, "SELECT 1", nil); err != driver.ErrBadConn {
		t.Errorf("closed Conn.ExecContext = %v, want driver.ErrBadConn", err)
	}
	if _, err := c.QueryContext(nil, "SELECT 1", nil); err != driver.ErrBadConn {
		t.Errorf("closed Conn.QueryContext = %v, want driver.ErrBadConn", err)
	}
}
