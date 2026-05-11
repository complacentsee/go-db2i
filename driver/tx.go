package driver

import (
	"fmt"

	"github.com/complacentsee/go-db2i/hostserver"
)

// Tx wraps Commit/Rollback through our hostserver primitives. After
// Commit or Rollback succeeds, autocommit is restored to ON so
// subsequent simple statements don't accumulate state.
type Tx struct {
	conn *Conn
}

// Commit issues a host-server COMMIT (CP 0x1807) on the underlying
// connection, then re-enables autocommit. Implements
// database/sql/driver.Tx.Commit.
func (t *Tx) Commit() error {
	if err := hostserver.Commit(t.conn.conn, t.conn.nextCorr()); err != nil {
		return t.conn.classifyConnErr(fmt.Errorf("db2i: commit: %w", err))
	}
	if err := hostserver.AutocommitOn(t.conn.conn, t.conn.nextCorr()); err != nil {
		return t.conn.classifyConnErr(fmt.Errorf("db2i: restore autocommit after commit: %w", err))
	}
	return nil
}

// Rollback issues a host-server ROLLBACK (CP 0x1808) on the
// underlying connection, then re-enables autocommit. Implements
// database/sql/driver.Tx.Rollback.
func (t *Tx) Rollback() error {
	if err := hostserver.Rollback(t.conn.conn, t.conn.nextCorr()); err != nil {
		return t.conn.classifyConnErr(fmt.Errorf("db2i: rollback: %w", err))
	}
	if err := hostserver.AutocommitOn(t.conn.conn, t.conn.nextCorr()); err != nil {
		return t.conn.classifyConnErr(fmt.Errorf("db2i: restore autocommit after rollback: %w", err))
	}
	return nil
}
