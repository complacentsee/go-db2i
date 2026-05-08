package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// TestIsConnLevelErr classifies the error types our wrappers detect
// as TCP-level failures. The pool-eviction logic depends on this
// being precisely tuned: false negatives mean dead conns linger;
// false positives mean valid conns get retired after recoverable
// SQL errors.
func TestIsConnLevelErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil -> false", nil, false},
		{"io.EOF -> true", io.EOF, true},
		{"io.ErrUnexpectedEOF -> true", io.ErrUnexpectedEOF, true},
		{"wrapped EOF -> true", fmt.Errorf("frame: %w", io.EOF), true},
		{"hostserver.ErrShortFrame -> true", hostserver.ErrShortFrame, true},
		{"net.OpError read timeout -> true",
			&net.OpError{Op: "read", Err: errors.New("i/o timeout")}, true},
		{"net.OpError write reset -> true",
			&net.OpError{Op: "write", Err: errors.New("connection reset by peer")}, true},
		{"plain errors.New -> false", errors.New("syntax error near 'SLECT'"), false},
		{"Db2Error 23505 (constraint) -> false",
			&hostserver.Db2Error{SQLCode: -803, SQLState: "23505"}, false},
		{"Db2Error 42704 (table-not-found) -> false",
			&hostserver.Db2Error{SQLCode: -204, SQLState: "42704"}, false},
		{"Db2Error 08001 (conn-lost) -> true",
			&hostserver.Db2Error{SQLCode: -30080, SQLState: "08001"}, true},
		{"wrapped Db2Error 08006 -> true",
			fmt.Errorf("commit: %w", &hostserver.Db2Error{SQLState: "08006"}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isConnLevelErr(tc.err); got != tc.want {
				t.Errorf("isConnLevelErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestConnClassifyConnErrMarksDead verifies that classifyConnErr
// (a) marks the Conn dead for connection-level failures so
// IsValid/ResetSession fail fast on the next pool checkout, and
// (b) wraps the conn-level error so it satisfies
// errors.Is(err, driver.ErrBadConn) (which makes database/sql's
// auto-retry kick in) while still letting callers errors.Unwrap to
// the original cause.
//
// SQL-level errors are returned unchanged so the caller sees the
// typed *Db2Error with SQLSTATE / SQLCODE / tokens.
func TestConnClassifyConnErrMarksDead(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantDead    bool
		wantBadConn bool
	}{
		{"EOF: dead + ErrBadConn", io.EOF, true, true},
		{"OpError: dead + ErrBadConn",
			&net.OpError{Op: "read", Err: errors.New("EOF")}, true, true},
		{"08xxx: dead + ErrBadConn",
			&hostserver.Db2Error{SQLState: "08001"}, true, true},
		{"SQL syntax: live, returned as-is",
			&hostserver.Db2Error{SQLCode: -104, SQLState: "42601"}, false, false},
		{"nil: no-op", nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{}
			got := c.classifyConnErr(tc.err)
			if c.closed != tc.wantDead {
				t.Errorf("c.closed = %v, want %v", c.closed, tc.wantDead)
			}
			if tc.err == nil {
				if got != nil {
					t.Errorf("classifyConnErr(nil) = %v, want nil", got)
				}
				return
			}
			isBad := errors.Is(got, driver.ErrBadConn)
			if isBad != tc.wantBadConn {
				t.Errorf("errors.Is(got, ErrBadConn) = %v, want %v (got=%v)", isBad, tc.wantBadConn, got)
			}
			// In both cases the original cause is reachable.
			if !errors.Is(got, tc.err) && got != tc.err {
				// errors.Is on a *Db2Error compares values; for the
				// non-wrap case got == tc.err so the second clause
				// covers it.
				t.Errorf("original cause not reachable from %v", got)
			}
		})
	}
}

// TestConnIsValidAndResetSession exercises the database/sql interface
// hooks that gate pool reuse. A live conn passes; a closed/dead conn
// fails IsValid and returns driver.ErrBadConn from ResetSession.
func TestConnIsValidAndResetSession(t *testing.T) {
	c := &Conn{}
	if !c.IsValid() {
		t.Error("fresh Conn.IsValid() = false, want true")
	}
	if err := c.ResetSession(context.Background()); err != nil {
		t.Errorf("fresh Conn.ResetSession = %v, want nil", err)
	}

	c.markDead()
	if c.IsValid() {
		t.Error("dead Conn.IsValid() = true, want false")
	}
	if err := c.ResetSession(context.Background()); !errors.Is(err, driver.ErrBadConn) {
		t.Errorf("dead Conn.ResetSession = %v, want driver.ErrBadConn", err)
	}
}
