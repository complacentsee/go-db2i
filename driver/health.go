package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"net"

	"github.com/complacentsee/go-db2i/hostserver"
)

// markDead flags the connection as unusable. Subsequent Prepare /
// Begin / ResetSession / IsValid calls signal driver.ErrBadConn so
// database/sql evicts the conn from its pool and creates a fresh
// one on the next checkout. Idempotent.
//
// We don't return ErrBadConn from the call that DETECTED the
// failure -- the caller still gets the original error so it can log
// the root cause -- but the conn won't be reused.
func (c *Conn) markDead() {
	c.closed = true // closed gates Prepare / Begin already; reuse it
}

// classifyConnErr inspects err and, if it indicates the underlying
// TCP connection is no longer usable, marks the Conn dead and
// returns an error that satisfies errors.Is(err, driver.ErrBadConn)
// while still wrapping the original cause (reachable via
// errors.Unwrap). Otherwise returns err unchanged.
//
// Wrapping with the badConnWrap shim is what makes database/sql's
// auto-retry kick in: when the conn was just checked out from the
// pool and no wire activity has succeeded yet, the sql package
// catches ErrBadConn and transparently retries on a fresh conn.
// Preserving the original error in the wrapper means callers who
// want to know WHY the conn died (TCP RST vs server-side 08xxx vs
// short-frame desync) can still get there via errors.As / Unwrap.
//
// The conditions we treat as "conn dead":
//
//   - io.EOF / io.ErrUnexpectedEOF: TCP peer closed mid-frame.
//   - hostserver.ErrShortFrame: framer bailed before the body
//     finished -- the wire is desynced and recovery would require
//     re-handshaking.
//   - *net.OpError on read/write: any network-level I/O error
//     (RST, EPIPE, deadline exceeded mid-frame, "use of closed
//     network connection", etc.). Distinct from a server-side SQL
//     error which arrives in a well-formed reply.
//   - *hostserver.Db2Error with SQLSTATE 08xxx: server explicitly
//     told us the session is gone (rare; usually the TCP layer
//     reports it first).
//
// Cases we do NOT classify as conn dead -- statement-level errors
// that don't compromise the underlying TCP socket:
//
//   - SQL errors with non-08xxx SQLSTATE (syntax, constraint, etc.)
//   - Driver-level binding errors (param-type mismatch, etc.)
//   - Context cancellation when ctx.Err() is the cause -- the
//     request didn't reach the server, but the conn is still
//     usable for the next operation. Caller decides via ctx.Err.
func (c *Conn) classifyConnErr(err error) error {
	if err == nil || c == nil {
		return err
	}
	if isConnLevelErr(err) {
		c.markDead()
		if c.log != nil {
			c.log.LogAttrs(context.Background(), slog.LevelWarn, "db2i: classified as ErrBadConn (pool will retire conn)",
				slog.String("err", err.Error()),
			)
		}
		return &badConnWrap{err: err}
	}
	// Surface non-fatal statement-level failures at ERROR so callers
	// who treat any ERROR-level event as actionable don't miss SQL
	// constraint violations / syntax errors / etc. when their logging
	// pipeline filters by level.
	if c.log != nil {
		c.log.LogAttrs(context.Background(), slog.LevelError, "db2i: operation failed",
			slog.String("err", err.Error()),
		)
	}
	return err
}

// badConnWrap is the wrapper that makes a conn-level error satisfy
// errors.Is(err, driver.ErrBadConn) for database/sql's retry logic
// while still letting callers errors.Unwrap to the original cause.
type badConnWrap struct {
	err error
}

func (w *badConnWrap) Error() string { return w.err.Error() }
func (w *badConnWrap) Unwrap() error { return w.err }
func (w *badConnWrap) Is(target error) bool {
	return target == driver.ErrBadConn
}

// isConnLevelErr is the predicate side of classifyConnErr. Exposed
// as a free function so tests can probe the classifier without
// constructing a *Conn.
func isConnLevelErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, hostserver.ErrShortFrame) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// All net.OpError variants we'd see (read/write/dial after
		// connect) mean the socket is in a bad state.
		return true
	}
	var dbErr *hostserver.Db2Error
	if errors.As(err, &dbErr) && dbErr.IsConnectionLost() {
		return true
	}
	return false
}

// IsValid implements driver.Validator. database/sql calls this
// before handing the conn to a caller; returning false makes the
// pool discard the conn without using it. Cheap (just a flag
// check); does not touch the wire.
func (c *Conn) IsValid() bool {
	return c != nil && !c.closed
}

// ResetSession implements driver.SessionResetter. database/sql calls
// this before reusing a connection from the pool. We use it as the
// "is this conn still alive?" gate -- returning driver.ErrBadConn
// here makes the pool discard the conn and create a fresh one,
// without the caller having to retry.
//
// We currently don't reset any session-level state on this hook
// (no temporary tables, no SET-options that survive across requests
// in the labelverification-gw use case). If that changes, this is
// the place to send the cleanup frames.
func (c *Conn) ResetSession(ctx context.Context) error {
	if c == nil || c.closed {
		return driver.ErrBadConn
	}
	return nil
}
