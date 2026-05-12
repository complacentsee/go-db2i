package driver

import (
	"context"
	"errors"
	"net"
	"time"
)

// withContextDeadline arms the underlying net.Conn so an in-flight
// host-server read/write unblocks when ctx is canceled or its
// deadline elapses. Returns a cleanup func the caller defers to
// restore the conn's prior deadline state.
//
// Mechanics:
//
//   - If ctx has a deadline, install it via SetDeadline. Any read/
//     write that exceeds it returns a *net.OpError with a timeout.
//   - Independently, register context.AfterFunc on ctx.Done(). When
//     the ctx is canceled (e.g. user pressed Ctrl-C, parent ctx
//     killed by graceful shutdown) the AfterFunc fires SetDeadline
//     to time.Now(), which unblocks the read/write immediately.
//
// Cleanup resets the deadline back to zero so the conn isn't left
// in a permanently-deadlined state for the next operation. If the
// ctx already canceled by the time the operation completes, the
// hostserver call returned an i/o error; the caller should check
// ctx.Err() and substitute that, since the i/o error is just the
// mechanism by which we delivered the cancellation.
//
// Returns a no-op cleanup if ctx is nil or already done.
func withContextDeadline(ctx context.Context, conn net.Conn) func() {
	return withContextDeadlineDefault(ctx, conn, 0)
}

// withContextDeadlineDefault is the SocketTimeout-aware variant of
// withContextDeadline (v0.7.16). When `ctx` has no deadline AND
// `defaultDur > 0`, the helper installs `time.Now().Add(defaultDur)`
// as the conn's read deadline -- giving callers a per-op safety net
// against unresponsive servers that don't drop the TCP connection.
// An explicit ctx deadline still wins (it's installed unchanged).
//
// Use this wrapper from sites that have access to Config.SocketTimeout
// (Stmt.ExecContext / QueryContext, Conn.BatchExec, BeginTx). Sites
// that legitimately need "no automatic timeout, the ctx alone is
// authoritative" can keep using plain withContextDeadline.
func withContextDeadlineDefault(ctx context.Context, conn net.Conn, defaultDur time.Duration) func() {
	if ctx == nil || conn == nil {
		return func() {}
	}
	// If ctx is already done, set deadline to past so the next i/o
	// returns immediately without writing anything to the wire.
	if err := ctx.Err(); err != nil {
		_ = conn.SetDeadline(time.Unix(1, 0))
		return func() { _ = conn.SetDeadline(time.Time{}) }
	}

	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else if defaultDur > 0 {
		// No caller-supplied deadline: arm the SocketTimeout default.
		// AfterFunc still fires on ctx cancellation; the deadline
		// triggers on inactivity past defaultDur.
		_ = conn.SetDeadline(time.Now().Add(defaultDur))
	}

	stop := context.AfterFunc(ctx, func() {
		// Force any pending read/write to unblock with a deadline
		// error. The caller's ctx.Err() check then surfaces the
		// real cancellation reason.
		_ = conn.SetDeadline(time.Unix(1, 0))
	})

	return func() {
		stop()
		_ = conn.SetDeadline(time.Time{})
	}
}

// resolveCtxErr substitutes ctx.Err() for ioErr when ctx has been
// canceled or its deadline elapsed. The hostserver layer reports
// I/O timeouts as *net.OpError, but those are an implementation
// detail of how we delivered the cancellation -- callers care about
// context.Canceled / context.DeadlineExceeded.
//
// If ctx is fine, returns ioErr unchanged.
func resolveCtxErr(ctx context.Context, ioErr error) error {
	if ctx == nil {
		return ioErr
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// Wrap so callers can errors.Is(err, context.Canceled) /
		// context.DeadlineExceeded but still see the underlying
		// transport error via Unwrap if they want to debug.
		if ioErr == nil {
			return ctxErr
		}
		return &ctxErrWrap{ctxErr: ctxErr, cause: ioErr}
	}
	return ioErr
}

// ctxErrWrap satisfies errors.Is(err, context.Canceled) /
// errors.Is(err, context.DeadlineExceeded) while preserving the
// underlying transport error for diagnostic Unwrap chains.
type ctxErrWrap struct {
	ctxErr error
	cause  error
}

func (w *ctxErrWrap) Error() string  { return w.ctxErr.Error() }
func (w *ctxErrWrap) Unwrap() []error { return []error{w.ctxErr, w.cause} }
func (w *ctxErrWrap) Is(target error) bool {
	return errors.Is(w.ctxErr, target)
}
