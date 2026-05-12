package driver

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// TestWithContextDeadlineForwardsDeadline confirms a ctx with a
// deadline propagates that deadline to the net.Conn so an
// in-flight read/write would be unblocked at the right time.
func TestWithContextDeadlineForwardsDeadline(t *testing.T) {
	conn := newDeadlineRecorder()
	deadline := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	cleanup := withContextDeadline(ctx, conn)
	defer cleanup()

	if got := conn.lastDeadline(); !got.Equal(deadline) {
		t.Errorf("conn.lastDeadline = %v, want %v", got, deadline)
	}
}

// TestWithContextDeadlineCancelUnblocks confirms canceling the ctx
// drives SetDeadline on the conn so any in-flight read/write
// returns immediately. We can't easily test the actual unblock
// here without a net.Pipe set up, so we just verify the AfterFunc
// fired by checking conn.SetDeadline was called with a past time.
func TestWithContextDeadlineCancelUnblocks(t *testing.T) {
	conn := newDeadlineRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	cleanup := withContextDeadline(ctx, conn)
	defer cleanup()

	cancel()

	// AfterFunc runs on a goroutine; spin briefly waiting for the
	// SetDeadline call. 100ms is way more than the runtime needs to
	// drain a single AfterFunc.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		last := conn.lastDeadline()
		if !last.IsZero() && last.Before(time.Now()) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("ctx cancel did not call SetDeadline with a past time; last=%v", conn.lastDeadline())
}

// TestWithContextDeadlineDefault_armsDefaultWhenCtxHasNone pins the
// v0.7.16 SocketTimeout safety net: with a background ctx (no
// deadline) and a positive default, the conn's deadline is set to
// roughly time.Now() + defaultDur. An explicit ctx deadline still
// takes precedence (covered by
// TestWithContextDeadlineForwardsDeadline above).
func TestWithContextDeadlineDefault_armsDefaultWhenCtxHasNone(t *testing.T) {
	conn := newDeadlineRecorder()
	const want = 250 * time.Millisecond
	before := time.Now()
	cleanup := withContextDeadlineDefault(context.Background(), conn, want)
	defer cleanup()

	got := conn.lastDeadline()
	if got.IsZero() {
		t.Fatal("expected SocketTimeout-default deadline to be installed; got zero")
	}
	elapsed := got.Sub(before)
	// Allow a generous slop window for scheduler jitter on shared
	// CI -- the test only cares the deadline landed in the
	// vicinity of `want`, not the exact nanosecond.
	if elapsed < want-50*time.Millisecond || elapsed > want+50*time.Millisecond {
		t.Errorf("default deadline offset = %v, want ~%v (±50ms slop)", elapsed, want)
	}
}

// TestWithContextDeadlineDefault_zeroIsNoOp confirms the
// historical (pre-v0.7.16) behaviour is preserved: with default=0
// and a deadline-less ctx, the helper installs no deadline so the
// op blocks indefinitely on the caller's ctx alone.
func TestWithContextDeadlineDefault_zeroIsNoOp(t *testing.T) {
	conn := newDeadlineRecorder()
	cleanup := withContextDeadlineDefault(context.Background(), conn, 0)
	defer cleanup()

	if got := conn.lastDeadline(); !got.IsZero() {
		t.Errorf("default=0 should not install a deadline; got %v", got)
	}
}

// TestWithContextDeadlineDefault_ctxDeadlineWins confirms an
// explicit ctx deadline takes priority over the SocketTimeout
// default, so an aggressive query-level ctx isn't padded by the
// connection-wide default.
func TestWithContextDeadlineDefault_ctxDeadlineWins(t *testing.T) {
	conn := newDeadlineRecorder()
	ctxDeadline := time.Now().Add(50 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), ctxDeadline)
	defer cancel()

	// Default is huge (10x larger). ctx must still win.
	cleanup := withContextDeadlineDefault(ctx, conn, 500*time.Millisecond)
	defer cleanup()

	got := conn.lastDeadline()
	if !got.Equal(ctxDeadline) {
		t.Errorf("ctx deadline didn't win: got %v, want %v", got, ctxDeadline)
	}
}

// TestConnect_LoginTimeoutOverride confirms the v0.7.16
// ?login-timeout= knob actually shortens the dial. We dial a
// guaranteed-unroutable IP (TEST-NET-3, RFC 5737) with a 250ms
// LoginTimeout and assert the connect fails inside ~3x that window
// -- catches a regression where the cfg knob is parsed but never
// fed to the dialer. (The historical 30s default would make the
// test wall-clock for the entire 30s even on an "unreachable"
// failure mode, so the short-timeout assertion is load-bearing.)
//
// Skipped if the kernel routes 203.0.113.1 anyway (would surprise
// us by either succeeding or returning ECONNREFUSED in <1ms,
// either of which still completes well under the upper bound).
func TestConnect_LoginTimeoutOverride(t *testing.T) {
	cfg := &Config{
		Host:         "203.0.113.1", // RFC 5737 TEST-NET-3, unroutable
		DBPort:       8471,
		SignonPort:   8476,
		User:         "u",
		Password:     "p",
		LoginTimeout: 250 * time.Millisecond,
	}
	connector := &Connector{cfg: cfg}

	start := time.Now()
	_, err := connector.Connect(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected dial error against TEST-NET-3 unroutable IP")
	}
	// Generous upper bound: 3x the timeout to absorb scheduler
	// jitter and Go's net package overhead. Failing at 5+ seconds
	// would indicate the knob silently fell back to the 30s default.
	upper := 3 * cfg.LoginTimeout
	if elapsed > upper {
		t.Errorf("dial took %v; LoginTimeout was %v (upper bound %v) -- knob may not be wired",
			elapsed, cfg.LoginTimeout, upper)
	}
}

// TestResolveCtxErrSubstitutesContextErr confirms that when ctx is
// canceled / timed out, resolveCtxErr returns a wrapper that
// satisfies errors.Is(err, context.Canceled) and errors.Is(err,
// context.DeadlineExceeded) regardless of the underlying I/O error
// the hostserver layer produced.
func TestResolveCtxErrSubstitutesContextErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ioErr := &net.OpError{Op: "read", Err: errors.New("i/o timeout")}
	got := resolveCtxErr(ctx, ioErr)
	if !errors.Is(got, context.Canceled) {
		t.Errorf("errors.Is(got, context.Canceled) = false; got=%v", got)
	}
	// Original cause still reachable.
	if !errors.Is(got, ioErr) {
		t.Errorf("original ioErr not unwrapable from %v", got)
	}

	// And when ctx is healthy, ioErr passes through unchanged.
	healthy := context.Background()
	if got := resolveCtxErr(healthy, ioErr); got != ioErr {
		t.Errorf("healthy ctx mutated err: got=%v, want=%v", got, ioErr)
	}

	// nil ioErr with canceled ctx returns ctx.Err().
	if got := resolveCtxErr(ctx, nil); !errors.Is(got, context.Canceled) {
		t.Errorf("nil ioErr + canceled ctx should return ctx.Err(); got=%v", got)
	}
}

// deadlineRecorder is a stub net.Conn that just records SetDeadline
// calls, so the context-plumbing tests can assert on what the
// driver did without needing a real socket. SetDeadline is called
// from both the main test goroutine (via withContextDeadline's
// initial SetDeadline) AND from the context.AfterFunc-spawned
// cancellation goroutine, so lastDeadline reads/writes go through
// a mutex.
type deadlineRecorder struct {
	net.Conn // unused; embedded so we satisfy the interface
	mu       sync.Mutex
	last     time.Time
}

func newDeadlineRecorder() *deadlineRecorder { return &deadlineRecorder{} }

func (d *deadlineRecorder) SetDeadline(t time.Time) error {
	d.mu.Lock()
	d.last = t
	d.mu.Unlock()
	return nil
}
func (d *deadlineRecorder) lastDeadline() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.last
}
func (d *deadlineRecorder) SetReadDeadline(t time.Time) error  { return nil }
func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error { return nil }
func (d *deadlineRecorder) Read(b []byte) (int, error)         { return 0, nil }
func (d *deadlineRecorder) Write(b []byte) (int, error)        { return len(b), nil }
func (d *deadlineRecorder) Close() error                       { return nil }
func (d *deadlineRecorder) LocalAddr() net.Addr                { return nil }
func (d *deadlineRecorder) RemoteAddr() net.Addr               { return nil }
