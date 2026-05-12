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
