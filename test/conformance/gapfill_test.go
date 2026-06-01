//go:build conformance

// gapfill_test.go covers the issue-#6 gap-fill items that aren't a
// data-type round-trip: connection-pool churn through the
// SessionResetter discard path, and the context-deadline timeout path.
// (Error-path coverage lives in errors_matrix_test.go; large-result
// streaming, cache invalidation, TLS, and mid-query cancellation are
// already pinned by TestRowsLazyMemoryBounded, the TestCacheHit_* family,
// TestTLSConnectivity, and TestContextCancellationMidQuery respectively.)
package conformance

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db2i "github.com/complacentsee/go-db2i/driver"
)

// TestPoolSessionResetterChurn exercises the pool's discard-and-redial
// path under concurrency. Each lease calls SetSchema -- which marks the
// connection sessionDirty (driver/conn_schema.go) -- reads CURRENT_SCHEMA
// back on the same connection to prove the change took, then returns the
// connection, where ResetSession returns ErrBadConn and the pool retires
// it (driver/health.go). Several goroutines do this concurrently against
// a small pool, so connections are continuously discarded and re-dialed.
// The test asserts every lease observes exactly the schema it set (no
// cross-checkout leakage) and that all ops complete.
//
// Concurrency is deliberately modest and connection acquisition tolerates
// a transient driver.ErrBadConn: a dirty-discard-on-every-lease workload
// re-dials the IBM i as-signon job hard, and a shared/free-tier target
// (PUB400) can briefly refuse a fresh dial under that burst. Retrying the
// lease -- which is what a resilient pool consumer does -- keeps the test
// about pool/resetter correctness rather than the target's signon
// capacity.
func TestPoolSessionResetterChurn(t *testing.T) {
	db := openDB(t)
	db.SetMaxOpenConns(2)

	targets := []string{schema(), "QGPL"}
	const goroutines = 4
	const iters = 4

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*iters)
	var ops atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				target := targets[(g+i)%len(targets)]
				if err := churnSetSchemaOnce(db, target); err != nil {
					errCh <- fmt.Errorf("goroutine %d iter %d (target %s): %w", g, i, target, err)
					return
				}
				ops.Add(1)
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if got, want := int(ops.Load()), goroutines*iters; got != want {
		t.Errorf("completed %d churn ops, want %d", got, want)
	}
}

// acquireConn leases a pooled connection, tolerating the transient
// driver.ErrBadConn the pool can surface when it is churning discarded
// (sessionDirty) connections faster than the as-signon job re-dials under
// load. Any other error is returned immediately.
func acquireConn(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	var last error
	for attempt := 0; attempt < 6; attempt++ {
		conn, err := db.Conn(ctx)
		if err == nil {
			return conn, nil
		}
		if !errors.Is(err, driver.ErrBadConn) {
			return nil, err
		}
		last = err
		time.Sleep(300 * time.Millisecond)
	}
	return nil, fmt.Errorf("ErrBadConn after retries: %w", last)
}

// churnSetSchemaOnce leases one pooled connection, sets CURRENT SCHEMA
// to target, verifies it on the same connection, and releases the conn
// (now sessionDirty) back to the pool for ResetSession to discard.
func churnSetSchemaOnce(db *sql.DB, target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := acquireConn(ctx, db)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Close()

	if err := conn.Raw(func(dc any) error {
		c, ok := dc.(*db2i.Conn)
		if !ok {
			return fmt.Errorf("driverConn is %T, want *db2i.Conn", dc)
		}
		return c.SetSchema(ctx, target)
	}); err != nil {
		return fmt.Errorf("SetSchema: %w", err)
	}

	var cur string
	if err := conn.QueryRowContext(ctx,
		"SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1").Scan(&cur); err != nil {
		return fmt.Errorf("read CURRENT_SCHEMA: %w", err)
	}
	// SetSchema upper-cases the name, so the server returns the folded
	// form -- compare case-insensitively (matching
	// TestSessionResetterDiscardsAfterSetSchema).
	if got := strings.TrimSpace(cur); !strings.EqualFold(got, target) {
		return fmt.Errorf("CURRENT_SCHEMA = %q, want %q (cross-checkout leak?)", got, target)
	}
	return nil
}

// TestContextDeadlineExceededMidQuery complements TestContextCancellation
// MidQuery (manual cancel) and TestSocketTimeoutFiresOnSlowQuery (DSN
// socket-timeout) by driving the third timeout source: a context
// deadline. A 2s-deadline context wraps a 10s server-side DLYJOB; the
// driver arms the net.Conn deadline from the context (driver/context.go)
// and resolveCtxErr substitutes the context error, so the call must
// return context.DeadlineExceeded well before the job would finish.
func TestContextDeadlineExceededMidQuery(t *testing.T) {
	db := openDB(t)
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := db.ExecContext(ctx, "CALL QSYS2.QCMDEXC(?)", "DLYJOB DLY(10)")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected a deadline error, got nil after %s", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want errors.Is(context.DeadlineExceeded)", err)
	}
	// The 10s DLYJOB must be cut off near the 2s deadline, not run to
	// completion. Allow generous slack for tunnel RTT / cleanup.
	if elapsed > 7*time.Second {
		t.Errorf("query ran %s; expected the ~2s deadline to fire", elapsed)
	}
}
