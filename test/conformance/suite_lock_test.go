//go:build conformance

// suite_lock_test.go serialises the whole conformance suite against a
// single shared IBM i schema.
//
// The suite creates and drops fixed-name objects -- GOSQL_* matrix
// tables, the GOTCHE *SQLPKG, and friends. IBM i's 10-character
// object-name limit (GOSQL_ + <=4) leaves no room for per-run-unique
// names, so two suites running against the same schema at the same time
// -- two PR CI jobs, the nightly run plus a manual one, or a CI job
// plus a developer's local run -- drop each other's tables mid-flight
// and fail with spurious SQL-204 (object not found). The CI workflow's
// concurrency group is keyed per ref, so different branches never
// serialise on their own.
//
// Rather than make every fixture name unique (impossible within 10
// chars) we serialise the runs themselves with a cross-process advisory
// lease. The lease is one row in a dedicated table:
//
//	GOJTLOCK(id, owner, heartbeat)
//
// A run claims the lease only when it is free or its heartbeat has gone
// stale (the previous holder crashed, or its CI job was cancelled
// mid-suite). It refreshes the heartbeat from a background goroutine
// while the suite runs and clears the owner on release. The claim is a
// single conditional UPDATE; IBM i holds a row lock for the duration of
// that statement, so two simultaneous claimants cannot both win.
//
// The lease is best-effort: any failure to set it up (no authority to
// CREATE the table, a network blip, an over-long wait) degrades to a
// logged warning and the suite runs anyway. It can slow a run down, but
// it can never break one.
package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/complacentsee/go-db2i/hostserver"
)

const (
	// suiteLockTable is deliberately outside the GOSQL_ / GOTCHE
	// families so no test's drop-on-entry or package wipe ever removes
	// it. It persists across runs -- it is the rendezvous point, not a
	// fixture.
	suiteLockTable = "GOJTLOCK"

	// suiteLockStaleSecs is how old a heartbeat may get before another
	// run treats the lease as abandoned and reclaims it. Five missed
	// heartbeats (see suiteLockBeat) -- enough slack for a GC pause or
	// a slow network round-trip, tight enough that a cancelled CI job
	// frees the lease within a couple of minutes.
	suiteLockStaleSecs = 150

	// suiteLockBeat is the heartbeat refresh interval while the suite
	// runs.
	suiteLockBeat = 30 * time.Second

	// suiteLockWait bounds how long acquire blocks before it gives up
	// and (best-effort) lets the suite run unserialised. Comfortably
	// covers a couple of back-to-back full suites queued ahead.
	suiteLockWait = 60 * time.Minute

	// suiteLockPoll is the gap between claim attempts while waiting.
	suiteLockPoll = 10 * time.Second
)

// suiteLock holds an acquired lease plus the resources that keep it
// alive. Release tears it all down.
type suiteLock struct {
	db    *sql.DB
	table string // schema-qualified
	owner string
	stop  chan struct{}
	wg    sync.WaitGroup
}

// acquireSuiteLock claims the suite lease, blocking (up to
// suiteLockWait) until it is free or stale, then starts the heartbeat.
// dsn is the plain connection string; schemaName is the schema the
// suite's fixtures live in (so the lease sits where the contention is).
// Returns (nil, err) on any setup or acquisition failure; the caller
// treats that as "run without the lease" rather than aborting.
func acquireSuiteLock(dsn, schemaName string) (*suiteLock, error) {
	db, err := sql.Open("db2i", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	// One connection is plenty -- the lease is a handful of tiny
	// statements -- and it keeps the lock's own footprint off the
	// shared as-signon port the suite is stressing.
	db.SetMaxOpenConns(1)

	sl := &suiteLock{
		db:    db,
		table: schemaName + "." + suiteLockTable,
		owner: suiteLockOwner(),
		stop:  make(chan struct{}),
	}
	if err := sl.ensureTable(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ensure lease table: %w", err)
	}
	if err := sl.acquire(); err != nil {
		db.Close()
		return nil, err
	}
	sl.wg.Add(1)
	go sl.heartbeatLoop()
	return sl, nil
}

// suiteLockOwner builds a best-effort-unique identifier for this run:
// host + pid + start nanos. Uniqueness only needs to distinguish
// concurrent runs, and the heartbeat column is the real liveness
// signal, so collisions here are harmless.
func suiteLockOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "host"
	}
	owner := fmt.Sprintf("%s/pid%d/%d", host, os.Getpid(), time.Now().UnixNano())
	if len(owner) > 120 {
		owner = owner[:120]
	}
	return owner
}

func (sl *suiteLock) ensureTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Idempotent create; another run racing the same CREATE just gets
	// SQL-601 (already exists), which is success for our purposes.
	create := "CREATE TABLE " + sl.table +
		" (id INTEGER NOT NULL PRIMARY KEY, owner VARCHAR(128), heartbeat TIMESTAMP NOT NULL)"
	if _, err := sl.db.ExecContext(ctx, create); err != nil && !isSQLCode(err, -601) {
		return err
	}
	// Seed the single lease row; a concurrent seeder gets SQL-803
	// (duplicate key), also success.
	seed := "INSERT INTO " + sl.table + " (id, owner, heartbeat) VALUES (1, '', CURRENT_TIMESTAMP)"
	if _, err := sl.db.ExecContext(ctx, seed); err != nil && !isSQLCode(err, -803) {
		return err
	}
	return nil
}

// tryClaim atomically takes the lease iff it is free or stale. The
// conditional UPDATE runs under IBM i's row lock, so concurrent
// claimants serialise and at most one sees RowsAffected == 1.
func (sl *suiteLock) tryClaim() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	q := fmt.Sprintf(
		"UPDATE %s SET owner = ?, heartbeat = CURRENT_TIMESTAMP "+
			"WHERE id = 1 AND (owner = '' OR owner IS NULL "+
			"OR heartbeat < CURRENT_TIMESTAMP - %d SECONDS)",
		sl.table, suiteLockStaleSecs)
	res, err := sl.db.ExecContext(ctx, q, sl.owner)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (sl *suiteLock) acquire() error {
	deadline := time.Now().Add(suiteLockWait)
	logged := false
	for {
		ok, err := sl.tryClaim()
		if err != nil {
			return fmt.Errorf("claim lease: %w", err)
		}
		if ok {
			if logged {
				fmt.Fprintf(os.Stderr, "conformance: suite lease acquired by %s\n", sl.owner)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("suite lease held by another run; gave up after %s", suiteLockWait)
		}
		if !logged {
			fmt.Fprintf(os.Stderr,
				"conformance: another run holds the suite lease; waiting (up to %s)...\n", suiteLockWait)
			logged = true
		}
		time.Sleep(suiteLockPoll)
	}
}

// heartbeatLoop refreshes the lease while the suite runs so other runs
// keep seeing it as live. A failed refresh is ignored: a transient
// blip self-corrects on the next tick, and a sustained outage simply
// lets the lease go stale, which is the intended crash behaviour.
func (sl *suiteLock) heartbeatLoop() {
	defer sl.wg.Done()
	t := time.NewTicker(suiteLockBeat)
	defer t.Stop()
	for {
		select {
		case <-sl.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_, _ = sl.db.ExecContext(ctx,
				"UPDATE "+sl.table+" SET heartbeat = CURRENT_TIMESTAMP WHERE id = 1 AND owner = ?",
				sl.owner)
			cancel()
		}
	}
}

// Release stops the heartbeat, frees the lease (only if we still own
// it), and closes the connection. Safe to call once; idempotency is
// not required because TestMain calls it exactly once.
func (sl *suiteLock) Release() {
	close(sl.stop)
	sl.wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	_, _ = sl.db.ExecContext(ctx,
		"UPDATE "+sl.table+" SET owner = '' WHERE id = 1 AND owner = ?", sl.owner)
	cancel()
	sl.db.Close()
}

// isSQLCode reports whether err is a *hostserver.Db2Error with the
// given SQLCODE (e.g. -601 object-exists, -803 duplicate-key).
func isSQLCode(err error, code int32) bool {
	var dbErr *hostserver.Db2Error
	if errors.As(err, &dbErr) {
		return dbErr.SQLCode == code
	}
	// Fall back to a substring check for the rare path where the code
	// rides in a wrapped non-Db2Error message.
	return strings.Contains(err.Error(), fmt.Sprintf("SQL%d", code))
}
