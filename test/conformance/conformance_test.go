//go:build conformance

// Package conformance ports bradfitz/go-sql-test's database/sql
// integration tests onto goJTOpen. Gated by a build tag because they
// require a live IBM i target -- see the package doc for env vars.
//
// Run with:
//
//	GOJTOPEN_DSN="gojtopen://USER:PWD@host:8471/?library=GOSQLTEST" \
//	  go test -tags=conformance ./test/conformance/...
//
// Tests adapted from github.com/bradfitz/go-sql-test (Apache-2.0
// equivalent license is fine to recreate the patterns; the actual
// test code below is a reimplementation against IBM i-specific SQL).
package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/complacentsee/goJTOpen/driver"
)

// dsn returns the connection string from GOJTOPEN_DSN, skipping the
// test if not set. Tests that mutate schema/data drop their own
// tables on entry to keep the target idempotent.
func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("GOJTOPEN_DSN")
	if v == "" {
		t.Skip("GOJTOPEN_DSN not set; skipping live conformance test")
	}
	return v
}

// schema returns the schema name to use for test tables; defaults to
// GOTEST. Override via GOJTOPEN_SCHEMA if your library list differs.
func schema() string {
	if v := os.Getenv("GOJTOPEN_SCHEMA"); v != "" {
		return v
	}
	return "GOTEST"
}

const tablePrefix = "GOSQL_"

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("gojtopen", dsn(t))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	// Cap concurrent conns so prepared-statement stress tests don't
	// hammer the as-signon port faster than a dev tunnel / free-tier
	// PUB400 can keep up. database/sql will queue extra goroutines
	// onto the existing conns rather than opening new ones.
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { db.Close() })

	// Warm up. Some IBM i + SSH-tunnel combinations stall on the
	// first as-signon read for ~30s after a quiet period -- driver
	// dial works, but the server's QZSOSGND job takes a beat to
	// answer the exchange-attributes frame. Retry a few times so
	// the per-test setup phase isn't gated on tunnel cold-start.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := db.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("warm-up Ping never succeeded: %v", err)
		}
		t.Logf("warm-up Ping failed, retrying: %v", err)
		time.Sleep(2 * time.Second)
	}
	return db
}

// dropTestTables wipes any leftover GOSQL_* tables in the schema.
// Best-effort -- a failure to drop is logged, not fatal.
func dropTestTables(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(
		`SELECT TABLE_NAME FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE ?`,
		schema(), tablePrefix+"%",
	)
	if err != nil {
		t.Logf("could not enumerate tables: %v", err)
		return
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err == nil {
			names = append(names, strings.TrimSpace(n))
		}
	}
	rows.Close()
	for _, n := range names {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s.%s", schema(), n)); err != nil {
			t.Logf("DROP %s.%s: %v", schema(), n, err)
		}
	}
}

// TestBlobs adapts bradfitz/go-sql-test's testBlobs: insert a 16-byte
// blob, scan it back as both []byte and string, and verify both
// shapes round-trip exactly.
func TestBlobs(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "blobs"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL, bar VARCHAR(16) FOR BIT DATA)`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	blob := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, bar) VALUES (?, ?)`, tbl), 0, blob); err != nil {
		t.Fatalf("insert: %v", err)
	}

	t.Run("scan into []byte", func(t *testing.T) {
		var got []byte
		err := db.QueryRow(fmt.Sprintf(`SELECT bar FROM %s WHERE id = ?`, tbl), 0).Scan(&got)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if want := fmt.Sprintf("%x", blob); fmt.Sprintf("%x", got) != want {
			t.Errorf("[]byte: got %x, want %s", got, want)
		}
	})

	t.Run("scan into string", func(t *testing.T) {
		var got string
		err := db.QueryRow(fmt.Sprintf(`SELECT bar FROM %s WHERE id = ?`, tbl), 0).Scan(&got)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if want := string(blob); got != want {
			t.Errorf("string: got %q, want %q", got, want)
		}
	})
}

// TestManyQueryRow exercises the prepared-statement reuse path: 10k
// QueryRow round-trips against the same SQL. The first iteration
// hits PREPARE; subsequent ones should pay only OPEN+FETCH+CLOSE.
//
// Originally bradfitz/go-sql-test's testManyQueryRow scaled to 10k;
// we cut to 1k by default since each round trip to IBM Cloud Power
// VS over an SSH tunnel is ~5-10ms and the full 10k takes ~90s.
// Bump GOJTOPEN_MANY_QUERY_ROW_N for stress testing.
func TestManyQueryRow(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "many"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL, name VARCHAR(50))`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (?, ?)`, tbl), 1, "bob"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	n := 1000
	if testing.Short() {
		n = 100
	}
	if v := os.Getenv("GOJTOPEN_MANY_QUERY_ROW_N"); v != "" {
		fmt.Sscanf(v, "%d", &n)
	}

	for i := 0; i < n; i++ {
		var name string
		if err := db.QueryRow(fmt.Sprintf(`SELECT name FROM %s WHERE id = ?`, tbl), 1).Scan(&name); err != nil {
			t.Fatalf("on query %d: %v", i, err)
		}
		if name != "bob" {
			t.Fatalf("on query %d: name=%q want bob", i, name)
		}
	}
}

// TestTxQuery exercises Begin / Exec / Query / Commit on a journaled
// table. Note: IBM i requires the table be journaled for COMMIT/
// ROLLBACK to function; the test creates one inline if possible.
func TestTxQuery(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "tx"
	// Plain CREATE TABLE -- if the schema has a journal, the table
	// auto-journals; otherwise fall back to a non-journaled table
	// and the test still exercises tx.Exec / tx.Query without
	// commitment validation.
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL, name VARCHAR(50))`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (?, ?)`, tbl), 1, "bob"); err != nil {
		t.Fatalf("tx Exec: %v", err)
	}
	rows, err := tx.Query(fmt.Sprintf(`SELECT name FROM %s WHERE id = ?`, tbl), 1)
	if err != nil {
		t.Fatalf("tx Query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		t.Fatal("expected one row in tx")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "bob" {
		t.Errorf("name=%q, want bob", name)
	}
}

// TestPreparedStmt exercises concurrent prepared-statement reuse.
// Two prepared statements (sel + ins) shared across 10 goroutines,
// each running 10 iterations. Catches Stmt-state races and stale
// RPB cleanup bugs.
//
// SQL_NO_ROWS_FOUND on the SELECT (empty table early in the run) is
// expected and silently swallowed -- mirrors bradfitz/go-sql-test's
// handling.
func TestPreparedStmt(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "prep"
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (cnt INTEGER NOT NULL)`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	sel, err := db.Prepare(fmt.Sprintf(`SELECT cnt FROM %s ORDER BY cnt DESC`, tbl))
	if err != nil {
		t.Fatalf("Prepare sel: %v", err)
	}
	defer sel.Close()
	ins, err := db.Prepare(fmt.Sprintf(`INSERT INTO %s (cnt) VALUES (?)`, tbl))
	if err != nil {
		t.Fatalf("Prepare ins: %v", err)
	}
	defer ins.Close()

	for n := 1; n <= 3; n++ {
		if _, err := ins.Exec(n); err != nil {
			t.Fatalf("seed insert(%d): %v", n, err)
		}
	}

	// 10 goroutines, 10 iterations each -- ~100 mixed Query/Exec
	// against the same prepared statements. database/sql serialises
	// per-Stmt operations on a Conn, but the pool may hand the same
	// Stmt to multiple Conns under contention; we want the driver to
	// behave correctly under that.
	const nRuns = 10
	var wg sync.WaitGroup
	for i := 0; i < nRuns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				var count int
				if err := sel.QueryRow().Scan(&count); err != nil && !errors.Is(err, sql.ErrNoRows) {
					t.Errorf("Query: %v", err)
					return
				}
				if _, err := ins.Exec(rand.Intn(100)); err != nil {
					t.Errorf("Insert: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
