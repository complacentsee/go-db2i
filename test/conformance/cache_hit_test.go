//go:build conformance

// Package conformance — cache_hit_test.go is the v0.7.2 live test
// matrix for the extended-dynamic + client-side cache-hit fast
// path. Verifies that:
//
//   - first-use files the prepared plan into the *PGM
//     (PREPARE_DESCRIBE round-trip)
//   - second-use against a fresh connection hits the cache
//     (PREPARE_DESCRIBE skipped, CP 0x3806 statement-name override)
//   - the cache-hit row decoder works correctly across the JDBC
//     type matrix (16 primitive types covered)
//   - edge cases survive: NULL bind, SQL +100 zero-rows result,
//     server-side error doesn't poison the RPB, multi-row
//     continuation FETCH, concurrent dispatch
//   - statements that aren't eligible (CALL with sql.Out, LOB bind)
//     correctly fall through to the cache-miss path
//
// All tests share a single package name (GOTCHE → GOTCHE9899 on
// the wire) in the test schema. The package object survives across
// test runs; tests file their own statements and rely on
// byte-equal SQL text matching for cache lookup so no cross-run
// collisions.
//
// Gated by `//go:build conformance` and the DB2I_DSN env var; run
// with the same invocation as conformance_test.go.
package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db2i "github.com/complacentsee/go-db2i/driver"
	"github.com/complacentsee/go-db2i/hostserver"
)

// cachePackageName is the shared package base for every cache-hit
// test. 6-char limit; resolves to "GOTCHE9899" on the wire with
// default session options.
const cachePackageName = "GOTCHE"

// requireFiling skips a test unless DB2I_TEST_FILING=1 is set in
// the environment. Cache-hit tests that depend on the server
// actually filing PREPAREd plans into the *PGM only pass when the
// target LPAR's QSQSRVR job has filing enabled. The IBM Cloud
// V7R6M0 environment go-db2i normally runs against doesn't file
// (verified by capturing JT400's wire on 2026-05-11: even JT400
// with package add=true never emits the WRITE_SQL_STATEMENT_TEXT
// CP -- client-side heuristic suppresses it). Set DB2I_TEST_FILING=1
// to opt into these tests on an LPAR where filing is known to work
// (e.g. PUB400 V7R5M0 historically). Without the env var the tests
// skip cleanly so the conformance run stays green.
func requireFiling(t *testing.T) {
	t.Helper()
	if os.Getenv("DB2I_TEST_FILING") != "1" {
		t.Skip("requires LPAR with SQL package filing enabled; set DB2I_TEST_FILING=1 to opt in. See docs/package-caching.md.")
	}
}

// cacheHitMsg / cacheHitMsgExec are the slog message texts the
// driver emits on a cache-hit dispatch (driver/stmt.go).
const (
	cacheHitQueryMsg = "db2i: query cache-hit"
	cacheHitExecMsg  = "db2i: exec cache-hit"
)

// configFromDSN parses DB2I_DSN into a *Config that
// NewConnector accepts. We can't reach into the unexported
// parseDSN, so this helper duplicates just enough of its surface
// (user/password/host/port + a few query keys) for the cache-hit
// tests. The cache-hit tests need NewConnector specifically so
// they can wire Config.Logger to a bytes.Buffer probe.
func configFromDSN(t *testing.T) *db2i.Config {
	t.Helper()
	raw := dsn(t)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	if u.Scheme != "db2i" {
		t.Fatalf("DSN scheme %q != %q", u.Scheme, "db2i")
	}
	host, portStr, _ := strings.Cut(u.Host, ":")
	port, _ := strconv.Atoi(portStr)
	if port == 0 {
		port = 8471
	}
	cfg := db2i.DefaultConfig()
	cfg.User = u.User.Username()
	cfg.Password, _ = u.User.Password()
	cfg.Host = host
	cfg.DBPort = port
	cfg.SignonPort = port + 5
	if lib := u.Query().Get("library"); lib != "" {
		cfg.Library = strings.ToUpper(lib)
	}
	return &cfg
}

// openDBWithPackageCache returns a *sql.DB wired for extended-
// dynamic + package-cache, plus a *bytes.Buffer that captures
// every DEBUG-level slog line the driver emits so tests can assert
// cache-hit dispatch by substring match. The package name is
// shared across tests (see cachePackageName) so the *PGM survives
// the test run.
//
// Caller is responsible for closing the returned DB if they want
// to force a fresh connect on the next call (cache state is per-
// connection, populated by the connect-time RETURN_PACKAGE round
// trip).
//
// criteria selects package-criteria; pass "" for the default.
func openDBWithPackageCache(t *testing.T, criteria string) (*sql.DB, *bytes.Buffer) {
	t.Helper()
	cfg := configFromDSN(t)
	cfg.ExtendedDynamic = true
	cfg.PackageName = cachePackageName
	cfg.PackageLibrary = schema()
	cfg.PackageCache = true
	if criteria != "" {
		cfg.PackageCriteria = criteria
	}

	buf := &bytes.Buffer{}
	// Serialise concurrent slog writes; the test handler is shared
	// across goroutines in the parallel-dispatch test.
	var mu sync.Mutex
	cfg.Logger = slog.New(slog.NewTextHandler(&syncWriter{w: buf, mu: &mu},
		&slog.HandlerOptions{Level: slog.LevelDebug}))

	connector, err := db2i.NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(2)
	t.Cleanup(func() { db.Close() })

	// Warm-up Ping with the same retry envelope conformance_test.go
	// uses for the as-signon cold-start case on tunneled hosts.
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
	return db, buf
}

// syncWriter serialises Write calls so concurrent slog handlers
// (one per goroutine) can share a single capture buffer without
// tearing line boundaries.
type syncWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// fillPackageCache files a single SQL statement into the shared
// *PGM by running it once on a transient cache-enabled connection
// and closing. Subsequent connections opened via
// openDBWithPackageCache download the now-populated cache and
// dispatch via the fast path on first use.
//
// kind picks Exec vs Query; for Query the rows are drained and
// discarded.
type fillKind int

const (
	fillExec fillKind = iota
	fillQuery
)

func fillPackageCache(t *testing.T, kind fillKind, sqlText string, args ...any) {
	t.Helper()
	db, _ := openDBWithPackageCache(t, "")
	defer db.Close()
	switch kind {
	case fillExec:
		if _, err := db.Exec(sqlText, args...); err != nil {
			t.Fatalf("fillPackageCache(Exec %q): %v", sqlText, err)
		}
	case fillQuery:
		rows, err := db.Query(sqlText, args...)
		if err != nil {
			t.Fatalf("fillPackageCache(Query %q): %v", sqlText, err)
		}
		for rows.Next() {
		}
		rows.Close()
	}
}

// expectCacheHit checks that buf contains at least one cache-hit
// dispatch line of the requested shape.
func expectCacheHit(t *testing.T, buf *bytes.Buffer, msg string) {
	t.Helper()
	out := buf.String()
	if !strings.Contains(out, msg) {
		t.Errorf("expected slog buffer to contain %q; got:\n%s", msg, out)
	}
}

// expectNoCacheHit fails when buf contains any cache-hit dispatch
// line. Used by fall-through tests (sql.Out, LOB bind, CALL).
func expectNoCacheHit(t *testing.T, buf *bytes.Buffer) {
	t.Helper()
	out := buf.String()
	if strings.Contains(out, cacheHitQueryMsg) || strings.Contains(out, cacheHitExecMsg) {
		t.Errorf("expected no cache-hit dispatch; got:\n%s", out)
	}
}

// countCacheHits returns the total number of cache-hit DEBUG lines
// (both query and exec variants) in buf.
func countCacheHits(buf *bytes.Buffer) int {
	out := buf.String()
	return strings.Count(out, cacheHitQueryMsg) + strings.Count(out, cacheHitExecMsg)
}

// makeCacheTestTable creates a fresh test table with the given
// suffix and returns the fully-qualified name. The table is
// dropped via t.Cleanup so each test gets a deterministic slate.
//
// suffix should be unique per test to avoid cross-test schema
// reuse; keep it short to stay within IBM i's 10-char table-name
// limit (tablePrefix is 6 chars, leaving 4 for the suffix).
func makeCacheTestTable(t *testing.T, db *sql.DB, suffix, schemaSQL string) string {
	t.Helper()
	tbl := schema() + "." + tablePrefix + suffix
	_, _ = db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec("CREATE TABLE " + tbl + " " + schemaSQL); err != nil {
		t.Fatalf("CREATE TABLE %s %s: %v", tbl, schemaSQL, err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + tbl) })
	return tbl
}

// TestCacheHit_FirstUseFilesStatement is the smoke test: a single
// SELECT against SYSIBM.SYSDUMMY1, run on a transient connection
// to seed the *PGM, then re-run on a fresh connection to verify
// the second call dispatches through ExecutePreparedCached.
func TestCacheHit_FirstUseFilesStatement(t *testing.T) {
	requireFiling(t)
	const q = `SELECT CURRENT_USER FROM SYSIBM.SYSDUMMY1 WHERE 1 = ?`
	fillPackageCache(t, fillQuery, q, 1)

	db, buf := openDBWithPackageCache(t, "")
	row := db.QueryRow(q, 1)
	var user string
	if err := row.Scan(&user); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if strings.TrimSpace(user) == "" {
		t.Errorf("CURRENT_USER scanned empty")
	}
	expectCacheHit(t, buf, cacheHitQueryMsg)
}

// typeMatrixCase is one row in the type-coverage table. Each case
// gets a private table (CREATE TABLE + INSERT seed) so the SELECT
// can be byte-equal across runs while the row content is
// deterministic per case.
type typeMatrixCase struct {
	name    string // subtest name (also the table suffix; ≤4 chars)
	colType string // column type for CREATE TABLE
	seed    any    // value bound on the INSERT
	want    func(t *testing.T, got any)
}

// TestCacheHit_SelectTypeMatrix iterates the JDBC type matrix and
// for each type:
//
//  1. CREATE TABLE GOSQL_<suffix> (id INTEGER, v <type>)
//  2. INSERT one row with the seed value
//  3. fillPackageCache: SELECT v WHERE id = ? — files the SELECT
//  4. On a fresh DB, run the same SELECT and assert it hits the
//     cache AND the round-trip value matches.
//
// The INSERT itself isn't asserted to cache-hit (it's run once on
// a per-test basis so the package only files one SELECT per case).
func TestCacheHit_SelectTypeMatrix(t *testing.T) {
	requireFiling(t)
	cases := []typeMatrixCase{
		{
			name: "int", colType: "INTEGER", seed: int64(42),
			want: func(t *testing.T, got any) { eqInt64(t, got, 42) },
		},
		{
			name: "bigi", colType: "BIGINT", seed: int64(9_000_000_000),
			want: func(t *testing.T, got any) { eqInt64(t, got, 9_000_000_000) },
		},
		{
			name: "smin", colType: "SMALLINT", seed: int64(31000),
			want: func(t *testing.T, got any) { eqInt64(t, got, 31000) },
		},
		{
			name: "dec", colType: "DECIMAL(15,4)", seed: "1234.5678",
			want: func(t *testing.T, got any) { eqString(t, got, "1234.5678") },
		},
		{
			name: "num", colType: "NUMERIC(10,2)", seed: "9876.54",
			want: func(t *testing.T, got any) { eqString(t, got, "9876.54") },
		},
		{
			name: "real", colType: "REAL", seed: float64(2.5),
			want: func(t *testing.T, got any) { eqFloat(t, got, 2.5, 1e-5) },
		},
		{
			name: "dbl", colType: "DOUBLE", seed: float64(3.14159),
			want: func(t *testing.T, got any) { eqFloat(t, got, 3.14159, 1e-9) },
		},
		{
			name: "dcf", colType: "DECFLOAT(16)", seed: "1.5E+5",
			want: func(t *testing.T, got any) { eqString(t, got, "150000") },
		},
		{
			name: "vchr", colType: "VARCHAR(64)", seed: "hello cache",
			want: func(t *testing.T, got any) { eqString(t, got, "hello cache") },
		},
		{
			name: "char", colType: "CHAR(20)", seed: "fixed",
			want: func(t *testing.T, got any) {
				// CHAR is space-padded on the wire; trim before compare.
				eqString(t, trimSpaceAny(got), "fixed")
			},
		},
		{
			name: "vbit", colType: "VARCHAR(40) FOR BIT DATA", seed: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			want: func(t *testing.T, got any) { eqBytes(t, got, []byte{0xDE, 0xAD, 0xBE, 0xEF}) },
		},
		{
			name: "date", colType: "DATE", seed: "2026-05-11",
			want: func(t *testing.T, got any) { eqString(t, got, "2026-05-11") },
		},
		{
			name: "time", colType: "TIME", seed: "14:30:00",
			want: func(t *testing.T, got any) { eqString(t, got, "14:30:00") },
		},
		{
			name: "ts", colType: "TIMESTAMP", seed: "2026-05-11 14:30:00.123456",
			want: func(t *testing.T, got any) {
				s, _ := got.(string)
				if !strings.HasPrefix(s, "2026-05-11 14:30:00") {
					t.Errorf("TIMESTAMP = %q, want prefix 2026-05-11 14:30:00", s)
				}
			},
		},
		{
			name: "bool", colType: "BOOLEAN", seed: true,
			want: func(t *testing.T, got any) {
				if b, ok := got.(bool); !ok || !b {
					t.Errorf("BOOLEAN = %v, want true", got)
				}
			},
		},
		{
			name: "bin", colType: "BINARY(16)", seed: []byte("0123456789abcdef"),
			want: func(t *testing.T, got any) { eqBytes(t, got, []byte("0123456789abcdef")) },
		},
		{
			name: "vbin", colType: "VARBINARY(64)", seed: []byte{0x01, 0x02, 0x03, 0x04},
			want: func(t *testing.T, got any) { eqBytes(t, got, []byte{0x01, 0x02, 0x03, 0x04}) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, buf := openDBWithPackageCache(t, "")
			tbl := makeCacheTestTable(t, db,
				"tm"+tc.name,
				"(id INTEGER NOT NULL, v "+tc.colType+")")

			// Seed the row. INSERT isn't part of the cache-hit
			// assertion -- we just need a value to read back.
			if _, err := db.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", 1, tc.seed); err != nil {
				t.Fatalf("INSERT: %v", err)
			}

			selectSQL := "SELECT v FROM " + tbl + " WHERE id = ?"

			// File the SELECT into the *PGM. fillPackageCache uses
			// a separate transient DB so the seed connection's
			// cache snapshot is irrelevant.
			fillPackageCache(t, fillQuery, selectSQL, 1)

			// Drop the slog buffer's pre-population log lines so
			// the assertion only sees this call's dispatch.
			buf.Reset()

			// Force a fresh connection -- close db, reopen with
			// the same package. The new conn's RETURN_PACKAGE
			// download pulls in the filed SELECT.
			db.Close()
			db2, buf2 := openDBWithPackageCache(t, "")
			row := db2.QueryRow(selectSQL, 1)
			got, err := scanInto(row, tc.colType)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			tc.want(t, got)
			expectCacheHit(t, buf2, cacheHitQueryMsg)
		})
	}
}

// scanInto picks an appropriately-typed destination per column
// type and scans the row into it. Returns the value boxed as any.
func scanInto(row *sql.Row, colType string) (any, error) {
	switch {
	case strings.HasPrefix(colType, "INTEGER"),
		strings.HasPrefix(colType, "BIGINT"),
		strings.HasPrefix(colType, "SMALLINT"):
		var v int64
		err := row.Scan(&v)
		return v, err
	case strings.HasPrefix(colType, "DECIMAL"),
		strings.HasPrefix(colType, "NUMERIC"),
		strings.HasPrefix(colType, "DECFLOAT"),
		strings.HasPrefix(colType, "DATE"),
		strings.HasPrefix(colType, "TIME"),
		strings.HasPrefix(colType, "TIMESTAMP"):
		var v string
		err := row.Scan(&v)
		return v, err
	case strings.HasPrefix(colType, "REAL"), strings.HasPrefix(colType, "DOUBLE"):
		var v float64
		err := row.Scan(&v)
		return v, err
	case strings.HasPrefix(colType, "VARCHAR("), strings.HasPrefix(colType, "CHAR("):
		// Distinguish VARCHAR(...) FOR BIT DATA from text VARCHAR by
		// scanning into a NullString first and falling back to []byte
		// on type mismatch.
		if strings.Contains(colType, "FOR BIT DATA") {
			var v []byte
			err := row.Scan(&v)
			return v, err
		}
		var v string
		err := row.Scan(&v)
		return v, err
	case strings.HasPrefix(colType, "BINARY"), strings.HasPrefix(colType, "VARBINARY"):
		var v []byte
		err := row.Scan(&v)
		return v, err
	case strings.HasPrefix(colType, "BOOLEAN"):
		var v bool
		err := row.Scan(&v)
		return v, err
	}
	return nil, fmt.Errorf("scanInto: unsupported column type %q", colType)
}

func eqInt64(t *testing.T, got any, want int64) {
	t.Helper()
	v, ok := got.(int64)
	if !ok {
		t.Errorf("type assertion: got %T, want int64", got)
		return
	}
	if v != want {
		t.Errorf("int64 round-trip: got %d, want %d", v, want)
	}
}

func eqString(t *testing.T, got any, want string) {
	t.Helper()
	v, ok := got.(string)
	if !ok {
		t.Errorf("type assertion: got %T, want string", got)
		return
	}
	if v != want {
		t.Errorf("string round-trip: got %q, want %q", v, want)
	}
}

func eqFloat(t *testing.T, got any, want, tol float64) {
	t.Helper()
	v, ok := got.(float64)
	if !ok {
		t.Errorf("type assertion: got %T, want float64", got)
		return
	}
	diff := v - want
	if diff < 0 {
		diff = -diff
	}
	if diff > tol {
		t.Errorf("float round-trip: got %v, want %v ± %v", v, want, tol)
	}
}

func eqBytes(t *testing.T, got any, want []byte) {
	t.Helper()
	v, ok := got.([]byte)
	if !ok {
		t.Errorf("type assertion: got %T, want []byte", got)
		return
	}
	if !bytes.Equal(v, want) {
		t.Errorf("bytes round-trip: got %x, want %x", v, want)
	}
}

func trimSpaceAny(v any) any {
	if s, ok := v.(string); ok {
		return strings.TrimRight(s, " ")
	}
	return v
}

// TestCacheHit_MultiRowSelect verifies the cached OPEN path drives
// continuation FETCH correctly. 200 rows is more than fits in the
// 32 KB initial buffer for a small column shape, so the cursor
// must round-trip at least one continuation FETCH while the cache
// hit is in effect.
func TestCacheHit_MultiRowSelect(t *testing.T) {
	requireFiling(t)
	const rowCount = 200
	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "mr",
		"(id INTEGER NOT NULL, label VARCHAR(32))")
	for i := 0; i < rowCount; i++ {
		if _, err := db.Exec("INSERT INTO "+tbl+" VALUES (?, ?)", i, "row"+strconv.Itoa(i)); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}

	const selectSQL = "SELECT id, label FROM %s WHERE id >= ? ORDER BY id"
	fmtSQL := fmt.Sprintf(selectSQL, tbl)
	fillPackageCache(t, fillQuery, fmtSQL, 0)
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	rows, err := db2.Query(fmtSQL, 0)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id int64
		var label string
		if err := rows.Scan(&id, &label); err != nil {
			t.Fatalf("scan row %d: %v", count, err)
		}
		if id != int64(count) {
			t.Errorf("row %d: id = %d, want %d", count, id, count)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if count != rowCount {
		t.Errorf("fetched %d rows, want %d", count, rowCount)
	}
	expectCacheHit(t, buf, cacheHitQueryMsg)
}

// TestCacheHit_ExecInsertUpdateDelete walks one I/U/D triple
// against a fresh table, asserting each step dispatches through
// the cache once the statement is filed.
func TestCacheHit_ExecInsertUpdateDelete(t *testing.T) {
	requireFiling(t)
	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "iud",
		"(id INTEGER NOT NULL PRIMARY KEY, label VARCHAR(32))")

	insertSQL := "INSERT INTO " + tbl + " (id, label) VALUES (?, ?)"
	updateSQL := "UPDATE " + tbl + " SET label = ? WHERE id = ?"
	deleteSQL := "DELETE FROM " + tbl + " WHERE id = ?"

	// File all three statements.
	fillPackageCache(t, fillExec, insertSQL, 999, "warmup")
	fillPackageCache(t, fillExec, updateSQL, "warmup2", 999)
	fillPackageCache(t, fillExec, deleteSQL, 999)
	db.Close()

	t.Run("insert", func(t *testing.T) {
		db, buf := openDBWithPackageCache(t, "")
		defer db.Close()
		res, err := db.Exec(insertSQL, 1, "alpha")
		if err != nil {
			t.Fatalf("INSERT: %v", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Errorf("INSERT RowsAffected = %d, want 1", n)
		}
		expectCacheHit(t, buf, cacheHitExecMsg)
	})

	t.Run("update", func(t *testing.T) {
		db, buf := openDBWithPackageCache(t, "")
		defer db.Close()
		res, err := db.Exec(updateSQL, "beta", 1)
		if err != nil {
			t.Fatalf("UPDATE: %v", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Errorf("UPDATE RowsAffected = %d, want 1", n)
		}
		expectCacheHit(t, buf, cacheHitExecMsg)
	})

	t.Run("delete", func(t *testing.T) {
		db, buf := openDBWithPackageCache(t, "")
		defer db.Close()
		res, err := db.Exec(deleteSQL, 1)
		if err != nil {
			t.Fatalf("DELETE: %v", err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			t.Errorf("DELETE RowsAffected = %d, want 1", n)
		}
		expectCacheHit(t, buf, cacheHitExecMsg)
	})
}

// TestCacheHit_NullBind verifies that binding nil through the
// cached EXECUTE path correctly signals NULL on the wire
// (IndicatorSize=2 short, value=0xFFFF) so the server stores NULL
// rather than zero / empty string.
func TestCacheHit_NullBind(t *testing.T) {
	requireFiling(t)
	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "nul",
		"(id INTEGER NOT NULL, v INTEGER)")

	insertSQL := "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)"
	fillPackageCache(t, fillExec, insertSQL, 0, 0)
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	if _, err := db2.Exec(insertSQL, 1, nil); err != nil {
		t.Fatalf("INSERT NULL: %v", err)
	}
	expectCacheHit(t, buf, cacheHitExecMsg)

	var v sql.NullInt64
	if err := db2.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", 1).Scan(&v); err != nil {
		t.Fatalf("read-back: %v", err)
	}
	if v.Valid {
		t.Errorf("expected NULL after cached INSERT(nil); got %d", v.Int64)
	}
}

// TestCacheHit_NoRowsResult verifies that an UPDATE that matches
// nothing reports RowsAffected=0 with no error (SQL +100), and
// that the SQL +100 success path doesn't leak as an error from
// the cached EXECUTE.
func TestCacheHit_NoRowsResult(t *testing.T) {
	requireFiling(t)
	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "no",
		"(id INTEGER NOT NULL, v INTEGER)")

	updateSQL := "UPDATE " + tbl + " SET v = ? WHERE id = ?"
	fillPackageCache(t, fillExec, updateSQL, 0, 0)
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	res, err := db2.Exec(updateSQL, 1, 999999)
	if err != nil {
		t.Fatalf("UPDATE no-match: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 0 {
		t.Errorf("RowsAffected on no-match = %d, want 0", n)
	}
	expectCacheHit(t, buf, cacheHitExecMsg)
}

// TestCacheHit_ServerErrorDoesntPoisonRPB verifies that when the
// cached EXECUTE returns a duplicate-key error (-803), the
// connection cleans up its RPB and a subsequent Exec on the same
// pool succeeds.
func TestCacheHit_ServerErrorDoesntPoisonRPB(t *testing.T) {
	requireFiling(t)
	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "err",
		"(id INTEGER NOT NULL PRIMARY KEY)")

	insertSQL := "INSERT INTO " + tbl + " (id) VALUES (?)"
	fillPackageCache(t, fillExec, insertSQL, 1)
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	if _, err := db2.Exec(insertSQL, 1); err != nil {
		t.Fatalf("first INSERT: %v", err)
	}

	// Same INSERT again -- expect SQL-803.
	_, err := db2.Exec(insertSQL, 1)
	if err == nil {
		t.Fatalf("expected duplicate-key error; got nil")
	}
	var dbErr *hostserver.Db2Error
	if !errors.As(err, &dbErr) {
		t.Fatalf("expected *hostserver.Db2Error; got %T: %v", err, err)
	}
	if dbErr.SQLCode != -803 {
		t.Errorf("SQLCode = %d, want -803 (duplicate key)", dbErr.SQLCode)
	}

	// Recovery: another INSERT with a fresh id must succeed on the
	// same pool. If the cached EXECUTE poisoned the RPB this would
	// fail with a stale-handle error.
	if _, err := db2.Exec(insertSQL, 2); err != nil {
		t.Fatalf("recovery INSERT: %v", err)
	}
	if got := countCacheHits(buf); got < 1 {
		t.Errorf("expected at least one cache-hit dispatch line; got %d", got)
	}
}

// TestCacheHit_OutParameterFallthrough verifies stored procedures
// with sql.Out destinations skip the cache (criteria filter +
// hostserver.ExecutePreparedCached guard). The conformance stored-
// proc fixtures live in GOSPROCS; we reuse P_LOOKUP.
func TestCacheHit_OutParameterFallthrough(t *testing.T) {
	setupDB := openDB(t)
	setUpStoredProcs(t, setupDB)
	setupDB.Close()

	db, buf := openDBWithPackageCache(t, "")
	defer db.Close()
	var name string
	var qty int
	if _, err := db.Exec("CALL "+procLibrary+".P_LOOKUP(?, ?, ?)",
		"WIDGET",
		sql.Out{Dest: &name},
		sql.Out{Dest: &qty},
	); err != nil {
		t.Fatalf("CALL P_LOOKUP: %v", err)
	}
	if qty != 100 {
		t.Errorf("OUT qty = %d, want 100", qty)
	}
	expectNoCacheHit(t, buf)
}

// TestCacheHit_ParallelDispatch fires N goroutines, each running
// the same cached SELECT, and asserts every dispatch succeeded
// AND that the cache-hit DEBUG line fired the expected number of
// times. Exercises shared-Conn cache-slice read safety.
func TestCacheHit_ParallelDispatch(t *testing.T) {
	requireFiling(t)
	const goroutines = 4
	const iterations = 25
	const totalCalls = goroutines * iterations

	const q = `SELECT CURRENT_TIMESTAMP, CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1`
	fillPackageCache(t, fillQuery, q, 1)

	db, buf := openDBWithPackageCache(t, "")
	db.SetMaxOpenConns(goroutines)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	var successes atomic.Int64
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				rows, err := db.Query(q, id*1000+i)
				if err != nil {
					errs <- fmt.Errorf("goroutine %d iter %d: %w", id, i, err)
					return
				}
				for rows.Next() {
				}
				rows.Close()
				successes.Add(1)
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("parallel dispatch: %v", err)
	}
	if int(successes.Load()) != totalCalls {
		t.Errorf("successful dispatches = %d, want %d", successes.Load(), totalCalls)
	}

	// Each goroutine's first call may land on a fresh conn whose
	// cache snapshot was taken before the warmup connection
	// closed; in that case it sees an empty cache and PREPAREs
	// instead. So the hit count is between (totalCalls -
	// goroutines) and totalCalls. The lower bound is what we
	// assert as a safety floor.
	got := countCacheHits(buf)
	minWant := totalCalls - goroutines
	if got < minWant {
		t.Errorf("cache-hit lines = %d, want >= %d (totalCalls=%d - goroutines=%d)",
			got, minWant, totalCalls, goroutines)
	}
}

// TestCacheHit_CrossConnectPreload files several statements via a
// transient connection, then opens a fresh connection and
// verifies the very first call to each filed SQL dispatches
// through the cache without an intervening PREPARE_DESCRIBE.
func TestCacheHit_CrossConnectPreload(t *testing.T) {
	requireFiling(t)
	queries := []string{
		`SELECT CURRENT_USER FROM SYSIBM.SYSDUMMY1 WHERE 1 = ?`,
		`SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1 WHERE 1 = ?`,
		`SELECT CURRENT_DATE FROM SYSIBM.SYSDUMMY1 WHERE 1 = ?`,
	}
	for _, q := range queries {
		fillPackageCache(t, fillQuery, q, 1)
	}

	db, buf := openDBWithPackageCache(t, "")
	defer db.Close()
	for i, q := range queries {
		rows, err := db.Query(q, 1)
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		for rows.Next() {
		}
		rows.Close()
	}
	if got := countCacheHits(buf); got != len(queries) {
		t.Errorf("cache-hit lines = %d, want %d", got, len(queries))
	}
}

// TestCacheHit_CriteriaSelect verifies package-criteria=select
// caches an unparameterised SELECT that the default criteria
// rejects. Compares a default-criteria control run that should
// NOT cache the same SQL.
func TestCacheHit_CriteriaSelect(t *testing.T) {
	const q = `SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1`

	t.Run("default rejects unparameterised SELECT", func(t *testing.T) {
		db, _ := openDBWithPackageCache(t, "default")
		defer db.Close()
		// File then re-run; default criteria refuses to file zero-
		// marker SELECTs, so the cache never sees this SQL.
		row := db.QueryRow(q)
		var ts string
		_ = row.Scan(&ts)
		db2, buf := openDBWithPackageCache(t, "default")
		defer db2.Close()
		_ = db2.QueryRow(q).Scan(&ts)
		expectNoCacheHit(t, buf)
	})

	t.Run("select accepts unparameterised SELECT", func(t *testing.T) {
		requireFiling(t)
		// File via a "select" criteria connection.
		warm, _ := openDBWithPackageCache(t, "select")
		_ = warm.QueryRow(q).Scan(new(string))
		warm.Close()

		db, buf := openDBWithPackageCache(t, "select")
		defer db.Close()
		var ts string
		if err := db.QueryRow(q).Scan(&ts); err != nil {
			t.Fatalf("QueryRow: %v", err)
		}
		if ts == "" {
			t.Errorf("CURRENT_TIMESTAMP scanned empty")
		}
		expectCacheHit(t, buf, cacheHitQueryMsg)
	})
}

// TestCacheHit_LOBBindFallthrough verifies that INSERTs binding a
// BLOB column through the WRITE_LOB_DATA path correctly skip the
// cache. LOB binds force a re-prepare per JT400's
// JDPackageManager filter, so the package never files them and
// the cache-hit path can't pick them up.
func TestCacheHit_LOBBindFallthrough(t *testing.T) {
	db, buf := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "lob",
		"(id INTEGER NOT NULL, b BLOB(64K))")

	payload := bytes.Repeat([]byte{0xAB}, 1024)
	insertSQL := "INSERT INTO " + tbl + " (id, b) VALUES (?, ?)"
	// Run twice on the SAME conn so we can be sure if the cache-
	// hit path fires, it'd surface here. Both runs MUST go through
	// the normal locator-bind path; neither should log a cache-hit.
	for i := 0; i < 2; i++ {
		if _, err := db.Exec(insertSQL, i, payload); err != nil {
			t.Fatalf("INSERT iter %d: %v", i, err)
		}
	}
	expectNoCacheHit(t, buf)

	// Sanity: the payload survived the locator path.
	var got []byte
	if err := db.QueryRow("SELECT b FROM "+tbl+" WHERE id = ?", 0).Scan(&got); err != nil {
		t.Fatalf("SELECT BLOB: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("BLOB round-trip mismatch (%d bytes back, %d sent)", len(got), len(payload))
	}
}
