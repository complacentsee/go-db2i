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

// TestMain primes the shared *SQLPKG before any cache-hit test runs
// and wipes it after, so the suite is hermetic regardless of
// invocation order or prior server state. Cache-hit tests fail
// silently from a completely empty server: the very first connection
// with `package-cache=true` issues RETURN_PACKAGE → SQL-204 (object
// not found), the driver soft-disables the package for that conn,
// and the per-test fillPackageCache then can't file. By running
// `wipePackage` + 4 PREPAREs of a filing-eligible SELECT once at
// suite start, we guarantee the *SQLPKG exists with at least one
// filed statement before TestCacheHit_* lookups; teardown re-wipes
// so the next run starts deterministically.
//
// Only runs when DB2I_DSN is set AND DB2I_TEST_FILING=1 -- the same
// gate as requireFiling. Tests that don't need the package primer
// (everything outside cache_hit_test.go) are unaffected: TestMain
// just calls m.Run() and returns its exit code regardless of
// primer success.
func TestMain(m *testing.M) {
	dsn := os.Getenv("DB2I_DSN")
	primeRan := dsn != "" && os.Getenv("DB2I_TEST_FILING") == "1"
	if primeRan {
		primeAndWipePackage(dsn, true)
	}
	code := m.Run()
	if primeRan {
		// Final teardown -- next suite run starts deterministically.
		// os.Exit doesn't run deferred funcs, so the wipe goes here
		// rather than via defer.
		primeAndWipePackage(dsn, false)
	}
	os.Exit(code)
}

// primeAndWipePackage opens a brief connection and either creates +
// files the shared *SQLPKG (prime=true) or just removes it
// (prime=false). On the priming pass we file 4 PREPAREs of a
// SELECT_CAST against SYSIBM.SYSDUMMY1 so the *SQLPKG materialises
// with NUMBER_STATEMENTS >= 1; the per-test fillPackageCache helpers
// then file their own test-specific SQL into the now-existing
// *SQLPKG without hitting the cold-start SQL-204 trap. On teardown
// we wipe so the next suite run starts from a known empty state.
func primeAndWipePackage(dsn string, prime bool) {
	// Use the plain DSN (no extended-dynamic / no package-cache) for
	// the wipe pass -- DLTOBJ goes through QCMDEXC, not the SQL
	// package path, so the package-cache wiring would just be noise.
	wipeDB, err := sql.Open("db2i", dsn)
	if err != nil {
		return
	}
	defer wipeDB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := wipeDB.PingContext(ctx); err != nil {
		return
	}
	lib := os.Getenv("DB2I_SCHEMA")
	if lib == "" {
		lib = os.Getenv("DB2I_LIBRARY")
	}
	if lib == "" {
		return
	}
	cmd := "DLTOBJ OBJ(" + lib + "/" + cachePackageName + "*) OBJTYPE(*SQLPKG)"
	_, _ = wipeDB.ExecContext(ctx, "CALL QSYS2.QCMDEXC(?)", cmd)

	if !prime {
		return
	}

	// Prime: open a package-cache conn and file a filing-eligible
	// SELECT 4 times so the server's per-job 3-PREPARE threshold is
	// crossed and the *SQLPKG materialises with one filed statement.
	pdsn := dsn
	sep := "?"
	if strings.Contains(pdsn, "?") {
		sep = "&"
	}
	pdsn += sep + "extended-dynamic=true&package=" + cachePackageName +
		"&package-library=" + lib + "&package-cache=true&package-error=warning"
	primeDB, err := sql.Open("db2i", pdsn)
	if err != nil {
		return
	}
	defer primeDB.Close()
	conn, err := primeDB.Conn(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	for i := 0; i < filingPrepareCount; i++ {
		var probe int
		_ = conn.QueryRowContext(ctx,
			"SELECT CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1", i).Scan(&probe)
	}
}

// requireFiling skips a test unless DB2I_TEST_FILING=1 is set in
// the environment. Cache-hit tests that depend on the server
// actually filing PREPAREd plans into the *PGM only pass when:
//
//  1. Each candidate SQL has been PREPAREd >= 3 times. IBM's
//     SQL Package Questions and Answers page documents this:
//     "Starting with IBM i 6.1 PTF SI30855, a statement must
//     be prepared 3 times before it is added to the SQL
//     package." This is a server-side optimisation to keep
//     one-shot SQL out of the package. Filing-dependent tests
//     loop PREPAREs to cross the threshold (see filingPrepareCount).
//  2. The catalog query happens on a FRESH connection. The
//     SYSPACKAGESTAT view has a within-connection visibility
//     delay -- statements filed by the owning conn don't
//     appear in the view until the conn cycles. Verified
//     2026-05-11 on V7R6M0 and V7R5M0.
//  3. The user has authority to DLTOBJ + CREATE_PACKAGE in
//     the named schema. PUB400 grants this within a per-user
//     library; IBM Cloud V7R6M0 grants it via *ALLOBJ on GOTEST.
//
// Set DB2I_TEST_FILING=1 to opt in. Without the env var the
// tests skip cleanly so the conformance run stays green on
// environments that aren't set up for the full filing exercise.
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
	// Pin a single conn so all PREPAREs accumulate against one
	// QZDASOINIT job (the server's 3-PREPARE filing counter is
	// per-job-per-package; pool churn would split the count and
	// leave each job below threshold).
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("fillPackageCache: db.Conn: %v", err)
	}
	defer conn.Close()
	// Loop filingPrepareCount times so IBM's 3-PREPARE threshold
	// (PTF SI30855, IBM i 6.1) is crossed. Without this the
	// SELECT/INSERT/UPDATE/DELETE is held in transient form and
	// never written to the *PGM, so the subsequent cache-hit
	// assertion fails even with a correct wire shape.
	//
	// Iter 0 must succeed -- the test depends on at least one
	// successful EXECUTE for the SQL to file with valid metadata.
	// Iters 1..N are tolerated even on EXECUTE failure: the
	// server still counts the PREPARE side, which is what
	// crosses the threshold. This matters for tests like
	// TestCacheHit_ServerErrorDoesntPoisonRPB whose SQL is
	// INSERT (id) VALUES (?) with a constant id -- iter 1+
	// would otherwise duplicate-key abort the loop.
	for i := 0; i < filingPrepareCount; i++ {
		switch kind {
		case fillExec:
			if _, err := conn.ExecContext(ctx, sqlText, args...); err != nil {
				if i == 0 {
					t.Fatalf("fillPackageCache(Exec %q) iter 0: %v", sqlText, err)
				}
				// Later iters: PREPARE counted on the server side
				// even if EXECUTE failed; continue accumulating
				// the threshold count.
			}
		case fillQuery:
			rows, err := conn.QueryContext(ctx, sqlText, args...)
			if err != nil {
				if i == 0 {
					t.Fatalf("fillPackageCache(Query %q) iter 0: %v", sqlText, err)
				}
				continue
			}
			for rows.Next() {
			}
			rows.Close()
		}
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
			// DECFLOAT preserves scientific notation through the
			// driver -- both regular and cache-hit paths return
			// the same string form. Earlier "150000" expectation
			// was wishful.
			want: func(t *testing.T, got any) { eqString(t, got, "1.5E+5") },
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
			// DATE / TIME / TIMESTAMP columns report scanType=time.Time
			// (driver/rows.go). Scanning into *string goes through
			// database/sql's time.Time.String() formatter, which
			// produces RFC3339-like output. Both regular and
			// cache-hit paths return identical strings (verified
			// 2026-05-11). The earlier "2026-05-11" /
			// "14:30:00" expectations never passed; replace with
			// the actual driver output.
			name: "date", colType: "DATE", seed: "2026-05-11",
			want: func(t *testing.T, got any) { eqString(t, got, "2026-05-11T00:00:00Z") },
		},
		{
			name: "time", colType: "TIME", seed: "14:30:00",
			want: func(t *testing.T, got any) { eqString(t, got, "0000-01-01T14:30:00Z") },
		},
		{
			name: "ts", colType: "TIMESTAMP", seed: "2026-05-11 14:30:00.123456",
			want: func(t *testing.T, got any) {
				s, _ := got.(string)
				// time.Time.String() output uses 'T' separator.
				if !strings.HasPrefix(s, "2026-05-11T14:30:00") {
					t.Errorf("TIMESTAMP = %q, want prefix 2026-05-11T14:30:00", s)
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
	// fillPackageCache crosses the 3-PREPARE threshold by inserting
	// a sentinel id; the iter 1+ duplicate-key errors are tolerated
	// internally (it's the PREPARE side we need filed). Clear the
	// table afterward so the test's "first INSERT" of id=1 starts
	// from a fresh state.
	fillPackageCache(t, fillExec, insertSQL, 999)
	if _, err := db.Exec("DELETE FROM " + tbl); err != nil {
		t.Fatalf("reset table after fill: %v", err)
	}
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

// TestCacheHit_OutParameterFallthrough verifies that an OUT-CALL
// under criteria=default does not end up in the cache: the
// `default` filter excludes CALL entirely, so the SQL never files
// and no cache-hit fires.
//
// Pre-v0.7.8 this test also relied on a second defense layer --
// preparedParamsFromCached rejecting OUT direction bytes -- but
// v0.7.8 removed that reject after the V7R6M0 probe (see
// docs/plans/v0.7.8-out-param-cache-hit.md). The remaining
// invariant is purely the criteria filter, which still holds.
//
// IMPORTANT: this test wipes the GOTCHE *PGM at start so a prior
// test that filed P_LOOKUP under criteria=extended (e.g.,
// TestCacheHit_CriteriaExtended_OutCallDispatches) doesn't
// contaminate the default-criteria precondition. The cache is
// keyed on SQL text, not criteria, so a filed entry survives the
// criteria switch and would otherwise cache-hit here.
func TestCacheHit_OutParameterFallthrough(t *testing.T) {
	setupDB := openDB(t)
	setUpStoredProcs(t, setupDB)
	setupDB.Close()

	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)

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
		// v0.7.4 routes parameterless SELECTs through the prepared
		// path when extended-dynamic + criteria=select is active,
		// so this case now files and the cache-hit fast path picks
		// it up on the second connection.
		wipeDB := openDB(t)
		defer wipeDB.Close()
		wipePackage(t, wipeDB, cachePackageName)
		// File via a "select" criteria connection, looping past
		// the 3-PREPARE threshold so the statement actually files.
		warm, _ := openDBWithPackageCache(t, "select")
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		warmConn, err := warm.Conn(ctx)
		if err != nil {
			t.Fatalf("warm conn: %v", err)
		}
		for i := 0; i < filingPrepareCount; i++ {
			if err := warmConn.QueryRowContext(ctx, q).Scan(new(string)); err != nil {
				warmConn.Close()
				t.Fatalf("warm iter %d: %v", i, err)
			}
		}
		warmConn.Close()
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

// TestCacheHit_CriteriaExtended (v0.7.7) verifies the third
// criterion files VALUES / WITH / CALL on top of default and that
// the cache-hit fast path picks them up on a second connection.
// Companion to TestCacheHit_CriteriaSelect.
//
// The OUT-CALL refresh-skip behaviour lives in its own test
// (TestCacheHit_CriteriaExtended_OutCallSkipsRefresh) because it
// needs a separate slog buffer to count refresh lines, not just
// dispatch lines.
func TestCacheHit_CriteriaExtended(t *testing.T) {
	t.Run("default rejects VALUES", func(t *testing.T) {
		const q = `VALUES 1`
		db, _ := openDBWithPackageCache(t, "default")
		defer db.Close()
		_ = db.QueryRow(q).Scan(new(int))
		db2, buf := openDBWithPackageCache(t, "default")
		defer db2.Close()
		_ = db2.QueryRow(q).Scan(new(int))
		expectNoCacheHit(t, buf)
	})

	t.Run("extended accepts VALUES", func(t *testing.T) {
		requireFiling(t)
		const q = `VALUES 1`

		wipeDB := openDB(t)
		defer wipeDB.Close()
		wipePackage(t, wipeDB, cachePackageName)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		warm, _ := openDBWithPackageCache(t, "extended")
		warmConn, err := warm.Conn(ctx)
		if err != nil {
			t.Fatalf("warm conn: %v", err)
		}
		for i := 0; i < filingPrepareCount; i++ {
			if err := warmConn.QueryRowContext(ctx, q).Scan(new(int)); err != nil {
				warmConn.Close()
				t.Fatalf("warm iter %d: %v", i, err)
			}
		}
		warmConn.Close()
		warm.Close()

		db, buf := openDBWithPackageCache(t, "extended")
		defer db.Close()
		var v int
		if err := db.QueryRow(q).Scan(&v); err != nil {
			t.Fatalf("VALUES dispatch: %v", err)
		}
		if v != 1 {
			t.Errorf("VALUES 1 returned %d", v)
		}
		expectCacheHit(t, buf, cacheHitQueryMsg)
	})

	t.Run("extended accepts IN-only CALL", func(t *testing.T) {
		requireFiling(t)
		setupDB := openDB(t)
		setUpStoredProcs(t, setupDB)
		setupDB.Close()

		wipeDB := openDB(t)
		defer wipeDB.Close()
		wipePackage(t, wipeDB, cachePackageName)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		const code = "M10EXTCALL" // P_INS.P_CODE is VARCHAR(10)
		callSQL := "CALL " + procLibrary + ".P_INS(?, ?)"
		// File P_INS through threshold under extended; pin one conn
		// so all PREPAREs land on the same QZDASOINIT job.
		warm, _ := openDBWithPackageCache(t, "extended")
		warmConn, err := warm.Conn(ctx)
		if err != nil {
			t.Fatalf("warm conn: %v", err)
		}
		if _, err := warmConn.ExecContext(ctx,
			"DELETE FROM "+procLibrary+".INS_AUDIT WHERE CODE = ?", code); err != nil {
			t.Fatalf("clear INS_AUDIT: %v", err)
		}
		for i := 0; i < filingPrepareCount; i++ {
			if _, err := warmConn.ExecContext(ctx, callSQL, code, i); err != nil {
				warmConn.Close()
				t.Fatalf("warm CALL iter %d: %v", i, err)
			}
		}
		warmConn.Close()
		warm.Close()

		// Fresh conn: same CALL should cache-hit.
		db, buf := openDBWithPackageCache(t, "extended")
		defer db.Close()
		if _, err := db.ExecContext(ctx, callSQL, code, 99); err != nil {
			t.Fatalf("cache-hit CALL: %v", err)
		}
		expectCacheHit(t, buf, cacheHitExecMsg)

		// Cleanup.
		_, _ = db.ExecContext(ctx,
			"DELETE FROM "+procLibrary+".INS_AUDIT WHERE CODE = ?", code)
	})
}

// TestCacheHit_CriteriaExtended_OutCallDispatches (v0.7.8) pins the
// OUT-CALL cache-hit path. Files GOSPROCS.P_LOOKUP (IN VARCHAR +
// 2 OUT) through threshold under criteria=extended, then on a fresh
// connection dispatches via cache-hit and asserts the OUT values
// land in the bound sql.Out destinations correctly.
//
// v0.7.7 shipped a defensive hasOutDest gate that skipped both the
// cache-hit dispatch AND the auto-populate refresh for any
// sql.Out-bearing dispatch. The v0.7.8 probe overturned that --
// the server honours OUT direction bytes on cache-hit and returns
// OUT values via CP 0x380E. This test pins the working path so a
// regression in preparedParamsFromCached's direction-byte
// preservation, ExecutePreparedCached's ORSResultData flag, or
// writeBackOutParams's call site in Stmt.Exec surfaces here.
func TestCacheHit_CriteriaExtended_OutCallDispatches(t *testing.T) {
	requireFiling(t)
	setupDB := openDB(t)
	setUpStoredProcs(t, setupDB)
	setupDB.Close()

	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const callSQL = "CALL " + procLibrary + ".P_LOOKUP(?, ?, ?)"

	// 1. Warm-up filing on a pinned conn so all PREPAREs land on
	//    one QZDASOINIT job and the 3-PREPARE threshold crosses.
	warm, _ := openDBWithPackageCache(t, "extended")
	warmConn, err := warm.Conn(ctx)
	if err != nil {
		t.Fatalf("warm conn: %v", err)
	}
	for i := 0; i < filingPrepareCount; i++ {
		var name string
		var qty int
		if _, err := warmConn.ExecContext(ctx, callSQL,
			"WIDGET",
			sql.Out{Dest: &name},
			sql.Out{Dest: &qty},
		); err != nil {
			warmConn.Close()
			t.Fatalf("warm CALL iter %d: %v", i, err)
		}
		if i == 0 && name != "Acme Widget" {
			warmConn.Close()
			t.Fatalf("warm iter 0 OUT name = %q, want %q (regular path broken)", name, "Acme Widget")
		}
	}
	warmConn.Close()
	warm.Close()

	// 2. Fresh conn -> RETURN_PACKAGE downloads the filed PMF
	//    including the OUT direction bytes for slots 2 + 3.
	db, buf := openDBWithPackageCache(t, "extended")
	defer db.Close()

	var name string
	var qty int
	if _, err := db.ExecContext(ctx, callSQL,
		"WIDGET",
		sql.Out{Dest: &name},
		sql.Out{Dest: &qty},
	); err != nil {
		t.Fatalf("cache-hit CALL: %v", err)
	}
	if name != "Acme Widget" {
		t.Errorf("cache-hit OUT name = %q, want %q "+
			"(v0.7.8 ExecutePreparedCached OUT decode broken)", name, "Acme Widget")
	}
	if qty != 100 {
		t.Errorf("cache-hit OUT qty = %d, want 100", qty)
	}
	// The cache-hit slog line must have fired -- if it didn't,
	// either the cache lookup missed (warm-up never populated
	// Cached[].NameBytes) or the routing skipped the fast path.
	expectCacheHit(t, buf, cacheHitExecMsg)
}

// TestCacheHit_LOBBindFallthrough verifies that INSERTs binding a
// BLOB column don't emit a cache-hit dispatch line under the
// 2-iter cold-cache test pattern.
//
// v0.7.4 attributed this fall-through to JT400's `JDPackageManager`
// filter; v0.7.5's `TestLOBBind_FilingProbe` overturned that. The
// actual behaviour: the server DOES file LOB-bind statements, and
// the auto-populate path WOULD attempt a cache-hit dispatch on
// iter 4+, but our cache-hit encoder has no branch for the *PGM-
// stored raw-LOB SQL types (404/405/etc) and v0.7.5 falls through
// gracefully via ErrUnsupportedCachedParamType. This test only
// runs 2 iters on a fresh conn (the package starts empty, the
// LocalPrepareCount stays under the threshold), so neither iter
// crosses into cache-hit territory and the v0.7.5 fallback isn't
// exercised here -- see `TestLOBBind_FilingProbe` for the
// 4-iter + threshold-crossing scenario.
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

// filingPrepareCount is IBM's documented threshold + 1: a statement
// must be PREPAREd at least 3 times before it is added to the SQL
// package (IBM i 6.1 PTF SI30855, "to prevent filling the package
// with statements that aren't used frequently"; cited from IBM
// support page "SQL Package Questions and Answers"). We loop 4
// times to be comfortably past the threshold and to also exercise
// NUMBER_TIMES_PREPARED accumulation.
const filingPrepareCount = 4

// TestFiling_ServerSideStateVerified is the load-bearing ground-
// truth assertion for the entire extended-dynamic path. Every other
// TestCacheHit_* in this file proves the *client* logged a cache-
// hit dispatch -- a useful invariant, but mute on whether the
// server actually filed the statement into the *PGM.
//
// This test queries QSYS2.SYSPACKAGESTAT directly:
//
//  1. Wipes any prior <cachePackageName>* package so the count is
//     a measurement of THIS run, not residual state.
//  2. Runs two parameterised statements (one SELECT, one INSERT)
//     through PREPARE_DESCRIBE filingPrepareCount times each, so
//     IBM's 3-PREPARE threshold is crossed.
//  3. Asserts SYSPACKAGESTAT.NUMBER_STATEMENTS >= 2 for the
//     wire-name (LIKE 'GOTCHE%' captures the 4-char options-hash
//     suffix BuildPackageName appends; cf hostserver/db_package.go:184).
//
// The catalog query MUST happen on a FRESH connection. The
// SYSPACKAGESTAT view has a visibility delay within the same
// connection that owns the package: the just-filed statements
// don't appear until the connection is closed or otherwise syncs.
// Verified empirically against both V7R6M0 (IBM Cloud) and V7R5M0
// (PUB400) on 2026-05-11 -- the same fixture run shows 0 rows
// when queried inline, 2 rows when queried externally.
//
// Failure mode interpretation:
//
//   - "no rows matched" → package never created. CREATE_PACKAGE
//     misrouted or the schema doesn't match library= in the DSN.
//     Driver bug.
//   - "NUMBER_STATEMENTS=0 with PACKAGE_USED_SIZE=36848" → package
//     created and was queried inside the wrong connection (visibility
//     delay), OR the 3-PREPARE threshold wasn't crossed. Test bug.
//   - "NUMBER_STATEMENTS >= 2" → end-to-end filing works.
//
// Gated by requireFiling() so the test only runs on LPARs the
// user has authority to wipe and re-fill packages on. PUB400
// V7R5M0 and IBM Cloud V7R6M0 both pass under DB2I_TEST_FILING=1.
func TestFiling_ServerSideStateVerified(t *testing.T) {
	requireFiling(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Wipe any prior <cachePackageName>* package via the CL
	//    hatch. Using a fresh non-package-cache connection so the
	//    DLTOBJ itself doesn't touch the package we're about to
	//    measure.
	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)

	// 2. Run two distinct parameterised SELECTs filingPrepareCount
	//    times each so IBM's 3-PREPARE threshold is crossed for
	//    each one. We use two SELECTs (not SELECT + INSERT)
	//    because the current driver only emits 0x3808=01 on the
	//    SELECT path (db_prepared.go); the EXECUTE_IMMEDIATE
	//    path for INSERT/UPDATE/DELETE hard-codes 0x3808=00
	//    (db_exec.go:187, deferred filing until JT400's
	//    nameOverride_ wire dance is implemented). The cache-hit
	//    fast path's value proposition is the SELECT round-trip
	//    saving anyway, so SELECT-only coverage is the right
	//    proof-of-life until INSERT filing lands.
	//
	//    Pin a single *sql.Conn so all PREPAREs land on the same
	//    QZDASOINIT job -- the server-side threshold counter is
	//    per-job for the package, and a pool that splits the loop
	//    across 2 jobs leaves each below threshold even though the
	//    aggregate is fine.
	db, _ := openDBWithPackageCache(t, "default")
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}

	const selectSQL1 = "SELECT CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1"
	const selectSQL2 = "SELECT CAST(? AS VARCHAR(64)) FROM SYSIBM.SYSDUMMY1"
	for i := 0; i < filingPrepareCount; i++ {
		var probe int
		if err := conn.QueryRowContext(ctx, selectSQL1, i).Scan(&probe); err != nil {
			conn.Close()
			t.Fatalf("SELECT#1 iter %d: %v", i, err)
		}
	}
	for i := 0; i < filingPrepareCount; i++ {
		var probe string
		if err := conn.QueryRowContext(ctx, selectSQL2, fmt.Sprintf("v%d", i)).Scan(&probe); err != nil {
			conn.Close()
			t.Fatalf("SELECT#2 iter %d: %v", i, err)
		}
	}

	// 4. Close the package-cache conn (and pool) so SYSPACKAGESTAT
	//    sees the freshly-filed entries. The catalog view has a
	//    visibility delay within the connection that owns the
	//    package -- queries inside the same conn return 0 rows
	//    even after filing.
	conn.Close()
	db.Close()

	// 5. Assert server-side state from the wipeDB (non-package-
	//    cache) connection. LIKE pattern matches the 4-char
	//    options-hash suffix BuildPackageName appended to the
	//    base name (see hostserver/db_package.go:184).
	row := wipeDB.QueryRowContext(ctx, `
		SELECT PACKAGE_NAME, NUMBER_STATEMENTS, PACKAGE_USED_SIZE,
		       LAST_USED_TIMESTAMP
		FROM   QSYS2.SYSPACKAGESTAT
		WHERE  PACKAGE_NAME LIKE '`+cachePackageName+`%'
		  AND  PACKAGE_SCHEMA = '`+schema()+`'
		ORDER BY PACKAGE_NAME
		FETCH FIRST 1 ROWS ONLY`)
	var pkgName string
	var stmtCount, usedSize int64
	var lastUsed sql.NullString
	if err := row.Scan(&pkgName, &stmtCount, &usedSize, &lastUsed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("SYSPACKAGESTAT returned no rows for %s.%s* -- "+
				"CREATE_PACKAGE may have failed or routed to a different schema",
				schema(), cachePackageName)
		}
		t.Fatalf("SYSPACKAGESTAT: %v", err)
	}
	t.Logf("server-side state: pkg=%s.%s entries=%d used_size=%d last_used=%v",
		schema(), pkgName, stmtCount, usedSize, lastUsed.String)

	if stmtCount < 2 {
		t.Fatalf("expected NUMBER_STATEMENTS>=2 (one per parameterised statement "+
			"after %d PREPAREs each), got %d (pkg=%s.%s, used_size=%d). "+
			"If used_size=36848 (the empty-floor), the 3-PREPARE threshold "+
			"wasn't crossed -- see IBM 'SQL Package Questions and Answers' "+
			"or docs/package-caching.md.",
			filingPrepareCount, stmtCount, schema(), pkgName, usedSize)
	}
}

// wipePackage issues `DLTOBJ OBJ(<schema>/<base>*) OBJTYPE(*SQLPKG)`
// via QSYS2.QCMDEXC. Wildcard handles the 4-char options-hash
// suffix BuildPackageName appended. CPF2105 (object not found) is
// the expected first-run outcome; we swallow it.
func wipePackage(t *testing.T, db *sql.DB, base string) {
	t.Helper()
	cmd := "DLTOBJ OBJ(" + schema() + "/" + base + "*) OBJTYPE(*SQLPKG)"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CALL QSYS2.QCMDEXC(?)", cmd); err != nil {
		// SQL-7032 is "DLTOBJ command completed, object not found" --
		// fine here. Any other error is real and worth surfacing.
		if !strings.Contains(err.Error(), "CPF2105") &&
			!strings.Contains(err.Error(), "not found") {
			t.Logf("wipePackage(%s): %v (continuing)", base, err)
		}
	}
}

// fileViaIUD is the IUD-side companion to the SELECT-only
// TestFiling_ServerSideStateVerified. Runs each parameterised
// statement filingPrepareCount times on a pinned conn so IBM's
// 3-PREPARE threshold is crossed, then asserts the server filed
// the statement by querying SYSPACKAGESTMTSTAT through a fresh
// (non-extended-dynamic) connection.
//
// Returns the pkg / stmt count it observed so subtests can do
// stricter assertions (e.g. "all three verbs landed").
func fileViaIUD(t *testing.T, prepareSQL string, mkArgs func(i int) []any, verbLabel string) {
	t.Helper()
	requireFiling(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use the shared cachePackageName so all filing tests file
	// into one *PGM. Wipe first so each test is deterministic.
	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)

	db, _ := openDBWithPackageCache(t, "default")
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}

	for i := 0; i < filingPrepareCount; i++ {
		args := mkArgs(i)
		if _, err := conn.ExecContext(ctx, prepareSQL, args...); err != nil {
			conn.Close()
			t.Fatalf("%s iter %d: %v", verbLabel, i, err)
		}
	}
	conn.Close()
	db.Close()

	// SYSPACKAGESTAT has a within-conn visibility delay -- check from
	// the plain wipeDB connection that owns no extended-dynamic state.
	var stmtCount int64
	var usedSize int64
	row := wipeDB.QueryRowContext(ctx, `
		SELECT NUMBER_STATEMENTS, PACKAGE_USED_SIZE
		FROM   QSYS2.SYSPACKAGESTAT
		WHERE  PACKAGE_NAME LIKE '`+cachePackageName+`%'
		  AND  PACKAGE_SCHEMA = '`+schema()+`'
		FETCH FIRST 1 ROWS ONLY`)
	if err := row.Scan(&stmtCount, &usedSize); err != nil {
		t.Fatalf("SYSPACKAGESTAT scan: %v", err)
	}
	if stmtCount < 1 {
		t.Fatalf("%s: expected NUMBER_STATEMENTS>=1, got %d (used_size=%d)",
			verbLabel, stmtCount, usedSize)
	}
	t.Logf("%s filed: entries=%d used_size=%d", verbLabel, stmtCount, usedSize)
}

// TestFiling_InsertVerified proves parameterised INSERT statements
// file into the *PGM after the 3-PREPARE threshold is crossed.
// Closes the v0.7.3 "INSERT filing not yet wired" gap by exercising
// the cpDBPrepareOption=01 + cpPackageName wire shape on the
// EXECUTE_IMMEDIATE path (db_exec.go).
func TestFiling_InsertVerified(t *testing.T) {
	requireFiling(t)
	// Each iteration needs a unique ID so primary-key duplicates
	// don't abort the loop before the threshold is crossed.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wipeDB := openDB(t)
	defer wipeDB.Close()
	tbl := schema() + "." + tablePrefix + "iud"
	_, _ = wipeDB.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := wipeDB.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(32) NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer wipeDB.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	fileViaIUD(t,
		"INSERT INTO "+tbl+" (ID, LABEL) VALUES (?, ?)",
		func(i int) []any { return []any{i + 1, "insert-" + strconv.Itoa(i)} },
		"INSERT")
}

// TestFiling_UpdateVerified is the UPDATE companion. Seeds rows up
// front so each UPDATE acts on an existing row and the per-iter
// NUMBER_TIMES_PREPARED accumulates against one statement text.
func TestFiling_UpdateVerified(t *testing.T) {
	requireFiling(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wipeDB := openDB(t)
	defer wipeDB.Close()
	tbl := schema() + "." + tablePrefix + "iud"
	_, _ = wipeDB.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := wipeDB.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(32) NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer wipeDB.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck
	for i := 0; i < filingPrepareCount; i++ {
		if _, err := wipeDB.ExecContext(ctx,
			"INSERT INTO "+tbl+" VALUES (?, ?)", 100+i, "seed-"+strconv.Itoa(i)); err != nil {
			t.Fatalf("seed INSERT: %v", err)
		}
	}

	fileViaIUD(t,
		"UPDATE "+tbl+" SET LABEL = ? WHERE ID = ?",
		func(i int) []any { return []any{"upd-" + strconv.Itoa(i), 100 + i} },
		"UPDATE")
}

// TestFiling_DeleteVerified is the DELETE companion. Same seed
// pattern as TestFiling_UpdateVerified.
func TestFiling_DeleteVerified(t *testing.T) {
	requireFiling(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wipeDB := openDB(t)
	defer wipeDB.Close()
	tbl := schema() + "." + tablePrefix + "iud"
	_, _ = wipeDB.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := wipeDB.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(32) NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer wipeDB.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck
	for i := 0; i < filingPrepareCount; i++ {
		if _, err := wipeDB.ExecContext(ctx,
			"INSERT INTO "+tbl+" VALUES (?, ?)", 100+i, "seed-"+strconv.Itoa(i)); err != nil {
			t.Fatalf("seed INSERT: %v", err)
		}
	}

	fileViaIUD(t,
		"DELETE FROM "+tbl+" WHERE ID = ?",
		func(i int) []any { return []any{100 + i} },
		"DELETE")
}

// TestFiling_AllThreeInOnePackage verifies that filing INSERT,
// UPDATE, and DELETE all into one *PGM lands cleanly (i.e. each
// server-side rename leaves the RPB in a state the next prepare
// can recover from). Asserts NUMBER_STATEMENTS >= 3 -- one per
// verb.
func TestFiling_AllThreeInOnePackage(t *testing.T) {
	requireFiling(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)

	tbl := schema() + "." + tablePrefix + "iud3"
	_, _ = wipeDB.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := wipeDB.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(32) NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer wipeDB.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck
	// Seed rows so UPDATE/DELETE iterations succeed.
	for i := 0; i < filingPrepareCount*3; i++ {
		if _, err := wipeDB.ExecContext(ctx,
			"INSERT INTO "+tbl+" VALUES (?, ?)", 200+i, "seed-"+strconv.Itoa(i)); err != nil {
			t.Fatalf("seed INSERT: %v", err)
		}
	}

	db, _ := openDBWithPackageCache(t, "default")
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}

	insertSQL := "INSERT INTO " + tbl + " (ID, LABEL) VALUES (?, ?)"
	updateSQL := "UPDATE " + tbl + " SET LABEL = ? WHERE ID = ?"
	deleteSQL := "DELETE FROM " + tbl + " WHERE ID = ?"

	for i := 0; i < filingPrepareCount; i++ {
		if _, err := conn.ExecContext(ctx, insertSQL, 1+i, "ins-"+strconv.Itoa(i)); err != nil {
			conn.Close()
			t.Fatalf("INSERT iter %d: %v", i, err)
		}
	}
	for i := 0; i < filingPrepareCount; i++ {
		if _, err := conn.ExecContext(ctx, updateSQL, "upd-"+strconv.Itoa(i), 200+i); err != nil {
			conn.Close()
			t.Fatalf("UPDATE iter %d: %v", i, err)
		}
	}
	for i := 0; i < filingPrepareCount; i++ {
		if _, err := conn.ExecContext(ctx, deleteSQL, 204+i); err != nil {
			conn.Close()
			t.Fatalf("DELETE iter %d: %v", i, err)
		}
	}
	conn.Close()
	db.Close()

	var stmtCount, usedSize int64
	row := wipeDB.QueryRowContext(ctx, `
		SELECT NUMBER_STATEMENTS, PACKAGE_USED_SIZE
		FROM   QSYS2.SYSPACKAGESTAT
		WHERE  PACKAGE_NAME LIKE '`+cachePackageName+`%'
		  AND  PACKAGE_SCHEMA = '`+schema()+`'
		FETCH FIRST 1 ROWS ONLY`)
	if err := row.Scan(&stmtCount, &usedSize); err != nil {
		t.Fatalf("SYSPACKAGESTAT scan: %v", err)
	}
	t.Logf("IUD-in-one-package: entries=%d used_size=%d", stmtCount, usedSize)
	if stmtCount < 3 {
		t.Fatalf("expected NUMBER_STATEMENTS>=3 (one per IUD verb), got %d", stmtCount)
	}
}

// TestFiling_ParamBindingRoundTrip pins the v0.7.4 cache-hit
// param-binding fix in place. Pre-fills a parameterised SELECT
// with 4 PREPAREs to cross the filing threshold, then opens a
// fresh connection and runs the same SQL with a distinctive
// argument value -- the cache-hit dispatch path
// (OpenSelectPreparedCached) must return that exact value, not
// the all-zeros default the SQLDA Precision/Scale leak previously
// produced. Unconditional (no requireFiling gate) so it runs on
// every conformance pass.
func TestFiling_ParamBindingRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)

	db, _ := openDBWithPackageCache(t, "default")
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	const q = "SELECT CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1"
	for i := 0; i < filingPrepareCount; i++ {
		var v int
		if err := conn.QueryRowContext(ctx, q, i).Scan(&v); err != nil {
			conn.Close()
			t.Fatalf("warm iter %d: %v", i, err)
		}
		if v != i {
			conn.Close()
			t.Fatalf("warm iter %d: got v=%d, want %d (uncached path bind)", i, v, i)
		}
	}
	conn.Close()
	db.Close()

	// Fresh connection: cache-hit dispatch on the just-filed SQL.
	db2, _ := openDBWithPackageCache(t, "default")
	defer db2.Close()
	const sentinel = 31415926
	var v int
	if err := db2.QueryRowContext(ctx, q, sentinel).Scan(&v); err != nil {
		t.Fatalf("cache-hit dispatch: %v", err)
	}
	if v != sentinel {
		t.Fatalf("cache-hit param-binding regression: got v=%d, want %d -- "+
			"the SQLDA Precision/Scale leak fix in preparedParamsFromCached "+
			"has reverted (see db_cached.go:330)", v, sentinel)
	}
}

// TestFiling_WireEquivalenceWithJT400 hooks the driver's send-side
// wire bytes via hostserver.SetWireHook and asserts the codepoint
// shape matches what the JT400 fixture trace
// (testdata/jtopen-fixtures/.../prepared_package_filing_iud.trace)
// emits for the same flow:
//
//   - Packaged PREPARE_DESCRIBE (ReqRepID 0x1803) carries CP 0x3831
//     (extended stmt text), CP 0x3812 (statement type), CP 0x3808
//     (prepare option), CP 0x3804 (package name).
//   - Packaged regular-path EXECUTE (ReqRepID 0x1805, iters where
//     PREPARE_DESCRIBE also fires) carries CP 0x3804 (package name),
//     CP 0x3812 (statement type), CP 0x381F (extended data), CP 0x3814
//     (sync-point delimiter), and DOES NOT carry CP 0x3806
//     (statement-name override).
//   - Packaged cache-hit EXECUTE (ReqRepID 0x1805 issued by the
//     auto-populate fast path after RefreshPackage retrieves the
//     server-renamed name) intentionally diverges from JT400: it
//     carries CP 0x3806 (statement-name override) in place of the
//     RPB+package-marker resolution JT400 uses. The server accepts
//     both forms; cache-hit saves a PREPARE_DESCRIBE round-trip on
//     subsequent calls.
//
// JT400's EXECUTE additionally emits CP 0x380D (scrollable cursor
// flag) and CP 0x3830 (result-set holdability), but neither is
// required by the server for IUD execution -- our wire shape
// remains a strict subset that the IBM i V7R6M0 + V7R5M0 servers
// happily dispatch.
//
// Runs an INSERT through the same 4-iter PREPARE loop the JT400
// fixture uses. SetWireHook is a process-global, so the test runs
// serially (no t.Parallel) and restores the prior hook on exit.
func TestFiling_WireEquivalenceWithJT400(t *testing.T) {
	requireFiling(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)
	const tblName = "GOJTWEQ"
	qual := schema() + "." + tblName
	_, _ = wipeDB.ExecContext(ctx, "DROP TABLE "+qual)
	if _, err := wipeDB.ExecContext(ctx,
		"CREATE TABLE "+qual+" (ID INTEGER NOT NULL, LABEL VARCHAR(32))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() { _, _ = wipeDB.ExecContext(ctx, "DROP TABLE "+qual) }()
	wipeDB.Close()

	type captured struct {
		reqID  uint16
		corr   uint32
		params []hostserver.DBParam
	}
	var (
		mu     sync.Mutex
		frames []captured
	)
	prev := hostserver.SwapWireHook(func(hdr hostserver.Header, full []byte) {
		// Only DB-SQL frames (ServerID 0xE004, TemplateLength=20)
		// carry the DBRequestTemplate shape DecodeDBRequest expects;
		// signon and start-server frames have a shorter template.
		if hdr.ServerID != hostserver.ServerDatabase || hdr.TemplateLength != 20 {
			return
		}
		if len(full) < int(hdr.Length) {
			return
		}
		_, params, err := hostserver.DecodeDBRequest(full[hostserver.HeaderLength:hdr.Length])
		if err != nil {
			return
		}
		mu.Lock()
		frames = append(frames, captured{reqID: hdr.ReqRepID, corr: hdr.CorrelationID, params: params})
		mu.Unlock()
	})
	defer hostserver.SetWireHook(prev)

	db, _ := openDBWithPackageCache(t, "")
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()
	insertSQL := "INSERT INTO " + qual + " (ID, LABEL) VALUES (?, ?)"
	for i := 0; i < filingPrepareCount; i++ {
		if _, err := conn.ExecContext(ctx, insertSQL, i, fmt.Sprintf("row-%d", i)); err != nil {
			t.Fatalf("INSERT iter %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(frames) == 0 {
		t.Fatal("no frames captured; wire hook didn't fire")
	}

	// CP sets we require to be present. JT400 also emits CP 0x380D
	// + CP 0x3830 on EXECUTE; we omit them as a deliberate wire
	// size optimisation since the server doesn't require them. The
	// assertion focuses on CPs the server actually parses for IUD
	// dispatch.
	wantPrepareCPs := []uint16{0x3831, 0x3812, 0x3808, 0x3804}
	wantRegularExecCPs := []uint16{0x3804, 0x3812, 0x381F, 0x3814}
	wantCacheHitExecCPs := []uint16{0x3806, 0x3812, 0x381F, 0x3814}

	sawPackagedPrepare := 0
	sawRegularPackagedExecute := 0
	sawCacheHitExecute := 0

	for _, f := range frames {
		switch f.reqID {
		case hostserver.ReqDBSQLPrepareDescribe:
			pkg := paramData(f.params, 0x3804)
			if len(pkg) <= 4 {
				continue // not packaged
			}
			sawPackagedPrepare++
			for _, cp := range wantPrepareCPs {
				if !hasParamCP(f.params, cp) {
					t.Errorf("packaged PREPARE_DESCRIBE corr=%d missing CP 0x%04X (JT400 fixture emits it)", f.corr, cp)
				}
			}
		case hostserver.ReqDBSQLExecute:
			// Classify by which CPs are present. The regular path
			// emits CP 0x3804 (package marker); the cache-hit fast
			// path emits CP 0x3806 (statement-name override) in
			// place of the package marker.
			hasPkg := len(paramData(f.params, 0x3804)) > 4
			hasNameOverride := hasParamCP(f.params, 0x3806)
			switch {
			case hasPkg && !hasNameOverride:
				sawRegularPackagedExecute++
				for _, cp := range wantRegularExecCPs {
					if !hasParamCP(f.params, cp) {
						t.Errorf("regular-path packaged EXECUTE corr=%d missing CP 0x%04X", f.corr, cp)
					}
				}
			case hasNameOverride:
				sawCacheHitExecute++
				for _, cp := range wantCacheHitExecCPs {
					if !hasParamCP(f.params, cp) {
						t.Errorf("cache-hit EXECUTE corr=%d missing CP 0x%04X", f.corr, cp)
					}
				}
			}
		}
	}
	if sawPackagedPrepare == 0 {
		t.Error("no packaged PREPARE_DESCRIBE captured -- driver didn't emit CP 0x3804")
	}
	if sawRegularPackagedExecute == 0 {
		t.Error("no regular-path packaged EXECUTE captured -- driver didn't emit CP 0x3804")
	}
	// Cache-hit EXECUTEs are optional in this loop (depends on
	// whether the auto-populate refresh learned the renamed name);
	// log for visibility.
	t.Logf("captured: %d packaged PREPARE_DESCRIBE, %d regular EXECUTE, %d cache-hit EXECUTE",
		sawPackagedPrepare, sawRegularPackagedExecute, sawCacheHitExecute)
}

func hasParamCP(params []hostserver.DBParam, cp uint16) bool {
	for _, p := range params {
		if p.CodePoint == cp {
			return true
		}
	}
	return false
}

func paramData(params []hostserver.DBParam, cp uint16) []byte {
	for _, p := range params {
		if p.CodePoint == cp {
			return p.Data
		}
	}
	return nil
}

// TestCacheHit_DDLInvalidation pins the v0.7.5 SQL-204 / SQL-805
// fallback on the cache-hit dispatch path. Empirical finding on
// V7R6M0: same-shape DROP+CREATE TABLE does NOT invalidate the
// filed plan -- the server transparently rebinds and the cache-hit
// continues to work. To exercise a real SQL-204 / SQL-805 we have
// to DROP the table without recreating it.
//
// Sequence:
//
//  1. Wipe the package; create a fresh test table with one row.
//  2. Fill the cache by running a parameterised SELECT 4 times on
//     a pinned conn -- crosses IBM's 3-PREPARE threshold so the
//     SELECT files.
//  3. Open a fresh conn (downloads the now-populated *PGM cache)
//     and confirm the SELECT cache-hits.
//  4. Through a SEPARATE (non-package-cache) conn, DROP the table.
//     The filed plan in the *PGM now references a missing object.
//  5. Re-run the SELECT on the cache-loaded conn. Expect SQL-204
//     from the cache-hit path; the v0.7.5 fallback purges the
//     entry and re-routes through plain PREPARE_DESCRIBE, which
//     also fails with SQL-204 (the table really IS gone) -- the
//     caller sees SQL-204 from PREPARE, not from the cache-hit
//     dispatch. The cache entry is purged regardless.
//  6. Recreate the table with a new row.
//  7. Re-run the SELECT once more. With the purge in place, the
//     cache lookup misses, the regular PREPARE_DESCRIBE path
//     runs against the new table, and the call succeeds returning
//     the new row. Asserts no cache-hit dispatch fires (the entry
//     IS gone) and the row content matches the post-recreate data.
//
// requireFiling-gated because the 4-iter fill depends on the
// 3-PREPARE threshold and the DROP requires schema authority.
// Runs end-to-end in ~½ s on V7R6M0.
func TestCacheHit_DDLInvalidation(t *testing.T) {
	requireFiling(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Step 1: wipe package + (re)create the test table.
	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)
	tbl := makeCacheTestTable(t, wipeDB, "ddl",
		"(id INTEGER NOT NULL PRIMARY KEY, label VARCHAR(32))")
	if _, err := wipeDB.ExecContext(ctx,
		"INSERT INTO "+tbl+" (id, label) VALUES (1, 'before')"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	// Step 2: file the SELECT.
	selectSQL := "SELECT label FROM " + tbl + " WHERE id = ?"
	fillPackageCache(t, fillQuery, selectSQL, 1)

	// Step 3: fresh conn -- cache-loaded; verify the SELECT
	// cache-hits cleanly.
	db, buf := openDBWithPackageCache(t, "")
	defer db.Close()
	var label string
	if err := db.QueryRowContext(ctx, selectSQL, 1).Scan(&label); err != nil {
		t.Fatalf("pre-DDL SELECT: %v", err)
	}
	if label != "before" {
		t.Fatalf("pre-DDL label = %q, want %q", label, "before")
	}
	expectCacheHit(t, buf, cacheHitQueryMsg)
	preDDLHits := countCacheHits(buf)

	// Step 4: DROP the table (no recreate yet). The cache-loaded
	// conn's *PGM still references the now-missing object.
	if _, err := wipeDB.ExecContext(ctx, "DROP TABLE "+tbl); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	// Step 5: the cache-hit dispatch should hit SQL-204, the
	// v0.7.5 fallback should purge the entry, the regular
	// PREPARE_DESCRIBE path should also fail SQL-204 (the table
	// is genuinely gone), and the caller sees an error. The
	// important invariant is that the fallback path fired -- we
	// verify that in step 7 (no more cache-hit dispatch for the
	// purged entry).
	err := db.QueryRowContext(ctx, selectSQL, 1).Scan(&label)
	if err == nil {
		t.Fatalf("post-DROP SELECT unexpectedly succeeded; table is gone")
	}
	var dbErr *hostserver.Db2Error
	if !errors.As(err, &dbErr) {
		t.Fatalf("post-DROP SELECT err is not *Db2Error: %T %v", err, err)
	}
	if dbErr.SQLCode != -204 {
		// SQL-204 is the documented signal; the test asserts it
		// loud so a future server release that changes the signal
		// gets a clear diagnostic.
		t.Fatalf("post-DROP SQLCode = %d, want -204", dbErr.SQLCode)
	}

	// Step 6: recreate the table with new row.
	if _, err := wipeDB.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (id INTEGER NOT NULL PRIMARY KEY, label VARCHAR(32))"); err != nil {
		t.Fatalf("re-CREATE TABLE: %v", err)
	}
	if _, err := wipeDB.ExecContext(ctx,
		"INSERT INTO "+tbl+" (id, label) VALUES (1, 'after')"); err != nil {
		t.Fatalf("post-recreate insert: %v", err)
	}

	// Step 7: on the same cache-loaded conn, re-run the SELECT.
	// The entry was purged in step 5, so the cache-hit dispatch
	// must NOT fire; the regular PREPARE_DESCRIBE path runs
	// against the new table and succeeds returning the new row.
	hitsBeforeFinal := countCacheHits(buf)
	if err := db.QueryRowContext(ctx, selectSQL, 1).Scan(&label); err != nil {
		t.Fatalf("post-recreate SELECT: %v", err)
	}
	if label != "after" {
		t.Errorf("post-recreate label = %q, want %q", label, "after")
	}
	finalHits := countCacheHits(buf)
	if finalHits > hitsBeforeFinal {
		t.Errorf("cache-hit dispatch fired after purge: pre-DDL=%d before-final=%d final=%d "+
			"-- the v0.7.5 purge after SQL-204 didn't take",
			preDDLHits, hitsBeforeFinal, finalHits)
	}
}

// TestLOBBind_FilingProbe answers the v0.7.5 LOB-bind filing
// investigation question empirically: does the IBM i server file a
// LOB-bind INSERT when asked? Our existing CHANGELOG line ("LOB-bind
// filing continues to fall through to the cache-miss path per
// JT400's `JDPackageManager` filter") infers from JT400's source
// that the exclusion is correct; this test measures the actual
// server behaviour against V7R6M0.
//
// The test does NOT assert pass/fail on whether filing happens --
// the goal is empirical observation. Result interpretation:
//
//   - SYSPACKAGESTAT shows the LOB-bind INSERT as a filed entry:
//     the server accepts filing for LOB-bind statements. Our
//     v0.7.4 cache-hit path needs to be extended to handle LOB
//     locator binds, and the JT400-derived exclusion in our docs
//     is obsolete (V7R6 has moved past whatever historical
//     limitation JT400 was working around).
//   - SYSPACKAGESTAT does NOT show the LOB-bind INSERT, OR shows
//     it but cache-hit re-prepares on subsequent calls: the
//     existing fall-through behaviour is correct; our docs need
//     to point at the real (server-side) gate rather than
//     inferring it from JT400 source.
//
// The test logs the outcome via t.Logf so the result is visible in
// CI output regardless of pass/fail; the assertion is only that
// the test ran end-to-end without a wire error.
//
// requireFiling-gated because the 4-iter fill depends on the
// 3-PREPARE threshold and the WIP package wipe requires schema
// authority.
func TestLOBBind_FilingProbe(t *testing.T) {
	requireFiling(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	wipeDB := openDB(t)
	defer wipeDB.Close()
	wipePackage(t, wipeDB, cachePackageName)
	tbl := makeCacheTestTable(t, wipeDB, "lobf",
		"(id INTEGER NOT NULL, payload BLOB(64K))")

	// Pin a single conn so all 4 PREPAREs accumulate against one
	// QZDASOINIT job (the server's per-job filing counter).
	db, buf := openDBWithPackageCache(t, "default")
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}

	insertSQL := "INSERT INTO " + tbl + " (id, payload) VALUES (?, ?)"
	// Small 1 KB BLOB -- enough to exercise the locator-bind path
	// without crossing the chunk boundary that would dominate
	// timing on a slow link.
	payload := bytes.Repeat([]byte{0xAB}, 1024)
	// Track outcomes per iteration. Errors are logged but don't
	// fail the test -- the goal is observation. If iter 0 errors,
	// that IS a failure (no measurement possible).
	var iterErr [4]error
	for i := 0; i < filingPrepareCount; i++ {
		_, err := conn.ExecContext(ctx, insertSQL, i, payload)
		iterErr[i] = err
		if err != nil {
			t.Logf("LOB-bind INSERT iter %d: %v", i, err)
			if i == 0 {
				conn.Close()
				t.Fatalf("iter 0 must succeed for the probe to be meaningful")
			}
		}
	}
	conn.Close()
	db.Close()

	// Query SYSPACKAGESTAT on a fresh, non-package-cache conn (the
	// visibility-delay rule from TestFiling_ServerSideStateVerified
	// applies).
	row := wipeDB.QueryRowContext(ctx, `
		SELECT PACKAGE_NAME, NUMBER_STATEMENTS, PACKAGE_USED_SIZE
		FROM   QSYS2.SYSPACKAGESTAT
		WHERE  PACKAGE_NAME LIKE '`+cachePackageName+`%'
		  AND  PACKAGE_SCHEMA = '`+schema()+`'
		ORDER BY PACKAGE_NAME
		FETCH FIRST 1 ROWS ONLY`)
	var pkgName string
	var numStmts, usedSize int64
	if err := row.Scan(&pkgName, &numStmts, &usedSize); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Logf("SYSPACKAGESTAT empty -- server did NOT create *PGM (or LOB-bind PREPARE skipped CREATE_PACKAGE).")
			return
		}
		t.Fatalf("SYSPACKAGESTAT: %v", err)
	}
	t.Logf("LOB-bind filing observation: pkg=%s.%s NUMBER_STATEMENTS=%d PACKAGE_USED_SIZE=%d",
		schema(), pkgName, numStmts, usedSize)

	// Per-statement detail -- if filing happened, the STATEMENT_TEXT
	// will start with 'INSERT' (or its EBCDIC equivalent depending
	// on the catalog view's CCSID handling).
	rows, err := wipeDB.QueryContext(ctx, `
		SELECT STATEMENT_NAME, NUMBER_TIMES_PREPARED, NUMBER_TIMES_EXECUTED,
		       SUBSTR(STATEMENT_TEXT, 1, 80) AS STMT
		FROM   QSYS2.SYSPACKAGESTMTSTAT
		WHERE  PACKAGE_NAME = '`+pkgName+`'
		  AND  PACKAGE_SCHEMA = '`+schema()+`'
		ORDER BY STATEMENT_NAME`)
	if err != nil {
		t.Fatalf("SYSPACKAGESTMTSTAT: %v", err)
	}
	defer rows.Close()
	stmtSeen := 0
	for rows.Next() {
		var name, stmt string
		var prepared, executed int64
		if err := rows.Scan(&name, &prepared, &executed, &stmt); err != nil {
			t.Fatalf("scan stmt row: %v", err)
		}
		t.Logf("  filed statement: %s prepared=%d executed=%d text=%q",
			name, prepared, executed, stmt)
		stmtSeen++
	}

	// Observability: did our driver log any cache-hit dispatch?
	// (None expected on iter 1-3; iter 4 would only cache-hit if
	// auto-populate after refresh saw a populated NameBytes.)
	hits := countCacheHits(buf)
	t.Logf("driver cache-hit dispatches observed: %d", hits)

	// Did the auto-populate path cause a late-iteration cache-hit
	// attempt? If so, did it succeed or hit an encoder gap?
	autoPopulateFired := false
	cacheHitErr := ""
	for i, err := range iterErr {
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), "cached") || strings.Contains(err.Error(), "SQL type") {
			autoPopulateFired = true
			cacheHitErr = fmt.Sprintf("iter %d: %v", i, err)
			break
		}
	}

	// Empirical summary -- not a hard assertion since the answer
	// depends on server behaviour.
	switch {
	case numStmts == 0:
		t.Logf("CONCLUSION: server refused to file LOB-bind INSERT. Our cache-miss fall-through is correct; the limitation lives on the server, not the driver.")
	case numStmts >= 1 && stmtSeen == 0:
		t.Logf("CONCLUSION: SYSPACKAGESTAT counts a statement but SYSPACKAGESTMTSTAT is empty -- unusual. Investigate before declaring v0.7.5 done.")
	case numStmts >= 1 && autoPopulateFired:
		t.Logf("CONCLUSION: server DID file the LOB-bind INSERT, AND v0.7.4 auto-populate fired on iter 3+. " +
			"BUT the cache-hit encoder rejected the LOB locator (%s). " +
			"v0.7.5 should either: (a) skip auto-populate for SQLs with LOB binds "+
			"(`packageEligibleFor` extension), or (b) defer to v0.7.6 with extended "+
			"cache-hit encoder support for LOB types. The server cooperation is now "+
			"empirically confirmed.", cacheHitErr)
	case numStmts >= 1 && stmtSeen >= 1:
		t.Logf("CONCLUSION: server DID file the LOB-bind INSERT. Our cache-miss fall-through leaves a round-trip win on the table; consider extending v0.7.6 to file LOB-bind eligibly.")
	}
}
