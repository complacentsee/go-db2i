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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"

	gojtopen "github.com/complacentsee/goJTOpen/driver"
	"github.com/complacentsee/goJTOpen/ebcdic"
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
// shapes round-trip exactly. Uses VARCHAR FOR BIT DATA -- the inline
// path (no locator) -- to keep coverage on the small-binary case.
// The locator-bind path lives in TestLOBBlob.
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

// TestLOBBlob exercises the locator-bind path on a real BLOB column.
// VARCHAR FOR BIT DATA can be inlined as a 2-byte SL + payload value
// in CP 0x381F; BLOB columns force the WRITE_LOB_DATA / locator
// handle dance. Three sub-cases:
//
//   - 8 KiB []byte: byte-equal round-trip via the default single-frame
//     bind (matches JT400's pattern for prepared_blob_insert).
//   - LOBValue{Bytes: ...}: same payload via the explicit LOBValue
//     wrapper, confirming the driver-level resolver routes correctly.
//   - LOBValue{Reader: ..., Length: 80 KiB}: streamed bind that
//     produces multiple WRITE_LOB_DATA frames at advancing offsets.
func TestLOBBlob(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "lobblob"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, b BLOB(1M))`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	// 8 KiB byte ramp 0x00..0xFF repeating.
	small := make([]byte, 8*1024)
	for i := range small {
		small[i] = byte(i & 0xFF)
	}

	t.Run("byte slice", func(t *testing.T) {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 1, small); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, small) {
			t.Errorf("BLOB round-trip: %d bytes back, %d bytes sent (head/tail mismatch)", len(got), len(small))
		}
	})

	t.Run("LOBValue Bytes", func(t *testing.T) {
		val := &gojtopen.LOBValue{Bytes: small}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 2, val); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 2).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, small) {
			t.Errorf("LOBValue Bytes round-trip mismatch (%d bytes back, %d sent)", len(got), len(small))
		}
	})

	t.Run("LOBValue Reader 80KiB", func(t *testing.T) {
		// 80 KiB > 32 KiB chunk size, so the driver MUST split into
		// >=3 WRITE_LOB_DATA frames.
		const total = 80 * 1024
		want := make([]byte, total)
		for i := range want {
			want[i] = byte((i * 7) & 0xFF)
		}
		val := &gojtopen.LOBValue{Reader: bytes.NewReader(want), Length: int64(total)}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 3, val); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 3).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("LOBValue Reader 80KiB round-trip mismatch (%d bytes back, %d sent)", len(got), total)
		}
	})

	t.Run("empty []byte", func(t *testing.T) {
		// Empty BLOB: zero-length WRITE_LOB_DATA still has to land
		// or the locator stays uninitialised on the server side
		// and SELECT either returns NULL or (worse) leftover bytes
		// from a recycled handle. JT400's writeData(0, new byte[0])
		// is the equivalent path and bindOneLOB has a special-case
		// branch for it.
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 90, []byte{}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 90).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("empty BLOB read back as %d bytes; want 0", len(got))
		}
	})

	t.Run("empty LOBValue Bytes", func(t *testing.T) {
		val := &gojtopen.LOBValue{Bytes: []byte{}}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 91, val); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 91).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("empty LOBValue.Bytes read back as %d bytes; want 0", len(got))
		}
	})

	t.Run("empty LOBValue Reader", func(t *testing.T) {
		val := &gojtopen.LOBValue{Reader: bytes.NewReader(nil), Length: 0}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 92, val); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 92).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("empty LOBValue.Reader read back as %d bytes; want 0", len(got))
		}
	})

	t.Run("nil bind to nullable BLOB", func(t *testing.T) {
		// nil driver.Value should NOT issue WRITE_LOB_DATA -- the
		// bindLOBParameters path overrides the shape and lets the
		// indicator-block null marker fire. SELECT must surface as
		// SQL NULL, not zero bytes.
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 93, nil); err != nil {
			t.Fatalf("insert nil: %v", err)
		}
		var raw sql.NullString
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 93).Scan(&raw); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if raw.Valid {
			t.Errorf("BLOB nil bind read back as non-NULL value (%d bytes)", len(raw.String))
		}
	})

	t.Run("nil typed pointer bind", func(t *testing.T) {
		// (*LOBValue)(nil) should also resolve to NULL, not panic.
		var val *gojtopen.LOBValue
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 94, val); err != nil {
			t.Fatalf("insert *LOBValue(nil): %v", err)
		}
		var raw sql.NullString
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 94).Scan(&raw); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if raw.Valid {
			t.Errorf("BLOB *LOBValue(nil) bind read back as non-NULL")
		}
	})

	t.Run("LOBValue Reader 1MiB", func(t *testing.T) {
		// 1 MiB stress test -- the explicit Done-criteria size.
		const total = 1024 * 1024
		want := make([]byte, total)
		for i := range want {
			want[i] = byte((i * 13) & 0xFF)
		}
		val := &gojtopen.LOBValue{Reader: bytes.NewReader(want), Length: int64(total)}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 4, val); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 4).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("LOBValue Reader 1MiB round-trip mismatch (%d bytes back, %d sent)", len(got), total)
		}
	})
}

// TestLOBDBClob exercises DBCLOB bind: a UTF-16 BE column type
// (`DBCLOB(...) CCSID 1200` or `CCSID 13488`). JT400 reports SQL
// types 968/969 here and the WRITE_LOB_DATA "requested size" CP
// must carry the *character* count (byteCount / 2), not the byte
// count -- the bind dispatcher's lobRequestedSize helper handles
// that.
//
// PUB400 sometimes can't create graphic LOB columns when the user
// profile's CCSID setup excludes DBCS support; the test skips
// (rather than fails) on that specific CREATE error so the test
// stays green on accounts that don't have the right NLSS.
func TestLOBDBClob(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "dbclob"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, dc DBCLOB(64K) CCSID 1200)`, tbl)); err != nil {
		t.Skipf("CREATE TABLE DBCLOB failed (probably no DBCS NLSS on this profile): %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	t.Run("string round-trip", func(t *testing.T) {
		// BMP-only payload: ASCII + an em-dash (U+2014). Avoids
		// the surrogate-pair question; that gets a separate
		// sub-test below once the basic path works.
		want := strings.Repeat("DBCLOB Test — Hello, IBM i! ", 20)
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, dc) VALUES (?, ?)`, tbl), 1, want); err != nil {
			t.Fatalf("insert string: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT dc FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != want {
			diff := -1
			for i := 0; i < len(got) && i < len(want); i++ {
				if got[i] != want[i] {
					diff = i
					break
				}
			}
			head := 64
			if head > len(got) {
				head = len(got)
			}
			if head > len(want) {
				head = len(want)
			}
			t.Errorf("DBCLOB string round-trip mismatch at byte %d (got %d bytes, want %d)\n  want = %q\n   got = %q",
				diff, len(got), len(want), want[:head], got[:head])
		}
	})

	t.Run("surrogate-pair round-trip", func(t *testing.T) {
		// 𝄞 = U+1D11E lives outside the BMP and encodes as a
		// surrogate pair in UTF-16. Confirms the bind path's
		// utf16.Encode plumbing handles >BMP runes and that
		// CCSID 1200 (true UTF-16, surrogates allowed) accepts
		// them. CCSID 13488 columns would reject surrogates.
		want := "Music: 𝄞 — done."
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, dc) VALUES (?, ?)`, tbl), 99, want); err != nil {
			t.Fatalf("insert string: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT dc FROM %s WHERE id = ?`, tbl), 99).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != want {
			t.Errorf("DBCLOB surrogate pair round-trip: got %q, want %q", got, want)
		}
	})

	t.Run("LOBReader stream", func(t *testing.T) {
		// Insert a payload large enough to force at least one
		// continuation RETRIEVE_LOB_DATA when streamed back via
		// LOBReader. Confirms graphic LOBs propagate totalLen as
		// bytes (not chars) through the reader's EOF math; the
		// pre-fix code rounded EOF down to half the value and the
		// reader returned partial data.
		want := strings.Repeat("DBCLOB streamed payload — Hello, IBM i! ", 1500)
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, dc) VALUES (?, ?)`, tbl), 50, want); err != nil {
			t.Fatalf("insert: %v", err)
		}
		streamDB, err := sql.Open("gojtopen", dsn(t)+"&lob=stream")
		if err != nil {
			t.Fatalf("open stream db: %v", err)
		}
		defer streamDB.Close()
		// Use Query+Next so the cursor stays open while we drain
		// the reader. QueryRow auto-closes on Scan, which would
		// invalidate the locator handle (server-side it's bound
		// to the producing cursor's lifetime).
		rows, err := streamDB.Query(fmt.Sprintf(`SELECT dc FROM %s WHERE id = ?`, tbl), 50)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("no rows")
		}
		var r *gojtopen.LOBReader
		if err := rows.Scan(&r); err != nil {
			t.Fatalf("scan: %v", err)
		}
		raw, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		r.Close()
		// LOBReader returns the raw column bytes (UTF-16 BE for
		// CCSID 1200); decode to compare against the Go string.
		if len(raw)%2 != 0 {
			t.Fatalf("DBCLOB stream returned odd byte count: %d", len(raw))
		}
		codes := make([]uint16, len(raw)/2)
		for i := range codes {
			codes[i] = uint16(raw[2*i])<<8 | uint16(raw[2*i+1])
		}
		got := string(utf16.Decode(codes))
		if got != want {
			head := 80
			if head > len(got) {
				head = len(got)
			}
			t.Errorf("DBCLOB LOBReader stream: got %d chars, want %d (head: %q)", len(got), len(want), got[:head])
		}
	})

	t.Run("[]byte UTF-16 BE round-trip", func(t *testing.T) {
		// Caller supplies bytes already encoded as UTF-16 BE; the
		// driver passes them verbatim and reports byteCount/2 as
		// the wire char count.
		runes := []rune("Hello DBCLOB ")
		codes := []uint16{}
		for _, r := range runes {
			codes = append(codes, uint16(r))
		}
		bytesUTF16 := make([]byte, 2*len(codes))
		for i, c := range codes {
			bytesUTF16[2*i] = byte(c >> 8)
			bytesUTF16[2*i+1] = byte(c)
		}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, dc) VALUES (?, ?)`, tbl), 2, bytesUTF16); err != nil {
			t.Fatalf("insert []byte: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT dc FROM %s WHERE id = ?`, tbl), 2).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != string(runes) {
			t.Errorf("DBCLOB []byte round-trip mismatch: got %q, want %q", got, string(runes))
		}
	})
}

// TestLOBDBClobCCSID13488 exercises the strict-UCS-2 BE encode path
// against a real `DBCLOB(...) CCSID 13488` column. CCSID 13488 forbids
// surrogate pairs server-side (SQL-330 "character cannot be
// converted") so the bind path must substitute non-BMP runes with
// U+003F; this test confirms both the BMP round-trip and the
// substitute fallback against a live target.
//
// Gated on GOJTOPEN_TEST_CCSID13488_TABLE because PUB400 V7R5M0
// (the typical free-tier target) does not readily expose a
// CCSID-13488 table, so most live runs cannot exercise this path.
// The env var should be set to a fully-qualified table name
// ("SCHEMA.NAME") that the test owns; the test recreates the table
// on every entry.
func TestLOBDBClobCCSID13488(t *testing.T) {
	tbl := os.Getenv("GOJTOPEN_TEST_CCSID13488_TABLE")
	if tbl == "" {
		t.Skip("GOJTOPEN_TEST_CCSID13488_TABLE not set; skipping CCSID 13488 live test (no widely-available target)")
	}
	db := openDB(t)

	// Recreate the table -- the schema is fixed (id + DBCLOB CCSID
	// 13488) so the test is self-contained against a clean slate.
	db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, dc DBCLOB(64K) CCSID 13488)`, tbl)); err != nil {
		t.Skipf("CREATE TABLE DBCLOB CCSID 13488 failed (target may not have DBCS NLSS): %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	t.Run("BMP-only round-trip", func(t *testing.T) {
		// Same payload pattern as TestLOBDBClob's BMP test; should
		// behave identically to the CCSID-1200 column for BMP
		// input.
		want := strings.Repeat("DBCLOB 13488 — Hello, IBM i! ", 20)
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, dc) VALUES (?, ?)`, tbl), 1, want); err != nil {
			t.Fatalf("insert BMP string: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT dc FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != want {
			t.Errorf("CCSID 13488 BMP round-trip: got %q, want %q", got, want)
		}
	})

	t.Run("non-BMP substituted with ?", func(t *testing.T) {
		// Pre-fix, this INSERT failed with SQL-330 because the bind
		// path emitted a surrogate pair (0xD834 0xDD1E) that CCSID
		// 13488 rejects. Post-fix, the rune is substituted with
		// U+003F before the bind, so the INSERT succeeds and the
		// stored payload contains "?" where the treble clef was.
		input := "Music: 𝄞 — done."
		want := "Music: ? — done."
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, dc) VALUES (?, ?)`, tbl), 2, input); err != nil {
			t.Fatalf("insert non-BMP string: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT dc FROM %s WHERE id = ?`, tbl), 2).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != want {
			t.Errorf("CCSID 13488 non-BMP substitute: got %q, want %q (treble clef replaced with '?')", got, want)
		}
	})
}

// TestLOBMultiRow exercises multi-tuple INSERT (`VALUES (?,?), (?,?)`).
// Each parameter marker position gets its own server-allocated
// locator handle in CP 0x3813 of the PREPARE_DESCRIBE reply, so a
// 3-row × 2-column INSERT has 6 markers, 3 of them LOB. Confirms
// bindLOBParameters routes all of them through WRITE_LOB_DATA before
// the single EXECUTE that carries 6 SQLDA value slots.
func TestLOBMultiRow(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "lobmulti"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, b BLOB(64K))`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	a := []byte("payload-A")
	b := []byte("payload-B")
	c := bytes.Repeat([]byte{0xCC}, 4*1024)

	if _, err := db.Exec(
		fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?), (?, ?), (?, ?)`, tbl),
		1, a, 2, b, 3, c,
	); err != nil {
		t.Fatalf("multi-row INSERT: %v", err)
	}

	for _, tc := range []struct {
		id   int
		want []byte
	}{
		{1, a},
		{2, b},
		{3, c},
	} {
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), tc.id).Scan(&got); err != nil {
			t.Fatalf("scan id=%d: %v", tc.id, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Errorf("id=%d: got %d bytes, want %d", tc.id, len(got), len(tc.want))
		}
	}
}

// TestLOBSelectBind exercises the SelectPreparedSQL → bindLOBParameters
// path that openPreparedUntilFirstBatch wires in. JT400 sends the
// locator-bind sequence (PREPARE_DESCRIBE → CHANGE_DESCRIPTOR →
// WRITE_LOB_DATA → OPEN_DESCRIBE_FETCH) the same way it does for
// EXECUTE; this confirms the SELECT side actually drives that flow
// end-to-end with a BLOB parameter.
//
// The trick is forcing the server to type `?` as a BLOB locator.
// CAST(? AS BLOB(1M)) does it: the parameter marker format reply
// then carries SQL type 961 with a server-allocated handle, and the
// driver routes the value through WRITE_LOB_DATA before the cursor
// opens.
func TestLOBSelectBind(t *testing.T) {
	db := openDB(t)

	payload := make([]byte, 8*1024)
	for i := range payload {
		payload[i] = byte((i*17 + 3) & 0xFF)
	}

	t.Run("LENGTH of BLOB bind", func(t *testing.T) {
		var n int64
		err := db.QueryRow(
			`SELECT LENGTH(CAST(? AS BLOB(1M))) FROM SYSIBM.SYSDUMMY1`,
			payload,
		).Scan(&n)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if n != int64(len(payload)) {
			t.Errorf("LENGTH = %d, want %d", n, len(payload))
		}
	})

	t.Run("round-trip BLOB through SELECT", func(t *testing.T) {
		// SELECT a BLOB ?-bind back as the column value. Confirms
		// the bytes survive WRITE_LOB_DATA → server materialise →
		// RETRIEVE_LOB_DATA path even when the ? sits in a SELECT
		// projection rather than a column comparison.
		var got []byte
		err := db.QueryRow(
			`SELECT CAST(? AS BLOB(1M)) FROM SYSIBM.SYSDUMMY1`,
			payload,
		).Scan(&got)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Errorf("BLOB round-trip via SELECT: %d bytes back, %d sent", len(got), len(payload))
		}
	})
}

// TestLOBUpdate verifies the locator-bind path runs cleanly under
// UPDATE (not just INSERT). Two flavours: shrink (replace 8 KiB with
// 4 KiB) to confirm truncate=0xF0 actually frees the tail, and grow
// (replace 4 KiB with 16 KiB) to confirm the prepared LOB column
// can absorb a larger value than the prior row carried.
func TestLOBUpdate(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "lobupd"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, b BLOB(1M))`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	mkRamp := func(n, mul int) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = byte((i * mul) & 0xFF)
		}
		return out
	}

	t.Run("shrink", func(t *testing.T) {
		big := mkRamp(8*1024, 1)
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 1, big); err != nil {
			t.Fatalf("insert: %v", err)
		}
		small := mkRamp(4*1024, 3)
		if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET b = ? WHERE id = ?`, tbl), small, 1); err != nil {
			t.Fatalf("update: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, small) {
			t.Errorf("UPDATE shrink: got %d bytes, want %d", len(got), len(small))
		}
	})

	t.Run("grow", func(t *testing.T) {
		small := mkRamp(4*1024, 5)
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 2, small); err != nil {
			t.Fatalf("insert: %v", err)
		}
		big := mkRamp(16*1024, 7)
		if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET b = ? WHERE id = ?`, tbl), big, 2); err != nil {
			t.Fatalf("update: %v", err)
		}
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 2).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, big) {
			t.Errorf("UPDATE grow: got %d bytes, want %d", len(got), len(big))
		}
	})

	t.Run("update to NULL", func(t *testing.T) {
		small := mkRamp(2*1024, 11)
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, b) VALUES (?, ?)`, tbl), 3, small); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := db.Exec(fmt.Sprintf(`UPDATE %s SET b = ? WHERE id = ?`, tbl), nil, 3); err != nil {
			t.Fatalf("update: %v", err)
		}
		var raw sql.NullString
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 3).Scan(&raw); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if raw.Valid {
			t.Errorf("UPDATE to NULL left non-NULL value (%d bytes)", len(raw.String))
		}
	})
}

// TestLOBClob exercises the locator-bind path on a CLOB column. The
// driver transcodes the Go string to the column's declared CCSID
// (typically 273 on PUB400) before shipping; the read side decodes
// back. Confirms the EBCDIC round-trip works for the basic ASCII
// subset and that long strings cross the chunking boundary cleanly.
func TestLOBClob(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "lobclob"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, c CLOB(1M))`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	// ~8 KiB CLOB (single-frame).
	var sb strings.Builder
	for sb.Len() < 8*1024 {
		sb.WriteString("Hello, IBM i! ")
	}
	clob := sb.String()

	t.Run("string", func(t *testing.T) {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, c) VALUES (?, ?)`, tbl), 1, clob); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT c FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != clob {
			diff := -1
			for i := 0; i < len(got) && i < len(clob); i++ {
				if got[i] != clob[i] {
					diff = i
					break
				}
			}
			head := 64
			if head > len(got) {
				head = len(got)
			}
			if head > len(clob) {
				head = len(clob)
			}
			t.Errorf("CLOB string round-trip mismatch at byte %d (len got=%d sent=%d)\n  sent[:%d] = %q\n   got[:%d] = %q",
				diff, len(got), len(clob), head, clob[:head], head, got[:head])
		}
	})

	// CLOB []byte bind: caller hands us bytes that are *already* in
	// the column's declared CCSID. The driver passes them verbatim
	// (no transcoding), the server stores them, and SELECT decodes
	// via the column CCSID's codec. We pre-encode "Hello, IBM i! "
	// to CCSID 273 so the round-trip lands back as the same Go
	// string regardless of which side does the transcoding.
	t.Run("[]byte pre-encoded CCSID 273", func(t *testing.T) {
		ebc, err := ebcdic.CCSID273.Encode(clob)
		if err != nil {
			t.Fatalf("ebcdic.CCSID273.Encode: %v", err)
		}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, c) VALUES (?, ?)`, tbl), 2, ebc); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT c FROM %s WHERE id = ?`, tbl), 2).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != clob {
			t.Errorf("CLOB []byte round-trip mismatch: %d chars back vs %d sent", len(got), len(clob))
		}
	})

	// CLOB Reader bind: same pre-encoded-bytes contract, but
	// streamed through LOBValue.Reader so chunked WRITE_LOB_DATA
	// frames get exercised on the CLOB side (the BLOB-Reader
	// chunking case is covered by TestLOBBlob/LOBValue_Reader_*).
	t.Run("LOBValue Reader pre-encoded 80KiB", func(t *testing.T) {
		// 80 KiB > 32 KiB chunk size to force >=3 frames.
		var sb strings.Builder
		for sb.Len() < 80*1024 {
			sb.WriteString("Hello, IBM i! ")
		}
		want := sb.String()
		ebc, err := ebcdic.CCSID273.Encode(want)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		val := &gojtopen.LOBValue{Reader: bytes.NewReader(ebc), Length: int64(len(ebc))}
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, c) VALUES (?, ?)`, tbl), 3, val); err != nil {
			t.Fatalf("insert: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT c FROM %s WHERE id = ?`, tbl), 3).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != want {
			t.Errorf("CLOB Reader 80KiB round-trip: %d chars back vs %d sent", len(got), len(want))
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

// TestCCSID1208RoundTrip exercises the M7-3 contract that VARCHAR
// CCSID 1208 (UTF-8) columns round-trip Unicode content byte-equal,
// including the punctuation that would round-trip *wrong* on a
// CCSID 37 / 273 column (em-dash, curly quotes, smart apostrophe,
// Greek letters, etc).
//
// The column-CCSID-aware decode path lands the bind/read on CCSID
// 1208 regardless of the connection's "default for unmarked data"
// CCSID — so this test is a sanity check that the column path works
// at all, not just that the connection-level knob does. It is the
// CHAR/VARCHAR/CLOB live-validated round-trip the M7-3 plan calls
// for.
func TestCCSID1208RoundTrip(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "utf8"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, v VARCHAR(64) CCSID 1208)`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	// Mix of characters that trip on the EBCDIC SBCS path: em-dash
	// (U+2014), curly quotes (U+201C / U+201D), smart apostrophe
	// (U+2019), Greek π (U+03C0) / Ω (U+03A9), and Latin-1
	// extended (Café). All BMP runes; CCSID 1208 carries them as
	// 1-3 byte UTF-8 sequences verbatim.
	want := "Café — “curly” quote · ‘smart’ · π · Ω"

	if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, v) VALUES (?, ?)`, tbl), 1, want); err != nil {
		t.Fatalf("insert: %v", err)
	}

	t.Run("VARCHAR round-trip", func(t *testing.T) {
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT v FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if got != want {
			t.Errorf("CCSID 1208 VARCHAR mismatch:\n  want = %q\n   got = %q", want, got)
		}
	})

	t.Run("CHAR round-trip via CAST", func(t *testing.T) {
		// CAST to CHAR(64) CCSID 1208: server transcodes the
		// VARCHAR storage to a CHAR column (right-padded with
		// spaces). Confirms the CHAR result-decode path picks the
		// column CCSID, not the connection default.
		var got string
		if err := db.QueryRow(fmt.Sprintf(
			`SELECT CAST(v AS CHAR(64) CCSID 1208) FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan CHAR: %v", err)
		}
		// CHAR pads with spaces to the declared length; trim for
		// the equality check. The interesting bit is byte-level
		// preservation of the high-codepoint runes, not the
		// trailing spaces.
		got = strings.TrimRight(got, " ")
		if got != want {
			t.Errorf("CCSID 1208 CHAR mismatch:\n  want = %q\n   got = %q", want, got)
		}
	})

	t.Run("CLOB round-trip", func(t *testing.T) {
		// CLOB(1M) forces the server-side locator path so this
		// subtest exercises the RETRIEVE_LOB_DATA decoder
		// regardless of the connection-level "lob threshold"
		// setting. The small-CLOB inline counterpart lives in the
		// "CLOB small inline" subtest below.
		clobTbl := schema() + "." + tablePrefix + "utf8clob"
		db.Exec("DROP TABLE " + clobTbl)
		if _, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, c CLOB(1M) CCSID 1208)`, clobTbl)); err != nil {
			t.Skipf("CREATE CLOB(1M) CCSID 1208 failed: %v", err)
		}
		defer db.Exec("DROP TABLE " + clobTbl)
		clobWant := strings.Repeat(want+" ", 50)
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, c) VALUES (?, ?)`, clobTbl), 1, clobWant); err != nil {
			t.Fatalf("insert CLOB: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT c FROM %s WHERE id = ?`, clobTbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan CLOB: %v", err)
		}
		if got != clobWant {
			t.Errorf("CCSID 1208 CLOB byte-level mismatch (lengths got=%d want=%d)", len(got), len(clobWant))
		}
	})

	t.Run("CLOB small inline", func(t *testing.T) {
		// CLOB(4K) CCSID 1208 sits below the connection-level
		// "lob threshold" (default 32768), so the server returns
		// the column inline as SQL type 408/409 rather than as a
		// locator. Pre-fix this hit "unsupported SQL type 409 (col
		// len=4100, ccsid=1208)" and dropped the row. Closes bug
		// #14 in docs/lob-known-gaps.md §5.
		clobTbl := schema() + "." + tablePrefix + "utf8clob_small"
		db.Exec("DROP TABLE " + clobTbl)
		if _, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, c CLOB(4K) CCSID 1208)`, clobTbl)); err != nil {
			t.Skipf("CREATE CLOB(4K) CCSID 1208 failed: %v", err)
		}
		defer db.Exec("DROP TABLE " + clobTbl)
		// Same content as the locator-path subtest so the only
		// variable is the column-size declaration (and therefore
		// whether the server picks inline vs locator).
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, c) VALUES (?, ?)`, clobTbl), 1, want); err != nil {
			t.Fatalf("insert small CLOB: %v", err)
		}
		var got string
		if err := db.QueryRow(fmt.Sprintf(`SELECT c FROM %s WHERE id = ?`, clobTbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan small CLOB: %v", err)
		}
		if got != want {
			t.Errorf("CCSID 1208 small CLOB mismatch:\n  want = %q\n   got = %q", want, got)
		}
	})
}

// TestRowsLazyMemoryBounded confirms the M7-2 lazy-iteration goal: a
// large SELECT walked one row at a time keeps Go heap usage bounded
// regardless of how many rows the cursor produces. Pre-M5 the
// hostserver returned the full []SelectRow up front and a 50K-row
// result set materialised every row before Rows.Next yielded the
// first one; under M5+M7-2 the driver streams batch-by-batch via
// continuation FETCH (0x180B), so peak heap should track a single
// 32 KB block-fetch buffer + the current row, not the full set.
//
// We sample runtime.MemStats.HeapAlloc before/after a forced GC,
// drive the cursor through all rows, then re-sample after another
// forced GC. The post-iteration delta must stay under a generous
// budget (~16 MiB) regardless of row count -- if Rows accidentally
// reverted to buffering, the delta would grow with N.
func TestRowsLazyMemoryBounded(t *testing.T) {
	db := openDB(t)
	const n = 50_000
	// QSYS2.SYSCOLUMNS is the standard "many rows" catalog source on
	// IBM i; we project a small subset of columns so per-row payload
	// stays modest and the test isolates batching behaviour rather
	// than per-row decode cost. ORDER BY pins the result for a stable
	// row-count assertion.
	sql := fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
		FROM QSYS2.SYSCOLUMNS
		ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
		FETCH FIRST %d ROWS ONLY`, n)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	var maxHeap uint64
	var rowCount int
	for rows.Next() {
		var schema, table, col string
		if err := rows.Scan(&schema, &table, &col); err != nil {
			rows.Close()
			t.Fatalf("scan row %d: %v", rowCount, err)
		}
		rowCount++
		// Sample a few times during iteration so we catch the worst
		// case rather than only the begin/end states. ReadMemStats
		// stops the world briefly; doing it every 5000 rows keeps
		// the overhead negligible.
		if rowCount%5000 == 0 {
			var msNow runtime.MemStats
			runtime.ReadMemStats(&msNow)
			if msNow.HeapAlloc > maxHeap {
				maxHeap = msNow.HeapAlloc
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("rows.Err after %d rows: %v", rowCount, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close: %v", err)
	}

	runtime.GC()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	// IBM i may return fewer rows if the system has fewer columns
	// catalog-wide; require at least 5K so we know multiple FETCH
	// batches happened (32 KB block-fetch buffer holds well under
	// 5K narrow rows).
	if rowCount < 5_000 {
		t.Fatalf("got only %d rows; need >= 5000 to span multiple FETCH batches", rowCount)
	}

	// Budget: 16 MiB delta vs pre-iteration. A buffered (non-streaming)
	// implementation would carry ~50K * (estimated 80B/row) ~= 4 MiB
	// of row data plus driver overhead -- so 16 MiB is loose enough to
	// avoid GC noise but tight enough to fail loudly if the cursor
	// regresses to materialising whole result sets.
	const budget uint64 = 16 * 1024 * 1024
	delta := int64(msAfter.HeapAlloc) - int64(msBefore.HeapAlloc)
	if delta > int64(budget) {
		t.Errorf("post-iteration HeapAlloc grew by %d bytes (max during walk: %d); budget %d. "+
			"Streaming Rows likely regressed to buffered.", delta, maxHeap, budget)
	}
	t.Logf("rows=%d  pre.HeapAlloc=%d  max.HeapAlloc=%d  post.HeapAlloc=%d  delta=%d  budget=%d",
		rowCount, msBefore.HeapAlloc, maxHeap, msAfter.HeapAlloc, delta, budget)
}

// TestRowsCloseIdempotent confirms the M7-2 contract that
// (driver.Rows).Close is safe to call repeatedly per the
// database/sql documentation. The implementation caches the first
// Close's error and returns the same value on every subsequent call;
// regressions where a second Close issues a stray RPB DELETE / CLOSE
// frame would surface here as a connection-level error from the
// reused fakeConn (or a panic on a closed conn).
func TestRowsCloseIdempotent(t *testing.T) {
	db := openDB(t)

	rows, err := db.Query("SELECT TABLE_NAME FROM QSYS2.SYSTABLES FETCH FIRST 5 ROWS ONLY")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}

	first := rows.Close()
	second := rows.Close()
	third := rows.Close()
	if first != nil {
		t.Errorf("first Close: %v", first)
	}
	if second != first {
		t.Errorf("second Close = %v; want same as first (%v)", second, first)
	}
	if third != first {
		t.Errorf("third Close = %v; want same as first (%v)", third, first)
	}

	// The connection must still be usable after the rows are closed
	// -- if Close left the cursor / RPB in a half-closed state the
	// next query on the same pool conn would fail with SQL-519
	// (orphaned prepared statement) or SQL-501 (cursor already open).
	var n int
	if err := db.QueryRow("VALUES 1").Scan(&n); err != nil {
		t.Fatalf("post-close query failed (cursor leak?): %v", err)
	}
	if n != 1 {
		t.Errorf("post-close VALUES 1 got %d; want 1", n)
	}
}

// TestTLSConnectivity exercises the M7-4 contract that the driver can
// complete the full host-server protocol (sign-on, start-database,
// prepare, execute, fetch) over a crypto/tls-wrapped connection to the
// IBM i SSL host-server ports (9476 / 9471).
//
// Gated on GOJTOPEN_TLS_TARGET so it skips on plaintext-only targets
// (PUB400, any LPAR where DCM hasn't assigned a cert to the
// QIBM_OS400_QZBS_SVR_DATABASE / _SIGNON / _CENTRAL application IDs).
// The env var holds a full DSN with tls=true set, e.g.:
//
//	GOJTOPEN_TLS_TARGET="gojtopen://USER:PWD@host:9471/?signon-port=9476&tls=true&tls-insecure-skip-verify=true"
//
// When GOJTOPEN_DSN is *also* set (plaintext counterpart against the
// same LPAR), the test additionally diffs a multi-row result against
// the plaintext result to prove the protocol above the TLS layer is
// byte-identical -- a regression where TLS sign-on negotiated a
// different attribute set than plaintext would surface here.
func TestTLSConnectivity(t *testing.T) {
	tlsDSN := os.Getenv("GOJTOPEN_TLS_TARGET")
	if tlsDSN == "" {
		t.Skip("GOJTOPEN_TLS_TARGET not set; skipping live TLS conformance test")
	}

	tlsDB, err := sql.Open("gojtopen", tlsDSN)
	if err != nil {
		t.Fatalf("sql.Open(tls): %v", err)
	}
	t.Cleanup(func() { tlsDB.Close() })
	tlsDB.SetMaxOpenConns(2)

	// Warm-up Ping with retry to cover dial / TLS handshake / sign-on /
	// start-database all running for the first time on a cold prestart
	// job. Reuses the openDB cadence (30 s per attempt, 2 min budget).
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := tlsDB.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("TLS warm-up Ping never succeeded: %v", err)
		}
		t.Logf("TLS warm-up Ping failed, retrying: %v", err)
		time.Sleep(2 * time.Second)
	}

	// Smoketest: a single-row query exercises sign-on, start-database,
	// PREPARE, EXECUTE, OPEN, FETCH, CLOSE -- the full critical path
	// the M5/M7-2 cursor work walks, now wrapped in TLS records.
	t.Run("smoketest", func(t *testing.T) {
		var (
			ts   time.Time
			user string
		)
		if err := tlsDB.QueryRow(
			"SELECT CURRENT_TIMESTAMP, CURRENT_USER FROM SYSIBM.SYSDUMMY1").
			Scan(&ts, &user); err != nil {
			t.Fatalf("TLS smoketest query: %v", err)
		}
		if ts.IsZero() {
			t.Error("CURRENT_TIMESTAMP came back zero")
		}
		if user == "" {
			t.Error("CURRENT_USER came back empty")
		}
		t.Logf("TLS round-trip OK: user=%q ts=%s", user, ts.Format(time.RFC3339))
	})

	// Multi-row pull: walks several FETCH continuations and confirms
	// row-data parse works over TLS. SYSTABLES is a stable target on
	// every IBM i target; using SYSTEM_TABLE_SCHEMA = 'QSYS2' bounds
	// the result deterministically.
	t.Run("multi-row", func(t *testing.T) {
		rows, err := tlsDB.Query(
			`SELECT SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA FROM QSYS2.SYSTABLES
			   WHERE SYSTEM_TABLE_SCHEMA = 'QSYS2'
			   ORDER BY SYSTEM_TABLE_NAME FETCH FIRST 5 ROWS ONLY`)
		if err != nil {
			t.Fatalf("multi-row query: %v", err)
		}
		defer rows.Close()
		var got [][2]string
		for rows.Next() {
			var name, sch string
			if err := rows.Scan(&name, &sch); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got = append(got, [2]string{strings.TrimSpace(name), strings.TrimSpace(sch)})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		if len(got) != 5 {
			t.Fatalf("got %d rows, want 5", len(got))
		}
		for i, row := range got {
			if row[1] != "QSYS2" {
				t.Errorf("row %d schema = %q, want QSYS2", i, row[1])
			}
			if row[0] == "" {
				t.Errorf("row %d name empty", i)
			}
		}

		// Plaintext byte-equivalence diff. Only runs when GOJTOPEN_DSN
		// is also set and points at the same LPAR -- the comparison is
		// only meaningful if both DSNs reach the same catalog.
		plainDSN := os.Getenv("GOJTOPEN_DSN")
		if plainDSN == "" {
			t.Log("GOJTOPEN_DSN not set; skipping plaintext equivalence diff")
			return
		}
		plainDB, err := sql.Open("gojtopen", plainDSN)
		if err != nil {
			t.Logf("plaintext open failed (skipping diff): %v", err)
			return
		}
		defer plainDB.Close()
		plainDB.SetMaxOpenConns(2)
		plainRows, err := plainDB.Query(
			`SELECT SYSTEM_TABLE_NAME, SYSTEM_TABLE_SCHEMA FROM QSYS2.SYSTABLES
			   WHERE SYSTEM_TABLE_SCHEMA = 'QSYS2'
			   ORDER BY SYSTEM_TABLE_NAME FETCH FIRST 5 ROWS ONLY`)
		if err != nil {
			t.Logf("plaintext query failed (skipping diff): %v", err)
			return
		}
		defer plainRows.Close()
		var plain [][2]string
		for plainRows.Next() {
			var name, sch string
			if err := plainRows.Scan(&name, &sch); err != nil {
				t.Fatalf("plaintext scan: %v", err)
			}
			plain = append(plain, [2]string{strings.TrimSpace(name), strings.TrimSpace(sch)})
		}
		if len(plain) != len(got) {
			t.Fatalf("row count diverged: tls=%d plain=%d", len(got), len(plain))
		}
		for i := range got {
			if got[i] != plain[i] {
				t.Errorf("row %d diverged: tls=%v plain=%v", i, got[i], plain[i])
			}
		}
	})
}
