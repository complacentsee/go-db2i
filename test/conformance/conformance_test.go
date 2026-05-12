//go:build conformance

// Package conformance ports bradfitz/go-sql-test's database/sql
// integration tests onto go-db2i. Gated by a build tag because they
// require a live IBM i target -- see the package doc for env vars.
//
// Run with:
//
//	DB2I_DSN="db2i://USER:PWD@host:8471/?library=GOSQLTEST" \
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
	"database/sql/driver"
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

	db2i "github.com/complacentsee/go-db2i/driver"
	"github.com/complacentsee/go-db2i/ebcdic"
)

// dsn returns the connection string from DB2I_DSN, skipping the
// test if not set. Tests that mutate schema/data drop their own
// tables on entry to keep the target idempotent.
func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("DB2I_DSN")
	if v == "" {
		t.Skip("DB2I_DSN not set; skipping live conformance test")
	}
	return v
}

// schema returns the schema name to use for test tables; defaults to
// GOTEST. Override via DB2I_SCHEMA if your library list differs.
func schema() string {
	if v := os.Getenv("DB2I_SCHEMA"); v != "" {
		return v
	}
	return "GOTEST"
}

const tablePrefix = "GOSQL_"

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("db2i", dsn(t))
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
		val := &db2i.LOBValue{Bytes: small}
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
		val := &db2i.LOBValue{Reader: bytes.NewReader(want), Length: int64(total)}
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
		val := &db2i.LOBValue{Bytes: []byte{}}
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
		val := &db2i.LOBValue{Reader: bytes.NewReader(nil), Length: 0}
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
		var val *db2i.LOBValue
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
		val := &db2i.LOBValue{Reader: bytes.NewReader(want), Length: int64(total)}
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
		streamDB, err := sql.Open("db2i", dsn(t)+"&lob=stream")
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
		var r *db2i.LOBReader
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
// Gated on DB2I_TEST_CCSID13488_TABLE because PUB400 V7R5M0
// (the typical free-tier target) does not readily expose a
// CCSID-13488 table, so most live runs cannot exercise this path.
// The env var should be set to a fully-qualified table name
// ("SCHEMA.NAME") that the test owns; the test recreates the table
// on every entry.
func TestLOBDBClobCCSID13488(t *testing.T) {
	tbl := os.Getenv("DB2I_TEST_CCSID13488_TABLE")
	if tbl == "" {
		t.Skip("DB2I_TEST_CCSID13488_TABLE not set; skipping CCSID 13488 live test (no widely-available target)")
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
// (273 here, declared explicitly) before shipping; the read side
// decodes back. Confirms the EBCDIC round-trip works for the basic
// ASCII subset and that long strings cross the chunking boundary
// cleanly.
//
// The column CCSID is pinned to 273 because the []byte-bind subtests
// pre-encode their payload via ebcdic.CCSID273.Encode -- if the
// column inherited the job-default CCSID (e.g. 37 on an English
// LPAR), the `!` character would round-trip as `|` (0x4F in 273
// decodes to `|` in 37). Declaring the column CCSID at table-create
// time keeps the test deterministic across LPAR job locales.
func TestLOBClob(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "lobclob"
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, c CLOB(1M) CCSID 273)`, tbl)); err != nil {
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
		val := &db2i.LOBValue{Reader: bytes.NewReader(ebc), Length: int64(len(ebc))}
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
// Bump DB2I_MANY_QUERY_ROW_N for stress testing.
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
	if v := os.Getenv("DB2I_MANY_QUERY_ROW_N"); v != "" {
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
		// SQL-7008 / SQLSTATE 55019: object not journaled and
		// cannot participate in commitment control. Standard
		// outcome on PUB400's shared free-tier libraries (no
		// STRJRN authority for end-users). The transaction
		// machinery is correctly engaged; the schema just can't
		// support it. Skip cleanly.
		if strings.Contains(err.Error(), "SQL-7008") ||
			strings.Contains(err.Error(), "55019") {
			t.Skipf("schema %s is not journaled; transactions require journaling: %v", schema(), err)
		}
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
// Gated on DB2I_TLS_TARGET so it skips on plaintext-only targets
// (PUB400, any LPAR where DCM hasn't assigned a cert to the
// QIBM_OS400_QZBS_SVR_DATABASE / _SIGNON / _CENTRAL application IDs).
// The env var holds a full DSN with tls=true set, e.g.:
//
//	DB2I_TLS_TARGET="db2i://USER:PWD@host:9471/?signon-port=9476&tls=true&tls-insecure-skip-verify=true"
//
// When DB2I_DSN is *also* set (plaintext counterpart against the
// same LPAR), the test additionally diffs a multi-row result against
// the plaintext result to prove the protocol above the TLS layer is
// byte-identical -- a regression where TLS sign-on negotiated a
// different attribute set than plaintext would surface here.
func TestTLSConnectivity(t *testing.T) {
	tlsDSN := os.Getenv("DB2I_TLS_TARGET")
	if tlsDSN == "" {
		t.Skip("DB2I_TLS_TARGET not set; skipping live TLS conformance test")
	}

	tlsDB, err := sql.Open("db2i", tlsDSN)
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

		// Plaintext byte-equivalence diff. Only runs when DB2I_DSN
		// is also set and points at the same LPAR -- the comparison is
		// only meaningful if both DSNs reach the same catalog.
		plainDSN := os.Getenv("DB2I_DSN")
		if plainDSN == "" {
			t.Log("DB2I_DSN not set; skipping plaintext equivalence diff")
			return
		}
		plainDB, err := sql.Open("db2i", plainDSN)
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

// TestExtendedMetadata exercises the M4 ?extended-metadata=true
// knob: when set, the driver requests CP 0x3811 from the server
// and surfaces per-column schema name, base table name, and base
// column name through go-db2i-specific Rows accessors. Closes the
// M4 "deferred: schema/table column metadata" gap from PLAN.md.
//
// Without the flag the same accessors return empty strings (the
// pre-flag wire shape stays byte-identical to historic captures).
// The test compares both modes against the same SELECT to pin the
// flag's effect.
//
// Reaches the driver-level Rows via sql.Conn.Raw + driver.Stmt
// directly because database/sql.Rows hides driver methods that
// aren't part of its frozen interface set. Bracketed by ConnRaw
// so the underlying go-db2i connection is the same one that
// supplied the driver.Rows.
func TestExtendedMetadata(t *testing.T) {
	tbl := schema() + "." + tablePrefix + "extmeta"
	tblShort := tablePrefix + "extmeta"

	setupDB := openDB(t)
	setupDB.Exec("DROP TABLE " + tbl)
	if _, err := setupDB.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(64))`, tbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
	defer setupDB.Exec("DROP TABLE " + tbl)
	if _, err := setupDB.Exec(fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (?, ?)`, tbl), 1, "alice"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Open a second DB with extended-metadata=true so we can compare
	// the same SELECT against the same row through both paths.
	extDSN := dsn(t)
	if strings.Contains(extDSN, "extended-metadata=") {
		t.Skip("DSN already sets extended-metadata; can't toggle deterministically")
	}
	sep := "?"
	if strings.Contains(extDSN, "?") {
		sep = "&"
	}
	extDB, err := sql.Open("db2i", extDSN+sep+"extended-metadata=true")
	if err != nil {
		t.Fatalf("sql.Open(ext): %v", err)
	}
	t.Cleanup(func() { extDB.Close() })

	query := fmt.Sprintf("SELECT id, name FROM %s WHERE id = 1", tbl)

	t.Run("flag off: schema and table empty", func(t *testing.T) {
		got := selectFirstColumnMetadata(t, setupDB, query)
		for i, m := range got {
			if m.schema != "" {
				t.Errorf("col %d Schema = %q without flag, want empty", i, m.schema)
			}
			if m.table != "" {
				t.Errorf("col %d Table = %q without flag, want empty", i, m.table)
			}
			if m.base != "" {
				t.Errorf("col %d BaseColumnName = %q without flag, want empty", i, m.base)
			}
		}
	})

	t.Run("flag on: schema and table populate", func(t *testing.T) {
		got := selectFirstColumnMetadata(t, extDB, query)
		wantTable := strings.ToUpper(tblShort)
		for i, want := range []struct{ schema, table, base string }{
			{schema(), wantTable, "ID"},
			{schema(), wantTable, "NAME"},
		} {
			if got[i].schema != want.schema {
				t.Errorf("col %d Schema = %q, want %q", i, got[i].schema, want.schema)
			}
			if got[i].table != want.table {
				t.Errorf("col %d Table = %q, want %q", i, got[i].table, want.table)
			}
			if got[i].base != want.base {
				t.Errorf("col %d BaseColumnName = %q, want %q", i, got[i].base, want.base)
			}
		}
	})
}

type extColMeta struct {
	schema, table, base string
}

// selectFirstColumnMetadata reaches the driver-level Rows via
// sql.Conn.Raw and pulls the extended-metadata fields off each
// column. The bridge sidesteps database/sql.Rows hiding driver-
// specific methods: Raw hands us the underlying driver.Conn, and
// from there we drive driver.Conn.Prepare + Stmt.Query directly.
func selectFirstColumnMetadata(t *testing.T, db *sql.DB, query string) []extColMeta {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Conn: %v", err)
	}
	defer c.Close()

	var out []extColMeta
	if err := c.Raw(func(rawConn any) error {
		dc, ok := rawConn.(driver.Conn)
		if !ok {
			t.Fatalf("raw conn type %T does not implement driver.Conn", rawConn)
		}
		stmt, err := dc.Prepare(query)
		if err != nil {
			return fmt.Errorf("driver Prepare: %w", err)
		}
		defer stmt.Close()
		queryer, ok := stmt.(driver.StmtQueryContext)
		if !ok {
			return errors.New("driver Stmt does not implement StmtQueryContext")
		}
		rows, err := queryer.QueryContext(ctx, nil)
		if err != nil {
			return fmt.Errorf("driver Query: %w", err)
		}
		defer rows.Close()
		mr, ok := rows.(interface {
			ColumnTypeSchemaName(int) string
			ColumnTypeTableName(int) string
			ColumnTypeBaseColumnName(int) string
		})
		if !ok {
			return errors.New("driver Rows missing extended-metadata accessors")
		}
		ncols := len(rows.Columns())
		out = make([]extColMeta, ncols)
		for i := 0; i < ncols; i++ {
			out[i] = extColMeta{
				schema: mr.ColumnTypeSchemaName(i),
				table:  mr.ColumnTypeTableName(i),
				base:   mr.ColumnTypeBaseColumnName(i),
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Raw: %v", err)
	}
	return out
}

// TestBinaryTypeRoundTrip exercises the three CCSID-65535 binary
// flavours on V7R3+: CHAR FOR BIT DATA (SQL types 452/453 +
// CCSID 65535), the native BINARY type (912/913), and the native
// VARBINARY type (908/909). Each round-trips a deterministic byte
// pattern and asserts byte-equality on the SELECT-back side.
//
// Closes the M4 "deferred: CCSID 65535 binary handling" gap from
// PLAN.md -- the decode path was always wired but had no live
// regression test. The TestBlobs case covers VARCHAR FOR BIT DATA
// (449 + CCSID 65535); this test adds the remaining three.
func TestBinaryTypeRoundTrip(t *testing.T) {
	db := openDB(t)
	tbl := schema() + "." + tablePrefix + "binary"
	db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, c CHAR(8) FOR BIT DATA, b BINARY(8), v VARBINARY(32))`, tbl)); err != nil {
		t.Skipf("CREATE TABLE with BINARY / VARBINARY failed (pre-V7R3?): %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	wantC := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	wantB := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11}
	wantV := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}

	if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, c, b, v) VALUES (?, ?, ?, ?)`, tbl),
		1, wantC, wantB, wantV); err != nil {
		t.Fatalf("insert: %v", err)
	}

	t.Run("CHAR FOR BIT DATA", func(t *testing.T) {
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT c FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, wantC) {
			t.Errorf("CHAR FOR BIT DATA mismatch: got %x, want %x", got, wantC)
		}
	})

	t.Run("BINARY", func(t *testing.T) {
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT b FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, wantB) {
			t.Errorf("BINARY mismatch: got %x, want %x", got, wantB)
		}
	})

	t.Run("VARBINARY", func(t *testing.T) {
		var got []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT v FROM %s WHERE id = ?`, tbl), 1).Scan(&got); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(got, wantV) {
			t.Errorf("VARBINARY mismatch: got %x, want %x", got, wantV)
		}
	})

	t.Run("all three in one row", func(t *testing.T) {
		// Multi-column read exercises the row-stride accounting: a
		// regression where the BINARY-fixed-length decoder advanced
		// by the wrong byte count would shift VARBINARY by 8 bytes
		// and surface as a content mismatch on `v`.
		var c, b, v []byte
		if err := db.QueryRow(fmt.Sprintf(`SELECT c, b, v FROM %s WHERE id = ?`, tbl), 1).Scan(&c, &b, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !bytes.Equal(c, wantC) || !bytes.Equal(b, wantB) || !bytes.Equal(v, wantV) {
			t.Errorf("multi-column shift detected:\n  c=%x want=%x\n  b=%x want=%x\n  v=%x want=%x",
				c, wantC, b, wantB, v, wantV)
		}
	})
}

// TestBooleanRoundTrip exercises V7R5+ native BOOLEAN columns
// (SQL types 2436 NN / 2437 nullable). The driver binds Go bool as
// SMALLINT(1) (the standard substitute IBM Db2 for i coerces into
// BOOLEAN server-side) and decodes BOOLEAN result columns via the
// 1-byte wire form (0xF0 = false, anything else = true) that
// mirrors JT400's `SQLBoolean.convertFromRawBytes`.
//
// Closes the M3 "deferred: bool parameter binding" gap from PLAN.md.
// Skips automatically if the LPAR doesn't accept `CREATE TABLE ...
// (flag BOOLEAN)` so the suite stays green on V7R4 and earlier.
func TestBooleanRoundTrip(t *testing.T) {
	db := openDB(t)
	tbl := schema() + "." + tablePrefix + "bool"
	db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, flag BOOLEAN)`, tbl)); err != nil {
		t.Skipf("CREATE TABLE ... BOOLEAN failed (V7R4 or earlier?): %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	// Four rows so any bit-position swap in the row-stride layout
	// (e.g. shifting between SQLDA slots) would diverge from the
	// expected sequence.
	cases := []struct {
		id   int
		want bool
	}{{1, true}, {2, false}, {3, true}, {4, false}}
	for _, c := range cases {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, flag) VALUES (?, ?)`, tbl), c.id, c.want); err != nil {
			t.Fatalf("insert id=%d flag=%v: %v", c.id, c.want, err)
		}
	}

	t.Run("scalar via parameter marker", func(t *testing.T) {
		for _, c := range cases {
			var got bool
			if err := db.QueryRow(fmt.Sprintf(`SELECT flag FROM %s WHERE id = ?`, tbl), c.id).Scan(&got); err != nil {
				t.Fatalf("scan id=%d: %v", c.id, err)
			}
			if got != c.want {
				t.Errorf("id=%d flag = %v, want %v", c.id, got, c.want)
			}
		}
	})

	t.Run("full pull preserves order", func(t *testing.T) {
		rows, err := db.Query(fmt.Sprintf(`SELECT id, flag FROM %s ORDER BY id`, tbl))
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var got []struct {
			id   int
			flag bool
		}
		for rows.Next() {
			var r struct {
				id   int
				flag bool
			}
			if err := rows.Scan(&r.id, &r.flag); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got = append(got, r)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		if len(got) != len(cases) {
			t.Fatalf("got %d rows, want %d", len(got), len(cases))
		}
		for i, c := range cases {
			if got[i].id != c.id || got[i].flag != c.want {
				t.Errorf("row %d: got id=%d flag=%v, want id=%d flag=%v",
					i, got[i].id, got[i].flag, c.id, c.want)
			}
		}
	})
}

// ----- M9 stored-procedure tests -----

// procLibrary is the dedicated library hosting all M9 fixture procs
// (P_INS / P_LOOKUP / P_INVENTORY / P_ROUNDTRIP) plus their supporting
// tables. Tied to the test schema bootstrap in setUpStoredProcs --
// hardcoded here to keep the conformance suite self-contained (no env
// override) since the M9 fixtures captured against the same name and
// the offline replay tests reference it verbatim.
const procLibrary = "GOSPROCS"

// setUpStoredProcs creates the GOSPROCS library, supporting tables,
// and the four stored procedures the M9 tests exercise. Idempotent:
// safe to call multiple times across test runs. CREATE OR REPLACE
// rebuilds the procedure bodies on each run; the supporting tables
// are dropped + recreated so the seed data is deterministic.
//
// Matches the WithStoredProcs setup() in the Java fixture harness
// (testdata/jtopen-fixtures/src/.../Cases.java) so the offline
// fixture replays and the live tests reference the same schema.
func setUpStoredProcs(t *testing.T, db *sql.DB) {
	t.Helper()

	// CREATE SCHEMA -- ignore SQLSTATE 42710 (already exists). DB2
	// for i has no CREATE SCHEMA IF NOT EXISTS. SQLSTATE 42502
	// (insufficient authority -- common on PUB400's shared free-
	// tier, where users get a pre-created personal library but no
	// authority to make more) skips the test cleanly instead of
	// failing it; the test doesn't apply to that environment.
	if _, err := db.Exec("CREATE SCHEMA " + procLibrary); err != nil {
		if strings.Contains(err.Error(), "42502") ||
			strings.Contains(err.Error(), "SQL-552") {
			t.Skipf("no authority to create schema %s: %v", procLibrary, err)
		}
		if !strings.Contains(err.Error(), "42710") &&
			!strings.Contains(err.Error(), "already exists") {
			t.Fatalf("CREATE SCHEMA %s: %v", procLibrary, err)
		}
	}

	for _, tbl := range []string{"INS_AUDIT", "WIDGETS", "INVENTORY"} {
		// DDL: object name is a SQL identifier; cannot be a bind
		// parameter. Constant + hardcoded loop value, no injection
		// surface here.
		_, _ = db.Exec("DROP TABLE " + procLibrary + "." + tbl)
	}
	mustExec := func(sqlText string) {
		t.Helper()
		if _, err := db.Exec(sqlText); err != nil {
			t.Fatalf("%s: %v", sqlText, err)
		}
	}
	mustExecArgs := func(sqlText string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(sqlText, args...); err != nil {
			t.Fatalf("%s: %v", sqlText, err)
		}
	}
	// DDL: column types / defaults / procedure bodies aren't bindable;
	// these statements are all interpolated identifiers + hardcoded
	// constants.
	mustExec("CREATE TABLE " + procLibrary + ".INS_AUDIT (" +
		"CODE VARCHAR(10), QTY INTEGER, " +
		"INSERTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	mustExec("CREATE TABLE " + procLibrary + ".WIDGETS (" +
		"CODE VARCHAR(10) NOT NULL PRIMARY KEY, " +
		"NAME VARCHAR(64), QTY INTEGER)")
	mustExec("CREATE TABLE " + procLibrary + ".INVENTORY (" +
		"CODE VARCHAR(10), QTY INTEGER, LOCATION VARCHAR(20))")

	// Seed data: VALUES *are* parameterisable, so route through the
	// driver's prepared-statement bind path. This both protects
	// against accidental SQL injection if these constants ever
	// pick up external input, and exercises ExecutePreparedSQL on
	// a known-good shape (VARCHAR + INTEGER) as a side effect.
	mustExecArgs("INSERT INTO "+procLibrary+".WIDGETS VALUES (?, ?, ?)",
		"WIDGET", "Acme Widget", 100)
	mustExecArgs("INSERT INTO "+procLibrary+".WIDGETS VALUES (?, ?, ?)",
		"GADGET", "Acme Gadget", 5)
	mustExecArgs("INSERT INTO "+procLibrary+".INVENTORY VALUES (?, ?, ?)",
		"LOW1", 2, "A1")
	mustExecArgs("INSERT INTO "+procLibrary+".INVENTORY VALUES (?, ?, ?)",
		"LOW2", 3, "A2")
	mustExecArgs("INSERT INTO "+procLibrary+".INVENTORY VALUES (?, ?, ?)",
		"HIGH1", 50, "B1")
	mustExecArgs("INSERT INTO "+procLibrary+".INVENTORY VALUES (?, ?, ?)",
		"HIGH2", 100, "B2")

	mustExec("CREATE OR REPLACE PROCEDURE " + procLibrary + ".P_INS " +
		"(IN P_CODE VARCHAR(10), IN P_QTY INTEGER) " +
		"LANGUAGE SQL " +
		"BEGIN " +
		"INSERT INTO " + procLibrary + ".INS_AUDIT (CODE, QTY) " +
		"VALUES (P_CODE, P_QTY); " +
		"END")
	mustExec("CREATE OR REPLACE PROCEDURE " + procLibrary + ".P_LOOKUP " +
		"(IN P_CODE VARCHAR(10), OUT P_NAME VARCHAR(64), OUT P_QTY INTEGER) " +
		"LANGUAGE SQL " +
		"BEGIN " +
		"SELECT NAME, QTY INTO P_NAME, P_QTY " +
		"FROM " + procLibrary + ".WIDGETS WHERE CODE = P_CODE; " +
		"END")
	mustExec("CREATE OR REPLACE PROCEDURE " + procLibrary + ".P_INVENTORY " +
		"(IN P_MIN_QTY INTEGER) " +
		"DYNAMIC RESULT SETS 2 " +
		"LANGUAGE SQL " +
		"BEGIN " +
		"DECLARE C1 CURSOR WITH RETURN FOR " +
		"SELECT CODE, QTY FROM " + procLibrary + ".INVENTORY " +
		"WHERE QTY < P_MIN_QTY ORDER BY CODE; " +
		"DECLARE C2 CURSOR WITH RETURN FOR " +
		"SELECT CODE, QTY FROM " + procLibrary + ".INVENTORY " +
		"WHERE QTY >= P_MIN_QTY ORDER BY CODE; " +
		"OPEN C1; " +
		"OPEN C2; " +
		"END")
	mustExec("CREATE OR REPLACE PROCEDURE " + procLibrary + ".P_ROUNDTRIP " +
		"(INOUT P_COUNTER INTEGER) " +
		"LANGUAGE SQL " +
		"BEGIN " +
		"SET P_COUNTER = P_COUNTER + 1; " +
		"END")
}

// TestStoredProcedureINOnly is M9-1's live-evidence test: invoke
// GOSPROCS.P_INS via db.Exec with parameter markers and confirm the
// proc body's INSERT landed a matching row in INS_AUDIT. Exercises
// the go-db2i CALL routing (db.Exec + isCall) + statement-type
// TYPE_CALL=3 + ExecutePreparedSQL flow end-to-end against the LPAR.
func TestStoredProcedureINOnly(t *testing.T) {
	db := openDB(t)
	setUpStoredProcs(t, db)

	// Pick a CODE that doesn't already exist in INS_AUDIT to make the
	// post-call SELECT unambiguous if this test races against any
	// other run leaving leftover rows behind.
	const code = "M9_INONLY"
	const qty = 7

	// Clear any prior debris -- INS_AUDIT is keyed on CODE+QTY+TS for
	// our purposes here, no PK to enforce uniqueness.
	if _, err := db.Exec("DELETE FROM " + procLibrary + ".INS_AUDIT WHERE CODE = ?", code); err != nil {
		t.Fatalf("clear INS_AUDIT: %v", err)
	}

	// The actual CALL. No OUT params; statement-type TYPE_CALL=3
	// drives the server-side dispatch.
	if _, err := db.Exec("CALL "+procLibrary+".P_INS(?, ?)", code, qty); err != nil {
		t.Fatalf("CALL P_INS: %v", err)
	}

	// Verify the proc's INSERT landed.
	var got int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM "+procLibrary+".INS_AUDIT WHERE CODE = ? AND QTY = ?",
		code, qty).Scan(&got); err != nil {
		t.Fatalf("SELECT COUNT(*): %v", err)
	}
	if got != 1 {
		t.Errorf("INS_AUDIT row count for %q/%d = %d, want 1", code, qty, got)
	}

	// Cleanup so re-runs start clean.
	if _, err := db.Exec("DELETE FROM "+procLibrary+".INS_AUDIT WHERE CODE = ?", code); err != nil {
		t.Logf("cleanup: %v", err)
	}
}

// TestStoredProcedureOUT is M9-2's primary live test: invoke
// GOSPROCS.P_LOOKUP via db.Exec with one IN VARCHAR + two
// sql.Out destinations (VARCHAR + INTEGER) and confirm both OUT
// values come back populated. Exercises the OUT-shape PMF fixup,
// the EXECUTE ORSResultData bit, the synthetic single-row 0x380E
// decode against synthetic SelectColumn entries, and reflect-based
// write-back into *string / *int.
func TestStoredProcedureOUT(t *testing.T) {
	db := openDB(t)
	setUpStoredProcs(t, db)

	var name string
	var qty int
	if _, err := db.Exec("CALL "+procLibrary+".P_LOOKUP(?, ?, ?)",
		"WIDGET",
		sql.Out{Dest: &name},
		sql.Out{Dest: &qty},
	); err != nil {
		t.Fatalf("CALL P_LOOKUP: %v", err)
	}
	if strings.TrimRight(name, " ") != "Acme Widget" {
		t.Errorf("OUT name = %q, want %q", name, "Acme Widget")
	}
	if qty != 100 {
		t.Errorf("OUT qty = %d, want 100", qty)
	}
}

// TestStoredProcedureMultiResultSet is M9-3's primary live test: a
// stored procedure that declares DYNAMIC RESULT SETS 2 returns two
// result sets through Rows + Rows.NextResultSet. P_INVENTORY opens
// two cursors WITH RETURN: one for rows below the IN min_qty
// threshold, one for rows at or above. With min_qty=5 the seed data
// yields {LOW1=2, LOW2=3} in set 1 and {HIGH1=50, HIGH2=100} in set 2.
func TestStoredProcedureMultiResultSet(t *testing.T) {
	db := openDB(t)
	setUpStoredProcs(t, db)

	rows, err := db.Query("CALL "+procLibrary+".P_INVENTORY(?)", 5)
	if err != nil {
		t.Fatalf("Query CALL P_INVENTORY: %v", err)
	}
	defer rows.Close()

	type entry = struct {
		code string
		qty  int
	}
	drain := func() []entry {
		var out []entry
		for rows.Next() {
			var e entry
			if err := rows.Scan(&e.code, &e.qty); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, entry{strings.TrimRight(e.code, " "), e.qty})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		return out
	}

	set1 := drain()
	want1 := []entry{{"LOW1", 2}, {"LOW2", 3}}
	if !sliceEqualEntries(set1, want1) {
		t.Errorf("first set = %v, want %v", set1, want1)
	}

	if !rows.NextResultSet() {
		t.Fatalf("NextResultSet returned false; expected a second result set (proc declares DYNAMIC RESULT SETS 2)")
	}

	set2 := drain()
	want2 := []entry{{"HIGH1", 50}, {"HIGH2", 100}}
	if !sliceEqualEntries(set2, want2) {
		t.Errorf("second set = %v, want %v", set2, want2)
	}

	if rows.NextResultSet() {
		t.Errorf("NextResultSet returned true after second set drained; expected false")
	}
}

// sliceEqualEntries compares two entry slices order-sensitively.
// Helper for TestStoredProcedureMultiResultSet -- the proc's
// ORDER BY CODE guarantees deterministic ordering on the wire.
func sliceEqualEntries(a, b []struct {
	code string
	qty  int
}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].code != b[i].code || a[i].qty != b[i].qty {
			return false
		}
	}
	return true
}

// TestMultiLibrary is M11-2's primary live test: open a connection
// with `?libraries=<schema>,GOSPROCS` and confirm that an
// unqualified `CALL P_INS(?, ?)` resolves to GOSPROCS.P_INS through
// the job's library list. The "wrong" qualifier path (default DSN
// with just `?library=<schema>`) returns SQL-204 for the same call
// -- proving the resolution actually used the new list, not a
// leftover *LIBL from a previous run.
//
// Bootstraps GOSPROCS first (via setUpStoredProcs against the
// default DSN) so the proc actually exists before the libraries-
// scoped connection looks for it.
func TestMultiLibrary(t *testing.T) {
	// Bootstrap GOSPROCS on the default-DSN connection.
	bootstrap := openDB(t)
	setUpStoredProcs(t, bootstrap)
	bootstrap.Close()

	// Reopen with libraries= appended. The DSN already has
	// `?library=<schema>` from the harness; the merge rule prepends
	// it (indicator 'C') and GOSPROCS goes after (indicator 'L').
	base := dsn(t)
	sep := "&"
	if !strings.Contains(base, "?") {
		sep = "?"
	}
	libsDSN := base + sep + "libraries=" + schema() + "," + procLibrary
	db, err := sql.Open("db2i", libsDSN)
	if err != nil {
		t.Fatalf("sql.Open(libs): %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)

	// Sanity-baseline: same call against the default-DSN connection
	// (no libraries=, so GOSPROCS is NOT on *LIBL by default) must
	// fail with SQL-204. If this baseline passes, the test isn't
	// proving anything -- e.g. a leftover library list on the job.
	t.Run("baseline_unqualified_resolves_to_204_without_libraries", func(t *testing.T) {
		base := openDB(t)
		defer base.Close()
		_, err := base.Exec("CALL P_INS(?, ?)", "M11_BASELINE", 1)
		if err == nil {
			t.Skip("baseline CALL resolved without libraries= -- job library list may already include GOSPROCS; cannot validate M11-2 conclusively in this environment")
		}
		if !strings.Contains(err.Error(), "204") {
			t.Logf("baseline error (informational; want SQL-204): %v", err)
		}
	})

	// The real test: unqualified CALL resolves via libraries=.
	// CODE is VARCHAR(10) in INS_AUDIT, so keep this <= 10 chars.
	const code = "M11_MULTI"
	const qty = 17
	if _, err := db.Exec("DELETE FROM "+procLibrary+".INS_AUDIT WHERE CODE = ?", code); err != nil {
		t.Fatalf("clear INS_AUDIT: %v", err)
	}
	if _, err := db.Exec("CALL P_INS(?, ?)", code, qty); err != nil {
		t.Fatalf("CALL P_INS (unqualified, libraries=...,GOSPROCS): %v", err)
	}
	var got int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM "+procLibrary+".INS_AUDIT WHERE CODE = ? AND QTY = ?",
		code, qty).Scan(&got); err != nil {
		t.Fatalf("SELECT COUNT(*): %v", err)
	}
	if got != 1 {
		t.Errorf("INS_AUDIT row count for %q/%d = %d, want 1 (proc resolved via libraries=)", code, qty, got)
	}
	if _, err := db.Exec("DELETE FROM "+procLibrary+".INS_AUDIT WHERE CODE = ?", code); err != nil {
		t.Logf("cleanup: %v", err)
	}
}

// TestSystemNaming is M11-3's primary live test: opening with
// `?naming=system` lets the server resolve `MYLIB/TABLE` (slash
// qualifier, the JT400 default) and conversely rejects the SQL-
// naming `MYLIB.TABLE` form. The default `naming=sql` is exercised
// implicitly by every other test in this file, so this test only
// has to flip the knob and confirm the wire byte (CP 0x380C value
// 1) is honoured end-to-end.
//
// Strategy: create + drop a one-row table under the harness schema
// (using SQL naming so the bootstrap doesn't depend on the knob
// being live), then re-open with `?naming=system` and confirm a
// `SELECT * FROM <schema>/<table>` query works.
func TestSystemNaming(t *testing.T) {
	// Bootstrap a known table via the default-naming connection.
	bootstrap := openDB(t)
	defer bootstrap.Close()
	const tbl = tablePrefix + "M11_NAMING"
	fqn := schema() + "." + tbl
	dropSQL := "DROP TABLE " + fqn
	bootstrap.Exec(dropSQL)
	if _, err := bootstrap.Exec("CREATE TABLE " + fqn + " (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(16))"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	t.Cleanup(func() { bootstrap.Exec(dropSQL) })
	if _, err := bootstrap.Exec("INSERT INTO " + fqn + " VALUES (1, 'hello')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Reopen with system naming. Slash-qualified queries now work;
	// period-qualified queries get parsed differently (the dot is
	// treated as schema-separator vs slash-separator).
	base := dsn(t)
	sep := "&"
	if !strings.Contains(base, "?") {
		sep = "?"
	}
	sysDB, err := sql.Open("db2i", base+sep+"naming=system")
	if err != nil {
		t.Fatalf("sql.Open(naming=system): %v", err)
	}
	defer sysDB.Close()

	// Slash-qualified resolves under system naming.
	slashFQN := schema() + "/" + tbl
	var id int
	var v string
	if err := sysDB.QueryRow("SELECT ID, V FROM " + slashFQN + " WHERE ID = 1").Scan(&id, &v); err != nil {
		t.Fatalf("SELECT %s under naming=system: %v", slashFQN, err)
	}
	if id != 1 || strings.TrimRight(v, " ") != "hello" {
		t.Errorf("row = (%d, %q), want (1, %q)", id, v, "hello")
	}
}

// TestTimeFormatUSA is M11-4's primary live test: opening with
// `?time-format=usa` flips CP 0x3809 to index 1 and the server
// formats subsequent TIME values in 12-hour clock with AM/PM.
// Default (no knob) returns ISO 24-hour "HH.MM.SS" on V7R6M0.
//
// The query CASTs the TIME literal to VARCHAR explicitly so we read
// the server's chosen rendering as a plain string. (The driver's
// TIME -> time.Time auto-promotion expects ISO; teaching it to
// parse JT400's other server-side time-format renderings is a
// separate work item, larger than M11's scope.)
func TestTimeFormatUSA(t *testing.T) {
	// Baseline: default time-format returns the server's HH.MM.SS.
	baseDB := openDB(t)
	var baseline string
	if err := baseDB.QueryRow("VALUES CAST(CAST('13:45:00' AS TIME) AS VARCHAR(11))").Scan(&baseline); err != nil {
		t.Fatalf("baseline VALUES TIME: %v", err)
	}
	baseDB.Close()
	baseline = strings.TrimSpace(baseline)
	if baseline == "" {
		t.Fatalf("baseline TIME formatted to empty string")
	}

	// USA: re-open with the knob; same query should now return
	// AM/PM form. The exact wire string depends on the server's
	// locale, so just assert "AM" or "PM" appears and the value
	// differs from the baseline.
	base := dsn(t)
	sep := "&"
	if !strings.Contains(base, "?") {
		sep = "?"
	}
	usaDB, err := sql.Open("db2i", base+sep+"time-format=usa")
	if err != nil {
		t.Fatalf("sql.Open(time-format=usa): %v", err)
	}
	defer usaDB.Close()

	var usa string
	if err := usaDB.QueryRow("VALUES CAST(CAST('13:45:00' AS TIME) AS VARCHAR(11))").Scan(&usa); err != nil {
		t.Fatalf("VALUES TIME under time-format=usa: %v", err)
	}
	usa = strings.TrimSpace(usa)
	t.Logf("baseline=%q usa=%q", baseline, usa)
	upper := strings.ToUpper(usa)
	if !strings.Contains(upper, "AM") && !strings.Contains(upper, "PM") {
		t.Errorf("time-format=usa output %q does not contain AM or PM", usa)
	}
	if usa == baseline {
		t.Errorf("time-format=usa output equals baseline %q -- knob had no effect", baseline)
	}
}

// TestStoredProcedureINOUT covers the INOUT direction byte (0xF2)
// via GOSPROCS.P_ROUNDTRIP, which simply increments its single
// INOUT INTEGER. Seed value 5 -> proc returns 6. Exercises the IN-
// side bind path (deref *Dest for the bind value) plus the OUT-side
// write-back through the same slot.
func TestStoredProcedureINOUT(t *testing.T) {
	db := openDB(t)
	setUpStoredProcs(t, db)

	counter := 5
	if _, err := db.Exec("CALL "+procLibrary+".P_ROUNDTRIP(?)",
		sql.Out{Dest: &counter, In: true},
	); err != nil {
		t.Fatalf("CALL P_ROUNDTRIP: %v", err)
	}
	if counter != 6 {
		t.Errorf("INOUT counter = %d, want 6 (seed 5 + 1)", counter)
	}
}

// TestSavepointRoundTrip exercises M12-1 end-to-end on a live LPAR:
// open a tx, INSERT one row, mark a SAVEPOINT, INSERT another row,
// ROLLBACK TO the savepoint, COMMIT. Only the first row should
// survive. Then re-run with RELEASE SAVEPOINT instead, asserting
// the release+commit path leaves both rows.
func TestSavepointRoundTrip(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "M12_SP"
	db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec("CREATE TABLE " + tbl + " (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(16))"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + tbl) })

	ctx := context.Background()

	// Savepoints require an active unit of work on IBM i (the server
	// rejects with SQL-880 / SQLSTATE 3B001 under autocommit). The
	// canonical pattern is: claim a sql.Conn, BeginTx on it to flip
	// autocommit off, then reach driver-typed savepoint methods via
	// conn.Raw -- Raw exposes the underlying driver.Conn directly,
	// which is the same wire the tx is using.
	//
	// Some shared LPARs (PUB400) don't grant journaling authority on
	// user schemas; an INSERT inside a tx on a non-journaled table
	// trips SQL-7008 / 55019 ("operation not allowed in the current
	// state of the connected database"). When that fires before any
	// savepoint code runs, treat the test as an environmental skip
	// rather than a savepoint regression.
	skipIfNoJournal := func(t *testing.T, err error) bool {
		t.Helper()
		if err == nil {
			return false
		}
		if strings.Contains(err.Error(), "7008") || strings.Contains(err.Error(), "55019") {
			t.Skipf("savepoint test requires journaled tables; this LPAR rejected INSERT-in-tx with %v", err)
			return true
		}
		return false
	}
	t.Run("rollback_to_savepoint", func(t *testing.T) {
		db.Exec("DELETE FROM " + tbl)
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("db.Conn: %v", err)
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?, ?)", 1, "before"); err != nil {
			tx.Rollback()
			if skipIfNoJournal(t, err) {
				return
			}
			t.Fatalf("INSERT before SP: %v", err)
		}
		if err := conn.Raw(func(driverConn any) error {
			return driverConn.(*db2i.Conn).Savepoint(ctx, "SP1")
		}); err != nil {
			tx.Rollback()
			t.Fatalf("Savepoint: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?, ?)", 2, "after"); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT after SP: %v", err)
		}
		if err := conn.Raw(func(driverConn any) error {
			return driverConn.(*db2i.Conn).RollbackToSavepoint(ctx, "SP1")
		}); err != nil {
			tx.Rollback()
			t.Fatalf("RollbackToSavepoint: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		var count int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&count); err != nil {
			t.Fatalf("SELECT COUNT(*) after ROLLBACK TO SP: %v", err)
		}
		if count != 1 {
			t.Errorf("after ROLLBACK TO SP1 + COMMIT: got %d rows, want 1 (only the pre-SP insert)", count)
		}
	})

	t.Run("release_savepoint", func(t *testing.T) {
		db.Exec("DELETE FROM " + tbl)
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("db.Conn: %v", err)
		}
		defer conn.Close()
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?, ?)", 1, "first"); err != nil {
			tx.Rollback()
			if skipIfNoJournal(t, err) {
				return
			}
			t.Fatalf("INSERT 1: %v", err)
		}
		if err := conn.Raw(func(driverConn any) error {
			return driverConn.(*db2i.Conn).Savepoint(ctx, "SP2")
		}); err != nil {
			tx.Rollback()
			t.Fatalf("Savepoint SP2: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?, ?)", 2, "second"); err != nil {
			tx.Rollback()
			t.Fatalf("INSERT 2: %v", err)
		}
		if err := conn.Raw(func(driverConn any) error {
			return driverConn.(*db2i.Conn).ReleaseSavepoint(ctx, "SP2")
		}); err != nil {
			tx.Rollback()
			t.Fatalf("ReleaseSavepoint SP2: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		var count int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&count); err != nil {
			t.Fatalf("SELECT COUNT(*): %v", err)
		}
		if count != 2 {
			t.Errorf("after RELEASE SP2 + COMMIT: got %d rows, want 2 (both inserts survive)", count)
		}
	})

	t.Run("bad_name_rejects_before_wire", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("db.Conn: %v", err)
		}
		defer conn.Close()
		if err := conn.Raw(func(driverConn any) error {
			return driverConn.(*db2i.Conn).Savepoint(ctx, "SP; DROP TABLE T;")
		}); err == nil {
			t.Errorf("Savepoint(injection): expected validation error, got nil")
		}
	})
}

// ensureSchemaB tries to create the second test library on demand
// so TestSetSchema / TestAddRemoveLibraries can exercise mid-
// session library mutation. Defaults to `<DB2I_SCHEMA>2`; respects
// DB2I_SCHEMA_B if set. Skips the calling test when the library
// can't be created (no CRTLIB authority) and isn't already
// present.
func ensureSchemaB(t *testing.T, db *sql.DB) string {
	t.Helper()
	b := os.Getenv("DB2I_SCHEMA_B")
	if b == "" {
		b = schema() + "2"
	}
	// Try to create; CPF2111 / "Library X already exists" is benign.
	_, err := db.Exec("CALL QSYS2.QCMDEXC(?)", "CRTLIB LIB("+b+") TEXT('go-db2i test')")
	if err != nil && !strings.Contains(err.Error(), "CPF2111") && !strings.Contains(err.Error(), "already exists") {
		t.Skipf("cannot create test library %s (need CRTLIB authority or set DB2I_SCHEMA_B to a pre-existing library): %v", b, err)
	}
	// Verify by querying QSYS2.LIBRARY_LIST_INFO or just trying a
	// CHKOBJ. Simpler: a SELECT against SYSCAT.
	var dummy int
	if err := db.QueryRow("SELECT 1 FROM QSYS2.SCHEMATA WHERE SCHEMA_NAME = ?", b).Scan(&dummy); err != nil {
		t.Skipf("library %s not visible after CRTLIB attempt: %v", b, err)
	}
	return b
}

// TestSetSchema exercises M12-2 via Conn.SetSchema: after switching
// schemas mid-session, unqualified SELECT resolves against the new
// schema's table. Bootstraps two schemas (A, B), each with a
// one-row table of the same name, switches between them, and
// asserts the right row comes back.
//
// The two "schemas" are actually two libraries in the harness
// schema's library-list neighbourhood -- IBM i "SCHEMA" and
// "library" are the same identifier in this context.
func TestSetSchema(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	schemaA := schema()
	schemaB := ensureSchemaB(t, db)

	// Bootstrap one-row tables in each schema. Use SQL-quoted names
	// so case is preserved.
	tbl := tablePrefix + "M12_SS"
	tblA := schemaA + "." + tbl
	tblB := schemaB + "." + tbl
	db.Exec("DROP TABLE " + tblA)
	db.Exec("DROP TABLE " + tblB)
	if _, err := db.Exec("CREATE TABLE " + tblA + " (V VARCHAR(8))"); err != nil {
		t.Fatalf("CREATE %s: %v", tblA, err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + tblA) })
	if _, err := db.Exec("CREATE TABLE " + tblB + " (V VARCHAR(8))"); err != nil {
		t.Fatalf("CREATE %s: %v", tblB, err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + tblB) })
	if _, err := db.Exec("INSERT INTO " + tblA + " VALUES ('A')"); err != nil {
		t.Fatalf("INSERT A: %v", err)
	}
	if _, err := db.Exec("INSERT INTO " + tblB + " VALUES ('B')"); err != nil {
		t.Fatalf("INSERT B: %v", err)
	}

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	// Switch to schemaB and run an unqualified SELECT.
	if err := conn.Raw(func(driverConn any) error {
		return driverConn.(*db2i.Conn).SetSchema(ctx, schemaB)
	}); err != nil {
		t.Fatalf("SetSchema(%s): %v", schemaB, err)
	}
	var got string
	if err := conn.QueryRowContext(ctx, "SELECT V FROM "+tbl).Scan(&got); err != nil {
		t.Fatalf("SELECT after SetSchema(%s): %v", schemaB, err)
	}
	if got != "B" {
		t.Errorf("after SetSchema(%s): got V=%q, want %q", schemaB, got, "B")
	}

	// Switch back to schemaA and confirm.
	if err := conn.Raw(func(driverConn any) error {
		return driverConn.(*db2i.Conn).SetSchema(ctx, schemaA)
	}); err != nil {
		t.Fatalf("SetSchema(%s): %v", schemaA, err)
	}
	if err := conn.QueryRowContext(ctx, "SELECT V FROM "+tbl).Scan(&got); err != nil {
		t.Fatalf("SELECT after SetSchema(%s): %v", schemaA, err)
	}
	if got != "A" {
		t.Errorf("after SetSchema(%s): got V=%q, want %q", schemaA, got, "A")
	}
}

// TestAddRemoveLibraries exercises M12-2: AddLibraries puts a
// second library on *LIBL so unqualified references resolve there;
// RemoveLibraries pulls it back off and the same unqualified
// reference fails. Requires DB2I_SCHEMA_B (the same env var
// TestSetSchema uses).
func TestAddRemoveLibraries(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	schemaB := ensureSchemaB(t, db)

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	libListContains := func(t *testing.T, lib string) bool {
		t.Helper()
		var n int
		if err := conn.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM QSYS2.LIBRARY_LIST_INFO WHERE SYSTEM_SCHEMA_NAME = ?",
			lib).Scan(&n); err != nil {
			t.Fatalf("LIBRARY_LIST_INFO probe: %v", err)
		}
		return n > 0
	}

	if libListContains(t, schemaB) {
		t.Logf("schemaB=%s already on *LIBL pre-test; removing for a clean baseline", schemaB)
		if err := conn.Raw(func(dc any) error {
			return dc.(*db2i.Conn).RemoveLibraries(ctx, []string{schemaB})
		}); err != nil {
			t.Fatalf("pre-clean RemoveLibraries: %v", err)
		}
	}
	if libListContains(t, schemaB) {
		t.Fatalf("pre-clean failed: %s still on *LIBL", schemaB)
	}

	// AddLibraries(schemaB) should put schemaB on the job's *LIBL.
	if err := conn.Raw(func(dc any) error {
		return dc.(*db2i.Conn).AddLibraries(ctx, []string{schemaB})
	}); err != nil {
		t.Fatalf("AddLibraries(%s): %v", schemaB, err)
	}
	if !libListContains(t, schemaB) {
		t.Errorf("after AddLibraries(%s): library not present in QSYS2.LIBRARY_LIST_INFO", schemaB)
	}

	// RemoveLibraries(schemaB) should pull it back off.
	if err := conn.Raw(func(dc any) error {
		return dc.(*db2i.Conn).RemoveLibraries(ctx, []string{schemaB})
	}); err != nil {
		t.Fatalf("RemoveLibraries(%s): %v", schemaB, err)
	}
	if libListContains(t, schemaB) {
		t.Errorf("after RemoveLibraries(%s): library still present in QSYS2.LIBRARY_LIST_INFO", schemaB)
	}

	// Idempotent: a second RemoveLibraries of an absent library is
	// downgraded to WARN (CPF2104 / "library not in list" from the
	// underlying CL) and does NOT return an error.
	if err := conn.Raw(func(dc any) error {
		return dc.(*db2i.Conn).RemoveLibraries(ctx, []string{schemaB})
	}); err != nil {
		t.Errorf("second RemoveLibraries (idempotent) failed: %v", err)
	}
}

// TestBlockSize exercises M12-3 by completing a streaming SELECT
// under each supported `?block-size` value and asserting each
// variant:
//
//   - opens the connection successfully (parses the DSN, sends the
//     correctly-formed CP `0x3834` parameter on the wire)
//   - completes the SELECT without error
//   - returns at least one row
//
// The wire-byte contents for each value are pinned offline by
// `hostserver/db_buffersize_test.go:TestBufferSizeParamExplicitValues`,
// so this live test is the "knob is plumbed all the way through"
// integration check. Cross-DSN row-count comparison is
// deliberately NOT asserted: pre-existing `fetchMoreRows` logic
// (the "zero-rows-as-exhausted" guard added for V7R3 PUB400)
// interacts non-determinstically with `FETCH FIRST N` on smaller
// buffers; that's a separate known-quirk unrelated to M12-3.
func TestBlockSize(t *testing.T) {
	base := dsn(t)
	sep := "&"
	if !strings.Contains(base, "?") {
		sep = "?"
	}

	for _, size := range []int{16, 32, 64, 128, 512} {
		size := size
		t.Run(fmt.Sprintf("block-size=%d", size), func(t *testing.T) {
			dsnX := base + sep + fmt.Sprintf("block-size=%d", size)
			db, err := sql.Open("db2i", dsnX)
			if err != nil {
				t.Fatalf("sql.Open(block-size=%d): %v", size, err)
			}
			defer db.Close()
			// Run a SELECT that returns at least one row. SYSDUMMY1
			// is the canonical IBM i one-row table; using it sidesteps
			// any user-table state issues.
			rows, err := db.Query("SELECT 1 FROM SYSIBM.SYSDUMMY1")
			if err != nil {
				t.Fatalf("SELECT under block-size=%d: %v", size, err)
			}
			defer rows.Close()
			seen := 0
			for rows.Next() {
				var x int
				if err := rows.Scan(&x); err != nil {
					t.Fatalf("Scan: %v", err)
				}
				seen++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("rows.Err: %v", err)
			}
			if seen == 0 {
				t.Fatalf("block-size=%d: SELECT returned 0 rows", size)
			}
		})
	}
}

// TestFetchFirstNExact pins the row-count contract for
// `FETCH FIRST N ROWS ONLY` SELECTs across different block-sizes:
// the cursor MUST return exactly N rows regardless of how many
// FETCH continuations the wire buffer forces. Regression test for
// the pre-v0.7.13 bug where fetchMoreRows early-returned on the
// exhausted signal without parsing the rows the server delivered
// in the same reply -- queries that exhausted naturally inside a
// batch silently truncated.
//
// The cap is 100 rows; with the wide SYSCOLUMNS rows even a 4 KiB
// buffer fits ~40 rows per batch, so 100 rows spans 3 batches and
// the LAST batch carries rows + EOD signal together (the bug's
// trigger). Block-size=512 KiB is the control: all 100 rows fit
// in one batch, so the bug doesn't manifest there.
func TestFetchFirstNExact(t *testing.T) {
	base := dsn(t)
	sep := "&"
	if !strings.Contains(base, "?") {
		sep = "?"
	}
	const want = 100
	query := fmt.Sprintf(`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
		FROM QSYS2.SYSCOLUMNS
		WHERE TABLE_SCHEMA = 'QSYS2'
		ORDER BY TABLE_NAME, ORDINAL_POSITION
		FETCH FIRST %d ROWS ONLY`, want)

	count := func(t *testing.T, db *sql.DB) int {
		t.Helper()
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("SELECT: %v", err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var a, b, c string
			if err := rows.Scan(&a, &b, &c); err != nil {
				t.Fatalf("Scan: %v", err)
			}
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		return n
	}

	for _, size := range []int{8, 16, 32, 64, 128, 512} {
		size := size
		t.Run(fmt.Sprintf("block-size=%d", size), func(t *testing.T) {
			dsnX := base + sep + fmt.Sprintf("block-size=%d", size)
			db, err := sql.Open("db2i", dsnX)
			if err != nil {
				t.Fatalf("sql.Open: %v", err)
			}
			defer db.Close()
			got := count(t, db)
			if got != want {
				t.Errorf("block-size=%d: got %d rows, want %d (FETCH FIRST N must return exact)", size, got, want)
			}
		})
	}
}
// TestUserTableLargeScanReturnsAllRows pins the v0.7.14 bug-#2 fix.
// Pre-fix, a fresh user-table streaming SELECT of 10000 INTEGER +
// VARCHAR + DECIMAL rows on V7R6M0 returned only 8625 rows -- the
// server delivered the closing batch carrying EC=2 RC=700 ("fetch/
// close, all delivered") with the remaining 1375 rows, but
// Cursor.Next dropped that batch on the exhausted path. The
// v0.7.13 bug-#1 fix had already taught fetchMoreRows to parse
// rows before honouring exhausted, but the cursor-level discard
// remained until v0.7.14. With both fixes in place, streamed
// equals the inserted count exactly.
//
// Live-test only (gated on DB2I_DSN like the rest of the
// conformance suite). Offline byte-equality of the continuation
// FETCH wire shape is covered separately by
// TestSentBytesMatchSelectLargeUserTableFixture.
func TestUserTableLargeScanReturnsAllRows(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	// Mirror JT400's select_large_user_table_10k case exactly:
	// 3-column schema (ID INT, NAME VARCHAR(40), AMT DECIMAL(11,2)),
	// 10-char system table name "GOJT_T1", 1..N IDs, no ORDER BY.
	tbl := schema() + ".GOJT_T1"
	db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec("CREATE TABLE " + tbl + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(40) NOT NULL, AMT DECIMAL(11,2) NOT NULL)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + tbl) })

	const n = 10000
	ins, err := db.Prepare("INSERT INTO " + tbl + " (ID, NAME, AMT) VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("prepare INSERT: %v", err)
	}
	for i := 1; i <= n; i++ {
		amt := fmt.Sprintf("%d.23", i)
		if _, err := ins.Exec(i, fmt.Sprintf("row-%05d", i), amt); err != nil {
			t.Fatalf("INSERT %d: %v", i, err)
		}
	}
	ins.Close()

	var c int
	if err := db.QueryRow("SELECT COUNT(*) FROM "+tbl).Scan(&c); err != nil {
		t.Fatalf("COUNT: %v", err)
	}

	rows, err := db.Query("SELECT ID, NAME, AMT FROM " + tbl)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	streamed := 0
	for rows.Next() {
		var id int
		var name, amt string
		rows.Scan(&id, &name, &amt)
		streamed++
	}
	rows.Close()
	t.Logf("inserted=%d  COUNT(*)=%d  streamed=%d", n, c, streamed)
	if streamed != n {
		t.Errorf("streamed=%d, expected %d (COUNT reported %d)", streamed, n, c)
	}
}
