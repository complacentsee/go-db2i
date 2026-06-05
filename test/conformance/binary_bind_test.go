//go:build conformance

// binary_bind_test.go is the live round-trip coverage for the
// describe-driven native binary bind (issue #40).
//
// The database/sql bind path maps every []byte to VARCHAR FOR BIT DATA
// (449); reconcileBinaryBindShapes reshapes a []byte bound into a native
// BINARY (912) / VARBINARY (908) column to the column's own
// parameter-marker shape so the wire bytes match JT400's describe-driven
// bind, and the package-cache fast path packs the same native shape
// recovered from the *PGM. These tests assert a []byte round-trips
// byte-for-byte on BOTH the cache-miss and cache-hit paths, including the
// 0x00 padding a fixed BINARY column applies to a short value.
//
// The cache-hit assertion (expectCacheHit) also serves as a live probe
// that the server reports the native 912/908 type in the parameter-marker
// format: were the column described as VARCHAR/CHAR FOR BIT DATA instead,
// the fixed-BINARY bind would fall back off the cache-hit path and the
// assertion would fail.
package conformance

import (
	"bytes"
	"database/sql"
	"testing"
)

// TestVarbinaryBindCacheHitAgreement round-trips a []byte into a native
// VARBINARY(64) column and confirms the cache-miss and cache-hit paths
// store identical bytes.
func TestVarbinaryBindCacheHitAgreement(t *testing.T) {
	requireFiling(t)

	wipeDB := openDB(t)
	wipePackage(t, wipeDB, cachePackageName)
	wipeDB.Close()

	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "vbch", "(id INTEGER NOT NULL, v VARBINARY(64))")
	insertSQL := "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)"

	val := []byte{0x01, 0x02, 0x03, 0x04, 0xCA, 0xFE}

	// Cache-miss control: stores via the native VARBINARY reshape.
	if _, err := db.Exec(insertSQL, 1, val); err != nil {
		t.Fatalf("cache-miss INSERT: %v", err)
	}
	miss := readBinary(t, db, tbl, 1)
	if !bytes.Equal(miss, val) {
		t.Fatalf("cache-miss read-back = % X, want % X", miss, val)
	}

	// File the INSERT across the 3-PREPARE threshold, then dispatch the
	// same SQL on a fresh connection: it must cache-hit and store the same
	// value (packed natively from the *PGM parameter-marker format).
	fillPackageCache(t, fillExec, insertSQL, 900, val)
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	defer db2.Close()
	if _, err := db2.Exec(insertSQL, 2, val); err != nil {
		t.Fatalf("cache-hit INSERT: %v", err)
	}
	expectCacheHit(t, buf, cacheHitExecMsg)

	hit := readBinary(t, db2, tbl, 2)
	if !bytes.Equal(hit, val) {
		t.Fatalf("cache-hit read-back = % X, want % X", hit, val)
	}
	if !bytes.Equal(hit, miss) {
		t.Fatalf("cache-hit % X disagrees with cache-miss % X", hit, miss)
	}
}

// TestBinaryFixedBindCacheHitAgreement round-trips a SHORT []byte into a
// fixed native BINARY(16) column. A fixed BINARY zero-pads to the column
// width; the native bind makes the pad byte deterministic (0x00) on both
// paths, the case TestTypeMatrixBinary deliberately avoids by using
// full-width values.
func TestBinaryFixedBindCacheHitAgreement(t *testing.T) {
	requireFiling(t)

	wipeDB := openDB(t)
	wipePackage(t, wipeDB, cachePackageName)
	wipeDB.Close()

	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "bfch", "(id INTEGER NOT NULL, v BINARY(16))")
	insertSQL := "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)"

	val := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	want := make([]byte, 16)
	copy(want, val) // 4 bytes + 12 bytes of 0x00 pad

	if _, err := db.Exec(insertSQL, 1, val); err != nil {
		t.Fatalf("cache-miss INSERT: %v", err)
	}
	miss := readBinary(t, db, tbl, 1)
	if !bytes.Equal(miss, want) {
		t.Fatalf("cache-miss read-back = % X, want % X (0x00-padded)", miss, want)
	}

	fillPackageCache(t, fillExec, insertSQL, 900, val)
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	defer db2.Close()
	if _, err := db2.Exec(insertSQL, 2, val); err != nil {
		t.Fatalf("cache-hit INSERT: %v", err)
	}
	expectCacheHit(t, buf, cacheHitExecMsg)

	hit := readBinary(t, db2, tbl, 2)
	if !bytes.Equal(hit, want) {
		t.Fatalf("cache-hit read-back = % X, want % X (0x00-padded)", hit, want)
	}
	if !bytes.Equal(hit, miss) {
		t.Fatalf("cache-hit % X disagrees with cache-miss % X", hit, miss)
	}
}

// TestBinaryBindAsSelectPredicate exercises the SELECT-path wiring of
// reconcileBinaryBindShapes (openPreparedUntilFirstBatch): a []byte bound
// as a WHERE predicate against a native binary column must round-trip the
// prepared SELECT and match the stored row. The INSERT binds id only
// (INTEGER) so the predicate bind is the sole binary parameter under test.
func TestBinaryBindAsSelectPredicate(t *testing.T) {
	db := openDB(t)

	cases := []struct {
		name   string
		valDDL string
		val    []byte
	}{
		{"varbinary", "VARBINARY(32)", []byte{0x01, 0x02, 0x03, 0x04, 0xCA, 0xFE}},
		// Full-width BINARY(16) so the predicate value matches the
		// stored (zero-padded) bytes without relying on the server's
		// pad-then-compare cast.
		{"binary_full", "BINARY(16)", []byte("0123456789abcdef")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := mkMatrixTable(t, db, "bs"+tc.name[:2], tc.valDDL, false)
			if _, err := db.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", 1, tc.val); err != nil {
				t.Fatalf("INSERT: %v", err)
			}
			var id int
			err := db.QueryRow("SELECT id FROM "+tbl+" WHERE v = ?", tc.val).Scan(&id)
			if err != nil {
				t.Fatalf("SELECT WHERE v = ? (binary predicate): %v", err)
			}
			if id != 1 {
				t.Errorf("binary predicate matched id %d, want 1", id)
			}
		})
	}
}

func readBinary(t *testing.T, db *sql.DB, tbl string, id int) []byte {
	t.Helper()
	var got []byte
	if err := db.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", id).Scan(&got); err != nil {
		t.Fatalf("read-back id=%d: %v", id, err)
	}
	return got
}
