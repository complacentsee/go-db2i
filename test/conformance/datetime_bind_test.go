//go:build conformance

// datetime_bind_test.go is the live round-trip coverage for the
// describe-driven native DATE/TIME bind (issue #40).
//
// The driver binds every time.Time as TIMESTAMP (393, 26-char);
// reconcileDateTimeBindShapes reshapes a time.Time bound into a native DATE
// (384) / TIME (388) column to the column's own parameter-marker shape so the
// wire bytes match JT400's describe-driven bind (10-char ISO DATE / 8-char
// "HH.MM.SS" TIME), and the package-cache fast path packs the same native
// shape recovered from the *PGM. These tests assert a time.Time round-trips
// identically on the cache-miss and cache-hit paths and that cache-hit
// dispatches. The TIME case also exercises the JT400 period separator
// ("HH.MM.SS") live -- a failed INSERT would mean the server rejects it.
package conformance

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestDateTimeBindCacheHitAgreement(t *testing.T) {
	requireFiling(t)

	// Wall clock carries both a date and a time component; the driver formats
	// it as-is (no UTC shift). DATE keeps the date, TIME keeps the time.
	val := time.Date(2026, 5, 7, 14, 23, 45, 0, time.UTC)

	cases := []struct {
		name    string
		colType string
		wantSub string // substring the read-back must contain
	}{
		{"date", "DATE", "2026-05-07"},
		{"time", "TIME", "14:23:45"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wipeDB := openDB(t)
			wipePackage(t, wipeDB, cachePackageName)
			wipeDB.Close()

			db, _ := openDBWithPackageCache(t, "")
			tbl := makeCacheTestTable(t, db, "dt"+tc.name, "(id INTEGER NOT NULL, v "+tc.colType+")")
			insertSQL := "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)"

			// Cache-miss control: time.Time stored via the native DATE/TIME
			// reshape (period TIME separator must be accepted by the server).
			if _, err := db.Exec(insertSQL, 1, val); err != nil {
				t.Fatalf("cache-miss INSERT: %v", err)
			}
			miss := readTemporal(t, db, tbl, 1)
			if !strings.Contains(miss, tc.wantSub) {
				t.Fatalf("cache-miss read-back %q does not contain %q", miss, tc.wantSub)
			}

			// File the INSERT across the 3-PREPARE threshold, then dispatch
			// the same SQL on a fresh connection: it must cache-hit and store
			// the same value.
			fillPackageCache(t, fillExec, insertSQL, 900, val)
			db.Close()

			db2, buf := openDBWithPackageCache(t, "")
			defer db2.Close()
			if _, err := db2.Exec(insertSQL, 2, val); err != nil {
				t.Fatalf("cache-hit INSERT: %v", err)
			}
			expectCacheHit(t, buf, cacheHitExecMsg)

			hit := readTemporal(t, db2, tbl, 2)
			if hit != miss {
				t.Fatalf("cache-hit read-back %q disagrees with cache-miss %q", hit, miss)
			}
		})
	}
}

func readTemporal(t *testing.T, db *sql.DB, tbl string, id int) string {
	t.Helper()
	var got string
	if err := db.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", id).Scan(&got); err != nil {
		t.Fatalf("read-back id=%d: %v", id, err)
	}
	return got
}
