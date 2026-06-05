//go:build conformance

// out_types_test.go is the live coverage for the describe-driven OUT /
// INOUT typed destinations added in v0.7.28 (the last bind sub-item of
// tracking issue #40). A stored-procedure OUT/INOUT parameter can now
// write back into a *[]byte, a *time.Time, or a math/big decimal
// carrier (big.Rat/big.Int/big.Float), in addition to the
// int*/int64/float*/string/bool destinations shipped earlier.
//
// The OUT wire + value decode were already correct (v0.7.8 added OUT
// cache-hit dispatch + CP 0x380E decode); these tests prove the new
// Go-side destination handling end-to-end against a live LPAR, on both
// the cache-miss path and the package-cache fast path.
package conformance

import (
	"bytes"
	"context"
	"database/sql"
	"math/big"
	"testing"
	"time"
)

// Per-run-unique names for the v0.7.28 OUT-type stored-proc fixtures.
// Distinct trailing letters from the M9 procs (I/L/N/R) and tables
// (A/W/V), same self-isolation rationale as the rest of the suite.
var (
	pOutByte   = "G" + runToken + "B" // INOUT + OUT VARBINARY
	pOutBin    = "G" + runToken + "F" // OUT fixed BINARY
	pOutDate   = "G" + runToken + "D" // IN/OUT/INOUT DATE
	pOutTime   = "G" + runToken + "T" // IN/OUT TIME
	pOutTs     = "G" + runToken + "S" // IN/OUT TIMESTAMP
	pOutDec    = "G" + runToken + "E" // IN/OUT/INOUT DECIMAL
	pOutCombo  = "G" + runToken + "K" // OUT VARBINARY + DECIMAL (cache-hit)
	pOutDateDv = "G" + runToken + "M" // OUT DATE (temporal-divert test)
)

// createProc runs CREATE OR REPLACE PROCEDURE, skipping cleanly on the
// shared-LPAR environmental failures (storage exhaustion / no authority)
// that the free-tier PUB400 profile hits, and registers a DROP cleanup.
func createProc(t *testing.T, db *sql.DB, name, signatureBody string) {
	t.Helper()
	full := procLibrary + "." + name
	stmt := "CREATE OR REPLACE PROCEDURE " + full + " " + signatureBody
	if _, err := db.Exec(stmt); err != nil {
		if isEnvProcErr(err) {
			t.Skipf("CREATE PROCEDURE %s failed (environmental -- shared LPAR storage/auth): %v", name, err)
		}
		t.Fatalf("CREATE PROCEDURE %s: %v", name, err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP PROCEDURE " + full) })
}

// isEnvProcErr reports whether err is one of the shared-environment
// failures (storage full on QSQCRTT, or no CREATE authority) that
// should skip rather than fail the suite.
func isEnvProcErr(err error) bool {
	s := err.Error()
	for _, sub := range []string{"904", "57011", "QSQCRTT", "42501", "SQL-552", "42502"} {
		if bytes.Contains([]byte(s), []byte(sub)) {
			return true
		}
	}
	return false
}

func setUpOutTypeProcs(t *testing.T, db *sql.DB) {
	t.Helper()
	createProc(t, db, pOutByte,
		"(INOUT P_IO VARBINARY(32), OUT P_ECHO VARBINARY(32)) "+
			"BEGIN SET P_ECHO = P_IO; SET P_IO = CAST(X'DEADBEEF' AS VARBINARY(32)); END")
	createProc(t, db, pOutBin,
		"(IN P_IN BINARY(8), OUT P_OUT BINARY(8)) "+
			"BEGIN SET P_OUT = P_IN; END")
	createProc(t, db, pOutDate,
		"(IN P_IN DATE, OUT P_OUT DATE, INOUT P_IO DATE) "+
			"BEGIN SET P_OUT = P_IN; SET P_IO = P_IO + 1 DAY; END")
	createProc(t, db, pOutTime,
		"(IN P_IN TIME, OUT P_OUT TIME) "+
			"BEGIN SET P_OUT = P_IN; END")
	createProc(t, db, pOutTs,
		"(IN P_IN TIMESTAMP, OUT P_OUT TIMESTAMP) "+
			"BEGIN SET P_OUT = P_IN; END")
	createProc(t, db, pOutDec,
		"(IN P_IN DECIMAL(15,4), OUT P_OUT DECIMAL(15,4), INOUT P_IO DECIMAL(15,4), "+
			"IN P_IINT DECIMAL(15,0), OUT P_OINT DECIMAL(15,0)) "+
			"BEGIN SET P_OUT = P_IN; SET P_IO = P_IO + 1; SET P_OINT = P_IINT; END")
}

// TestCallOutTypedDestinations is the cache-miss live test: it calls
// each fixture procedure and confirms the OUT/INOUT values write back
// into *[]byte, *time.Time, and big.Rat/big.Int destinations.
func TestCallOutTypedDestinations(t *testing.T) {
	db := openDB(t)
	setUpOutTypeProcs(t, db)
	callPrefix := "CALL " + procLibrary + "."

	t.Run("varbinary inout+out", func(t *testing.T) {
		io := []byte{0x0A, 0x0B, 0x0C}
		var echo []byte
		if _, err := db.Exec(callPrefix+pOutByte+"(?, ?)",
			sql.Out{Dest: &io, In: true},
			sql.Out{Dest: &echo},
		); err != nil {
			t.Fatalf("CALL %s: %v", pOutByte, err)
		}
		// OUT P_ECHO proves the INOUT IN-side []byte was delivered.
		if !bytes.Equal(echo, []byte{0x0A, 0x0B, 0x0C}) {
			t.Errorf("OUT echo = % X, want 0A 0B 0C", echo)
		}
		// INOUT P_IO proves write-back delivered the server's new value.
		if !bytes.Equal(io, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
			t.Errorf("INOUT io = % X, want DE AD BE EF", io)
		}
	})

	t.Run("fixed binary out", func(t *testing.T) {
		in := []byte{1, 2, 3, 4, 5, 6, 7, 8}
		var out []byte
		if _, err := db.Exec(callPrefix+pOutBin+"(?, ?)",
			in,
			sql.Out{Dest: &out},
		); err != nil {
			t.Fatalf("CALL %s: %v", pOutBin, err)
		}
		if !bytes.Equal(out, in) {
			t.Errorf("OUT binary = % X, want % X", out, in)
		}
	})

	t.Run("date out+inout", func(t *testing.T) {
		din := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
		var dout time.Time
		dio := din
		if _, err := db.Exec(callPrefix+pOutDate+"(?, ?, ?)",
			din,
			sql.Out{Dest: &dout},
			sql.Out{Dest: &dio, In: true},
		); err != nil {
			t.Fatalf("CALL %s: %v", pOutDate, err)
		}
		if got := dout.Format("2006-01-02"); got != "2026-05-07" {
			t.Errorf("OUT date = %s, want 2026-05-07", got)
		}
		if got := dio.Format("2006-01-02"); got != "2026-05-08" {
			t.Errorf("INOUT date (+1 day) = %s, want 2026-05-08", got)
		}
	})

	t.Run("time out", func(t *testing.T) {
		tin := time.Date(2026, 5, 7, 14, 23, 45, 0, time.UTC)
		var tout time.Time
		if _, err := db.Exec(callPrefix+pOutTime+"(?, ?)",
			tin,
			sql.Out{Dest: &tout},
		); err != nil {
			t.Fatalf("CALL %s: %v", pOutTime, err)
		}
		if got := tout.Format("15:04:05"); got != "14:23:45" {
			t.Errorf("OUT time = %s, want 14:23:45", got)
		}
	})

	t.Run("timestamp out", func(t *testing.T) {
		sin := time.Date(2026, 5, 7, 14, 23, 45, 123456000, time.UTC)
		var sout time.Time
		if _, err := db.Exec(callPrefix+pOutTs+"(?, ?)",
			sin,
			sql.Out{Dest: &sout},
		); err != nil {
			t.Fatalf("CALL %s: %v", pOutTs, err)
		}
		const layout = "2006-01-02 15:04:05.000000"
		if got, want := sout.Format(layout), sin.Format(layout); got != want {
			t.Errorf("OUT timestamp = %s, want %s", got, want)
		}
	})

	t.Run("decimal out+inout+int", func(t *testing.T) {
		inDec := big.NewRat(617, 50) // 12.34
		var outRat *big.Rat          // pointer form -> allocated by write-back
		ioRat := new(big.Rat)
		ioRat.SetString("100.5")
		iint := big.NewInt(42)
		var oint big.Int // value form

		if _, err := db.Exec(callPrefix+pOutDec+"(?, ?, ?, ?, ?)",
			inDec,
			sql.Out{Dest: &outRat},
			sql.Out{Dest: ioRat, In: true},
			iint,
			sql.Out{Dest: &oint},
		); err != nil {
			t.Fatalf("CALL %s: %v", pOutDec, err)
		}
		if outRat == nil || outRat.FloatString(4) != "12.3400" {
			t.Errorf("OUT decimal (*big.Rat) = %v, want 12.3400", outRat)
		}
		if ioRat.FloatString(1) != "101.5" {
			t.Errorf("INOUT decimal (+1) = %s, want 101.5", ioRat.FloatString(1))
		}
		if oint.Int64() != 42 {
			t.Errorf("OUT integer-decimal (*big.Int) = %s, want 42", oint.String())
		}
	})
}

// TestCallOutTypedCacheHitAgreement files a CALL with *[]byte and
// *big.Rat OUT destinations across the 3-PREPARE threshold, then
// dispatches the same CALL on a fresh package-cache connection: it must
// take the cache-hit fast path AND decode the OUT values identically to
// the cache-miss reference. Mirrors TestCacheHit_CriteriaExtended_OutCallDispatches
// (the v0.7.8 OUT cache-hit pin) but exercises the v0.7.28 typed dests.
//
// Temporal (*time.Time) OUT is deliberately excluded here -- a DATE/TIME/
// TIMESTAMP OUT param raises SQL-180 under the package fast path, so the
// driver diverts those CALLs to the regular path; that diversion is
// pinned by TestCallOutTemporalDivertsFromPackageCache instead.
func TestCallOutTypedCacheHitAgreement(t *testing.T) {
	requireFiling(t)

	setupDB := openDB(t)
	createProc(t, setupDB, pOutCombo,
		"(IN P_S VARCHAR(10), OUT P_B VARBINARY(16), OUT P_N DECIMAL(15,4)) "+
			"BEGIN "+
			"SET P_B = CAST(X'0102030405' AS VARBINARY(16)); "+
			"SET P_N = 12.3400; "+
			"END")
	setupDB.Close()

	wipeDB := openDB(t)
	wipePackage(t, wipeDB, cachePackageName)
	wipeDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	callSQL := "CALL " + procLibrary + "." + pOutCombo + "(?, ?, ?)"

	wantBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	const wantDec = "12.3400"

	// 1. Warm-up filing on a pinned conn so all PREPAREs land on one
	//    QZDASOINIT job and the 3-PREPARE threshold crosses.
	warm, _ := openDBWithPackageCache(t, "extended")
	warmConn, err := warm.Conn(ctx)
	if err != nil {
		t.Fatalf("warm conn: %v", err)
	}
	for i := 0; i < filingPrepareCount; i++ {
		var b []byte
		var n *big.Rat
		if _, err := warmConn.ExecContext(ctx, callSQL,
			"x",
			sql.Out{Dest: &b},
			sql.Out{Dest: &n},
		); err != nil {
			warmConn.Close()
			t.Fatalf("warm CALL iter %d: %v", i, err)
		}
		if i == 0 {
			// Cache-miss reference (regular path) -- if this is wrong
			// the feature is broken before caching even enters.
			if !bytes.Equal(b, wantBytes) {
				warmConn.Close()
				t.Fatalf("warm iter 0 OUT bytes = % X, want % X (cache-miss path broken)", b, wantBytes)
			}
			if n == nil || n.FloatString(4) != wantDec {
				warmConn.Close()
				t.Fatalf("warm iter 0 OUT decimal = %v, want %s", n, wantDec)
			}
		}
	}
	warmConn.Close()
	warm.Close()

	// 2. Fresh conn -> RETURN_PACKAGE downloads the filed PMF including
	//    the OUT direction bytes + declared types for slots 2..3.
	db, buf := openDBWithPackageCache(t, "extended")
	defer db.Close()

	var b []byte
	var n *big.Rat
	if _, err := db.ExecContext(ctx, callSQL,
		"x",
		sql.Out{Dest: &b},
		sql.Out{Dest: &n},
	); err != nil {
		t.Fatalf("cache-hit CALL: %v", err)
	}
	if !bytes.Equal(b, wantBytes) {
		t.Errorf("cache-hit OUT bytes = % X, want % X", b, wantBytes)
	}
	if n == nil || n.FloatString(4) != wantDec {
		t.Errorf("cache-hit OUT decimal = %v, want %s", n, wantDec)
	}
	// The cache-hit slog line must have fired.
	expectCacheHit(t, buf, cacheHitExecMsg)
}

// TestCallOutTemporalDivertsFromPackageCache pins the v0.7.28
// auto-fallback: a CALL with a *time.Time OUT parameter, issued on a
// package-cache (extended-dynamic) connection, must succeed via the
// regular PREPARE_DESCRIBE path and never dispatch through the cache
// (a temporal OUT param under the package fast path raises SQL-180).
// Even past the filing threshold the statement is never filed, so no
// cache-hit line is ever emitted.
func TestCallOutTemporalDivertsFromPackageCache(t *testing.T) {
	requireFiling(t)

	setupDB := openDB(t)
	createProc(t, setupDB, pOutDateDv,
		"(IN P_IN DATE, OUT P_OUT DATE) BEGIN SET P_OUT = P_IN; END")
	setupDB.Close()

	wipeDB := openDB(t)
	wipePackage(t, wipeDB, cachePackageName)
	wipeDB.Close()

	db, buf := openDBWithPackageCache(t, "extended")
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	callSQL := "CALL " + procLibrary + "." + pOutDateDv + "(?, ?)"
	din := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)

	// Call past the filing threshold: every call must succeed via the
	// regular path and return the right date.
	for i := 0; i < filingPrepareCount+1; i++ {
		var dout time.Time
		if _, err := db.ExecContext(ctx, callSQL, din, sql.Out{Dest: &dout}); err != nil {
			t.Fatalf("temporal OUT CALL under package cache iter %d: %v", i, err)
		}
		if got := dout.Format("2006-01-02"); got != "2026-05-07" {
			t.Fatalf("iter %d OUT date = %s, want 2026-05-07", i, got)
		}
	}
	// The temporal-OUT diversion must have kept this CALL off the fast
	// path entirely -- no cache-hit line.
	expectNoCacheHit(t, buf)
}
