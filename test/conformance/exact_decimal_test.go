//go:build conformance

// exact_decimal_test.go is the live round-trip coverage for the
// describe-driven exact-decimal bind funnel (issue #40, plan §P1-F).
//
// The funnel admits exact-decimal Go types -- *math/big.Rat / *big.Int /
// *big.Float and struct-shaped fmt.Stringer decimals -- that database/sql's
// default converter would otherwise reject, renders each to a canonical
// plain-decimal string, and binds it through the precision-preserving VARCHAR
// path (server-cast on cache-miss; native packed/zoned BCD from the *PGM
// parameter-marker format on cache-hit). These tests assert the values
// round-trip exactly at the DECIMAL(31,7) precision edge, that negatives and
// over-scale behave, that a non-terminating *big.Rat is refused rather than
// silently rounded, and that the cache-miss and cache-hit paths agree.
package conformance

import (
	"math/big"
	"testing"
)

// liveDecimal is a struct-shaped fmt.Stringer decimal (NOT a driver.Valuer),
// the exact shape the funnel rescues from database/sql's default converter.
// A driver.Valuer decimal (e.g. shopspring/decimal) already binds via Value()
// and a string-kind decimal binds via the default String-kind path; both are
// covered implicitly by the existing string-bind matrix, so this local type
// targets the genuinely new admission path.
type liveDecimal struct{ s string }

func (d liveDecimal) String() string { return d.s }

// TestExactDecimalBind round-trips exact-decimal Go types through the
// cache-miss bind path (INSERT ? -> server-cast VARCHAR -> DECIMAL/NUMERIC),
// reading each back as a string and comparing by numeric value.
func TestExactDecimalBind(t *testing.T) {
	db := openDB(t)

	t.Run("DECIMAL_31_7", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "xd1", "DECIMAL(31,7)", false)
		// 24 integer digits (31-7) at full scale is the precision edge.
		const edgePos = "999999999999999999999999.1234567"
		const edgeNeg = "-999999999999999999999999.1234567"
		runScalarCases(t, db, tbl, []scalarCase{
			{"stringer_edge_pos", liveDecimal{edgePos}, wantDec(edgePos)},
			{"stringer_edge_neg", liveDecimal{edgeNeg}, wantDec(edgeNeg)},
			{"stringer_small_frac", liveDecimal{"-0.0000007"}, wantDec("-0.0000007")},
			{"bigrat_three_halves", big.NewRat(3, 2), wantDec("1.5")},
			{"bigrat_eighth", big.NewRat(1, 8), wantDec("0.125")},
			{"bigrat_neg", big.NewRat(-7, 4), wantDec("-1.75")},
			{"bigint", big.NewInt(-42), wantDec("-42")},
			{"bigfloat", big.NewFloat(2.75), wantDec("2.75")},
		})
	})

	// Over-scale: more fractional digits than the column scale. The bound
	// string carries all 8 fractional digits; the server applies its
	// VARCHAR->DECIMAL assignment cast (truncation toward zero, the
	// ROUND_DOWN-equivalent JT400 also uses) to fit scale 7. We assert the
	// truncated form; the matching cache-hit case in
	// TestExactDecimalCacheHitAgreement confirms both paths land identically.
	t.Run("DECIMAL_over_scale", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "xd2", "DECIMAL(31,7)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"truncates_to_scale", liveDecimal{"1.23456789"}, wantDec("1.2345678")},
		})
	})

	t.Run("NUMERIC_9_2", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "xn1", "NUMERIC(9,2)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"stringer", liveDecimal{"-1234567.89"}, wantDec("-1234567.89")},
			{"bigrat", big.NewRat(5, 2), wantDec("2.5")},
			{"bigint", big.NewInt(1000), wantDec("1000")},
		})
	})

	// A *big.Rat with no terminating decimal expansion (1/3) cannot be
	// represented in a fixed-scale column without silently choosing a
	// rounding; the driver refuses the bind rather than guess.
	t.Run("bigrat_nonterminating_rejected", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "xd3", "DECIMAL(31,7)", false)
		_, err := db.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", 1, big.NewRat(1, 3))
		if err == nil {
			t.Fatalf("INSERT *big.Rat 1/3: expected error (non-terminating decimal), got nil")
		}
	})
}

// TestExactDecimalCacheHitAgreement files a DECIMAL INSERT into the *PGM, then
// on a fresh cache-enabled connection binds an exact-decimal Go value through
// the cache-hit fast path -- where the value is packed as native packed BCD
// from the *PGM parameter-marker format, not server-cast from VARCHAR -- and
// asserts it both dispatches via the cache and stores the same value the
// cache-miss path produces. This is the end-to-end exercise of the funnel's
// canonical string flowing into hostserver toDecimalString -> encodePackedBCD.
func TestExactDecimalCacheHitAgreement(t *testing.T) {
	requireFiling(t)

	// Reset the shared *PGM so a prior run's filing doesn't mask the
	// fill->cache-hit transition under test.
	wipeDB := openDB(t)
	wipePackage(t, wipeDB, cachePackageName)
	wipeDB.Close()

	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "xdch", "(id INTEGER NOT NULL, v DECIMAL(31,7))")
	insertSQL := "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)"

	// Cache-miss control: bind the exact value via the funnel and read back
	// the server-cast result as the reference string.
	const exact = "1234567.7654321" // 14 sig digits, fits DECIMAL(31,7) exactly
	if _, err := db.Exec(insertSQL, 1, liveDecimal{exact}); err != nil {
		t.Fatalf("cache-miss INSERT: %v", err)
	}
	var miss string
	if err := db.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", 1).Scan(&miss); err != nil {
		t.Fatalf("cache-miss read-back: %v", err)
	}
	decEqual(t, miss, exact)

	// File the INSERT across the 3-PREPARE threshold, then dispatch the same
	// SQL with an exact-decimal bind on a fresh connection: it must cache-hit
	// (native BCD pack) and store the same value.
	fillPackageCache(t, fillExec, insertSQL, 900, liveDecimal{exact})
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	defer db2.Close()
	if _, err := db2.Exec(insertSQL, 2, big.NewRat(3, 2)); err != nil {
		t.Fatalf("cache-hit INSERT: %v", err)
	}
	expectCacheHit(t, buf, cacheHitExecMsg)

	var hit string
	if err := db2.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", 2).Scan(&hit); err != nil {
		t.Fatalf("cache-hit read-back: %v", err)
	}
	decEqual(t, hit, "1.5")

	// And the same exact value over the cache-hit path agrees with the
	// cache-miss reference captured above.
	if _, err := db2.Exec(insertSQL, 3, liveDecimal{exact}); err != nil {
		t.Fatalf("cache-hit exact INSERT: %v", err)
	}
	var hitExact string
	if err := db2.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", 3).Scan(&hitExact); err != nil {
		t.Fatalf("cache-hit exact read-back: %v", err)
	}
	decEqual(t, hitExact, miss)
}

// TestDecimalOverScaleRoundDownCacheHitAgreement pins the v0.7.29 ROUND_DOWN
// fix end to end. An over-scale decimal (more fractional digits than the column
// scale) bound on the cache-hit fast path is now truncated toward zero in place
// by the native packed-BCD encoder -- matching the server's VARCHAR assignment
// cast on cache-miss and JT400's BigDecimal.setScale(scale, ROUND_DOWN). Before
// the fix the over-scale value wrapped ErrUnsupportedCachedParamType and fell
// back off the fast path to a fresh PREPARE, so the expectCacheHit assertion
// below is what distinguishes the fixed behavior (stays cached, same stored
// value) from the old one.
func TestDecimalOverScaleRoundDownCacheHitAgreement(t *testing.T) {
	requireFiling(t)

	// Reset the shared *PGM so a prior run's filing doesn't mask the
	// fill->cache-hit transition under test.
	wipeDB := openDB(t)
	wipePackage(t, wipeDB, cachePackageName)
	wipeDB.Close()

	db, _ := openDBWithPackageCache(t, "")
	tbl := makeCacheTestTable(t, db, "xdos", "(id INTEGER NOT NULL, v DECIMAL(15,2))")
	insertSQL := "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)"

	// Cache-miss control: the over-scale value ships as VARCHAR and the server
	// truncates it to scale 2. Capture the stored form as the reference.
	const overScale = "12.3456" // 4 fractional digits into scale 2 -> 12.34
	if _, err := db.Exec(insertSQL, 1, liveDecimal{overScale}); err != nil {
		t.Fatalf("cache-miss INSERT: %v", err)
	}
	var miss string
	if err := db.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", 1).Scan(&miss); err != nil {
		t.Fatalf("cache-miss read-back: %v", err)
	}
	decEqual(t, miss, "12.34")

	// File the INSERT across the 3-PREPARE threshold, then bind the same
	// over-scale value on a fresh cache-enabled connection.
	fillPackageCache(t, fillExec, insertSQL, 900, liveDecimal{overScale})
	db.Close()

	db2, buf := openDBWithPackageCache(t, "")
	defer db2.Close()
	if _, err := db2.Exec(insertSQL, 2, liveDecimal{overScale}); err != nil {
		t.Fatalf("cache-hit over-scale INSERT: %v", err)
	}
	// The discriminating assertion: pre-fix the over-scale bind fell back to
	// PREPARE (no cache-hit line); post-fix it stays on the fast path.
	expectCacheHit(t, buf, cacheHitExecMsg)

	var hit string
	if err := db2.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", 2).Scan(&hit); err != nil {
		t.Fatalf("cache-hit read-back: %v", err)
	}
	decEqual(t, hit, miss)

	// A negative over-scale value that truncates to all-zero digits must be
	// accepted by the server and stored as a clean zero (the packed sign nibble
	// is normalized to positive). Reset the probe buffer so the cache-hit
	// assertion reflects only this dispatch.
	buf.Reset()
	if _, err := db2.Exec(insertSQL, 3, liveDecimal{"-0.009"}); err != nil {
		t.Fatalf("cache-hit negative-zero INSERT: %v", err)
	}
	expectCacheHit(t, buf, cacheHitExecMsg)
	var negZero string
	if err := db2.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", 3).Scan(&negZero); err != nil {
		t.Fatalf("cache-hit negative-zero read-back: %v", err)
	}
	decEqual(t, negZero, "0.00")
}
