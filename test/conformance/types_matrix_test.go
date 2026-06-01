//go:build conformance

// types_matrix_test.go is the systematic data-type round-trip matrix
// for issue #6 (Track B). It exercises every SQL type the driver
// supports end-to-end -- read decode plus parameter bind -- with NULL,
// boundary, and representative values, across the CCSIDs that matter on
// a live IBM i target.
//
// Bind reality (see driver/stmt.go bindArgsToPreparedParams): the
// database/sql layer only ever emits a narrow set of wire SQL types --
// int64->BIGINT, float64->DOUBLE, bool->SMALLINT, string->VARCHAR,
// []byte->VARCHAR FOR BIT DATA, time.Time->TIMESTAMP, nil->NULL, and
// *LOBValue->locator. The native DECIMAL/NUMERIC/DECFLOAT/DATE/TIME and
// GRAPHIC IN-binds are NOT selected by the driver; a Go value lands in
// such a column only because the *server* implicitly casts the VARCHAR/
// numeric bind to the column type. These tests therefore assert the
// realistic contract -- "the column accepts the Go bind and the value
// round-trips" -- rather than "the driver emitted the native wire type."
//
// All tables use the GOSQL_ prefix (mkMatrixTable drops on entry and
// registers a Cleanup drop), so the suite stays idempotent. Tests that
// need a feature the profile may lack (DBCS graphic columns, native
// BOOLEAN) t.Skip on CREATE failure rather than fail, mirroring the
// existing graphic/DBCLOB tests.
package conformance

import (
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"
)

// mkMatrixTable drops and (re)creates schema().GOSQL_<suffix> with a
// single nullable value column `v` of the given DDL type, plus an
// INTEGER primary key `id`. It registers a Cleanup drop. When CREATE
// fails and skipOnErr is set -- the convention for DBCS-graphic and
// native-BOOLEAN columns that some profiles/VRMs cannot host -- the test
// skips cleanly instead of failing.
//
// suffix must stay <= 4 chars: GOSQL_ (6) + suffix must fit IBM i's
// 10-char system-name limit.
func mkMatrixTable(t *testing.T, db *sql.DB, suffix, valDDL string, skipOnErr bool) string {
	t.Helper()
	tbl := schema() + "." + tablePrefix + suffix
	db.Exec("DROP TABLE " + tbl)
	ddl := fmt.Sprintf("CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, v %s)", tbl, valDDL)
	if _, err := db.Exec(ddl); err != nil {
		if skipOnErr {
			t.Skipf("CREATE TABLE (v %s) failed (feature unavailable on this profile?): %v", valDDL, err)
		}
		t.Fatalf("CREATE TABLE (v %s): %v", valDDL, err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE " + tbl) })
	return tbl
}

// scalarCase is one row of a single-column round-trip table: bind is
// inserted via a ?-marker (nil => SQL NULL), scan reads the column back
// and asserts. The case name doubles as the subtest name.
type scalarCase struct {
	name string
	bind any
	scan func(t *testing.T, row *sql.Row)
}

// runScalarCases inserts every case into tbl (id = ordinal) then reads
// each back under its own subtest. Inserting all rows before reading
// keeps a per-case failure localized to the read assertion.
func runScalarCases(t *testing.T, db *sql.DB, tbl string, cases []scalarCase) {
	t.Helper()
	for i, c := range cases {
		if _, err := db.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", i+1, c.bind); err != nil {
			t.Fatalf("case %q: INSERT: %v", c.name, err)
		}
	}
	for i, c := range cases {
		i, c := i, c
		t.Run(c.name, func(t *testing.T) {
			c.scan(t, db.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", i+1))
		})
	}
}

// ---- scan/assert factories -------------------------------------------

func wantInt64(want int64) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got int64
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan int64: %v", err)
		}
		if got != want {
			t.Errorf("int64 round-trip: got %d, want %d", got, want)
		}
	}
}

func wantNullInt() func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var n sql.NullInt64
		if err := row.Scan(&n); err != nil {
			t.Fatalf("scan NullInt64: %v", err)
		}
		if n.Valid {
			t.Errorf("expected SQL NULL, got %d", n.Int64)
		}
	}
}

func wantFloat(want, tol float64) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got float64
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan float64: %v", err)
		}
		d := got - want
		if d < 0 {
			d = -d
		}
		if d > tol {
			t.Errorf("float round-trip: got %v, want %v (+/- %v)", got, want, tol)
		}
	}
}

func wantNullFloat() func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var n sql.NullFloat64
		if err := row.Scan(&n); err != nil {
			t.Fatalf("scan NullFloat64: %v", err)
		}
		if n.Valid {
			t.Errorf("expected SQL NULL, got %v", n.Float64)
		}
	}
}

// decEqual compares two decimal renderings by numeric value, so the
// driver's scale-padded / scientific-notation output (e.g. "0" stored
// in DECIMAL(7,2) reads back "0.00"; DECFLOAT "150000" reads "1.5E+5")
// compares equal to the bound value. Non-finite DECFLOAT specials
// (Infinity/NaN) fall back to a case-insensitive string match.
func decEqual(t *testing.T, got, want string) {
	t.Helper()
	got = strings.TrimSpace(got)
	gr, gok := new(big.Rat).SetString(got)
	wr, wok := new(big.Rat).SetString(want)
	if gok && wok {
		if gr.Cmp(wr) != 0 {
			t.Errorf("decimal value: got %q, want %q (numerically unequal)", got, want)
		}
		return
	}
	if !strings.EqualFold(got, want) {
		t.Errorf("decimal: got %q, want %q", got, want)
	}
}

func wantDec(want string) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got string
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan decimal string: %v", err)
		}
		decEqual(t, got, want)
	}
}

func wantStr(want string) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got string
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan string: %v", err)
		}
		if got != want {
			t.Errorf("string round-trip: got %q, want %q", got, want)
		}
	}
}

// wantStrTrim trims trailing spaces before comparing -- fixed CHAR /
// GRAPHIC columns are blank-padded to their declared width on read.
func wantStrTrim(want string) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got string
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan string: %v", err)
		}
		if g := strings.TrimRight(got, " "); g != want {
			t.Errorf("fixed-width round-trip: got %q (trimmed %q), want %q", got, g, want)
		}
	}
}

// wantNull asserts the column reads back as SQL NULL. A NullString works
// for any column type that scans into a string/[]byte (decimal, char,
// binary, LOB) -- we only check Valid.
func wantNull() func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var n sql.NullString
		if err := row.Scan(&n); err != nil {
			t.Fatalf("scan NULL: %v", err)
		}
		if n.Valid {
			t.Errorf("expected SQL NULL, got %q", n.String)
		}
	}
}

func wantBytes(want []byte) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got []byte
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan []byte: %v", err)
		}
		if fmt.Sprintf("%x", got) != fmt.Sprintf("%x", want) {
			t.Errorf("bytes round-trip: got %x, want %x", got, want)
		}
	}
}

func wantBool(want bool) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got bool
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan bool: %v", err)
		}
		if got != want {
			t.Errorf("bool round-trip: got %v, want %v", got, want)
		}
	}
}

func wantNullBool() func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var n sql.NullBool
		if err := row.Scan(&n); err != nil {
			t.Fatalf("scan NullBool: %v", err)
		}
		if n.Valid {
			t.Errorf("expected SQL NULL, got %v", n.Bool)
		}
	}
}

func wantTimeEqual(want time.Time) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got time.Time
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan time.Time: %v", err)
		}
		if !got.Equal(want) {
			t.Errorf("timestamp round-trip: got %v, want %v", got, want)
		}
	}
}

func wantNullTime() func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var n sql.NullTime
		if err := row.Scan(&n); err != nil {
			t.Fatalf("scan NullTime: %v", err)
		}
		if n.Valid {
			t.Errorf("expected SQL NULL, got %v", n.Time)
		}
	}
}

func wantDateYMD(y int, mo time.Month, d int) func(*testing.T, *sql.Row) {
	return func(t *testing.T, row *sql.Row) {
		t.Helper()
		var got time.Time
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan date: %v", err)
		}
		if got.Year() != y || got.Month() != mo || got.Day() != d {
			t.Errorf("DATE = %v, want %04d-%02d-%02d", got, y, int(mo), d)
		}
	}
}

// ---- integer types ----------------------------------------------------

// TestTypeMatrixIntegers round-trips SMALLINT / INTEGER / BIGINT at their
// signed boundaries, zero, and a representative value, plus NULL. The
// driver binds every Go int as BIGINT and the server narrows it to the
// column type, so the boundary cases also prove the server-side narrowing
// cast accepts the full declared range.
func TestTypeMatrixIntegers(t *testing.T) {
	db := openDB(t)

	t.Run("SMALLINT", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "isi", "SMALLINT", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"min", int64(-32768), wantInt64(-32768)},
			{"max", int64(32767), wantInt64(32767)},
			{"zero", int64(0), wantInt64(0)},
			{"neg", int64(-1), wantInt64(-1)},
			{"null", nil, wantNullInt()},
		})
	})

	t.Run("INTEGER", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "iin", "INTEGER", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"min", int64(-2147483648), wantInt64(-2147483648)},
			{"max", int64(2147483647), wantInt64(2147483647)},
			{"zero", int64(0), wantInt64(0)},
			{"null", nil, wantNullInt()},
		})
	})

	t.Run("BIGINT", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "ibi", "BIGINT", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"min", int64(-9223372036854775808), wantInt64(-9223372036854775808)},
			{"max", int64(9223372036854775807), wantInt64(9223372036854775807)},
			{"zero", int64(0), wantInt64(0)},
			{"null", nil, wantNullInt()},
		})
	})
}

// ---- floating types ---------------------------------------------------

// TestTypeMatrixFloats round-trips REAL and DOUBLE. Values are chosen to
// be exactly representable so equality holds within a tight tolerance
// (REAL is single-precision on the wire; the driver widens it to
// float64, so the tolerance is the float32 step at that magnitude).
func TestTypeMatrixFloats(t *testing.T) {
	db := openDB(t)

	t.Run("REAL", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "fre", "REAL", false)
		const tol = 1e-3
		runScalarCases(t, db, tbl, []scalarCase{
			{"zero", float64(0), wantFloat(0, tol)},
			{"half", float64(0.5), wantFloat(0.5, tol)},
			{"neg", float64(-2.25), wantFloat(-2.25, tol)},
			{"pow2", float64(16777216), wantFloat(16777216, 1)}, // 2^24, exact in float32
			{"null", nil, wantNullFloat()},
		})
	})

	t.Run("DOUBLE", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "fdb", "DOUBLE", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"zero", float64(0), wantFloat(0, 0)},
			{"pi", float64(3.141592653589793), wantFloat(3.141592653589793, 1e-12)},
			{"neg", float64(-2.5), wantFloat(-2.5, 0)},
			{"big", float64(1e300), wantFloat(1e300, 1e288)},
			{"small", float64(1e-300), wantFloat(1e-300, 1e-312)},
			{"null", nil, wantNullFloat()},
		})
	})
}

// ---- fixed/floating decimal -------------------------------------------

// TestTypeMatrixDecimal round-trips DECIMAL (packed BCD), NUMERIC (zoned
// BCD), and DECFLOAT(16)/DECFLOAT(34). The driver returns all four as
// strings; values are bound as strings (server-cast from VARCHAR) and
// compared numerically (decEqual), so scale padding and scientific
// notation don't cause spurious mismatches. DECFLOAT(34) additionally
// covers the special values via SQL literals.
func TestTypeMatrixDecimal(t *testing.T) {
	db := openDB(t)

	t.Run("DECIMAL", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "dec", "DECIMAL(7,2)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"pos", "123.45", wantDec("123.45")},
			{"neg", "-9999.99", wantDec("-9999.99")},
			{"scale_pad", "12.5", wantDec("12.50")},
			{"zero", "0", wantDec("0.00")},
			{"null", nil, wantNull()},
		})
	})

	t.Run("DECIMAL_max_precision", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "dmx", "DECIMAL(31,0)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"max", "9999999999999999999999999999999", wantDec("9999999999999999999999999999999")},
			{"negmax", "-9999999999999999999999999999999", wantDec("-9999999999999999999999999999999")},
			{"null", nil, wantNull()},
		})
	})

	t.Run("NUMERIC", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "num", "NUMERIC(7,2)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"pos", "9876.54", wantDec("9876.54")},
			{"neg", "-1.20", wantDec("-1.20")},
			{"zero", "0", wantDec("0.00")},
			{"null", nil, wantNull()},
		})
	})

	t.Run("DECFLOAT16", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "d16", "DECFLOAT(16)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"sci", "1.5E+5", wantDec("150000")},
			{"frac", "0.001", wantDec("0.001")},
			{"neg", "-42.5", wantDec("-42.5")},
			{"null", nil, wantNull()},
		})
	})

	t.Run("DECFLOAT34", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "d34", "DECFLOAT(34)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"wide", "123456789012345678901234567890.123", wantDec("123456789012345678901234567890.123")},
			{"neg", "-0.0000001", wantDec("-0.0000001")},
			{"null", nil, wantNull()},
		})
	})

	// DECFLOAT34_high_precision pins the recommended lossless path for
	// high-precision decimals (issue #12). A value with far more
	// significant digits than an IEEE-754 float64 can represent
	// (float64 caps at ~15-17) round-trips exactly when bound as a
	// *string*: it ships as VARCHAR and the server casts the text
	// straight to DECFLOAT(34). The same number bound as a float64
	// would route through DOUBLE and be silently truncated -- e.g.
	// float64(pi) keeps only "3.141592653589793". String binds are the
	// only lossless path into DECIMAL(p,s) / DECFLOAT columns.
	t.Run("DECFLOAT34_high_precision", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "dhp", "DECFLOAT(34)", false)
		// pi to 30 significant figures -- ~13 digits past float64's
		// limit, comfortably within DECFLOAT(34)'s 34-digit coefficient.
		const piHP = "3.14159265358979323846264338327"
		runScalarCases(t, db, tbl, []scalarCase{
			{"pi30", piHP, wantDec(piHP)},
			{"null", nil, wantNull()},
		})
	})

	// DECFLOAT special values can't be reached through a VARCHAR bind
	// cast, so they go in as SQL literals. The driver decodes them to
	// the canonical "Infinity"/"-Infinity"/"NaN" spellings (signaling
	// NaN normalizes to NaN on read).
	t.Run("DECFLOAT34_specials", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "dsp", "DECFLOAT(34)", false)
		specials := []struct {
			lit, want string
		}{
			{"INFINITY", "Infinity"},
			{"-INFINITY", "-Infinity"},
			{"NAN", "NaN"},
			{"SNAN", "NaN"},
		}
		for i, sp := range specials {
			if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, v) VALUES (?, %s)", tbl, sp.lit), i+1); err != nil {
				t.Fatalf("INSERT literal %s: %v", sp.lit, err)
			}
		}
		for i, sp := range specials {
			i, sp := i, sp
			t.Run(sp.lit, func(t *testing.T) {
				var got string
				if err := db.QueryRow("SELECT v FROM "+tbl+" WHERE id = ?", i+1).Scan(&got); err != nil {
					t.Fatalf("scan: %v", err)
				}
				if got != sp.want {
					t.Errorf("DECFLOAT %s: got %q, want %q", sp.lit, got, sp.want)
				}
			})
		}
	})
}

// ---- character types across CCSIDs ------------------------------------

// TestTypeMatrixCharCCSID round-trips VARCHAR and fixed CHAR across the
// CCSIDs the driver decodes faithfully: 37 (US EBCDIC), 273 (German
// EBCDIC, exercising the divergent code points), and 1208 (UTF-8, the
// V7R3+ bind auto-pick, exercising multibyte + non-BMP). Columns pin the
// CCSID at CREATE time so the round-trip is deterministic regardless of
// the job's default CCSID.
func TestTypeMatrixCharCCSID(t *testing.T) {
	db := openDB(t)

	type ccsidSpec struct {
		suffix string
		ccsid  int
		// extra is a CCSID-specific rich string (empty = skip the case).
		extra string
	}
	specs := []ccsidSpec{
		{"vc37", 37, ""},
		{"vc73", 273, "Größe äöü ÄÖÜ"}, // Größe äöü ÄÖÜ
		{"vcu8", 1208, "café — π ☃ 日本語 \U0001D11E"},
	}
	for _, sp := range specs {
		sp := sp
		t.Run(fmt.Sprintf("VARCHAR_ccsid%d", sp.ccsid), func(t *testing.T) {
			tbl := mkMatrixTable(t, db, sp.suffix, fmt.Sprintf("VARCHAR(64) CCSID %d", sp.ccsid), false)
			cases := []scalarCase{
				{"ascii", "Hello, IBM i!", wantStr("Hello, IBM i!")},
				{"empty", "", wantStr("")},
				{"maxlen", strings.Repeat("A", 64), wantStr(strings.Repeat("A", 64))},
				{"null", nil, wantNull()},
			}
			if sp.extra != "" {
				cases = append(cases, scalarCase{"rich", sp.extra, wantStr(sp.extra)})
			}
			runScalarCases(t, db, tbl, cases)
		})
	}

	// Fixed CHAR(n) is blank-padded to its width on read; trim to compare
	// content, then lock in the padded width.
	t.Run("CHAR_fixed_width", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "cfix", "CHAR(20) CCSID 1208", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"padded", "fixed", wantStrTrim("fixed")},
			{"empty", "", wantStrTrim("")},
			{"null", nil, wantNull()},
		})
		// Width check on the padded value.
		var got string
		if err := db.QueryRow("SELECT v FROM " + tbl + " WHERE id = 1").Scan(&got); err != nil {
			t.Fatalf("scan width: %v", err)
		}
		if len([]rune(got)) != 20 {
			t.Errorf("CHAR(20) width = %d runes, want 20 (space-padded)", len([]rune(got)))
		}
	})
}

// ---- binary types -----------------------------------------------------

// TestTypeMatrixBinary round-trips the FOR BIT DATA (CCSID 65535) and
// native BINARY/VARBINARY types as raw []byte. Variable widths cover the
// empty-slice boundary; fixed widths use full-width values so the test
// does not depend on the pad byte. NULL reads round-trip as SQL NULL.
func TestTypeMatrixBinary(t *testing.T) {
	db := openDB(t)

	full8 := []byte{0x00, 0x11, 0x22, 0x33, 0xCC, 0xDD, 0xEE, 0xFF}

	t.Run("VARCHAR_FOR_BIT_DATA", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "bfb", "VARCHAR(32) FOR BIT DATA", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"bytes", []byte{0xDE, 0xAD, 0xBE, 0xEF}, wantBytes([]byte{0xDE, 0xAD, 0xBE, 0xEF})},
			{"empty", []byte{}, wantBytes([]byte{})},
			{"full", make([]byte, 32), wantBytes(make([]byte, 32))},
			{"null", nil, wantNull()},
		})
	})

	t.Run("VARBINARY", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "bvb", "VARBINARY(32)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"bytes", []byte{0x01, 0x02, 0x03, 0x04}, wantBytes([]byte{0x01, 0x02, 0x03, 0x04})},
			{"empty", []byte{}, wantBytes([]byte{})},
			// A direct nil ?-bind now round-trips as SQL NULL: the
			// driver adopts the column's declared shape from the
			// parameter-marker format instead of the INTEGER-NULL
			// marker the server refused to cast to a native binary
			// column (issue #11).
			{"null", nil, wantNull()},
		})
	})

	t.Run("BINARY_fixed", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "bbn", "BINARY(8)", false)
		runScalarCases(t, db, tbl, []scalarCase{
			{"full", full8, wantBytes(full8)},
			// Direct nil ?-bind into a fixed native BINARY column now
			// round-trips as SQL NULL (issue #11).
			{"null", nil, wantNull()},
		})
	})
}

// ---- temporal types ---------------------------------------------------

// TestTypeMatrixTemporal round-trips DATE, TIME, and TIMESTAMP. The
// driver surfaces all three as time.Time on read. TIMESTAMP additionally
// exercises the native time.Time bind path (the only temporal type the
// driver binds natively, as SQL type 393); DATE/TIME are bound as ISO
// strings the server casts. Microsecond precision is the wire limit, so
// the bound time.Time avoids sub-microsecond nanoseconds.
func TestTypeMatrixTemporal(t *testing.T) {
	db := openDB(t)

	t.Run("DATE", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "tdt", "DATE", false)
		// Stay inside 1940-2039 so the case is independent of the
		// session date format -- *MDY/*DMY/*YMD only span those years,
		// and year-0001/9999 extremes round-trip inconsistently across
		// format configs.
		runScalarCases(t, db, tbl, []scalarCase{
			{"iso", "2026-05-11", wantDateYMD(2026, time.May, 11)},
			{"past", "1985-04-12", wantDateYMD(1985, time.April, 12)},
			{"null", nil, wantNullTime()},
		})
	})

	t.Run("TIME", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "ttm", "TIME", false)
		clock := func(h, m, s int) func(*testing.T, *sql.Row) {
			return func(t *testing.T, row *sql.Row) {
				var got time.Time
				if err := row.Scan(&got); err != nil {
					t.Fatalf("scan: %v", err)
				}
				if got.Hour() != h || got.Minute() != m || got.Second() != s {
					t.Errorf("TIME = %02d:%02d:%02d, want %02d:%02d:%02d",
						got.Hour(), got.Minute(), got.Second(), h, m, s)
				}
			}
		}
		runScalarCases(t, db, tbl, []scalarCase{
			{"midday", "14:30:00", clock(14, 30, 0)},
			{"midnight", "00:00:00", clock(0, 0, 0)},
			{"eod", "23:59:59", clock(23, 59, 59)},
			{"null", nil, wantNullTime()},
		})
	})

	t.Run("TIMESTAMP", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "tts", "TIMESTAMP", false)
		tm := time.Date(2026, 5, 11, 14, 30, 0, 123456000, time.UTC)
		runScalarCases(t, db, tbl, []scalarCase{
			{"time_native", tm, wantTimeEqual(tm)},
			{"string_bind", "2026-01-02 03:04:05.000000",
				wantTimeEqual(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))},
			{"null", nil, wantNullTime()},
		})
	})
}

// ---- graphic (DBCS) parameter binds -----------------------------------

// TestTypeMatrixGraphicBind closes the issue-#3/#6 gap on the WRITE side:
// the existing graphic tests only populate graphic columns via UX'...'
// SQL literals or stored-proc OUT params. Here a plain Go string ?-bind
// lands in a scalar GRAPHIC / VARGRAPHIC / LONG VARGRAPHIC *column* (the
// server implicitly casts the VARCHAR bind to the graphic type) and
// round-trips. CCSID 1200 preserves surrogate pairs; CCSID 13488 is
// BMP-only. Skips when the profile lacks DBCS-capable NLSS.
func TestTypeMatrixGraphicBind(t *testing.T) {
	db := openDB(t)

	// U+1D11E (treble clef) is a surrogate pair on the UTF-16 wire; the
	// em-dash is a BMP non-ASCII rune.
	const bmp = "Hi — there"
	const nonBMP = "Music \U0001D11E end"

	t.Run("ccsid1200", func(t *testing.T) {
		t.Run("VARGRAPHIC", func(t *testing.T) {
			tbl := mkMatrixTable(t, db, "gv12", "VARGRAPHIC(64) CCSID 1200", true)
			runScalarCases(t, db, tbl, []scalarCase{
				{"ascii", "hello", wantStr("hello")},
				{"bmp", bmp, wantStr(bmp)},
				{"surrogate", nonBMP, wantStr(nonBMP)},
				{"empty", "", wantStr("")},
				{"null", nil, wantNull()},
			})
		})
		t.Run("LONG_VARGRAPHIC", func(t *testing.T) {
			tbl := mkMatrixTable(t, db, "gl12", "LONG VARGRAPHIC CCSID 1200", true)
			runScalarCases(t, db, tbl, []scalarCase{
				{"bmp", bmp, wantStr(bmp)},
				{"surrogate", nonBMP, wantStr(nonBMP)},
				{"null", nil, wantNull()},
			})
		})
		t.Run("GRAPHIC_fixed", func(t *testing.T) {
			tbl := mkMatrixTable(t, db, "gg12", "GRAPHIC(10) CCSID 1200", true)
			runScalarCases(t, db, tbl, []scalarCase{
				{"padded", "Hi", wantStrTrim("Hi")},
				{"null", nil, wantNull()},
			})
		})
	})

	// CCSID 13488 (strict UCS-2) rejects surrogates server-side, so this
	// leg stays BMP-only.
	t.Run("ccsid13488_bmp", func(t *testing.T) {
		tbl := mkMatrixTable(t, db, "gv34", "VARGRAPHIC(64) CCSID 13488", true)
		runScalarCases(t, db, tbl, []scalarCase{
			{"ascii", "hello", wantStr("hello")},
			{"bmp", bmp, wantStr(bmp)},
			{"null", nil, wantNull()},
		})
	})

	// CCSID 65535 (no-conversion "bit data") graphic is intentionally NOT
	// bound here. The driver exposes no IN-bind shape the server will
	// accept for such a column: a []byte bind (VARCHAR FOR BIT DATA, CCSID
	// 65535) and a string bind (VARCHAR CCSID 1208) both fail the implicit
	// cast to VARGRAPHIC CCSID 65535 with SQL-332 (character conversion not
	// defined) -- live-confirmed on PUB400 V7R5M0. Populating a 65535
	// graphic column needs a raw same-CCSID source that the database/sql
	// bind surface doesn't offer. The graphic READ path is covered by
	// TestGraphicScalarColumns; graphic support tracks issues #3 (read) /
	// #5 (bind).
}

// ---- boolean ----------------------------------------------------------

// TestTypeMatrixBoolean round-trips the native V7R5+ BOOLEAN type: a Go
// bool bind (the driver sends SMALLINT, the server casts) and NULL.
// Skips on pre-V7R5 targets where CREATE TABLE ... BOOLEAN fails.
func TestTypeMatrixBoolean(t *testing.T) {
	db := openDB(t)
	tbl := mkMatrixTable(t, db, "bln", "BOOLEAN", true)
	runScalarCases(t, db, tbl, []scalarCase{
		{"true", true, wantBool(true)},
		{"false", false, wantBool(false)},
		{"null", nil, wantNullBool()},
	})
}

// ---- LOB NULL / empty distinction -------------------------------------

// TestTypeMatrixLOBNullAndEmpty pins the empty-vs-NULL distinction on the
// character LOB types (the BLOB case is already covered by TestLOBBlob).
// An empty string is a present, zero-length value (Valid, len 0); a nil
// bind is SQL NULL (not Valid). DBCLOB skips when DBCS NLSS is absent.
func TestTypeMatrixLOBNullAndEmpty(t *testing.T) {
	db := openDB(t)

	check := func(t *testing.T, tbl string) {
		t.Helper()
		if _, err := db.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", 1, ""); err != nil {
			t.Fatalf("INSERT empty: %v", err)
		}
		if _, err := db.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", 2, nil); err != nil {
			t.Fatalf("INSERT null: %v", err)
		}
		var empty, null sql.NullString
		if err := db.QueryRow("SELECT v FROM " + tbl + " WHERE id = 1").Scan(&empty); err != nil {
			t.Fatalf("scan empty: %v", err)
		}
		if err := db.QueryRow("SELECT v FROM " + tbl + " WHERE id = 2").Scan(&null); err != nil {
			t.Fatalf("scan null: %v", err)
		}
		if !empty.Valid || empty.String != "" {
			t.Errorf("empty LOB: Valid=%v String=%q, want Valid=true len 0", empty.Valid, empty.String)
		}
		if null.Valid {
			t.Errorf("NULL LOB read back as non-NULL (%q)", null.String)
		}
	}

	t.Run("CLOB", func(t *testing.T) {
		check(t, mkMatrixTable(t, db, "lcl", "CLOB(1M) CCSID 1208", false))
	})
	t.Run("DBCLOB", func(t *testing.T) {
		check(t, mkMatrixTable(t, db, "ldb", "DBCLOB(64K) CCSID 1200", true))
	})
}
