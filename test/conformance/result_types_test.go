//go:build conformance

package conformance

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// TestResultTypeCoverage is the live evidence for issue #39: the
// result-type coverage additions in the row-decode path -- ROWID,
// DATALINK, XML, NVARCHAR (a GRAPHIC CCSID-1200 type), DECFLOAT
// NaN/SNaN/Infinity specials, and a DECIMAL stored small enough to ship
// as a scaled binary integer. Each producible type is exercised in its
// own t.Run subtest so a type that the target can't create (e.g.
// DATALINK without a DataLink File Manager, or XML on a back-level
// LPAR) skips just that subtest rather than failing the suite.
//
// The whole test gates on dsn() via openDB(t). Each subtest documents,
// in a comment, the exact SQL it issues so the parent can reproduce and
// debug live. Confidence per subtest (for the parent's live run) is
// noted inline: ROWID and DATALINK wire shapes are JT400-documented but
// not fixture-captured, so they are the most likely to need a tweak;
// XML/NVARCHAR/DECFLOAT/scaled-DECIMAL are high-confidence.
func TestResultTypeCoverage(t *testing.T) {
	db := openDB(t)

	// DECFLOAT specials: CAST('NaN'/'-NaN'/'SNaN'/'Infinity'/'-Infinity'
	// AS DECFLOAT). The driver surfaces DECFLOAT as a decimal string, so
	// the specials scan into a string. JT400's DECFLOAT toString spells
	// signaling NaN as "SNaN"/"-SNaN" and quiet as "NaN"/"-NaN".
	//
	// HIGH CONFIDENCE: decode is bit-exact against AS400DecFloat.toObject.
	t.Run("DECFLOAT specials", func(t *testing.T) {
		// SQL issued, one per row:
		//   VALUES CAST('NaN'       AS DECFLOAT(34))
		//   VALUES CAST('-NaN'      AS DECFLOAT(34))
		//   VALUES CAST('SNaN'      AS DECFLOAT(34))
		//   VALUES CAST('Infinity'  AS DECFLOAT(34))
		//   VALUES CAST('-Infinity' AS DECFLOAT(34))
		// Both DECFLOAT(16) and DECFLOAT(34) precisions are tested so the
		// decimal64 and decimal128 NaN/SNaN bit positions both get cover.
		for _, prec := range []int{16, 34} {
			cases := []struct {
				lit  string
				want string
			}{
				{"NaN", "NaN"},
				{"-NaN", "-NaN"},
				{"SNaN", "SNaN"},
				{"-SNaN", "-SNaN"},
				{"Infinity", "Infinity"},
				{"-Infinity", "-Infinity"},
			}
			for _, tc := range cases {
				name := fmt.Sprintf("DECFLOAT(%d)/%s", prec, tc.lit)
				t.Run(name, func(t *testing.T) {
					q := fmt.Sprintf("VALUES CAST('%s' AS DECFLOAT(%d))", tc.lit, prec)
					var got string
					if err := db.QueryRow(q).Scan(&got); err != nil {
						// Some LPARs reject CAST('SNaN' ...) at parse time;
						// skip that literal rather than fail.
						t.Skipf("%s not producible (%v)", q, err)
					}
					if got != tc.want {
						t.Errorf("%s = %q, want %q", q, got, tc.want)
					}
				})
			}
		}
	})

	// Scaled binary integer: a DECIMAL with small precision that IBM i
	// may ship as a binary INTEGER carrying the scale in the descriptor.
	// Whether the server packs it as packed-decimal or binary, the
	// decoded string value must be the exact fixed-point number -- this
	// subtest pins the value regardless of wire encoding, and the
	// offline unit test (TestDecodeColumnScaledInteger) pins the binary
	// path directly.
	//
	// HIGH CONFIDENCE: asserts the decoded value, agnostic to wire form.
	t.Run("scaled DECIMAL value", func(t *testing.T) {
		// SQL issued: VALUES CAST(123.45 AS DECIMAL(5,2))
		// Expected decoded string: "123.45".
		var got string
		if err := db.QueryRow("VALUES CAST(123.45 AS DECIMAL(5,2))").Scan(&got); err != nil {
			t.Fatalf("scan scaled DECIMAL: %v", err)
		}
		if got != "123.45" {
			t.Errorf("DECIMAL(5,2) = %q, want %q", got, "123.45")
		}

		// A negative small scaled value too.
		// SQL issued: VALUES CAST(-1.07 AS DECIMAL(4,2))
		var got2 string
		if err := db.QueryRow("VALUES CAST(-1.07 AS DECIMAL(4,2))").Scan(&got2); err != nil {
			t.Fatalf("scan negative scaled DECIMAL: %v", err)
		}
		if got2 != "-1.07" {
			t.Errorf("DECIMAL(4,2) = %q, want %q", got2, "-1.07")
		}
	})

	// ROWID: GENERATE_UNIQUE() produces a 13-byte binary unique value
	// typed compatibly with ROWID; a real ROWID column is the stronger
	// test. We try a ROWID column first (CREATE TABLE ... GENERATED
	// ALWAYS AS ROWID) and fall back to nothing -- ROWID scans into
	// []byte.
	//
	// NEEDS LIVE CONFIRMATION: the 2-byte SL wire shape is
	// JT400-documented (SQLRowID) but not fixture-captured. If this
	// subtest byte-shifts, the SL arithmetic in decodeColumn (case
	// 904/905) is the place to check.
	t.Run("ROWID column", func(t *testing.T) {
		dropTestTables(t, db)
		tbl := schema() + "." + tablePrefix + "rowid"
		// SQL issued:
		//   CREATE TABLE <tbl> (
		//     rid ROWID GENERATED ALWAYS NOT NULL,
		//     n INTEGER NOT NULL PRIMARY KEY)
		//   INSERT INTO <tbl> (n) VALUES (1)
		//   SELECT rid, n FROM <tbl> WHERE n = 1
		//
		// rid is selected FIRST so the trailing INTEGER n exercises the
		// column-shift hazard: a wrong ROWID footprint slides n. We assert
		// n == 1, which fails loudly if ROWID over- or under-advances.
		if _, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE %s (rid ROWID GENERATED ALWAYS NOT NULL, n INTEGER NOT NULL PRIMARY KEY)`, tbl)); err != nil {
			t.Skipf("CREATE TABLE with ROWID failed: %v", err)
		}
		defer db.Exec("DROP TABLE " + tbl)

		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (n) VALUES (1)`, tbl)); err != nil {
			t.Fatalf("insert ROWID row: %v", err)
		}
		var rid []byte
		var n int
		if err := db.QueryRow(fmt.Sprintf(`SELECT rid, n FROM %s WHERE n = 1`, tbl)).Scan(&rid, &n); err != nil {
			t.Fatalf("scan ROWID, n: %v", err)
		}
		// A ROWID is opaque but non-empty and at most 40 bytes.
		if len(rid) == 0 {
			t.Error("ROWID decoded to empty []byte, want non-empty")
		}
		if len(rid) > 40 {
			t.Errorf("ROWID decoded to %d bytes, want <= 40", len(rid))
		}
		// Column-shift guard: n must survive decoding AFTER the ROWID.
		if n != 1 {
			t.Errorf("INTEGER n after ROWID = %d, want 1 (ROWID footprint shifted the column?)", n)
		}
	})

	// ROWID via GENERATE_UNIQUE(): a zero-setup alternative producer. The
	// result is a 13-byte CHAR(13) FOR BIT DATA on most LPARs (not a true
	// ROWID type), so this is a softer check -- it confirms the scalar is
	// readable as bytes. Kept separate from the ROWID-column subtest so a
	// type mismatch is visible.
	//
	// NEEDS LIVE CONFIRMATION: the server may type GENERATE_UNIQUE() as
	// CHAR FOR BIT DATA rather than ROWID; the parent should note which.
	t.Run("GENERATE_UNIQUE bytes", func(t *testing.T) {
		// SQL issued: VALUES GENERATE_UNIQUE()
		var b []byte
		if err := db.QueryRow("VALUES GENERATE_UNIQUE()").Scan(&b); err != nil {
			t.Skipf("VALUES GENERATE_UNIQUE() not producible (%v)", err)
		}
		if len(b) == 0 {
			t.Error("GENERATE_UNIQUE() decoded to empty []byte, want non-empty")
		}
	})

	// XML: XMLSERIALIZE renders an XML value to a character string, so
	// the decoded column is a plain VARCHAR/CLOB string -- this confirms
	// XML is reachable via serialization even though a bare XML column
	// hits the typed UnsupportedResultTypeError (988/989). A bare XML
	// column subtest below documents that the typed error is the
	// expected, classifiable outcome.
	//
	// HIGH CONFIDENCE: XMLSERIALIZE yields a normal string column.
	t.Run("XML via XMLSERIALIZE", func(t *testing.T) {
		// SQL issued:
		//   VALUES XMLSERIALIZE(XMLELEMENT(NAME "a", '1') AS VARCHAR(100))
		// Expected decoded string contains the element markup.
		var got string
		q := `VALUES XMLSERIALIZE(XMLELEMENT(NAME "a", '1') AS VARCHAR(100))`
		if err := db.QueryRow(q).Scan(&got); err != nil {
			t.Skipf("XMLSERIALIZE not producible (%v)", err)
		}
		if !strings.Contains(got, "<a>") || !strings.Contains(got, "1") {
			t.Errorf("XMLSERIALIZE = %q, want it to contain <a>...1...</a>", got)
		}
	})

	// Bare XML column: selecting an XML column WITHOUT serialization
	// should surface the typed hostserver.UnsupportedResultTypeError
	// (SQL type 988/989) rather than a generic opaque failure. This
	// documents the intended classifiable-failure behaviour for the
	// types we deliberately don't decode.
	//
	// NEEDS LIVE CONFIRMATION: confirms the server actually ships XML as
	// 988/989 inline (vs an XML locator 2452, which would also hit the
	// typed error). Either way the failure must be classifiable.
	t.Run("bare XML column is typed-unsupported", func(t *testing.T) {
		dropTestTables(t, db)
		tbl := schema() + "." + tablePrefix + "xml"
		// SQL issued:
		//   CREATE TABLE <tbl> (id INTEGER NOT NULL PRIMARY KEY, doc XML)
		//   INSERT INTO <tbl> (id, doc) VALUES (1, XMLPARSE(DOCUMENT '<a>1</a>'))
		//   SELECT doc FROM <tbl> WHERE id = 1   -- expected to error
		if _, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, doc XML)`, tbl)); err != nil {
			t.Skipf("CREATE TABLE with XML failed (no XML support on this LPAR?): %v", err)
		}
		defer db.Exec("DROP TABLE " + tbl)

		if _, err := db.Exec(fmt.Sprintf(
			`INSERT INTO %s (id, doc) VALUES (1, XMLPARSE(DOCUMENT '<a>1</a>'))`, tbl)); err != nil {
			t.Fatalf("insert XML row: %v", err)
		}
		var got string
		err := db.QueryRow(fmt.Sprintf(`SELECT doc FROM %s WHERE id = 1`, tbl)).Scan(&got)
		if err == nil {
			// If the server inlines XML as a CLOB string the decode may
			// succeed; that's acceptable. Log it so the parent records
			// which behaviour the target exhibits.
			t.Logf("bare XML column decoded as string %q (server inlined XML; typed error not triggered)", got)
			return
		}
		// The error must mention the unsupported SQL type so it's
		// classifiable, not opaque.
		if !strings.Contains(err.Error(), "unsupported SQL type") {
			t.Errorf("bare XML select error = %v, want it to name the unsupported SQL type", err)
		}
	})

	// NVARCHAR / NCHAR are GRAPHIC-family types in CCSID 1200 -- IBM i
	// has no distinct NVARCHAR native type code, it maps to VARGRAPHIC
	// CCSID 1200 (read path already covered by the graphic decode). This
	// subtest confirms a CAST(... AS NVARCHAR(n)) string round-trips,
	// proving the N* spelling routes through the existing graphic
	// decoder. NCHAR/NVARCHAR scan into a string.
	//
	// HIGH CONFIDENCE: routes through the proven VARGRAPHIC decode path.
	t.Run("NVARCHAR via CAST", func(t *testing.T) {
		// SQL issued: VALUES CAST('Hi' AS NVARCHAR(10))
		var got string
		if err := db.QueryRow("VALUES CAST('Hi' AS NVARCHAR(10))").Scan(&got); err != nil {
			t.Skipf("CAST AS NVARCHAR not producible (probably no DBCS NLSS): %v", err)
		}
		if got != "Hi" {
			t.Errorf("NVARCHAR round-trip = %q, want %q", got, "Hi")
		}
	})

	// DATALINK: only producible where a DataLink File Manager (DLFM) is
	// configured, so this subtest is the most likely to skip. The decode
	// surfaces the link value as a string.
	//
	// NEEDS LIVE CONFIRMATION: DATALINK wire shape is JT400-documented
	// (SQLDatalink, 2-byte SL + CCSID-decoded chars) but not
	// fixture-captured; most PUB400-class profiles have no DLFM so this
	// will t.Skip. If creatable, confirm the string equals the link.
	t.Run("DATALINK column", func(t *testing.T) {
		dropTestTables(t, db)
		tbl := schema() + "." + tablePrefix + "dlink"
		// SQL issued:
		//   CREATE TABLE <tbl> (id INTEGER NOT NULL PRIMARY KEY, d DATALINK)
		//   INSERT INTO <tbl> (id, d)
		//     VALUES (1, DLVALUE('http://example.com/a.txt'))
		//   SELECT d FROM <tbl> WHERE id = 1
		if _, err := db.Exec(fmt.Sprintf(
			`CREATE TABLE %s (id INTEGER NOT NULL PRIMARY KEY, d DATALINK)`, tbl)); err != nil {
			t.Skipf("CREATE TABLE with DATALINK failed (no DLFM on this LPAR): %v", err)
		}
		defer db.Exec("DROP TABLE " + tbl)

		if _, err := db.Exec(fmt.Sprintf(
			`INSERT INTO %s (id, d) VALUES (1, DLVALUE('http://example.com/a.txt'))`, tbl)); err != nil {
			t.Skipf("insert DATALINK value failed: %v", err)
		}
		var got sql.NullString
		if err := db.QueryRow(fmt.Sprintf(`SELECT d FROM %s WHERE id = 1`, tbl)).Scan(&got); err != nil {
			t.Fatalf("scan DATALINK: %v", err)
		}
		// Db2 for i normalises the DATALINK scheme + host to UPPERCASE
		// (e.g. HTTP://EXAMPLE.COM/a.txt) while preserving the path case,
		// so compare case-insensitively. The point is that the link
		// decoded to its string form -- the var-length DATALINK decode
		// (2-byte SL + CCSID chars) worked end to end.
		if !got.Valid || !strings.Contains(strings.ToLower(got.String), "example.com") {
			t.Errorf("DATALINK = %+v, want a string containing example.com (case-insensitive)", got)
		}
	})

	// ARRAY is NOT a result-set column type on DB2 for i. This subtest is
	// the live contract for the issue #39 ARRAY finding: a SELECT that
	// projects an array value is rejected by the server at
	// PREPARE/DESCRIBE -- it never reaches the row decoder as an array
	// column. (The offline byte-exact half, hostserver
	// TestArrayCrossesWireAsParameterNotResultColumn, shows the only wire
	// shape an array takes: a stored-procedure parameter descriptor with
	// an array flag bit.)
	//
	// On V7R5M0/V7R6M0 every array projection returns SQL-20441 /
	// SQLSTATE 428H2 ("an array value or array type is not allowed in
	// this context"). We assert the statement fails and, for the common
	// case, that the error names the array-context restriction; an
	// install that phrases the refusal differently still satisfies the
	// core contract (no array result column is ever returned).
	//
	// HIGH CONFIDENCE: live-verified on PUB400 V7R5M0 across all four
	// projection forms below.
	t.Run("ARRAY not a result column", func(t *testing.T) {
		cases := []string{
			`VALUES ARRAY[1,2,3]`,
			`SELECT ARRAY[1,2,3] FROM SYSIBM.SYSDUMMY1`,
			`SELECT ARRAY_AGG(C) FROM (SELECT 1 C FROM SYSIBM.SYSDUMMY1 UNION ALL SELECT 2 FROM SYSIBM.SYSDUMMY1) t`,
			`VALUES CAST(ARRAY[1,2,3] AS INTEGER ARRAY[10])`,
		}
		for _, q := range cases {
			t.Run(q, func(t *testing.T) {
				// The whole-row decode must never hand back an array
				// column: the statement has to fail before any rows.
				var anyVal any
				err := db.QueryRow(q).Scan(&anyVal)
				if err == nil {
					t.Fatalf("array projection %q unexpectedly succeeded (server returned an array result column?)", q)
				}
				msg := strings.ToUpper(err.Error())
				// The canonical refusal names the array-context
				// restriction. Accept either the SQLCODE or SQLSTATE;
				// log (don't fail) any other non-nil error so a
				// back-level / differently-worded refusal still passes
				// the core "no array result column" contract.
				if !strings.Contains(msg, "20441") && !strings.Contains(msg, "428H2") && !strings.Contains(msg, "ARRAY") {
					t.Logf("array projection rejected, but not via the expected SQL-20441/428H2 array-context error: %v", err)
				}
			})
		}
	})
}
