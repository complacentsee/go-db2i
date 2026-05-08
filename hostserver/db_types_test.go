package hostserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// typeCase is one row of the table-driven type-replay test. The trace
// fixture supplies the captured PREPARE_DESCRIBE + OPEN replies; the
// golden file supplies the expected decoded value. SQL is the
// statement JTOpen ran against PUB400, mirrored here so the replay
// path matches what the captured server would have responded to.
type typeCase struct {
	name   string
	sql    string
	golden string
	trace  string
}

// TestStaticSQLTypeRoundTrip replays each types_*.trace through
// SelectStaticSQL and asserts the decoded value (as written to
// SelectRow) compares correctly to the value in the matching
// .golden.json. It's the M4 equivalent of TestSentBytesMatchSelectDummy:
// every type the harness captures becomes a regression test for the
// row decoder, so adding a new column-type decoder breaks visibly if
// any other type drifts.
//
// Cases that fail are logged with the underlying decoder error so the
// gap is concrete (e.g. "unsupported SQL type 480"); we don't t.Skip
// because that hides regressions. Adding a case here as we land each
// type makes sure the new decoder covers the captured row.
func TestStaticSQLTypeRoundTrip(t *testing.T) {
	cases := []typeCase{
		{name: "smallint", sql: "VALUES CAST(-12345 AS SMALLINT)", trace: "types_smallint.trace", golden: "types_smallint.golden.json"},
		{name: "integer", sql: "VALUES CAST(123456789 AS INTEGER)", trace: "types_integer.trace", golden: "types_integer.golden.json"},
		{name: "bigint", sql: "VALUES CAST(9223372036854775807 AS BIGINT)", trace: "types_bigint.trace", golden: "types_bigint.golden.json"},
		{name: "double", sql: "VALUES CAST(2.718281828459045 AS DOUBLE)", trace: "types_double.trace", golden: "types_double.golden.json"},
		{name: "real", sql: "VALUES CAST(3.14 AS REAL)", trace: "types_real.trace", golden: "types_real.golden.json"},
		{name: "char_10", sql: "VALUES CAST('hi' AS CHAR(10))", trace: "types_char_10.trace", golden: "types_char_10.golden.json"},
		{name: "varchar_100", sql: "VALUES CAST('hello' AS VARCHAR(100))", trace: "types_varchar_100.trace", golden: "types_varchar_100.golden.json"},
		{name: "varchar_empty", sql: "VALUES CAST('' AS VARCHAR(10))", trace: "types_varchar_empty.trace", golden: "types_varchar_empty.golden.json"},
		{name: "decimal_5_2", sql: "VALUES CAST(-123.45 AS DECIMAL(5,2))", trace: "types_decimal_5_2.trace", golden: "types_decimal_5_2.golden.json"},
		{name: "decimal_31_5", sql: "VALUES CAST(99999999999999999999999999.12345 AS DECIMAL(31,5))", trace: "types_decimal_31_5.trace", golden: "types_decimal_31_5.golden.json"},
		{name: "decimal_negative_31_5", sql: "VALUES CAST(-99999999999999999999999999.12345 AS DECIMAL(31,5))", trace: "types_decimal_negative_31_5.trace", golden: "types_decimal_negative_31_5.golden.json"},
		{name: "date", sql: "VALUES CURRENT_DATE", trace: "types_date.trace", golden: "types_date.golden.json"},
		{name: "time", sql: "VALUES CURRENT_TIME", trace: "types_time.trace", golden: "types_time.golden.json"},
		{name: "timestamp", sql: "VALUES CURRENT_TIMESTAMP", trace: "types_timestamp.trace", golden: "types_timestamp.golden.json"},
		{name: "null", sql: "VALUES (CAST(NULL AS INTEGER), CAST(NULL AS DECIMAL(5,2)), CAST(NULL AS VARCHAR(10)), CAST(NULL AS TIMESTAMP))", trace: "types_null.trace", golden: "types_null.golden.json"},
		{name: "numeric_5_2", sql: "VALUES CAST(-123.45 AS NUMERIC(5,2))", trace: "types_numeric_5_2.trace", golden: "types_numeric_5_2.golden.json"},
		{name: "numeric_31_5", sql: "VALUES CAST(12345678901234567890123456.78901 AS NUMERIC(31,5))", trace: "types_numeric_31_5.trace", golden: "types_numeric_31_5.golden.json"},
		{name: "decfloat_16", sql: "VALUES CAST(123456.7890123456 AS DECFLOAT(16))", trace: "types_decfloat_16.trace", golden: "types_decfloat_16.golden.json"},
		{name: "decfloat_34", sql: "VALUES CAST(1.234567890123456789012345678901234E+100 AS DECFLOAT(34))", trace: "types_decfloat_34.trace", golden: "types_decfloat_34.golden.json"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runStaticTypeReplay(t, tc)
		})
	}
}

func runStaticTypeReplay(t *testing.T, tc typeCase) {
	t.Helper()
	// Pull the SQL-server received frames (filtering on ServerID
	// 0xE004) since per-fixture connIDs aren't stable.
	all := allReceivedsFromFixture(t, tc.trace)
	var sqlReceiveds [][]byte
	for _, b := range all {
		if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
			sqlReceiveds = append(sqlReceiveds, b)
		}
	}
	// Order: XChgRandSeed reply, StartServer reply, SET_SQL_ATTRIBUTES
	// reply, PREPARE_DESCRIBE reply, OPEN_DESCRIBE_FETCH reply,
	// RPB DELETE reply. SelectStaticSQL consumes the PREPARE +
	// OPEN replies; the M4 RPB-DELETE cleanup adds a third read.
	if len(sqlReceiveds) < 5 {
		t.Skipf("fixture %s has only %d SQL receiveds (need >= 5)", tc.trace, len(sqlReceiveds))
	}
	// SelectStaticSQL with start corr=3 sends:
	//   corr 3 CREATE_RPB, corr 4 PREPARE_DESCRIBE,
	//   corr 5 OPEN_DESCRIBE_FETCH, corr 6 continuation FETCH,
	//   corr 7 RPB DELETE.
	// PREPARE+OPEN replies come from the fixture; FETCH-end and
	// RPB-DELETE replies are synthesised because JTOpen captured
	// these traces before the M5 continuation loop existed.
	conn := newFakeConn(
		sqlReceiveds[3],
		sqlReceiveds[4],
		syntheticFetchEndReply(6),
		syntheticCloseReply(7),
		syntheticRPBDeleteReply(8),
	)
	res, err := SelectStaticSQL(conn, tc.sql, 3)
	if err != nil {
		t.Fatalf("SelectStaticSQL: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}

	// Compare decoded row to golden.json.
	g, err := loadTypeGolden(tc.golden)
	if err != nil {
		t.Fatalf("load golden: %v", err)
	}
	if len(g.ResultSets) != 1 || len(g.ResultSets[0].Rows) != 1 || len(g.ResultSets[0].Rows[0]) != len(res.Rows[0]) {
		t.Fatalf("golden shape mismatch: rs=%d rows=%d cols-want=%d cols-got=%d",
			len(g.ResultSets),
			func() int {
				if len(g.ResultSets) == 0 {
					return 0
				}
				return len(g.ResultSets[0].Rows)
			}(),
			len(g.ResultSets[0].Rows[0]),
			len(res.Rows[0]),
		)
	}
	want := g.ResultSets[0].Rows[0]
	for i := range want {
		if !valuesEqual(res.Rows[0][i], want[i]) {
			t.Errorf("col %d: got %v (%T), want %v (%T)",
				i, res.Rows[0][i], res.Rows[0][i], want[i], want[i])
		}
	}
}

func loadTypeGolden(name string) (*goldenJSON, error) {
	path := filepath.Join(fixturesDir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var g goldenJSON
	if err := json.Unmarshal(b, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

// valuesEqual compares a Go-decoded value against a JSON-decoded
// expected value with type tolerance: JSON numbers come back as
// float64, but our row decoders produce int32/int64. We accept any
// integer Go type that round-trips to the same float, plus exact
// match for strings.
func valuesEqual(got, want any) bool {
	if got == nil && want == nil {
		return true
	}
	if got == nil || want == nil {
		return false
	}
	switch w := want.(type) {
	case float64:
		switch g := got.(type) {
		case float64:
			return g == w
		case float32:
			// REAL widens to float64 with the float32's own
			// rounding, so a literal "3.1415927" in JSON
			// parses to a different float64 than
			// float64(float32(3.1415927)). Compare at the
			// narrower precision so REAL round-trips.
			return float32(w) == g
		case int32:
			return float64(g) == w
		case int64:
			return float64(g) == w
		case int16:
			return float64(g) == w
		}
	case string:
		if g, ok := got.(string); ok {
			return g == w
		}
	case bool:
		if g, ok := got.(bool); ok {
			return g == w
		}
	}
	return fmt.Sprint(got) == fmt.Sprint(want)
}

// goldenForRowCol returns the i-th column of the first row in the
// golden file, fail-fast if the shape isn't what we expect. Used by
// individual subtests when only one column matters.
func (g *goldenJSON) firstRowCol(i int) any {
	if len(g.ResultSets) == 0 || len(g.ResultSets[0].Rows) == 0 || len(g.ResultSets[0].Rows[0]) <= i {
		return nil
	}
	return g.ResultSets[0].Rows[0][i]
}

var _ = bytes.Equal // keep `bytes` imported for future use without disruption

// syntheticFetchEndReply builds a 0x2800 (DBReply) frame whose
// SQLCA carries SQLCODE +100 (end-of-data). M5 SelectStaticSQL /
// SelectPreparedSQL now loop continuation FETCHes after the initial
// OPEN; replaying captured fixtures where JTOpen got everything in
// a single batch needs a synthetic "no more rows" reply so the
// continuation loop terminates cleanly. corr is the FETCH
// correlation ID we expect back.
func syntheticFetchEndReply(corr uint32) []byte {
	hdr := Header{
		Length:         40,
		ServerID:       ServerDatabase,
		CorrelationID:  corr,
		TemplateLength: 20,
		ReqRepID:       RepDBReply,
	}
	payload := make([]byte, 20)
	// Echo ORS bitmap fields used by FETCH so a future stricter
	// reply parser accepts the frame too.
	payload[0] = 0x86
	payload[1] = 0x04
	payload[10] = 0x18 // function ID echo
	payload[11] = 0x0B
	payload[12] = 0x18
	payload[13] = 0x0B
	// ReturnCode = 100 (SQLCODE end-of-data). Bytes 16..19.
	payload[16] = 0x00
	payload[17] = 0x00
	payload[18] = 0x00
	payload[19] = 0x64

	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		panic(fmt.Sprintf("synthesise FETCH end-of-data reply: %v", err))
	}
	return buf.Bytes()
}

// syntheticCloseReply builds a 40-byte 0x2800 (DBReply) frame that
// mimics a successful CLOSE (0x180A) response: ORS bitmap echo,
// ReqRepID echoes, and zeroed ErrorClass + ReturnCode. Cursor.Close
// always sends CLOSE before RPB DELETE -- the captured-fixture-
// driven tests need a synthetic reply to drive that frame, since
// the original .trace files pre-date the cursor refactor.
func syntheticCloseReply(corr uint32) []byte {
	hdr := Header{
		Length:         40,
		ServerID:       ServerDatabase,
		CorrelationID:  corr,
		TemplateLength: 20,
		ReqRepID:       RepDBReply,
	}
	payload := make([]byte, 20)
	// ORS bitmap echo matching what we send (0x86040000 for CLOSE).
	payload[0] = 0x86
	payload[1] = 0x04
	payload[10] = 0x18 // function ID echo: 0x180A
	payload[11] = 0x0A
	payload[12] = 0x18
	payload[13] = 0x0A

	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		panic(fmt.Sprintf("synthesise CLOSE reply: %v", err))
	}
	return buf.Bytes()
}

// syntheticRPBDeleteReply builds a 40-byte 0x2800 (DBReply)
// frame that mimics a successful RPB DELETE response: ORS bitmap
// echo, ReqRepID echoes (0x1D02), and zeroed ErrorClass +
// ReturnCode. fakeConn replays this when the original fixture
// pre-dates the M4 RPB-DELETE cleanup so type-decoder tests don't
// need fresh captures.
func syntheticRPBDeleteReply(corr uint32) []byte {
	hdr := Header{
		Length:         40,
		ServerID:       ServerDatabase,
		CorrelationID:  corr,
		TemplateLength: 20,
		ReqRepID:       RepDBReply,
	}
	payload := make([]byte, 20)
	// ORS bitmap echo (matches what we send): 0x80040000.
	payload[0] = 0x80
	payload[1] = 0x04
	// Handles + function ID echo (0x1D02 twice) at template
	// offsets 8..13. We zero the rest -- ParseDBReply only
	// reads ErrorClass (14..15) and ReturnCode (16..19), both
	// of which stay zero for success.
	payload[10] = 0x1D
	payload[11] = 0x02
	payload[12] = 0x1D
	payload[13] = 0x02

	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		panic(fmt.Sprintf("synthesise RPB DELETE reply: %v", err))
	}
	return buf.Bytes()
}
