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
	// reply, PREPARE_DESCRIBE reply, OPEN_DESCRIBE_FETCH reply.
	// SelectStaticSQL only consumes the last two.
	if len(sqlReceiveds) < 5 {
		t.Skipf("fixture %s has only %d SQL receiveds (need >= 5)", tc.trace, len(sqlReceiveds))
	}
	conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4])
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
