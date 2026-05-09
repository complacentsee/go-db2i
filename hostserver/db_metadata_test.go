package hostserver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// metaGolden mirrors the column-metadata fields each .golden.json
// carries. We use this to assert SelectColumn fields populated by
// sqlTypeMetadata match what JTOpen reports for the same fixture.
type metaGolden struct {
	ResultSets []struct {
		Columns []struct {
			Name        string `json:"name"`
			TypeName    string `json:"typeName"`
			SQLType     int    `json:"sqlType"`
			DisplaySize int    `json:"displaySize"`
			Precision   int    `json:"precision"`
			Scale       int    `json:"scale"`
			Nullable    int    `json:"nullable"`
			Signed      bool   `json:"signed"`
		} `json:"columns"`
	} `json:"resultSets"`
}

// TestColumnMetadataAgainstGoldens drives every captured types_*
// fixture through the type round-trip pipeline and asserts the
// derived SelectColumn metadata (TypeName, DisplaySize, Nullable,
// Signed) matches what JTOpen / JDBC ResultSetMetaData reports in
// the matching .golden.json. Catches drift between our
// sqlTypeMetadata switch and JTOpen's java.sql.Types mapping.
func TestColumnMetadataAgainstGoldens(t *testing.T) {
	cases := []struct {
		name   string
		trace  string
		golden string
	}{
		{"smallint", "types_smallint.trace", "types_smallint.golden.json"},
		{"integer", "types_integer.trace", "types_integer.golden.json"},
		{"bigint", "types_bigint.trace", "types_bigint.golden.json"},
		{"real", "types_real.trace", "types_real.golden.json"},
		{"double", "types_double.trace", "types_double.golden.json"},
		{"char_10", "types_char_10.trace", "types_char_10.golden.json"},
		{"varchar_100", "types_varchar_100.trace", "types_varchar_100.golden.json"},
		{"date", "types_date.trace", "types_date.golden.json"},
		{"time", "types_time.trace", "types_time.golden.json"},
		{"timestamp", "types_timestamp.trace", "types_timestamp.golden.json"},
		{"decimal_5_2", "types_decimal_5_2.trace", "types_decimal_5_2.golden.json"},
		{"decimal_31_5", "types_decimal_31_5.trace", "types_decimal_31_5.golden.json"},
		{"numeric_5_2", "types_numeric_5_2.trace", "types_numeric_5_2.golden.json"},
		{"decfloat_16", "types_decfloat_16.trace", "types_decfloat_16.golden.json"},
		{"decfloat_34", "types_decfloat_34.trace", "types_decfloat_34.golden.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols := decodeFixtureColumns(t, tc.trace)
			g := loadMetaGolden(t, tc.golden)
			if len(g.ResultSets) == 0 || len(g.ResultSets[0].Columns) != len(cols) {
				t.Fatalf("golden has %d cols, decoded %d", len(g.ResultSets[0].Columns), len(cols))
			}
			for i, want := range g.ResultSets[0].Columns {
				got := cols[i]
				if got.TypeName != want.TypeName {
					t.Errorf("col %d (%s): TypeName = %q, want %q", i, want.Name, got.TypeName, want.TypeName)
				}
				if got.DisplaySize != want.DisplaySize {
					t.Errorf("col %d (%s): DisplaySize = %d, want %d", i, want.Name, got.DisplaySize, want.DisplaySize)
				}
				wantNullable := want.Nullable == 1
				if got.Nullable != wantNullable {
					t.Errorf("col %d (%s): Nullable = %v, want %v", i, want.Name, got.Nullable, wantNullable)
				}
				if got.Signed != want.Signed {
					t.Errorf("col %d (%s): Signed = %v, want %v", i, want.Name, got.Signed, want.Signed)
				}
			}
		})
	}
}

// decodeFixtureColumns runs the PREPARE_DESCRIBE reply through our
// parser (without actually executing the cursor) and returns the
// resulting SelectColumn metadata.
func decodeFixtureColumns(t *testing.T, fixture string) []SelectColumn {
	t.Helper()
	all := allReceivedsFromFixture(t, fixture)
	var sqlReceiveds [][]byte
	for _, b := range all {
		if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
			sqlReceiveds = append(sqlReceiveds, b)
		}
	}
	if len(sqlReceiveds) < 6 {
		t.Skipf("fixture %s has only %d SQL receiveds (need >= 6)", fixture, len(sqlReceiveds))
	}
	// PREPARE_DESCRIBE + OPEN_DESCRIBE_FETCH + RPB DELETE replies.
	// The OPEN reply's JT400 fetch/close signal lets the cursor
	// skip continuation FETCH and explicit CLOSE.
	conn := newFakeConn(
		sqlReceiveds[3],
		sqlReceiveds[4],
		sqlReceiveds[5],
	)
	res, err := SelectStaticSQL(conn, "VALUES 1", 3)
	if err != nil {
		t.Fatalf("SelectStaticSQL: %v", err)
	}
	return res.Columns
}

func loadMetaGolden(t *testing.T, name string) *metaGolden {
	t.Helper()
	path := filepath.Join(fixturesDir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var g metaGolden
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("parse golden %s: %v", name, err)
	}
	return &g
}
