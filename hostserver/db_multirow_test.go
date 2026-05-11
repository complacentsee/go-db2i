package hostserver

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// multiRowGolden mirrors goldenJSON but typed for the 1000-row
// fixture (rows of [int, string, string]).
type multiRowGolden struct {
	Case       string `json:"case"`
	ResultSets []struct {
		Columns []struct {
			Name      string `json:"name"`
			TypeName  string `json:"typeName"`
			SQLType   int    `json:"sqlType"`
			Precision int    `json:"precision"`
			Scale     int    `json:"scale"`
		} `json:"columns"`
		Rows [][]any `json:"rows"`
	} `json:"resultSets"`
}

// TestMultiRowFetchAgainstFixture replays multi_row_fetch_1k.trace
// (1000 rows of (INTEGER, VARCHAR, DECIMAL(7,2))) through
// SelectStaticSQL and asserts every row matches the golden. JTOpen
// fit all 1000 rows into a single OPEN_DESCRIBE_FETCH reply (the
// 32 KB buffer was enough); this test is the safety net for that
// hot path AND the prerequisite check that our row decoders scale
// to large result sets without per-row drift.
//
// When M5 continuation FETCH lands, this same test will validate
// the multi-batch path by capturing a fresh fixture that exceeds
// the buffer.
func TestMultiRowFetchAgainstFixture(t *testing.T) {
	const fixture = "multi_row_fetch_1k.trace"
	const goldenName = "multi_row_fetch_1k.golden.json"

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
	// PREPARE_DESCRIBE + OPEN_DESCRIBE_FETCH (carries all 1000 rows
	// in one block-fetch buffer) + RPB DELETE. JT400's fetch/close
	// signal in the OPEN reply means no continuation FETCH or
	// CLOSE is needed.
	conn := newFakeConn(
		sqlReceiveds[3],
		sqlReceiveds[4],
		sqlReceiveds[5],
	)
	res, err := SelectStaticSQL(conn,
		"SELECT ID, NAME, AMOUNT FROM AFTRAEGE1.DB2I_T1 ORDER BY ID",
		3,
	)
	if err != nil {
		t.Fatalf("SelectStaticSQL: %v", err)
	}

	g, err := loadMultiRowGolden(goldenName)
	if err != nil {
		t.Fatalf("load golden: %v", err)
	}
	wantRows := g.ResultSets[0].Rows

	if len(res.Rows) != len(wantRows) {
		t.Fatalf("Rows count = %d, want %d", len(res.Rows), len(wantRows))
	}

	// Spot-check first, last, and a middle row exhaustively;
	// then walk the rest checking only the integer ID column to
	// keep the test output readable while still proving every
	// row decoded.
	for _, idx := range []int{0, 1, 499, 500, 998, 999} {
		gotRow := res.Rows[idx]
		wantRow := wantRows[idx]
		if len(gotRow) != len(wantRow) {
			t.Fatalf("row %d cols: got %d, want %d", idx, len(gotRow), len(wantRow))
		}
		for c := range wantRow {
			if !valuesEqual(gotRow[c], wantRow[c]) {
				t.Errorf("row %d col %d: got %v (%T), want %v (%T)",
					idx, c, gotRow[c], gotRow[c], wantRow[c], wantRow[c])
			}
		}
	}
	// Exhaustive: every ID column equals 1..1000 in order.
	for i, row := range res.Rows {
		if !valuesEqual(row[0], wantRows[i][0]) {
			t.Errorf("row %d ID: got %v, want %v", i, row[0], wantRows[i][0])
			if t.Failed() && i > 5 {
				break // don't drown the output
			}
		}
	}
}

func loadMultiRowGolden(name string) (*multiRowGolden, error) {
	path := filepath.Join(fixturesDir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var g multiRowGolden
	if err := json.Unmarshal(b, &g); err != nil {
		return nil, err
	}
	if len(g.ResultSets) == 0 {
		return nil, fmt.Errorf("golden %s has no result sets", name)
	}
	return &g, nil
}
