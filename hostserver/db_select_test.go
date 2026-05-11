package hostserver

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/complacentsee/go-db2i/internal/wirelog"
)

// goldenJSON is just enough of select_dummy.golden.json to extract
// the row values for comparison. Re-running the fixture-capture
// harness against PUB400 regenerates these timestamps, so the test
// reads them from the file rather than hardcoding.
type goldenJSON struct {
	Case       string `json:"case"`
	ResultSets []struct {
		Rows [][]any `json:"rows"`
	} `json:"resultSets"`
}

func loadGolden(t *testing.T, name string) *goldenJSON {
	t.Helper()
	path := filepath.Join(fixturesDir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("golden %s not present: %v", name, err)
	}
	var g goldenJSON
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("parse golden %s: %v", name, err)
	}
	return &g
}

// allReceivedsFromFixture returns every consolidated Received frame
// in the fixture, regardless of connID. Useful when a test wants to
// filter on DSS-header ServerID instead of connID (e.g. for
// fixtures captured in a different JTOpen session than the
// hardcoded asDatabaseFixtureConnID).
func allReceivedsFromFixture(t *testing.T, name string) [][]byte {
	t.Helper()
	frames := wirelog.Consolidate(loadFixture(t, name))
	var out [][]byte
	for _, f := range frames {
		if f.Direction == wirelog.Received {
			out = append(out, f.Bytes)
		}
	}
	return out
}

// TestSelectStaticSQLAgainstFixture replays the captured replies
// from select_dummy.trace -- the PREPARE_DESCRIBE reply (recv #1
// of the as-database connID, after the handshake handful) and the
// OPEN_DESCRIBE_FETCH reply -- and confirms the parsed row matches
// the values in select_dummy.golden.json:
//
//	[ "2026-05-07T19:08:04.161301", "AFTRAEGE1", "PUB400" ]
//
// The test doesn't speak to the server: SelectStaticSQL writes
// three frames to a fakeConn, the conn replays our captured replies
// when read, and we validate the surface of what came back.
func TestSelectStaticSQLAgainstFixture(t *testing.T) {
	// We don't filter by connID here -- select_dummy.trace was
	// captured in a different JTOpen session than connect_only,
	// so its as-database connID isn't the one
	// dbReceivedsFromFixture hardcodes. Filter directly on the
	// DSS header's ServerID byte instead, keeping only frames
	// from server 0xE004 (SQL).
	all := allReceivedsFromFixture(t, "select_dummy.trace")
	var sqlReceiveds [][]byte
	for _, b := range all {
		if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
			sqlReceiveds = append(sqlReceiveds, b)
		}
	}
	// As-database SQL receiveds in select_dummy.trace, in order:
	//   [0] XChgRandSeed reply (0xF001)
	//   [1] StartServer reply (0xF002)
	//   [2] SET_SQL_ATTRIBUTES reply (0x2800 / CP 0x3804)
	//   [3] PREPARE_DESCRIBE reply (super-extended data format)
	//   [4] OPEN_DESCRIBE_FETCH reply (extended result data)
	if len(sqlReceiveds) < 5 {
		t.Fatalf("need >= 5 SQL-server receiveds in select_dummy, got %d", len(sqlReceiveds))
	}

	// SelectStaticSQL reads three replies in order: PREPARE_DESCRIBE
	// (sqlReceiveds[3]), OPEN_DESCRIBE_FETCH (sqlReceiveds[4]), and
	// RPB DELETE (sqlReceiveds[5]). CREATE_RPB is fire-and-forget so
	// no reply is expected for it. The OPEN reply carries JT400's
	// "fetch/close" signal (EC=2, RC=700) which means the server
	// already auto-closed the cursor and delivered all rows; the
	// driver therefore skips continuation FETCH and explicit CLOSE.
	if len(sqlReceiveds) < 6 {
		t.Fatalf("need >= 6 SQL-server receiveds in select_dummy (incl. RPB DELETE), got %d", len(sqlReceiveds))
	}
	conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4], sqlReceiveds[5])

	res, err := SelectStaticSQL(conn,
		"SELECT CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_SERVER FROM SYSIBM.SYSDUMMY1",
		3, // CorrelationID; matches the fixture (CREATE_RPB=3, PREPARE_DESCRIBE=4, OPEN=5)
	)
	if err != nil {
		t.Fatalf("SelectStaticSQL: %v", err)
	}
	if got, want := len(res.Columns), 3; got != want {
		t.Fatalf("Columns count = %d, want %d", got, want)
	}
	if res.Columns[0].SQLType != SQLTypeTimestampNN {
		t.Errorf("col 0 SQL type = %d, want %d (TIMESTAMP NN)", res.Columns[0].SQLType, SQLTypeTimestampNN)
	}
	if res.Columns[1].SQLType != SQLTypeVarChar {
		t.Errorf("col 1 SQL type = %d, want %d (VARCHAR)", res.Columns[1].SQLType, SQLTypeVarChar)
	}

	if got, want := len(res.Rows), 1; got != want {
		t.Fatalf("Rows count = %d, want %d", got, want)
	}
	row := res.Rows[0]
	if got, want := len(row), 3; got != want {
		t.Fatalf("Row 0 column count = %d, want %d", got, want)
	}

	// Compare against select_dummy.golden.json -- read at runtime
	// so re-captures (which change the timestamp) don't break
	// the test. The user/server columns are stable.
	golden := loadGolden(t, "select_dummy.golden.json")
	if len(golden.ResultSets) != 1 || len(golden.ResultSets[0].Rows) != 1 || len(golden.ResultSets[0].Rows[0]) != 3 {
		t.Fatalf("golden shape unexpected: %+v", golden)
	}
	want := golden.ResultSets[0].Rows[0]
	if got := row[0]; got != want[0] {
		t.Errorf("col 0 (timestamp) = %q, want %q (from golden)", got, want[0])
	}
	if got := row[1]; got != want[1] {
		t.Errorf("col 1 (user) = %q, want %q (from golden)", got, want[1])
	}
	if got := row[2]; got != want[2] {
		t.Errorf("col 2 (server) = %q, want %q (from golden)", got, want[2])
	}

	// Sanity: 4 frames written (CREATE_RPB, PREPARE_DESCRIBE,
	// OPEN_DESCRIBE_FETCH, RPB DELETE). The OPEN reply's JT400
	// "fetch/close" signal (EC=2 RC=700) tells the cursor the
	// server already delivered all rows AND auto-closed the cursor,
	// so no continuation FETCH or explicit CLOSE is emitted -- this
	// matches JT400's own wire pattern. Pre-refactor we always sent
	// CLOSE+RPB DELETE which required synthetic test stubs because
	// the captured .trace files don't contain replies for frames
	// JT400 never sends.
	r := bytes.NewReader(conn.written.Bytes())
	for i, want := range []uint16{ReqDBSQLRPBCreate, ReqDBSQLPrepareDescribe, ReqDBSQLOpenDescribeFetch, ReqDBSQLRPBDelete} {
		hdr, _, err := ReadFrame(r)
		if err != nil {
			t.Fatalf("re-parse sent frame %d: %v", i, err)
		}
		if hdr.ReqRepID != want {
			t.Errorf("sent frame %d ReqRepID = 0x%04X, want 0x%04X", i, hdr.ReqRepID, want)
		}
	}
}
