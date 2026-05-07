package hostserver

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/complacentsee/goJTOpen/internal/wirelog"
)

// allSentsByServerID returns every individual DSS Sent frame in the
// fixture whose DSS-header ServerID equals want, in capture order.
//
// JTOpen back-to-back writes (e.g. CREATE_RPB followed immediately
// by PREPARE_DESCRIBE without an intervening reply) get
// concatenated into one wirelog Consolidate output frame; this
// helper walks the consolidated bytes via the Length prefix and
// yields each DSS frame separately so per-frame byte-equality tests
// can see them as the server does.
//
// Lets a test compare against fixtures whose connID isn't the
// hardcoded asDatabaseFixtureConnID -- per-fixture JTOpen-session
// connIDs aren't stable across re-captures, but the ServerID byte
// is.
func allSentsByServerID(t *testing.T, name string, want ServerID) [][]byte {
	t.Helper()
	frames := wirelog.Consolidate(loadFixture(t, name))
	var out [][]byte
	for _, f := range frames {
		if f.Direction != wirelog.Sent {
			continue
		}
		// Walk concatenated DSS frames inside this consolidated
		// blob; each starts with a 4-byte big-endian Length.
		b := f.Bytes
		for len(b) >= 8 {
			ln := binary.BigEndian.Uint32(b[0:4])
			if ln < 8 || ln > uint32(len(b)) {
				t.Fatalf("malformed DSS length %d at consolidated offset (have %d bytes) in %s", ln, len(b), name)
			}
			one := b[:ln]
			if ServerID(binary.BigEndian.Uint16(one[6:8])) == want {
				out = append(out, append([]byte(nil), one...))
			}
			b = b[ln:]
		}
	}
	return out
}

// TestSentBytesMatchSelectDummyFixture confirms our request encoders
// produce exactly the bytes JTOpen sends for the four SQL-service
// frames that make up a static SELECT round trip. Without this test,
// subtle template-handle or parameter-encoding bugs slip through to
// live-PUB400 testing where they look like SQL -401 / -502 from the
// server with no obvious encoding cause.
//
// The fixture's SQL-service Sent frames are, in order:
//
//	[0] SET_SQL_ATTRIBUTES   (1F80, corr 1)
//	[1] CREATE_RPB           (1D00, corr 3)
//	[2] PREPARE_DESCRIBE     (1803, corr 4)
//	[3] OPEN_DESCRIBE_FETCH  (180E, corr 5)
//
// The NDB ADD_LIBRARY_LIST frame between [0] and [1] sits on
// ServerID 0xE005 and is checked separately by
// TestSentBytesMatchNDBAddLibraryListFixture.
func TestSentBytesMatchSelectDummyFixture(t *testing.T) {
	all := allSentsByServerID(t, "select_dummy.trace", ServerDatabase)
	var sqlSents [][]byte
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		rid := binary.BigEndian.Uint16(b[18:20])
		switch rid {
		case ReqDBSetSQLAttributes,
			ReqDBSQLRPBCreate,
			ReqDBSQLPrepareDescribe,
			ReqDBSQLOpenDescribeFetch:
			sqlSents = append(sqlSents, b)
		}
	}
	if len(sqlSents) != 4 {
		t.Fatalf("expected 4 SQL-service post-handshake sent frames in select_dummy.trace, got %d", len(sqlSents))
	}

	// ---- Frame 0: SET_SQL_ATTRIBUTES ----
	{
		hdr, payload, err := SetSQLAttributesRequest(DefaultDBAttributesOptions())
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		hdr.CorrelationID = 1
		var buf bytes.Buffer
		if err := WriteFrame(&buf, hdr, payload); err != nil {
			t.Fatalf("WriteFrame SET_SQL_ATTRIBUTES: %v", err)
		}
		assertBytesEqualWithDiff(t, "SET_SQL_ATTRIBUTES", buf.Bytes(), sqlSents[0])
	}

	// ---- Frames 1-3: CREATE_RPB, PREPARE_DESCRIBE, OPEN_DESCRIBE_FETCH ----
	// SelectStaticSQL writes all three back-to-back. To capture
	// exactly what it sends, we hand it the captured replies for
	// frames 2 & 3 (CREATE_RPB has no reply expected) and let it
	// run the full sequence against a fakeConn.
	receivedFrames := allReceivedsFromFixture(t, "select_dummy.trace")
	var sqlReceiveds [][]byte
	for _, b := range receivedFrames {
		if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
			sqlReceiveds = append(sqlReceiveds, b)
		}
	}
	// SQL receiveds in order:
	//   [0] XChgRandSeed reply (0xF001)
	//   [1] StartServer reply (0xF002)
	//   [2] SET_SQL_ATTRIBUTES reply
	//   [3] PREPARE_DESCRIBE reply
	//   [4] OPEN_DESCRIBE_FETCH reply
	if len(sqlReceiveds) < 5 {
		t.Fatalf("need >= 5 SQL receiveds, got %d", len(sqlReceiveds))
	}

	conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4])
	if _, err := SelectStaticSQL(conn,
		"SELECT CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_SERVER FROM SYSIBM.SYSDUMMY1",
		3,
	); err != nil {
		t.Fatalf("SelectStaticSQL: %v", err)
	}

	r := bytes.NewReader(conn.written.Bytes())
	names := []string{"CREATE_RPB", "PREPARE_DESCRIBE", "OPEN_DESCRIBE_FETCH"}
	for i := 0; i < 3; i++ {
		hdr, payload, err := ReadFrame(r)
		if err != nil {
			t.Fatalf("re-parse sent frame %d (%s): %v", i, names[i], err)
		}
		var got bytes.Buffer
		if err := WriteFrame(&got, hdr, payload); err != nil {
			t.Fatalf("re-encode sent frame %d (%s): %v", i, names[i], err)
		}
		assertBytesEqualWithDiff(t, names[i], got.Bytes(), sqlSents[i+1])
	}
}

// TestSentBytesMatchNDBAddLibraryListFixture confirms the NDB
// ADD_LIBRARY_LIST frame (ServerID 0xE005) we emit byte-matches the
// one JTOpen sends in select_dummy.trace.
func TestSentBytesMatchNDBAddLibraryListFixture(t *testing.T) {
	const ndbServerID ServerID = 0xE005
	sents := allSentsByServerID(t, "select_dummy.trace", ndbServerID)
	if len(sents) < 1 {
		t.Fatalf("expected at least 1 NDB-service sent frame in select_dummy.trace, got %d", len(sents))
	}
	want := sents[0]

	// The NDB call wraps a fakeConn since NDBAddLibraryList expects
	// to read the reply. Hand it the corresponding NDB reply from
	// the fixture so it doesn't EOF. Pull NDB receiveds the same
	// way we pull NDB sents.
	frames := wirelog.Consolidate(loadFixture(t, "select_dummy.trace"))
	var ndbRecv [][]byte
	for _, f := range frames {
		if f.Direction != wirelog.Received || len(f.Bytes) < 8 {
			continue
		}
		if ServerID(binary.BigEndian.Uint16(f.Bytes[6:8])) == ndbServerID {
			ndbRecv = append(ndbRecv, f.Bytes)
		}
	}
	if len(ndbRecv) < 1 {
		t.Fatalf("expected at least 1 NDB-service received frame, got %d", len(ndbRecv))
	}

	// AFTRAEGE11 is the library JTOpen sent; correlationID 2 is the
	// one in the fixture (xchg-rand-seed=0, start-server=1, NDB=2).
	conn := newFakeConn(ndbRecv[0])
	if err := NDBAddLibraryList(conn, "AFTRAEGE11", 2); err != nil {
		t.Fatalf("NDBAddLibraryList: %v", err)
	}
	got := conn.written.Bytes()
	assertBytesEqualWithDiff(t, "NDB ADD_LIBRARY_LIST", got, want)
}

// assertBytesEqualWithDiff reports a helpful diff: hex of both
// buffers, first byte that differs, and a short window of bytes
// around it. Tests fail fast on the first frame that differs so the
// hex output stays readable.
func assertBytesEqualWithDiff(t *testing.T, label string, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		return
	}
	t.Errorf("%s: bytes differ (got %d, want %d)", label, len(got), len(want))
	t.Errorf("  got:  %s", hex.EncodeToString(got))
	t.Errorf("  want: %s", hex.EncodeToString(want))
	n := len(got)
	if len(want) < n {
		n = len(want)
	}
	for i := 0; i < n; i++ {
		if got[i] != want[i] {
			lo := i - 8
			if lo < 0 {
				lo = 0
			}
			hi := i + 24
			if hi > n {
				hi = n
			}
			t.Errorf("  first diff at offset %d: got 0x%02X, want 0x%02X", i, got[i], want[i])
			t.Errorf("  context [%d:%d] got=%X want=%X", lo, hi, got[lo:hi], want[lo:hi])
			return
		}
	}
	if len(got) != len(want) {
		t.Errorf("  bytes match through offset %d but lengths differ", n)
	}
}
