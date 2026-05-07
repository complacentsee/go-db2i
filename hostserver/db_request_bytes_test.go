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

	conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4], syntheticRPBDeleteReply(6))
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

// TestSentBytesMatchPreparedIntParamFixture extends byte-equality
// coverage to a different SQL statement with a parameter marker.
// Even though M3 (prepared statements with bound parameters) hasn't
// landed yet, the first four database-service Sent frames are
// byte-identical to what SelectStaticSQL would emit for the same
// SQL: SET_SQL_ATTRIBUTES is connection-init and doesn't see SQL,
// CREATE_RPB always uses the STMT0001/CRSR0001 pair, and
// PREPARE_DESCRIBE is parameterised on SQL text but uses the same
// encoder we already have. The 0x1E00 (EXECUTE) and parameter-bind
// frames are explicitly outside M2 scope and skipped here -- they
// land with M3.
func TestSentBytesMatchPreparedIntParamFixture(t *testing.T) {
	const fixture = "prepared_int_param.trace"
	const preparedSQL = "SELECT CAST(? AS INTEGER) AS V FROM SYSIBM.SYSDUMMY1"

	all := allSentsByServerID(t, fixture, ServerDatabase)

	// Pick out the post-handshake SQL frames we currently encode.
	// The fixture has more frames than select_dummy (PREPARE +
	// EXECUTE separately for prepared statements), so we filter
	// on (ReqRepID, correlation) instead of expecting exactly 4.
	want := map[uint16][]byte{}
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		rid := binary.BigEndian.Uint16(b[18:20])
		switch rid {
		case ReqDBSetSQLAttributes,
			ReqDBSQLRPBCreate,
			ReqDBSQLPrepareDescribe:
			// First instance only (prepared_int_param sends each
			// of these once on the database connection).
			if _, ok := want[rid]; !ok {
				want[rid] = b
			}
		}
	}
	for _, rid := range []uint16{ReqDBSetSQLAttributes, ReqDBSQLRPBCreate, ReqDBSQLPrepareDescribe} {
		if _, ok := want[rid]; !ok {
			t.Fatalf("fixture %s missing frame ReqRepID 0x%04X", fixture, rid)
		}
	}

	// ---- SET_SQL_ATTRIBUTES ----
	t.Run("SET_SQL_ATTRIBUTES", func(t *testing.T) {
		hdr, payload, err := SetSQLAttributesRequest(DefaultDBAttributesOptions())
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		hdr.CorrelationID = 1
		var buf bytes.Buffer
		if err := WriteFrame(&buf, hdr, payload); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		assertBytesEqualWithDiff(t, "SET_SQL_ATTRIBUTES", buf.Bytes(), want[ReqDBSetSQLAttributes])
	})

	// SelectStaticSQL writes CREATE_RPB + PREPARE_DESCRIBE
	// back-to-back; capture both, then split for per-frame
	// assertion. We borrow a successful PREPARE_DESCRIBE reply
	// from select_dummy so SelectStaticSQL doesn't bail on
	// its own read -- we discard the parsed result and only
	// inspect the frames our encoder wrote.
	dummy := allReceivedsFromFixture(t, "select_dummy.trace")
	var sqlReceiveds [][]byte
	for _, b := range dummy {
		if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
			sqlReceiveds = append(sqlReceiveds, b)
		}
	}
	if len(sqlReceiveds) < 5 {
		t.Fatalf("need >= 5 SQL receiveds in select_dummy, got %d", len(sqlReceiveds))
	}
	conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4], syntheticRPBDeleteReply(6))
	_, _ = SelectStaticSQL(conn, preparedSQL, 3)
	r := bytes.NewReader(conn.written.Bytes())

	// ---- CREATE_RPB ----
	t.Run("CREATE_RPB", func(t *testing.T) {
		hdr, payload, err := ReadFrame(r)
		if err != nil {
			t.Fatalf("re-parse: %v", err)
		}
		var got bytes.Buffer
		if err := WriteFrame(&got, hdr, payload); err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		assertBytesEqualWithDiff(t, "CREATE_RPB", got.Bytes(), want[ReqDBSQLRPBCreate])
	})

	// ---- PREPARE_DESCRIBE ----
	// SelectStaticSQL toggles ORSParameterMarkerFmt (bit 16)
	// when the SQL contains '?' markers, so PREPARE_DESCRIBE
	// for parameterised SQL is byte-identical to JTOpen's even
	// before EXECUTE/bind support lands.
	t.Run("PREPARE_DESCRIBE", func(t *testing.T) {
		hdr, payload, err := ReadFrame(r)
		if err != nil {
			t.Fatalf("re-parse: %v", err)
		}
		var got bytes.Buffer
		if err := WriteFrame(&got, hdr, payload); err != nil {
			t.Fatalf("re-encode: %v", err)
		}
		assertBytesEqualWithDiff(t, "PREPARE_DESCRIBE", got.Bytes(), want[ReqDBSQLPrepareDescribe])
	})

	// ---- M3: CHANGE_DESCRIPTOR (0x1E00) + OPEN_DESCRIBE_FETCH with input params ----
	// Pull these targets from the fixture using the same Sent-by-
	// ServerID walker. CHANGE_DESCRIPTOR sits on ServerID 0xE004
	// like the SQL frames; we filter on ReqRepID and pick the
	// first occurrence.
	var wantChangeDesc, wantOpenWithParams []byte
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		switch binary.BigEndian.Uint16(b[18:20]) {
		case ReqDBSQLChangeDescriptor:
			if wantChangeDesc == nil {
				wantChangeDesc = b
			}
		case ReqDBSQLOpenDescribeFetch:
			if wantOpenWithParams == nil {
				wantOpenWithParams = b
			}
		}
	}

	t.Run("CHANGE_DESCRIPTOR", func(t *testing.T) {
		if wantChangeDesc == nil {
			t.Fatalf("fixture missing 0x1E00 frame")
		}
		shapes := []PreparedParam{{
			SQLType:     497, // INTEGER nullable
			FieldLength: 4,
			Precision:   10,
			Scale:       0,
			CCSID:       0,
		}}
		hdr, payload, err := ChangeDescriptorRequest(shapes)
		if err != nil {
			t.Fatalf("ChangeDescriptorRequest: %v", err)
		}
		hdr.CorrelationID = 5
		var got bytes.Buffer
		if err := WriteFrame(&got, hdr, payload); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		assertBytesEqualWithDiff(t, "CHANGE_DESCRIPTOR", got.Bytes(), wantChangeDesc)
	})

	t.Run("OPEN_DESCRIBE_FETCH_with_params", func(t *testing.T) {
		if wantOpenWithParams == nil {
			t.Fatalf("fixture missing parameterised 0x180E frame")
		}
		// SelectPreparedSQL writes 4 frames; we want the last
		// (OPEN_DESCRIBE_FETCH with bound INTEGER value 42).
		// We borrow a successful PREPARE_DESCRIBE reply so
		// SelectPreparedSQL doesn't bail on its own read; we
		// don't care about the parsed result here, only the
		// wire bytes it emitted.
		dummy := allReceivedsFromFixture(t, "select_dummy.trace")
		var sqlReceiveds [][]byte
		for _, b := range dummy {
			if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
				sqlReceiveds = append(sqlReceiveds, b)
			}
		}
		conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4], syntheticRPBDeleteReply(6))
		shapes := []PreparedParam{{
			SQLType:     497,
			FieldLength: 4,
			Precision:   10,
			Scale:       0,
			CCSID:       0,
		}}
		_, _ = SelectPreparedSQL(conn, preparedSQL, shapes, []any{int32(42)}, 3)

		// Walk the four frames (CREATE_RPB / PREPARE_DESCRIBE /
		// CHANGE_DESCRIPTOR / OPEN) and assert byte-equality on
		// the OPEN frame.
		r := bytes.NewReader(conn.written.Bytes())
		for i := 0; i < 3; i++ {
			if _, _, err := ReadFrame(r); err != nil {
				t.Fatalf("re-parse pre-OPEN frame %d: %v", i, err)
			}
		}
		hdr, payload, err := ReadFrame(r)
		if err != nil {
			t.Fatalf("re-parse OPEN: %v", err)
		}
		var got bytes.Buffer
		if err := WriteFrame(&got, hdr, payload); err != nil {
			t.Fatalf("re-encode OPEN: %v", err)
		}
		assertBytesEqualWithDiff(t, "OPEN_DESCRIBE_FETCH(prepared)", got.Bytes(), wantOpenWithParams)
	})
}

// TestSentBytesMatchPreparedStringParamFixture validates that
// SelectPreparedSQL emits byte-identical bytes to JTOpen for a
// VARCHAR(50) parameter bound with "hello, IBM i". Same scaffolding
// as the int-param test; differences land in the input parameter
// shape (CCSID 273, length 52, type 449) and the value bytes (12
// EBCDIC chars + 38 zero padding inside a 52-byte row).
func TestSentBytesMatchPreparedStringParamFixture(t *testing.T) {
	const fixture = "prepared_string_param.trace"
	const preparedSQL = "SELECT CAST(? AS VARCHAR(50)) FROM SYSIBM.SYSDUMMY1"

	all := allSentsByServerID(t, fixture, ServerDatabase)

	var wantChangeDesc, wantOpenWithParams []byte
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		switch binary.BigEndian.Uint16(b[18:20]) {
		case ReqDBSQLChangeDescriptor:
			if wantChangeDesc == nil {
				wantChangeDesc = b
			}
		case ReqDBSQLOpenDescribeFetch:
			if wantOpenWithParams == nil {
				wantOpenWithParams = b
			}
		}
	}

	shapes := []PreparedParam{{
		SQLType:     449, // VARCHAR nullable
		FieldLength: 52,  // 2-byte SL + 50 EBCDIC bytes
		Precision:   50,
		Scale:       0,
		CCSID:       273, // German EBCDIC -- PUB400 default
	}}

	t.Run("CHANGE_DESCRIPTOR", func(t *testing.T) {
		if wantChangeDesc == nil {
			t.Fatalf("fixture missing 0x1E00 frame")
		}
		hdr, payload, err := ChangeDescriptorRequest(shapes)
		if err != nil {
			t.Fatalf("ChangeDescriptorRequest: %v", err)
		}
		hdr.CorrelationID = 5
		var got bytes.Buffer
		if err := WriteFrame(&got, hdr, payload); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		assertBytesEqualWithDiff(t, "CHANGE_DESCRIPTOR(varchar)", got.Bytes(), wantChangeDesc)
	})

	t.Run("OPEN_DESCRIBE_FETCH_with_string_param", func(t *testing.T) {
		if wantOpenWithParams == nil {
			t.Fatalf("fixture missing parameterised 0x180E frame")
		}
		dummy := allReceivedsFromFixture(t, "select_dummy.trace")
		var sqlReceiveds [][]byte
		for _, b := range dummy {
			if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
				sqlReceiveds = append(sqlReceiveds, b)
			}
		}
		conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4], syntheticRPBDeleteReply(6))
		_, _ = SelectPreparedSQL(conn, preparedSQL, shapes, []any{"hello, IBM i"}, 3)

		r := bytes.NewReader(conn.written.Bytes())
		for i := 0; i < 3; i++ {
			if _, _, err := ReadFrame(r); err != nil {
				t.Fatalf("re-parse pre-OPEN frame %d: %v", i, err)
			}
		}
		hdr, payload, err := ReadFrame(r)
		if err != nil {
			t.Fatalf("re-parse OPEN: %v", err)
		}
		var got bytes.Buffer
		if err := WriteFrame(&got, hdr, payload); err != nil {
			t.Fatalf("re-encode OPEN: %v", err)
		}
		assertBytesEqualWithDiff(t, "OPEN_DESCRIBE_FETCH(varchar)", got.Bytes(), wantOpenWithParams)
	})
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
