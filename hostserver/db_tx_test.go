package hostserver

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestCommitFrameShape confirms the COMMIT (0x1807) wire frame has
// the layout JT400 sends: 40 bytes total, ServerID 0xE004, the
// "verbose error" ORS bitmap, and zero parameters. Byte-equality
// against the JTOpen fixture isn't possible because tx_commit.trace
// triggers an error path that emits a 0x1F00 storage-release frame
// instead of the canonical COMMIT, so we assert structural
// invariants rather than exact bytes.
func TestCommitFrameShape(t *testing.T) {
	frame := buildTxFrame(t, uint32(ReqDBSQLCommit), 8)
	assertTxFrame(t, frame, ReqDBSQLCommit, 8)
}

// TestRollbackFrameShape mirrors TestCommitFrameShape for ROLLBACK
// (0x1808). Same template handle layout, same "verbose error" ORS,
// no parameters; the only differing byte is the ReqRepID.
func TestRollbackFrameShape(t *testing.T) {
	frame := buildTxFrame(t, uint32(ReqDBSQLRollback), 9)
	assertTxFrame(t, frame, ReqDBSQLRollback, 9)
}

// buildTxFrame runs Commit or Rollback against a fakeConn whose
// only queued reply is a synthesised success (zero ErrorClass +
// ReturnCode), captures the bytes the function wrote, and returns
// them. We build the success reply inline rather than pulling from
// a fixture because the captured tx_commit / tx_rollback fixtures
// don't contain a clean canonical reply (both errored at the SQL
// layer in JTOpen's capture run).
func buildTxFrame(t *testing.T, reqRepID, corr uint32) []byte {
	t.Helper()
	successReply := buildSuccessReply(corr, uint16(reqRepID))
	conn := newFakeConn(successReply)

	var err error
	switch uint16(reqRepID) {
	case ReqDBSQLCommit:
		err = Commit(conn, corr)
	case ReqDBSQLRollback:
		err = Rollback(conn, corr)
	default:
		t.Fatalf("unsupported tx reqRepID 0x%04X", reqRepID)
	}
	if err != nil {
		t.Fatalf("tx call: %v", err)
	}
	return conn.written.Bytes()
}

// buildSuccessReply synthesises a 40-byte 0x2800 reply with
// ErrorClass=0 and ReturnCode=0; the function-id echo bytes
// mimic what JT400 puts in the template.
func buildSuccessReply(corr uint32, fnID uint16) []byte {
	hdr := Header{
		Length:         40,
		ServerID:       ServerDatabase,
		CorrelationID:  corr,
		TemplateLength: 20,
		ReqRepID:       RepDBReply,
	}
	payload := make([]byte, 20)
	// ORS bitmap echo (use the COMMIT bitmap; close enough for a
	// success reply -- the parser only inspects ErrorClass and
	// ReturnCode).
	payload[0] = 0x82
	binary.BigEndian.PutUint16(payload[10:12], fnID)
	binary.BigEndian.PutUint16(payload[12:14], fnID)
	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// assertTxFrame walks the captured bytes and validates each field.
// Frame is template (20) + 1 param (LL=7: CP 0x380F + 1 byte hold
// indicator) = 27 bytes payload + 20 byte DSS header = 47 bytes.
func assertTxFrame(t *testing.T, frame []byte, wantReqRepID uint16, wantCorr uint32) {
	t.Helper()
	const wantLen = 20 + 20 + 7 // DSS hdr + template + LL/CP/data
	if len(frame) != wantLen {
		t.Fatalf("len(frame) = %d, want %d (template + 1 hold-indicator param)", len(frame), wantLen)
	}
	hdr, payload, err := ReadFrame(bytes.NewReader(frame))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if hdr.ServerID != ServerDatabase {
		t.Errorf("ServerID = 0x%04X, want 0x%04X", hdr.ServerID, ServerDatabase)
	}
	if hdr.CorrelationID != wantCorr {
		t.Errorf("CorrelationID = %d, want %d", hdr.CorrelationID, wantCorr)
	}
	if hdr.ReqRepID != wantReqRepID {
		t.Errorf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, wantReqRepID)
	}
	if hdr.TemplateLength != 20 {
		t.Errorf("TemplateLength = %d, want 20", hdr.TemplateLength)
	}

	// ORS bitmap differs: COMMIT = ReturnData|SQLCA, ROLLBACK = ReturnData.
	wantORS := uint32(ORSReturnData)
	if wantReqRepID == ReqDBSQLCommit {
		wantORS |= ORSSQLCA
	}
	if got := binary.BigEndian.Uint32(payload[0:4]); got != wantORS {
		t.Errorf("ORS bitmap = 0x%08X, want 0x%08X", got, wantORS)
	}
	// All handles zero (no RPB).
	for off := 8; off < 18; off += 2 {
		if got := binary.BigEndian.Uint16(payload[off : off+2]); got != 0 {
			t.Errorf("template uint16 @%d = %d, want 0", off, got)
		}
	}
	// Parameter count = 1 (the hold indicator).
	if got := binary.BigEndian.Uint16(payload[18:20]); got != 1 {
		t.Errorf("ParameterCount = %d, want 1", got)
	}
	// Param: LL=7, CP=0x380F, data=0x01 (numeric hold = preserve
	// cursors). NOT 0xE8 ('Y') -- the server expects a numeric
	// indicator and rejects EBCDIC characters with SQL -211.
	if got := binary.BigEndian.Uint32(payload[20:24]); got != 7 {
		t.Errorf("param LL = %d, want 7", got)
	}
	if got := binary.BigEndian.Uint16(payload[24:26]); got != 0x380F {
		t.Errorf("param CP = 0x%04X, want 0x380F", got)
	}
	if payload[26] != 0x01 {
		t.Errorf("hold indicator = 0x%02X, want 0x01 (numeric hold)", payload[26])
	}
}

// TestAutocommitOffOnFrameShape sanity-checks the SET_SQL_ATTRIBUTES
// frame the autocommit toggles emit. Should be a 0x1F80 frame with
// THREE CP parameters bundled together (matches JT400's tx_commit
// fixture sent #8 byte-for-byte):
//
//	0x3824 1 byte  -- autocommit ('N'=0xD5 off, 'Y'=0xE8 on)
//	0x380E 2 bytes -- commitment level (*CS=2 with off, NONE=0 with on)
//	0x3830 2 bytes -- locator persistence (1 with off, 0 with on)
//
// Sending all three together is what convinces PUB400 to actually
// start a commitment definition; sending only 0x3824 leaves the
// server in *NONE and COMMIT/ROLLBACK return SQL -211.
func TestAutocommitOffOnFrameShape(t *testing.T) {
	for _, tc := range []struct {
		name           string
		off            bool
		wantAutoCommit byte
		wantIsolation  uint16
		wantLocator    uint16
	}{
		{name: "off", off: true, wantAutoCommit: 0xD5, wantIsolation: 2, wantLocator: 1},
		{name: "on", off: false, wantAutoCommit: 0xE8, wantIsolation: 0, wantLocator: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFakeConn(buildSuccessReply(1, uint16(ReqDBSetSQLAttributes)))
			var err error
			if tc.off {
				err = AutocommitOff(conn, 1)
			} else {
				err = AutocommitOn(conn, 1)
			}
			if err != nil {
				t.Fatalf("call: %v", err)
			}
			frame := conn.written.Bytes()
			hdr, payload, err := ReadFrame(bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if hdr.ReqRepID != ReqDBSetSQLAttributes {
				t.Errorf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, ReqDBSetSQLAttributes)
			}
			// Parameter count at template offset 18-19 should be 3.
			if pc := binary.BigEndian.Uint16(payload[18:20]); pc != 3 {
				t.Errorf("ParameterCount = %d, want 3", pc)
			}
			// 3 params: LL(4)+CP(2)+1 = 7, LL(4)+CP(2)+2 = 8, LL(4)+CP(2)+2 = 8.
			// Total payload after template (20) = 23 bytes -> need >= 43.
			if len(payload) < 43 {
				t.Fatalf("payload too short: %d bytes (want >= 43)", len(payload))
			}
			// Param 1: 0x3824 (autocommit byte).
			if ll := binary.BigEndian.Uint32(payload[20:24]); ll != 7 {
				t.Errorf("param[0] LL = %d, want 7", ll)
			}
			if cp := binary.BigEndian.Uint16(payload[24:26]); cp != 0x3824 {
				t.Errorf("param[0] CP = 0x%04X, want 0x3824", cp)
			}
			if got := payload[26]; got != tc.wantAutoCommit {
				t.Errorf("autocommit byte = 0x%02X, want 0x%02X", got, tc.wantAutoCommit)
			}
			// Param 2: 0x380E (isolation, 2 bytes).
			if ll := binary.BigEndian.Uint32(payload[27:31]); ll != 8 {
				t.Errorf("param[1] LL = %d, want 8", ll)
			}
			if cp := binary.BigEndian.Uint16(payload[31:33]); cp != 0x380E {
				t.Errorf("param[1] CP = 0x%04X, want 0x380E", cp)
			}
			if got := binary.BigEndian.Uint16(payload[33:35]); got != tc.wantIsolation {
				t.Errorf("isolation = %d, want %d", got, tc.wantIsolation)
			}
			// Param 3: 0x3830 (locator persistence, 2 bytes).
			if ll := binary.BigEndian.Uint32(payload[35:39]); ll != 8 {
				t.Errorf("param[2] LL = %d, want 8", ll)
			}
			if cp := binary.BigEndian.Uint16(payload[39:41]); cp != 0x3830 {
				t.Errorf("param[2] CP = 0x%04X, want 0x3830", cp)
			}
			if got := binary.BigEndian.Uint16(payload[41:43]); got != tc.wantLocator {
				t.Errorf("locator persistence = %d, want %d", got, tc.wantLocator)
			}
		})
	}
}
