package hostserver

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestContinuationFetchWireShapeMatchesJT400 pins the v0.7.14 bug-#2
// fix at the wire-shape level. JT400's capture of the 10000-row
// streaming SELECT (testdata/jtopen-fixtures/fixtures/select_large_
// user_table_10k.trace) shows the server reliably delivers all rows
// when continuation FETCH (0x180B) carries exactly two parameters:
//
//	CP 0x380E FetchScrollOption (2 bytes, FETCH_NEXT = 0x0000)
//	CP 0x380C BlockingFactor    (4 bytes, big-endian rows/block)
//
// with ORS bitmap 0x84040000 (ReturnData + ResultData + RLE; no SQLCA).
// Pre-v0.7.14 we sent BufferSize + VariableFieldCompr +
// ScrollableCursorFlag on continuation FETCH, which the server
// accepted for catalog scans (TestRowsLazyMemoryBounded covers that
// path with 49000+ rows) but routed large user-table scans into the
// premature "fetch/close" path -- capping delivery at ~8625 of 10000
// rows on V7R6M0. This test asserts both that JT400's fixture
// continues to carry the expected shape AND that our fetchMoreRows
// encoder emits the same CP set + ORS bits.
func TestContinuationFetchWireShapeMatchesJT400(t *testing.T) {
	all := allSentsByServerID(t, "select_large_user_table_10k.trace", ServerDatabase)

	// JT400's wire bytes: pick out every continuation FETCH (0x180B)
	// frame and verify they all match the expected shape.
	var contFetches [][]byte
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		if binary.BigEndian.Uint16(b[18:20]) == ReqDBSQLFetch {
			contFetches = append(contFetches, b)
		}
	}
	if len(contFetches) == 0 {
		t.Fatal("fixture missing continuation FETCH frames (0x180B)")
	}

	for i, b := range contFetches {
		hdr, tpl, params, err := DecodeDBRequestFrame(b)
		if err != nil {
			t.Fatalf("[%02d] decode FETCH: %v", i, err)
		}
		if hdr.ReqRepID != ReqDBSQLFetch {
			t.Errorf("[%02d] ReqRepID 0x%04X != 0x%04X", i, hdr.ReqRepID, ReqDBSQLFetch)
		}
		wantORS := uint32(ORSReturnData | ORSResultData | ORSDataCompression) // 0x84040000
		if tpl.ORSBitmap != wantORS {
			t.Errorf("[%02d] ORSBitmap 0x%08X != 0x%08X", i, tpl.ORSBitmap, wantORS)
		}
		if tpl.RPBHandle != 1 {
			t.Errorf("[%02d] RPBHandle %d != 1", i, tpl.RPBHandle)
		}
		if len(params) != 2 {
			t.Errorf("[%02d] params=%d, want 2 (FetchScrollOption + BlockingFactor)", i, len(params))
			continue
		}
		if params[0].CodePoint != cpDBFetchScrollOption {
			t.Errorf("[%02d] params[0].CP=0x%04X, want 0x%04X (FetchScrollOption)", i, params[0].CodePoint, cpDBFetchScrollOption)
		}
		if len(params[0].Data) != 2 || binary.BigEndian.Uint16(params[0].Data) != 0 {
			t.Errorf("[%02d] params[0] data=% X, want 00 00 (FETCH_NEXT)", i, params[0].Data)
		}
		if params[1].CodePoint != cpDBBlockingFactor {
			t.Errorf("[%02d] params[1].CP=0x%04X, want 0x%04X (BlockingFactor)", i, params[1].CodePoint, cpDBBlockingFactor)
		}
		if len(params[1].Data) != 4 {
			t.Errorf("[%02d] params[1] len=%d, want 4 (BlockingFactor uint32)", i, len(params[1].Data))
		}
	}

	// Our fetchMoreRows encoder: build a frame against a fakeConn,
	// pull it back through ReadFrame, and assert the same shape
	// invariants. We don't compare exact bytes -- BlockingFactor is
	// a column-width-derived heuristic that doesn't have to match
	// JT400's per-query value -- but every other field should be
	// byte-identical.
	cols := []SelectColumn{
		{Length: 4},  // ID INTEGER
		{Length: 42}, // NAME VARCHAR(40) (40 + 2 SL)
		{Length: 7},  // AMT DECIMAL(11,2)
	}
	rep := buildOneRowReply(t, cols) // synthesise a no-row reply so fetchMoreRows doesn't EOF
	fc := newFakeConn(rep)
	if _, _, err := fetchMoreRows(fc, cols, 99, 32); err != nil {
		t.Fatalf("fetchMoreRows: %v", err)
	}
	r := bytes.NewReader(fc.written.Bytes())
	hdr, payload, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("re-parse fetchMoreRows output: %v", err)
	}
	if hdr.ReqRepID != ReqDBSQLFetch {
		t.Fatalf("our FETCH ReqRepID 0x%04X != 0x%04X", hdr.ReqRepID, ReqDBSQLFetch)
	}
	tpl, params, err := DecodeDBRequest(payload)
	if err != nil {
		t.Fatalf("decode our FETCH payload: %v", err)
	}
	_ = hdr
	wantORS := uint32(ORSReturnData | ORSResultData | ORSDataCompression)
	if tpl.ORSBitmap != wantORS {
		t.Errorf("our ORSBitmap 0x%08X != JT400's 0x%08X", tpl.ORSBitmap, wantORS)
	}
	if len(params) != 2 {
		t.Fatalf("our params=%d, want 2", len(params))
	}
	if params[0].CodePoint != cpDBFetchScrollOption || len(params[0].Data) != 2 || binary.BigEndian.Uint16(params[0].Data) != 0 {
		t.Errorf("our params[0] = CP 0x%04X data % X, want FetchScrollOption=0x0000", params[0].CodePoint, params[0].Data)
	}
	if params[1].CodePoint != cpDBBlockingFactor || len(params[1].Data) != 4 {
		t.Errorf("our params[1] = CP 0x%04X data % X, want BlockingFactor (4 bytes)", params[1].CodePoint, params[1].Data)
	}
}

// buildOneRowReply synthesises a minimal FETCH reply that ParseDBReply
// will accept: zero error class, zero return code, no result data
// params. fetchMoreRows treats this as exhaustion (no row data CP →
// empty rows; empty batch → exhausted=true), which is all we need
// to run the encoder once and inspect what it wrote.
func buildOneRowReply(t *testing.T, cols []SelectColumn) []byte {
	t.Helper()
	_ = cols
	// 20-byte reply template + zero CPs: ORS bitmap (4), 10 bytes
	// reserved handle echoes, EC (2), RC (4). All zeros = success
	// with no rows. ParseDBReply enforces len(payload) >= 20.
	payload := make([]byte, 20)
	var buf bytes.Buffer
	hdr := Header{
		ServerID:      ServerDatabase,
		ReqRepID:      RepDBReply,
		CorrelationID: 99,
	}
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		t.Fatalf("WriteFrame reply: %v", err)
	}
	return buf.Bytes()
}
