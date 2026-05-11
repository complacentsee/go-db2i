package hostserver

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/complacentsee/goJTOpen/internal/wirelog"
)

// TestCallInOnlyFixtureWireShape replays the database-side request
// flow that JT400 21.0.4 generates for `CALL GOSPROCS.P_INS('A', 10)`
// (no parameter markers, IN-only literal args) and confirms goJTOpen
// produces the same three-frame shape with statement-type
// TYPE_CALL=3.
//
// What JT400 sent in prepared_call_in_only.trace (second connection,
// the post-VRM-detect actual CALL invocation):
//
//	CREATE_RPB        (0x1D00)  -- STMT0001 / CRSR0001
//	PREPARE_DESCRIBE  (0x1803)  -- text = "CALL GOSPROCS.P_INS('A', 10)"
//	                              statement type = 3 (TYPE_CALL)
//	EXECUTE           (0x1805)  -- statement type = 3
//	                              NO CHANGE_DESCRIPTOR (no markers)
//	RPB DELETE        (0x1D02)
//
// Byte-for-byte equality is not asserted because JT400 sets a few
// ORS bits we don't currently mirror on PREPARE_DESCRIBE (ORSDataFormat)
// and on EXECUTE (CursorAttributes 0x00008000); those differences are
// pre-existing across the M2-M8 wire path. What this test pins is
// the property M9-1 actually adds: statement-type byte 0x03 on both
// PREPARE_DESCRIBE and EXECUTE for CALL, and the four-frame sequence
// with no CHANGE_DESCRIPTOR.
//
// The trace also exercises a second connection JT400 21.0.4 opens
// for the SYSIBMADM.ENVSYSINFO VRM-detect query (a side effect of
// skipSignonServer). The relevant CALL frames live on the second
// connID; the test filters on that.
func TestCallInOnlyFixtureWireShape(t *testing.T) {
	const fixture = "prepared_call_in_only.trace"
	const callSQL = "CALL GOSPROCS.P_INS('A', 10)"

	frames := wirelog.Consolidate(loadFixture(t, fixture))

	// Group consolidated sent frames by connID. Pick the second
	// connection -- the first is JT400's VRM auto-detect SELECT
	// against SYSIBMADM.ENVSYSINFO that fires when the URL carries
	// a port (skipSignonServer_ path).
	byConn := map[uint32][][]byte{}
	connOrder := []uint32{}
	for _, f := range frames {
		if f.Direction != wirelog.Sent {
			continue
		}
		if _, ok := byConn[f.ConnID]; !ok {
			connOrder = append(connOrder, f.ConnID)
		}
		// Walk concatenated DSS frames in the consolidated blob.
		b := f.Bytes
		for len(b) >= 8 {
			ln := binary.BigEndian.Uint32(b[0:4])
			if ln < 8 || ln > uint32(len(b)) {
				t.Fatalf("malformed DSS length %d in %s", ln, fixture)
			}
			byConn[f.ConnID] = append(byConn[f.ConnID], append([]byte(nil), b[:ln]...))
			b = b[ln:]
		}
	}
	if len(connOrder) < 2 {
		t.Fatalf("fixture %s: expected >=2 connections (VRM detect + CALL), got %d", fixture, len(connOrder))
	}
	callConn := connOrder[1]
	callSents := byConn[callConn]

	// Locate the CALL-relevant frames by ReqRepID + ServerID 0xE004.
	type frameView struct {
		kind  uint16
		bytes []byte
	}
	wantedKinds := map[uint16]string{
		ReqDBSQLRPBCreate:       "CREATE_RPB",
		ReqDBSQLPrepareDescribe: "PREPARE_DESCRIBE",
		ReqDBSQLExecute:         "EXECUTE",
		ReqDBSQLRPBDelete:       "RPB_DELETE",
	}
	var calls []frameView
	for _, b := range callSents {
		if len(b) < 20 {
			continue
		}
		// ServerID = bytes 6..7. Only SQL service (0xE004).
		if binary.BigEndian.Uint16(b[6:8]) != uint16(ServerDatabase) {
			continue
		}
		rid := binary.BigEndian.Uint16(b[18:20])
		if _, ok := wantedKinds[rid]; ok {
			calls = append(calls, frameView{kind: rid, bytes: b})
		}
	}

	// We must see CREATE_RPB, PREPARE_DESCRIBE, EXECUTE, RPB_DELETE
	// in that order. EXECUTE_IMMEDIATE (0x1806) must NOT appear --
	// confirming JT400 uses PREPARE+EXECUTE for CallableStatement.
	wantSeq := []uint16{
		ReqDBSQLRPBCreate,
		ReqDBSQLPrepareDescribe,
		ReqDBSQLExecute,
		ReqDBSQLRPBDelete,
	}
	if len(calls) != len(wantSeq) {
		var got []string
		for _, c := range calls {
			got = append(got, wantedKinds[c.kind])
		}
		t.Fatalf("fixture CALL frame sequence: got %v (%d frames), want CREATE_RPB/PREPARE_DESCRIBE/EXECUTE/RPB_DELETE",
			got, len(calls))
	}
	for i, want := range wantSeq {
		if calls[i].kind != want {
			t.Fatalf("fixture CALL frame[%d]: kind=0x%04X, want 0x%04X (%s)",
				i, calls[i].kind, want, wantedKinds[want])
		}
	}

	// EXECUTE_IMMEDIATE (0x1806) must not appear on the CALL connection.
	for _, b := range callSents {
		if len(b) >= 20 &&
			binary.BigEndian.Uint16(b[6:8]) == uint16(ServerDatabase) &&
			binary.BigEndian.Uint16(b[18:20]) == ReqDBSQLExecuteImmediate {
			t.Fatalf("fixture CALL path uses EXECUTE_IMMEDIATE; JT400 should send PREPARE+EXECUTE for CallableStatement")
		}
	}

	// JT400's PREPARE_DESCRIBE carries CP 0x3812 (statement type)
	// with value 0x0003 (TYPE_CALL).
	prepareBytes := calls[1].bytes
	if got, ok := findStmtTypeInFrame(prepareBytes); !ok || got != 3 {
		t.Errorf("JT400 PREPARE_DESCRIBE statement-type CP 0x3812 = %d ok=%v, want 3 (TYPE_CALL)", got, ok)
	}
	executeBytes := calls[2].bytes
	if got, ok := findStmtTypeInFrame(executeBytes); !ok || got != 3 {
		t.Errorf("JT400 EXECUTE statement-type CP 0x3812 = %d ok=%v, want 3 (TYPE_CALL)", got, ok)
	}

	// Now drive ExecutePreparedSQL with the same SQL + empty params
	// and assert the same shape comes out the other side. Borrow the
	// captured PREPARE_DESCRIBE + EXECUTE + RPB_DELETE replies from
	// the fixture so ExecutePreparedSQL doesn't stall on a read.
	receivedByConn := map[uint32][][]byte{}
	for _, f := range frames {
		if f.Direction != wirelog.Received {
			continue
		}
		b := f.Bytes
		for len(b) >= 8 {
			ln := binary.BigEndian.Uint32(b[0:4])
			if ln < 8 || ln > uint32(len(b)) {
				break
			}
			receivedByConn[f.ConnID] = append(receivedByConn[f.ConnID], append([]byte(nil), b[:ln]...))
			b = b[ln:]
		}
	}
	// SQL-service replies on the CALL connection. The reply ReqRepID
	// for every SQL request is universally RepDBReply (0x2800); the
	// original request type lives embedded in the reply payload, not
	// in the DSS header. So we filter on ServerDatabase + RepDBReply
	// and rely on send order: the last three SQL-service replies on
	// the CALL connection are PREPARE_DESCRIBE, EXECUTE, RPB_DELETE
	// in that order (the connection-warmup SET_SQL_ATTRIBUTES reply
	// comes earlier in the stream).
	var sqlReplies [][]byte
	for _, b := range receivedByConn[callConn] {
		if len(b) < 20 {
			continue
		}
		if binary.BigEndian.Uint16(b[6:8]) != uint16(ServerDatabase) {
			continue
		}
		if binary.BigEndian.Uint16(b[18:20]) != RepDBReply {
			continue
		}
		sqlReplies = append(sqlReplies, b)
	}
	if len(sqlReplies) < 3 {
		t.Fatalf("need >=3 SQL replies on CALL connection; got %d", len(sqlReplies))
	}
	// Pick the last three: PREPARE_DESCRIBE / EXECUTE / RPB_DELETE.
	callReplies := sqlReplies[len(sqlReplies)-3:]
	conn := newFakeConn(callReplies[0], callReplies[1], callReplies[2])
	if _, err := ExecutePreparedSQL(conn, callSQL, nil, nil, 3); err != nil {
		t.Fatalf("ExecutePreparedSQL(CALL): %v", err)
	}

	// Walk goJTOpen's emitted frames; assert the same 4-frame shape
	// (CREATE_RPB, PREPARE_DESCRIBE, EXECUTE, RPB_DELETE) and the same
	// statement-type=3 invariant. No CHANGE_DESCRIPTOR -- the CALL
	// has no parameter markers so paramShapes is empty.
	r := bytes.NewReader(conn.written.Bytes())
	gotKinds := []uint16{}
	var gotPrepare, gotExecute []byte
	for {
		hdr, payload, err := ReadFrame(r)
		if err != nil {
			break
		}
		gotKinds = append(gotKinds, hdr.ReqRepID)
		// Re-encode so we can inspect by ReqRepID.
		var buf bytes.Buffer
		if err := WriteFrame(&buf, hdr, payload); err != nil {
			t.Fatalf("re-encode emitted frame 0x%04X: %v", hdr.ReqRepID, err)
		}
		switch hdr.ReqRepID {
		case ReqDBSQLPrepareDescribe:
			gotPrepare = buf.Bytes()
		case ReqDBSQLExecute:
			gotExecute = buf.Bytes()
		}
	}
	wantKinds := []uint16{
		ReqDBSQLRPBCreate,
		ReqDBSQLPrepareDescribe,
		ReqDBSQLChangeDescriptor, // absent expected
	}
	_ = wantKinds // silence unused
	wantSeqDriver := []uint16{ReqDBSQLRPBCreate, ReqDBSQLPrepareDescribe, ReqDBSQLExecute, ReqDBSQLRPBDelete}
	if len(gotKinds) != len(wantSeqDriver) {
		t.Fatalf("goJTOpen emitted %d frames; want 4 (CREATE_RPB / PREPARE_DESCRIBE / EXECUTE / RPB_DELETE) -- got kinds=%v",
			len(gotKinds), gotKinds)
	}
	for i, w := range wantSeqDriver {
		if gotKinds[i] != w {
			t.Errorf("goJTOpen frame[%d] = 0x%04X, want 0x%04X (%s)",
				i, gotKinds[i], w, wantedKinds[w])
		}
	}
	if got, ok := findStmtTypeInFrame(gotPrepare); !ok || got != 3 {
		t.Errorf("goJTOpen PREPARE_DESCRIBE statement-type CP 0x3812 = %d ok=%v, want 3 (TYPE_CALL)", got, ok)
	}
	if got, ok := findStmtTypeInFrame(gotExecute); !ok || got != 3 {
		t.Errorf("goJTOpen EXECUTE statement-type CP 0x3812 = %d ok=%v, want 3 (TYPE_CALL)", got, ok)
	}
}

// findStmtTypeInFrame scans a full DSS frame (DSS header + template +
// LL/CP params) for the statement-type CP (0x3812, 2-byte short
// payload) and returns its value. The DSS header is 20 bytes and the
// request template is another 20 bytes, so LL/CP scanning starts at
// offset 40. Returns ok=false when the CP is absent or the frame is
// malformed.
//
// Sibling helper to db_attributes_test.go's findShortCP, which
// operates on payload bytes (frame minus DSS header) and is unrelated
// to this layer.
func findStmtTypeInFrame(frame []byte) (uint16, bool) {
	const startOff = 40
	off := startOff
	for off+6 <= len(frame) {
		ll := binary.BigEndian.Uint32(frame[off : off+4])
		if ll < 6 || off+int(ll) > len(frame) {
			return 0, false
		}
		cp := binary.BigEndian.Uint16(frame[off+4 : off+6])
		if cp == cpDBStatementType && ll == 8 {
			return binary.BigEndian.Uint16(frame[off+6 : off+8]), true
		}
		off += int(ll)
	}
	return 0, false
}
