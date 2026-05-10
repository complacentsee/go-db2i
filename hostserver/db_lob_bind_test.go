package hostserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// TestParameterMarkerFormatLocatorsMatchFixture confirms the parser
// for CP 0x3813 (Super Extended Parameter Marker Format) extracts
// the server-allocated LOB locator handles from the captured
// PREPARE_DESCRIBE reply -- the values JT400 then ships in the
// WRITE_LOB_DATA frames. PUB400 V7R5M0 emits 0x100 for the first
// LOB parameter and 0x200 for the second; if the parser drifts
// (off-by-one in field offset, wrong field record size, etc.) the
// whole bind path silently sends WRITE_LOB_DATA against handle 0
// and the server returns SQL -204.
func TestParameterMarkerFormatLocatorsMatchFixture(t *testing.T) {
	// Find the PREPARE_DESCRIBE reply in the small-LOB fixture.
	all := allReceivedsFromFixture(t, "prepared_blob_insert.trace")
	var prepReply []byte
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		if b[6] == 0xE0 && b[7] == 0x04 && binary.BigEndian.Uint16(b[18:20]) == RepDBReply {
			// Walk the reply's CPs looking for 0x3813 to identify
			// the right reply (there are several DB_REPLYs in the
			// trace; we want the one carrying the param marker
			// format).
			rep, err := parseReplyForTest(b)
			if err != nil {
				continue
			}
			for _, p := range rep.Params {
				if p.CodePoint == 0x3813 {
					prepReply = b
					break
				}
			}
			if prepReply != nil {
				break
			}
		}
	}
	if prepReply == nil {
		t.Fatalf("prepared_blob_insert.trace had no DB_REPLY carrying CP 0x3813")
	}

	rep, err := parseReplyForTest(prepReply)
	if err != nil {
		t.Fatalf("parse PREPARE_DESCRIBE reply: %v", err)
	}
	pmf, err := rep.findSuperExtendedParameterMarkerFormat()
	if err != nil {
		t.Fatalf("findSuperExtendedParameterMarkerFormat: %v", err)
	}
	if len(pmf) != 3 {
		t.Fatalf("PMF field count = %d, want 3 (ID INTEGER, B BLOB, C CLOB)", len(pmf))
	}
	cases := []struct {
		idx        int
		wantSQL    uint16
		wantCCSID  uint16
		wantLocator uint32
		wantIsLOB  bool
	}{
		{0, 496, 0x0000, 0x00000000, false},      // ID INTEGER NN
		{1, 961, 0xFFFF, 0x00000100, true},       // B BLOB
		{2, 965, 0x0111, 0x00000200, true},       // C CLOB CCSID 273
	}
	for _, tc := range cases {
		f := pmf[tc.idx]
		if f.SQLType != tc.wantSQL {
			t.Errorf("field %d SQLType = 0x%04X (%d), want 0x%04X (%d)", tc.idx, f.SQLType, f.SQLType, tc.wantSQL, tc.wantSQL)
		}
		if f.CCSID != tc.wantCCSID {
			t.Errorf("field %d CCSID = 0x%04X, want 0x%04X", tc.idx, f.CCSID, tc.wantCCSID)
		}
		if f.LOBLocator != tc.wantLocator {
			t.Errorf("field %d LOBLocator = 0x%08X, want 0x%08X", tc.idx, f.LOBLocator, tc.wantLocator)
		}
		if got := f.IsLOB(); got != tc.wantIsLOB {
			t.Errorf("field %d IsLOB() = %v, want %v", tc.idx, got, tc.wantIsLOB)
		}
		if f.FieldLength != 4 {
			t.Errorf("field %d FieldLength = %d, want 4 (locator slot)", tc.idx, f.FieldLength)
		}
	}
}

// TestWriteLOBDataRequestBytesMatchFixture re-runs the encoder for
// the BLOB and CLOB WRITE_LOB_DATA frames in
// prepared_blob_insert.trace and asserts byte-equality. Confirms the
// CP order (3822, 3818, 3819, 381A, 381B, 381D) and the CP 0x381D
// payload header layout (CCSID 0xFFFF + 4-byte length) match JT400's.
func TestWriteLOBDataRequestBytesMatchFixture(t *testing.T) {
	all := allSentsByServerID(t, "prepared_blob_insert.trace", ServerDatabase)
	var writeBLOB, writeCLOB []byte
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		if binary.BigEndian.Uint16(b[18:20]) != ReqDBSQLWriteLOBData {
			continue
		}
		switch {
		case writeBLOB == nil:
			writeBLOB = b
		case writeCLOB == nil:
			writeCLOB = b
		}
	}
	if writeBLOB == nil || writeCLOB == nil {
		t.Fatalf("expected 2 WRITE_LOB_DATA frames in fixture, got blob=%v clob=%v",
			writeBLOB != nil, writeCLOB != nil)
	}

	// BLOB: 8 KiB byte ramp 0x00..0xFF repeating.
	blobBytes := make([]byte, 8*1024)
	for i := range blobBytes {
		blobBytes[i] = byte(i & 0xFF)
	}

	// Re-encode using the same correlation IDs the trace used.
	t.Run("BLOB", func(t *testing.T) {
		gotCorr := binary.BigEndian.Uint32(writeBLOB[12:16])
		var conn fakeReadWriter
		conn.replies = append(conn.replies, makeOKReply(gotCorr))
		err := WriteLOBData(&conn, 0x00000100, 0, 8192, blobBytes, true, false, gotCorr)
		if err != nil {
			t.Fatalf("WriteLOBData BLOB: %v", err)
		}
		assertBytesEqualWithDiff(t, "WRITE_LOB_DATA(BLOB)", conn.written.Bytes(), writeBLOB)
	})

	// CLOB: 8 KiB+ of "Hello, IBM i! " repeated, EBCDIC-CCSID-273
	// pre-encoded -- which on PUB400 is identical to CCSID 37 for
	// the basic ASCII subset, so the ebcdic.CCSID273 codec gives
	// us bytes byte-equal to JT400's. Fixture's CP 0x3819 says
	// 8204 bytes (length JT400's converter produced from the
	// repeated source string).
	clobBytes := repeatedClobBytes(t, "Hello, IBM i! ", 8204)

	t.Run("CLOB", func(t *testing.T) {
		gotCorr := binary.BigEndian.Uint32(writeCLOB[12:16])
		var conn fakeReadWriter
		conn.replies = append(conn.replies, makeOKReply(gotCorr))
		err := WriteLOBData(&conn, 0x00000200, 0, 8204, clobBytes, true, false, gotCorr)
		if err != nil {
			t.Fatalf("WriteLOBData CLOB: %v", err)
		}
		assertBytesEqualWithDiff(t, "WRITE_LOB_DATA(CLOB)", conn.written.Bytes(), writeCLOB)
	})
}

// TestWriteLOBDataLargeFixture checks the 64 KiB BLOB INSERT in
// prepared_blob_insert_large.trace ships in a single DSS frame -- no
// chunking, matching JT400's default behaviour. If the encoder ever
// starts splitting frames automatically, this test fails (which we
// want -- chunking is opt-in via the LOBStream path).
func TestWriteLOBDataLargeFixture(t *testing.T) {
	all := allSentsByServerID(t, "prepared_blob_insert_large.trace", ServerDatabase)
	var writeBLOB []byte
	for _, b := range all {
		if len(b) < 20 {
			continue
		}
		if binary.BigEndian.Uint16(b[18:20]) == ReqDBSQLWriteLOBData {
			writeBLOB = b
			break
		}
	}
	if writeBLOB == nil {
		t.Fatalf("expected WRITE_LOB_DATA frame in large fixture")
	}
	if got := binary.BigEndian.Uint32(writeBLOB[0:4]); got <= 65536 {
		t.Fatalf("expected single >64 KiB frame, got DSS Length %d", got)
	}

	// Recreate the 64 KiB ramp the fixture used.
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte((i * 31) & 0xFF)
	}

	gotCorr := binary.BigEndian.Uint32(writeBLOB[12:16])
	var conn fakeReadWriter
	conn.replies = append(conn.replies, makeOKReply(gotCorr))
	err := WriteLOBData(&conn, 0x00000100, 0, 65536, payload, true, false, gotCorr)
	if err != nil {
		t.Fatalf("WriteLOBData: %v", err)
	}
	assertBytesEqualWithDiff(t, "WRITE_LOB_DATA(64KB BLOB)", conn.written.Bytes(), writeBLOB)
}

// TestEncodeDBExtendedDataLOBBind confirms the CP 0x381F payload at a
// LOB slot is the 4-byte locator handle, not the LOB content. Mirrors
// the EXECUTE bind-block layout captured in prepared_blob_insert.trace
// where the BLOB and CLOB locators (0x100, 0x200) round-trip through
// the SQLDA value block.
func TestEncodeDBExtendedDataLOBBind(t *testing.T) {
	shapes := []PreparedParam{
		{SQLType: 497, FieldLength: 4},  // ID INTEGER nullable
		{SQLType: 961, FieldLength: 4, CCSID: 0xFFFF}, // BLOB locator
		{SQLType: 965, FieldLength: 4, CCSID: 0x0111}, // CLOB locator (CCSID 273)
	}
	values := []any{int32(1), uint32(0x00000100), uint32(0x00000200)}
	got, err := EncodeDBExtendedData(shapes, values)
	if err != nil {
		t.Fatalf("EncodeDBExtendedData: %v", err)
	}
	// Layout: 20-byte header + 6-byte indicator block + 12-byte row data.
	want := []byte{
		// Header
		0x00, 0x00, 0x00, 0x01, // ConsistencyToken
		0x00, 0x00, 0x00, 0x01, // RowCount = 1
		0x00, 0x03,             // ColumnCount = 3
		0x00, 0x02,             // IndicatorSize = 2
		0x00, 0x00, 0x00, 0x00, // reserved
		0x00, 0x00, 0x00, 0x0c, // RowSize = 12
		// Indicators (3 cols * 2 bytes, all non-null)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Row data: ID=1, BLOB handle=0x100, CLOB handle=0x200
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x01, 0x00,
		0x00, 0x00, 0x02, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeDBExtendedData mismatch:\n got: % x\nwant: % x", got, want)
	}
}

// TestBindLOBStreamChunking confirms the streamed bind path emits
// multiple WRITE_LOB_DATA frames at advancing offsets when the
// declared length exceeds LOBStreamChunkSize. Validates the chunking
// invariants (truncate=false until the last frame, monotonically
// growing offsets, sum of chunk sizes equals declared length).
func TestBindLOBStreamChunking(t *testing.T) {
	const total = 80 * 1024 // 80 KiB triggers 3 chunks at 32 KiB chunk size
	src := newRepeatingReader(total, 0xAA)
	stream := &simpleStream{r: src, length: total}

	var conn fakeReadWriter
	corr := uint32(100)
	for i := 0; i < 4; i++ {
		conn.replies = append(conn.replies, makeOKReply(corr+uint32(i)))
	}
	corrFn := func() uint32 {
		c := corr
		corr++
		return c
	}
	pmf := []ParameterMarkerField{
		{SQLType: 961, FieldLength: 4, CCSID: 0xFFFF, LOBLocator: 0x12345678},
	}
	shapes := []PreparedParam{{SQLType: 449, FieldLength: 4 + 2, Precision: 4, CCSID: 0xFFFF}}
	values := []any{stream}
	if err := bindLOBParameters(&conn, shapes, values, pmf, corrFn); err != nil {
		t.Fatalf("bindLOBParameters: %v", err)
	}

	frames := splitDSSFrames(t, conn.written.Bytes())
	var writes []frameView
	for _, f := range frames {
		if f.reqRepID == ReqDBSQLWriteLOBData {
			writes = append(writes, f)
		}
	}
	if len(writes) != 3 {
		t.Fatalf("WRITE_LOB_DATA frame count = %d, want 3", len(writes))
	}
	var sumSize uint32
	prevOff := int64(-1)
	for i, w := range writes {
		cps, err := parseCPs(w.payload)
		if err != nil {
			t.Fatalf("frame %d parse CPs: %v", i, err)
		}
		off, _ := cpUint32(cps, cpDBStartOffset)
		size, _ := cpUint32(cps, cpDBRequestedSize)
		trunc, _ := cpByte(cps, cpDBLOBTruncation)
		if int64(off) <= prevOff {
			t.Errorf("frame %d offset %d not strictly greater than previous %d", i, off, prevOff)
		}
		prevOff = int64(off)
		sumSize += size
		isLast := i == len(writes)-1
		wantTrunc := byte(0xF1)
		if isLast {
			wantTrunc = 0xF0
		}
		if trunc != wantTrunc {
			t.Errorf("frame %d truncate = 0x%02X, want 0x%02X (isLast=%v)", i, trunc, wantTrunc, isLast)
		}
	}
	if sumSize != total {
		t.Errorf("sum of WRITE_LOB_DATA RequestedSize = %d, want %d", sumSize, total)
	}
	// After bindLOBParameters runs, the value at slot 0 must have
	// been replaced with the locator handle so EXECUTE encodes
	// the right SQLDA bytes.
	if got, ok := values[0].(uint32); !ok || got != 0x12345678 {
		t.Errorf("values[0] post-bind = %#v, want uint32(0x12345678)", values[0])
	}
	// Shape must now reflect the LOB locator type.
	if shapes[0].SQLType != 961 || shapes[0].FieldLength != 4 {
		t.Errorf("shapes[0] post-bind = %+v, want SQLType=961 FieldLength=4", shapes[0])
	}
}

// TestBindLOBBytesChunking pins the chunked WRITE_LOB_DATA shape for
// []byte BLOB binds. Single-frame uploads of multi-megabyte LOBs are
// catastrophically slow against IBM i V7R6 (a 64 MiB BLOB INSERT
// spent ~3 minutes server-side in our testing); we mirror JT400's
// 1 MB chunking to keep the server inside its fast path. This test
// asserts:
//
//   - a 2.5 MiB []byte produces 3 WRITE_LOB_DATA frames at LOBBlockSize=1MB
//   - StartOffset advances strictly per frame in byte units
//   - LOBTruncation = 0xF1 ("don't truncate") on every non-last frame
//     and 0xF0 ("truncate") on the last
//   - sum of RequestedSize equals total payload bytes
func TestBindLOBBytesChunking(t *testing.T) {
	const total = LOBBlockSize*2 + 500_000 // 3 frames: 1MB, 1MB, 500KB
	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i)
	}

	var conn fakeReadWriter
	corr := uint32(200)
	for i := 0; i < 4; i++ {
		conn.replies = append(conn.replies, makeOKReply(corr+uint32(i)))
	}
	corrFn := func() uint32 {
		c := corr
		corr++
		return c
	}
	pmf := []ParameterMarkerField{
		{SQLType: 961, FieldLength: 4, CCSID: 0xFFFF, LOBLocator: 0xCAFEBABE},
	}
	shapes := []PreparedParam{{SQLType: 449, FieldLength: 4 + 2, Precision: 4, CCSID: 0xFFFF}}
	values := []any{data}
	if err := bindLOBParameters(&conn, shapes, values, pmf, corrFn); err != nil {
		t.Fatalf("bindLOBParameters: %v", err)
	}

	frames := splitDSSFrames(t, conn.written.Bytes())
	var writes []frameView
	for _, f := range frames {
		if f.reqRepID == ReqDBSQLWriteLOBData {
			writes = append(writes, f)
		}
	}
	if len(writes) != 3 {
		t.Fatalf("WRITE_LOB_DATA frame count = %d, want 3 (1MB+1MB+500KB)", len(writes))
	}
	var sumSize uint32
	prevOff := int64(-1)
	for i, w := range writes {
		cps, err := parseCPs(w.payload)
		if err != nil {
			t.Fatalf("frame %d parse CPs: %v", i, err)
		}
		off, _ := cpUint32(cps, cpDBStartOffset)
		size, _ := cpUint32(cps, cpDBRequestedSize)
		trunc, _ := cpByte(cps, cpDBLOBTruncation)
		if int64(off) <= prevOff {
			t.Errorf("frame %d offset %d not strictly greater than previous %d", i, off, prevOff)
		}
		prevOff = int64(off)
		sumSize += size
		isLast := i == len(writes)-1
		wantTrunc := byte(0xF1)
		if isLast {
			wantTrunc = 0xF0
		}
		if trunc != wantTrunc {
			t.Errorf("frame %d truncate = 0x%02X, want 0x%02X (isLast=%v)", i, trunc, wantTrunc, isLast)
		}
	}
	if sumSize != total {
		t.Errorf("sum of WRITE_LOB_DATA RequestedSize = %d, want %d", sumSize, total)
	}
	if got, ok := values[0].(uint32); !ok || got != 0xCAFEBABE {
		t.Errorf("values[0] post-bind = %#v, want uint32(0xCAFEBABE)", values[0])
	}
}

// TestBindLOBBytesChunkingEmpty confirms the zero-length BLOB bind
// still emits exactly one WRITE_LOB_DATA frame with truncate=true,
// matching JT400's `setBytes(2, new byte[0])` behaviour. Without this
// frame the server would carry the locator handle through EXECUTE
// with no content uploaded, leaving stale data from a prior batch
// row in the column.
func TestBindLOBBytesChunkingEmpty(t *testing.T) {
	var conn fakeReadWriter
	conn.replies = append(conn.replies, makeOKReply(300))
	corr := uint32(300)
	corrFn := func() uint32 {
		c := corr
		corr++
		return c
	}
	pmf := []ParameterMarkerField{
		{SQLType: 961, FieldLength: 4, CCSID: 0xFFFF, LOBLocator: 0x11223344},
	}
	shapes := []PreparedParam{{SQLType: 449, FieldLength: 4 + 2, Precision: 4, CCSID: 0xFFFF}}
	values := []any{[]byte{}}
	if err := bindLOBParameters(&conn, shapes, values, pmf, corrFn); err != nil {
		t.Fatalf("bindLOBParameters: %v", err)
	}
	frames := splitDSSFrames(t, conn.written.Bytes())
	var writes []frameView
	for _, f := range frames {
		if f.reqRepID == ReqDBSQLWriteLOBData {
			writes = append(writes, f)
		}
	}
	if len(writes) != 1 {
		t.Fatalf("empty LOB bind: WRITE_LOB_DATA frame count = %d, want 1", len(writes))
	}
	cps, err := parseCPs(writes[0].payload)
	if err != nil {
		t.Fatalf("parse CPs: %v", err)
	}
	if size, _ := cpUint32(cps, cpDBRequestedSize); size != 0 {
		t.Errorf("empty LOB RequestedSize = %d, want 0", size)
	}
	if trunc, _ := cpByte(cps, cpDBLOBTruncation); trunc != 0xF0 {
		t.Errorf("empty LOB truncate = 0x%02X, want 0xF0 (truncate)", trunc)
	}
}

// ---- helpers ----

// fakeReadWriter implements io.ReadWriter for tests that need to
// inspect what the encoder wrote and feed canned replies. Replies
// are consumed in order; once exhausted, Read returns io.EOF.
type fakeReadWriter struct {
	written bytes.Buffer
	replies [][]byte
	cursor  []byte
}

func (f *fakeReadWriter) Write(p []byte) (int, error) {
	return f.written.Write(p)
}

func (f *fakeReadWriter) Read(p []byte) (int, error) {
	if len(f.cursor) == 0 {
		if len(f.replies) == 0 {
			return 0, io.EOF
		}
		f.cursor = f.replies[0]
		f.replies = f.replies[1:]
	}
	n := copy(p, f.cursor)
	f.cursor = f.cursor[n:]
	return n, nil
}

// makeOKReply builds a minimal DB_REPLY frame that ParseDBReply +
// makeDb2Error consider successful (errorClass=0, returnCode=0, no
// SQLCA / SQLERRMC). Used to drive WRITE_LOB_DATA tests that don't
// care about the reply CPs, only that the function returns nil.
func makeOKReply(corr uint32) []byte {
	// 20-byte header + 20-byte template + zero CPs.
	frame := make([]byte, 40)
	binary.BigEndian.PutUint32(frame[0:4], 40)
	// HeaderID
	binary.BigEndian.PutUint16(frame[4:6], 0)
	binary.BigEndian.PutUint16(frame[6:8], uint16(ServerDatabase))
	binary.BigEndian.PutUint32(frame[8:12], 0)
	binary.BigEndian.PutUint32(frame[12:16], corr)
	binary.BigEndian.PutUint16(frame[16:18], 20)
	binary.BigEndian.PutUint16(frame[18:20], RepDBReply)
	// Template (offset 20..39): all zero is OK -- error class = 0,
	// return code = 0, ORS bitmap zero. ParseDBReply only looks at
	// the param-count field and the optional CP list.
	return frame
}

// repeatedClobBytes builds the EBCDIC bytes JT400 emits for the CLOB
// fixture: "Hello, IBM i! " repeated until the byte count reaches
// totalBytes. We use ebcdicForCCSID(273) so CCSID 273's specific
// codepoints fall out -- on PUB400 this is identical to CCSID 37 for
// the basic ASCII subset of "Hello, IBM i!".
func repeatedClobBytes(t *testing.T, unit string, totalBytes int) []byte {
	t.Helper()
	conv := ebcdicForCCSID(273)
	unitEbcdic, err := conv.Encode(unit)
	if err != nil {
		t.Fatalf("encode CLOB unit: %v", err)
	}
	out := make([]byte, 0, totalBytes)
	for len(out) < totalBytes {
		out = append(out, unitEbcdic...)
	}
	return out[:totalBytes]
}

// repeatingReader returns count bytes of the given fill value,
// bounded; like bytes.Repeat but lazy so a 1 MiB test doesn't
// allocate a 1 MiB buffer up front.
func newRepeatingReader(count int64, fill byte) io.Reader {
	return &repeatingReader{remaining: count, fill: fill}
}

type repeatingReader struct {
	remaining int64
	fill      byte
}

func (r *repeatingReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	for i := 0; i < n; i++ {
		p[i] = r.fill
	}
	r.remaining -= int64(n)
	return n, nil
}

// simpleStream wraps an io.Reader as hostserver.LOBStream.
type simpleStream struct {
	r      io.Reader
	length int64
}

func (s *simpleStream) LOBLength() int64 { return s.length }
func (s *simpleStream) LOBNextChunk(buf []byte) (int, error) {
	return s.r.Read(buf)
}

// frameView is a parsed DSS frame ready for CP-level inspection.
type frameView struct {
	reqRepID uint16
	payload  []byte // bytes after the 20-byte template
}

// splitDSSFrames walks the consolidated wire bytes and returns each
// DSS frame's ReqRepID + the bytes after its 20-byte template.
func splitDSSFrames(t *testing.T, b []byte) []frameView {
	t.Helper()
	var out []frameView
	for len(b) >= 20 {
		ln := binary.BigEndian.Uint32(b[0:4])
		if ln < 40 || int(ln) > len(b) {
			t.Fatalf("malformed DSS at len %d in %d-byte buffer", ln, len(b))
		}
		out = append(out, frameView{
			reqRepID: binary.BigEndian.Uint16(b[18:20]),
			payload:  append([]byte(nil), b[40:ln]...),
		})
		b = b[ln:]
	}
	return out
}

// parseCPs decodes the LL/CP/Data list following a request template.
func parseCPs(b []byte) (map[uint16][]byte, error) {
	out := map[uint16][]byte{}
	for len(b) >= 6 {
		ll := binary.BigEndian.Uint32(b[0:4])
		if ll < 6 || int(ll) > len(b) {
			return nil, errors.New("malformed CP")
		}
		cp := binary.BigEndian.Uint16(b[4:6])
		out[cp] = append([]byte(nil), b[6:ll]...)
		b = b[ll:]
	}
	return out, nil
}

func cpUint32(cps map[uint16][]byte, cp uint16) (uint32, bool) {
	d, ok := cps[cp]
	if !ok || len(d) < 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(d[:4]), true
}

func cpByte(cps map[uint16][]byte, cp uint16) (byte, bool) {
	d, ok := cps[cp]
	if !ok || len(d) < 1 {
		return 0, false
	}
	return d[0], true
}

// parseReplyForTest is a thin wrapper -- ParseDBReply doesn't accept
// a full DSS frame, only the payload after the 20-byte header. Tests
// frequently want the convenience path.
func parseReplyForTest(frame []byte) (*DBReply, error) {
	if len(frame) < 40 {
		return nil, errors.New("frame too short")
	}
	return ParseDBReply(frame[20:])
}
