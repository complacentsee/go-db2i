package hostserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/complacentsee/go-db2i/internal/wirelog"
)

// Issue #68 Phase 0 -- offline decode + confirm of the live JT400 captures
// of stored-procedure ARRAY parameters on PUB400 V7R5M0.
//
// On DB2 for i an ARRAY crosses the host-server wire ONLY as a
// stored-procedure parameter (IN/OUT/INOUT), never a result column
// (#39 / SQL-20441). When any parameter is an array, the IN/INOUT marker
// data rides in CP 0x382F (DBVariableData) instead of the scalar
// CP 0x381F, and the OUT/INOUT values come back in CP 0x3901
// (DBVariableData) instead of the scalar CP 0x380E.
//
// Until this capture the 0x382F/0x3901 byte layout was INFERRED from the
// JTOpen source. These fixtures are the real frames JT400 21.0.4
// generated against PUB400 (see testdata/jtopen-fixtures Cases.java
// "array_*"), and this test parses them and confirms the layout against
// the inferred spec, flagging any divergence. It deliberately writes NO
// production encoder/decoder -- Phase 0 is capture-and-confirm only.
//
// Confirmed DBVariableData layout (big-endian):
//
//	[4B consistency token][2B column count][descriptors...][indicators...][data...]
//
// Descriptor shapes (DBVariableData.java:186-227, setHeaderColumnInfo):
//   - non-null array : 12B = 0x9911 | elemType(2) | elemDataLen(4) | cardinality(4)
//   - whole-null array:  4B = 0x9911 0xFFFF                 (no indicators, no data)
//   - scalar         :  8B = 0x9912 | sqlType(2)  | dataLen(4)
//
// Indicators are always 2B each, grouped per-column-then-per-element,
// contiguous, before all data. 0x0000 = not null, 0xFFFF = null element.
// Data uses a FIXED stride of elemDataLen per element; null elements
// still occupy a (zeroed) slot. VARCHAR(n) stride = n+2 (a 2B character-
// length prefix + char bytes + zero-fill).

const (
	cpArrayExecuteVarData = 0x382F // IN/INOUT request DBVariableData
	cpArrayReplyVarData   = 0x3901 // OUT/INOUT reply DBVariableData
)

// varDataCol is one decoded column of a DBVariableData structure.
type varDataCol struct {
	tag         uint16 // 0x9911 (array) or 0x9912 (scalar)
	isArray     bool
	isNullArray bool   // whole-array NULL: 0x9911 0xFFFF, no indicators/data
	elemType    uint16 // element/scalar SQL type code
	elemLen     uint32 // per-element fixed stride
	count       int    // indicator/data slot count: cardinality (array), 1 (scalar), 0 (null array)
	indicators  []uint16
	slots       [][]byte // each elemLen bytes
}

// varData is a decoded CP 0x382F / 0x3901 DBVariableData payload.
type varData struct {
	token     uint32
	colCount  int
	cols      []varDataCol
	structEnd int    // offset where indicators+data end
	trailing  []byte // bytes after structEnd (JTOpen's overlay navigates past these)
}

// decodeVarData parses a DBVariableData payload (the CP 0x382F/0x3901
// Data bytes, i.e. everything after the 4-byte LL + 2-byte CP) into its
// header/indicator/data regions, mirroring DBVariableData.overlay.
func decodeVarData(p []byte) (*varData, error) {
	be := binary.BigEndian
	if len(p) < 6 {
		return nil, fmt.Errorf("vardata: %d bytes < 6 (token+count)", len(p))
	}
	vd := &varData{token: be.Uint32(p[0:4]), colCount: int(be.Uint16(p[4:6]))}
	off := 6

	// Descriptors.
	for i := 0; i < vd.colCount; i++ {
		if off+2 > len(p) {
			return nil, fmt.Errorf("vardata: descriptor %d: truncated tag", i)
		}
		c := varDataCol{tag: be.Uint16(p[off : off+2])}
		switch c.tag {
		case 0x9911:
			if off+4 > len(p) {
				return nil, fmt.Errorf("vardata: descriptor %d: truncated array head", i)
			}
			if be.Uint16(p[off+2:off+4]) == 0xFFFF {
				c.isArray, c.isNullArray = true, true
				off += 4
			} else {
				if off+12 > len(p) {
					return nil, fmt.Errorf("vardata: descriptor %d: truncated array descriptor", i)
				}
				c.isArray = true
				c.elemType = be.Uint16(p[off+2 : off+4])
				c.elemLen = be.Uint32(p[off+4 : off+8])
				c.count = int(be.Uint32(p[off+8 : off+12]))
				off += 12
			}
		case 0x9912:
			if off+8 > len(p) {
				return nil, fmt.Errorf("vardata: descriptor %d: truncated scalar descriptor", i)
			}
			c.elemType = be.Uint16(p[off+2 : off+4])
			c.elemLen = be.Uint32(p[off+4 : off+8])
			c.count = 1
			off += 8
		default:
			return nil, fmt.Errorf("vardata: descriptor %d: unknown tag 0x%04X", i, c.tag)
		}
		vd.cols = append(vd.cols, c)
	}

	// Indicator region: every column's indicators, contiguous.
	for i := range vd.cols {
		c := &vd.cols[i]
		c.indicators = make([]uint16, c.count)
		for e := 0; e < c.count; e++ {
			if off+2 > len(p) {
				return nil, fmt.Errorf("vardata: col %d indicator %d truncated", i, e)
			}
			c.indicators[e] = be.Uint16(p[off : off+2])
			off += 2
		}
	}

	// Data region: every column's fixed-stride slots, contiguous.
	for i := range vd.cols {
		c := &vd.cols[i]
		if c.isNullArray {
			continue
		}
		c.slots = make([][]byte, c.count)
		for e := 0; e < c.count; e++ {
			end := off + int(c.elemLen)
			if end > len(p) {
				return nil, fmt.Errorf("vardata: col %d data slot %d truncated (need %d, have %d)", i, e, c.elemLen, len(p)-off)
			}
			c.slots[e] = p[off:end]
			off = end
		}
	}

	vd.structEnd = off
	vd.trailing = p[off:]
	return vd, nil
}

// asInt32 interprets a 4-byte fixed INTEGER element slot.
func asInt32(b []byte) int32 { return int32(binary.BigEndian.Uint32(b)) }

// varcharSlot splits a VARCHAR element slot into its 2-byte char-length
// prefix and the raw (pre-EBCDIC) character bytes (prefix-length of them).
func varcharSlot(b []byte) (clen int, chars []byte) {
	clen = int(binary.BigEndian.Uint16(b[:2]))
	return clen, b[2 : 2+clen]
}

// arrayDSSFrames returns each complete DSS frame (header+payload) for the
// given direction across all connections in a JT400 trace fixture.
func arrayDSSFrames(t *testing.T, traceName string, dir wirelog.Direction) [][]byte {
	t.Helper()
	var out [][]byte
	for _, f := range wirelog.Consolidate(loadFixture(t, traceName)) {
		if f.Direction != dir {
			continue
		}
		b := f.Bytes
		for len(b) >= HeaderLength {
			ln := binary.BigEndian.Uint32(b[0:4])
			if ln < HeaderLength || ln > uint32(len(b)) {
				break
			}
			out = append(out, append([]byte(nil), b[:ln]...))
			b = b[ln:]
		}
	}
	return out
}

// findArrayRequestParam returns the Data of the first database-request
// parameter with the given codepoint (e.g. CP 0x382F).
func findArrayRequestParam(t *testing.T, traceName string, cp uint16) ([]byte, bool) {
	t.Helper()
	for _, frame := range arrayDSSFrames(t, traceName, wirelog.Sent) {
		var hdr Header
		if err := hdr.UnmarshalBinary(frame[:HeaderLength]); err != nil || hdr.ServerID != ServerDatabase {
			continue
		}
		_, params, err := DecodeDBRequest(frame[HeaderLength:])
		if err != nil {
			continue // not a SQL request frame; skip
		}
		for _, p := range params {
			if p.CodePoint == cp {
				return p.Data, true
			}
		}
	}
	return nil, false
}

// findArrayReplyParam returns the Data of the first database-reply
// parameter with the given codepoint (e.g. CP 0x3901).
func findArrayReplyParam(t *testing.T, traceName string, cp uint16) ([]byte, bool) {
	t.Helper()
	for _, frame := range arrayDSSFrames(t, traceName, wirelog.Received) {
		var hdr Header
		if err := hdr.UnmarshalBinary(frame[:HeaderLength]); err != nil || hdr.ServerID != ServerDatabase {
			continue
		}
		rep, err := ParseDBReply(frame[HeaderLength:])
		if err != nil {
			continue
		}
		for _, p := range rep.Params {
			if p.CodePoint == cp {
				return p.Data, true
			}
		}
	}
	return nil, false
}

// --- Canonical .bin fixtures (extracted CP payloads) -----------------

// TestArrayParamExecute382FFixture confirms the IN-array EXECUTE payload
// (CP 0x382F) for the canonical INTEGER case captured in
// array_in_int_basic: CALL ...(?) with INTEGER ARRAY = [10,20,30,40,50].
func TestArrayParamExecute382FFixture(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("testdata", "array_param_execute_382f.bin"))
	if err != nil {
		t.Skipf("fixture not present (run TestRefileArrayWireFixtures): %v", err)
	}
	vd, err := decodeVarData(payload)
	if err != nil {
		t.Fatalf("decodeVarData: %v", err)
	}
	if vd.token != 1 {
		t.Errorf("consistency token = %d, want 1", vd.token)
	}
	if vd.colCount != 1 || len(vd.cols) != 1 {
		t.Fatalf("colCount = %d, want 1", vd.colCount)
	}
	c := vd.cols[0]
	if c.tag != 0x9911 || !c.isArray || c.isNullArray {
		t.Fatalf("col0 tag=0x%04X isArray=%v isNull=%v, want non-null array (0x9911)", c.tag, c.isArray, c.isNullArray)
	}
	// Request side uses the EVEN/not-null element type code (INTEGER 496);
	// the reply (0x3901) and describe (0x3813) use 497 (nullable).
	if c.elemType != 496 {
		t.Errorf("element SQL type = %d, want 496 (INTEGER, not-null code on the request side)", c.elemType)
	}
	if c.elemLen != 4 {
		t.Errorf("element data length = %d, want 4 (INTEGER stride)", c.elemLen)
	}
	if c.count != 5 {
		t.Errorf("cardinality = %d, want 5", c.count)
	}
	for i, ind := range c.indicators {
		if ind != 0x0000 {
			t.Errorf("indicator[%d] = 0x%04X, want 0x0000 (not null)", i, ind)
		}
	}
	want := []int32{10, 20, 30, 40, 50}
	for i, w := range want {
		if got := asInt32(c.slots[i]); got != w {
			t.Errorf("element[%d] = %d, want %d", i, got, w)
		}
	}
	if len(vd.trailing) != 0 {
		t.Logf("note: %d trailing byte(s) after the 0x382F structure: % X", len(vd.trailing), vd.trailing)
	}
}

// TestArrayParamReply3901Fixture confirms the OUT-array reply payload
// (CP 0x3901) for the canonical INTEGER case captured in array_out_int:
// the proc SET P_A = ARRAY[11,22,33].
func TestArrayParamReply3901Fixture(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("testdata", "array_param_reply_3901.bin"))
	if err != nil {
		t.Skipf("fixture not present (run TestRefileArrayWireFixtures): %v", err)
	}
	vd, err := decodeVarData(payload)
	if err != nil {
		t.Fatalf("decodeVarData: %v", err)
	}
	if vd.token != 0 {
		t.Errorf("consistency token = %d, want 0 (reply side)", vd.token)
	}
	if vd.colCount != 1 || len(vd.cols) != 1 {
		t.Fatalf("colCount = %d, want 1", vd.colCount)
	}
	c := vd.cols[0]
	if c.tag != 0x9911 || !c.isArray || c.isNullArray {
		t.Fatalf("col0 tag=0x%04X isArray=%v isNull=%v, want non-null array (0x9911)", c.tag, c.isArray, c.isNullArray)
	}
	// Reply side uses the ODD/nullable element type code (INTEGER 497).
	if c.elemType != 497 {
		t.Errorf("element SQL type = %d, want 497 (INTEGER, nullable code on the reply side)", c.elemType)
	}
	if c.elemLen != 4 {
		t.Errorf("element data length = %d, want 4", c.elemLen)
	}
	if c.count != 3 {
		t.Errorf("cardinality = %d, want 3", c.count)
	}
	want := []int32{11, 22, 33}
	for i, w := range want {
		if got := asInt32(c.slots[i]); got != w {
			t.Errorf("element[%d] = %d, want %d", i, got, w)
		}
	}
	// JT400's overlay computes its navigation offsets from the descriptor
	// and ignores any bytes past the data region; the server appends 4
	// trailing zero bytes on the 0x3901 reply. Pin the observation.
	if len(vd.trailing) != 4 {
		t.Logf("note: 0x3901 trailing = %d byte(s) (% X); INTEGER capture showed 4", len(vd.trailing), vd.trailing)
	}
}

// --- Comprehensive across-case confirmation ---------------------------

// TestArrayParamWireLayoutAcrossCases decodes the 0x382F request and/or
// 0x3901 reply from every captured array case and confirms the inferred
// DBVariableData layout end to end: INTEGER and VARCHAR element types,
// a null element, a whole-null array, and a mixed scalar+array proc.
func TestArrayParamWireLayoutAcrossCases(t *testing.T) {
	// array_in_int_basic: IN INTEGER[5] = [10,20,30,40,50].
	t.Run("in_int_basic", func(t *testing.T) {
		vd := mustReq(t, "array_in_int_basic.trace")
		c := onlyCol(t, vd, 0x9911)
		assertArrayInts(t, c, 496, 4, []int32{10, 20, 30, 40, 50}, nil)
	})

	// array_in_int_nullelem: element[2] is NULL -> indicator 0xFFFF,
	// data slot present but zeroed.
	t.Run("in_int_nullelem", func(t *testing.T) {
		vd := mustReq(t, "array_in_int_nullelem.trace")
		c := onlyCol(t, vd, 0x9911)
		assertArrayInts(t, c, 496, 4, []int32{10, 20, 0, 40, 50}, map[int]uint16{2: 0xFFFF})
		if asInt32(c.slots[2]) != 0 {
			t.Errorf("null element data slot = %d, want 0 (zeroed slot still present)", asInt32(c.slots[2]))
		}
	})

	// array_in_int_wholenull: the array value itself is SQL NULL ->
	// descriptor 0x9911 0xFFFF, no indicators, no data.
	t.Run("in_int_wholenull", func(t *testing.T) {
		vd := mustReq(t, "array_in_int_wholenull.trace")
		if vd.colCount != 1 {
			t.Fatalf("colCount = %d, want 1", vd.colCount)
		}
		c := vd.cols[0]
		if !c.isArray || !c.isNullArray {
			t.Fatalf("want whole-null array (0x9911 0xFFFF), got tag=0x%04X isArray=%v isNull=%v", c.tag, c.isArray, c.isNullArray)
		}
		if len(c.indicators) != 0 || len(c.slots) != 0 {
			t.Errorf("whole-null array carried %d indicators / %d data slots, want 0 / 0", len(c.indicators), len(c.slots))
		}
	})

	// array_out_int: OUT INTEGER[3] = [11,22,33] in the 0x3901 reply.
	t.Run("out_int_reply", func(t *testing.T) {
		vd := mustReply(t, "array_out_int.trace")
		c := onlyCol(t, vd, 0x9911)
		assertArrayInts(t, c, 497, 4, []int32{11, 22, 33}, nil)
		// The pure-OUT proc's 0x382F request carries zero INPUT columns.
		req := mustReq(t, "array_out_int.trace")
		if req.colCount != 0 {
			t.Errorf("pure-OUT proc 0x382F colCount = %d, want 0 (input params only)", req.colCount)
		}
	})

	// array_inout_int: request [1,2,3] (type 496) and reply [100,200,300]
	// (type 497) in one CALL.
	t.Run("inout_int", func(t *testing.T) {
		req := onlyCol(t, mustReq(t, "array_inout_int.trace"), 0x9911)
		assertArrayInts(t, req, 496, 4, []int32{1, 2, 3}, nil)
		rep := onlyCol(t, mustReply(t, "array_inout_int.trace"), 0x9911)
		assertArrayInts(t, rep, 497, 4, []int32{100, 200, 300}, nil)
	})

	// array_inout_vc: VARCHAR(20) elements resolve the stride question.
	// Request ["AB","CDE",null] -> fixed 22-byte stride, null elem 0xFFFF.
	t.Run("inout_vc", func(t *testing.T) {
		req := onlyCol(t, mustReq(t, "array_inout_vc.trace"), 0x9911)
		if req.elemType != 448 {
			t.Errorf("VARCHAR request element type = %d, want 448 (VARCHAR not-null code)", req.elemType)
		}
		if req.elemLen != 22 {
			t.Errorf("VARCHAR(20) element stride = %d, want 22 (maxLen+2 fixed stride)", req.elemLen)
		}
		if req.count != 3 {
			t.Fatalf("VARCHAR request cardinality = %d, want 3", req.count)
		}
		if req.indicators[0] != 0 || req.indicators[1] != 0 || req.indicators[2] != 0xFFFF {
			t.Errorf("VARCHAR request indicators = %v, want [0 0 0xFFFF]", req.indicators)
		}
		if cl, ch := varcharSlot(req.slots[0]); cl != 2 || !bytes.Equal(ch, []byte{0xC1, 0xC2}) {
			t.Errorf(`element[0] len=%d bytes=% X, want len=2 "AB" (EBCDIC C1 C2)`, cl, ch)
		}
		if cl, ch := varcharSlot(req.slots[1]); cl != 3 || !bytes.Equal(ch, []byte{0xC3, 0xC4, 0xC5}) {
			t.Errorf(`element[1] len=%d bytes=% X, want len=3 "CDE" (EBCDIC C3 C4 C5)`, cl, ch)
		}
		// Null element still occupies a full 22-byte slot (length prefix 0).
		if cl, _ := varcharSlot(req.slots[2]); cl != 0 {
			t.Errorf("null element length prefix = %d, want 0 (zeroed but present slot)", cl)
		}
		if len(req.slots[2]) != 22 {
			t.Errorf("null element slot width = %d, want 22 (fixed stride retained)", len(req.slots[2]))
		}

		// Reply ["XX","YYY"] (type 449 nullable).
		rep := onlyCol(t, mustReply(t, "array_inout_vc.trace"), 0x9911)
		if rep.elemType != 449 {
			t.Errorf("VARCHAR reply element type = %d, want 449 (VARCHAR nullable code)", rep.elemType)
		}
		if rep.elemLen != 22 || rep.count != 2 {
			t.Errorf("VARCHAR reply stride/card = %d/%d, want 22/2", rep.elemLen, rep.count)
		}
		if cl, ch := varcharSlot(rep.slots[0]); cl != 2 || !bytes.Equal(ch, []byte{0xE7, 0xE7}) {
			t.Errorf(`reply element[0] len=%d bytes=% X, want len=2 "XX" (EBCDIC E7 E7)`, cl, ch)
		}
		if cl, ch := varcharSlot(rep.slots[1]); cl != 3 || !bytes.Equal(ch, []byte{0xE8, 0xE8, 0xE8}) {
			t.Errorf(`reply element[1] len=%d bytes=% X, want len=3 "YYY" (EBCDIC E8 E8 E8)`, cl, ch)
		}
	})

	// array_mixed: IN INTEGER 7, IN INTEGER ARRAY [100,200,300], OUT
	// INTEGER. Request 0x382F has count=2 (scalar 0x9912 + array 0x9911,
	// the OUT excluded); reply 0x3901 carries the scalar OUT via 0x9912.
	t.Run("mixed_scalar_array", func(t *testing.T) {
		req := mustReq(t, "array_mixed.trace")
		if req.colCount != 2 {
			t.Fatalf("mixed request colCount = %d, want 2 (scalar + array; OUT excluded)", req.colCount)
		}
		sc := req.cols[0]
		if sc.tag != 0x9912 || sc.isArray {
			t.Errorf("col0 tag=0x%04X isArray=%v, want scalar 0x9912", sc.tag, sc.isArray)
		}
		if sc.elemLen != 4 || asInt32(sc.slots[0]) != 7 {
			t.Errorf("scalar col0 len=%d value=%d, want 4/7", sc.elemLen, asInt32(sc.slots[0]))
		}
		ar := req.cols[1]
		assertArrayInts(t, ar, 496, 4, []int32{100, 200, 300}, nil)

		rep := mustReply(t, "array_mixed.trace")
		if rep.colCount != 1 {
			t.Fatalf("mixed reply colCount = %d, want 1 (the scalar OUT)", rep.colCount)
		}
		ro := rep.cols[0]
		if ro.tag != 0x9912 || ro.isArray {
			t.Errorf("reply col0 tag=0x%04X isArray=%v, want scalar 0x9912 (OUT in 0x3901, not 0x380E)", ro.tag, ro.isArray)
		}
		if asInt32(ro.slots[0]) != 10 { // 7 + cardinality(3)
			t.Errorf("OUT CNT = %d, want 10 (7 + CARDINALITY([100,200,300]))", asInt32(ro.slots[0]))
		}
	})
}

// --- helpers ----------------------------------------------------------

func mustReq(t *testing.T, trace string) *varData {
	t.Helper()
	data, ok := findArrayRequestParam(t, trace, cpArrayExecuteVarData)
	if !ok {
		t.Fatalf("%s: no CP 0x382F request found", trace)
	}
	vd, err := decodeVarData(data)
	if err != nil {
		t.Fatalf("%s: decode 0x382F: %v", trace, err)
	}
	return vd
}

func mustReply(t *testing.T, trace string) *varData {
	t.Helper()
	data, ok := findArrayReplyParam(t, trace, cpArrayReplyVarData)
	if !ok {
		t.Fatalf("%s: no CP 0x3901 reply found", trace)
	}
	vd, err := decodeVarData(data)
	if err != nil {
		t.Fatalf("%s: decode 0x3901: %v", trace, err)
	}
	return vd
}

func onlyCol(t *testing.T, vd *varData, wantTag uint16) varDataCol {
	t.Helper()
	if vd.colCount != 1 || len(vd.cols) != 1 {
		t.Fatalf("colCount = %d, want 1", vd.colCount)
	}
	if vd.cols[0].tag != wantTag {
		t.Fatalf("col0 tag = 0x%04X, want 0x%04X", vd.cols[0].tag, wantTag)
	}
	return vd.cols[0]
}

func assertArrayInts(t *testing.T, c varDataCol, wantType uint16, wantLen uint32, want []int32, nullElems map[int]uint16) {
	t.Helper()
	if !c.isArray || c.isNullArray {
		t.Fatalf("want non-null array, got isArray=%v isNull=%v", c.isArray, c.isNullArray)
	}
	if c.elemType != wantType {
		t.Errorf("element type = %d, want %d", c.elemType, wantType)
	}
	if c.elemLen != wantLen {
		t.Errorf("element stride = %d, want %d", c.elemLen, wantLen)
	}
	if c.count != len(want) {
		t.Fatalf("cardinality = %d, want %d", c.count, len(want))
	}
	for i, w := range want {
		if got := asInt32(c.slots[i]); got != w {
			t.Errorf("element[%d] = %d, want %d", i, got, w)
		}
		wantInd := uint16(0)
		if nullElems != nil {
			if v, ok := nullElems[i]; ok {
				wantInd = v
			}
		}
		if c.indicators[i] != wantInd {
			t.Errorf("indicator[%d] = 0x%04X, want 0x%04X", i, c.indicators[i], wantInd)
		}
	}
}

// TestEncodeDBVariableDataMatchesFixture proves the Phase 3 encoder
// produces byte-identical CP 0x382F output to the live JT400 capture for
// the canonical INTEGER IN array [10,20,30,40,50] (the proc param is
// INTEGER ARRAY[10]; the describe element shape is SQLType 497 / len 4).
func TestEncodeDBVariableDataMatchesFixture(t *testing.T) {
	want, err := os.ReadFile(filepath.Join("testdata", "array_param_execute_382f.bin"))
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	// Post-reconcile shape: IN array, element INTEGER (497), 4-byte stride.
	params := []PreparedParam{{SQLType: 497, FieldLength: 4, IsArray: true, ParamType: 0x00}}
	values := []any{ArrayValue{Elements: []any{int64(10), int64(20), int64(30), int64(40), int64(50)}}}
	got, err := EncodeDBVariableData(params, values)
	if err != nil {
		t.Fatalf("EncodeDBVariableData: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("0x382F encode mismatch with JT400 capture\n got: % X\nwant: % X", got, want)
	}
}

// TestParseVariableResultDataMatchesFixture proves the Phase 4 decoder
// reads the live JT400 CP 0x3901 capture (OUT INTEGER array [11,22,33])
// back into an ArrayValue.
func TestParseVariableResultDataMatchesFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "array_param_reply_3901.bin"))
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	// One OUT array slot; element CCSID irrelevant for INTEGER.
	shapes := []PreparedParam{{SQLType: 497, FieldLength: 4, IsArray: true, ParamType: 0xF1}}
	out, types, err := parseVariableResultData(data, shapes)
	if err != nil {
		t.Fatalf("parseVariableResultData: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("out len = %d, want 1", len(out))
	}
	av, ok := out[0].(ArrayValue)
	if !ok {
		t.Fatalf("out[0] = %T, want ArrayValue", out[0])
	}
	if av.Null {
		t.Fatal("array decoded as whole-null, want 3 elements")
	}
	if len(av.Elements) != 3 {
		t.Fatalf("decoded %d elements, want 3", len(av.Elements))
	}
	want := []int64{11, 22, 33}
	for i, w := range want {
		gi, err := toInt64(av.Elements[i])
		if err != nil {
			t.Fatalf("element %d not integer: %T", i, av.Elements[i])
		}
		if gi != w {
			t.Errorf("element %d = %d, want %d", i, gi, w)
		}
	}
	if types[0] != 497 {
		t.Errorf("reply element type = %d, want 497", types[0])
	}
}

// TestEncodeDBVariableDataRoundTripsCases exercises the encoder across
// the captured shapes -- null element, whole-null array, VARCHAR fixed
// stride, and a mixed scalar+array row -- against the live JT400 0x382F
// frames extracted from the trace corpus.
func TestEncodeDBVariableDataRoundTripsCases(t *testing.T) {
	// VARCHAR(20) IN array ["AB","CDE",null]: element 449 / stride 22,
	// EBCDIC CCSID 273 (the capture's PUB400 user CCSID).
	t.Run("varchar_in", func(t *testing.T) {
		want, ok := findArrayRequestParam(t, "array_inout_vc.trace", cpArrayExecuteVarData)
		if !ok {
			t.Skip("no 0x382F in array_inout_vc.trace")
		}
		params := []PreparedParam{{SQLType: 449, FieldLength: 22, CCSID: 273, IsArray: true, ParamType: 0xF2}}
		values := []any{ArrayValue{Elements: []any{"AB", "CDE", nil}}}
		got, err := EncodeDBVariableData(params, values)
		if err != nil {
			t.Fatalf("EncodeDBVariableData: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("VARCHAR 0x382F mismatch\n got: % X\nwant: % X", got, want)
		}
	})

	// Whole-null INTEGER array: descriptor 0x9911 0xFFFF, no ind/data.
	t.Run("whole_null", func(t *testing.T) {
		want, ok := findArrayRequestParam(t, "array_in_int_wholenull.trace", cpArrayExecuteVarData)
		if !ok {
			t.Skip("no 0x382F in array_in_int_wholenull.trace")
		}
		params := []PreparedParam{{SQLType: 497, FieldLength: 4, IsArray: true, ParamType: 0x00}}
		values := []any{ArrayValue{Null: true}}
		got, err := EncodeDBVariableData(params, values)
		if err != nil {
			t.Fatalf("EncodeDBVariableData: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("whole-null 0x382F mismatch\n got: % X\nwant: % X", got, want)
		}
	})

	// Null element: indicators 0000 0000 FFFF 0000 0000, null slot zeroed.
	t.Run("null_element", func(t *testing.T) {
		want, ok := findArrayRequestParam(t, "array_in_int_nullelem.trace", cpArrayExecuteVarData)
		if !ok {
			t.Skip("no 0x382F in array_in_int_nullelem.trace")
		}
		params := []PreparedParam{{SQLType: 497, FieldLength: 4, IsArray: true, ParamType: 0x00}}
		values := []any{ArrayValue{Elements: []any{int64(10), int64(20), nil, int64(40), int64(50)}}}
		got, err := EncodeDBVariableData(params, values)
		if err != nil {
			t.Fatalf("EncodeDBVariableData: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("null-element 0x382F mismatch\n got: % X\nwant: % X", got, want)
		}
	})
}

// TestRefileArrayWireFixtures extracts the canonical CP 0x382F and 0x3901
// payloads from the captured traces and (re)writes the .bin fixtures.
// Gated so a normal `go test` run never rewrites committed fixtures.
//
//	DB2I_REFILE_ARRAY_FIXTURES=1 go test ./hostserver -run TestRefileArrayWireFixtures
func TestRefileArrayWireFixtures(t *testing.T) {
	if os.Getenv("DB2I_REFILE_ARRAY_FIXTURES") != "1" {
		t.Skip("set DB2I_REFILE_ARRAY_FIXTURES=1 to (re)extract the .bin fixtures from the .trace captures")
	}
	exec, ok := findArrayRequestParam(t, "array_in_int_basic.trace", cpArrayExecuteVarData)
	if !ok {
		t.Fatal("array_in_int_basic.trace: no CP 0x382F to extract")
	}
	reply, ok := findArrayReplyParam(t, "array_out_int.trace", cpArrayReplyVarData)
	if !ok {
		t.Fatal("array_out_int.trace: no CP 0x3901 to extract")
	}
	writeFixture(t, "array_param_execute_382f.bin", exec)
	writeFixture(t, "array_param_reply_3901.bin", reply)
}

func writeFixture(t *testing.T, name string, data []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote %s (%d bytes)", path, len(data))
}
