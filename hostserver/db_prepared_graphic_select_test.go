package hostserver

import (
	"encoding/binary"
	"testing"
)

// makeGraphicPMFReply builds a PREPARE_DESCRIBE reply (DSS header +
// 20-byte template + a CP 0x3813 super-extended parameter-marker
// format) declaring a single CCSID-65535 ("bit data") fixed-length
// GRAPHIC parameter. The 48-byte field record layout matches
// parseSuperExtendedParameterMarkerFormat: SQL type at offset 2,
// field length at 4, scale/precision at 8/10, CCSID at 12, parameter
// type at 14. No CP 0x3812 (column format) is needed -- the SELECT
// path tolerates its absence and we only care about the input shape.
func makeGraphicPMFReply(corr uint32, sqlType uint16, fieldLen uint32, ccsid uint16) []byte {
	const (
		pmfHeaderLen = 16
		perFieldLen  = 48
	)
	pmf := make([]byte, pmfHeaderLen+perFieldLen)
	be := binary.BigEndian
	be.PutUint32(pmf[0:4], 1) // consistency token
	be.PutUint32(pmf[4:8], 1) // one field
	be.PutUint32(pmf[12:16], fieldLen)
	base := pmfHeaderLen
	be.PutUint16(pmf[base+2:base+4], sqlType)
	be.PutUint32(pmf[base+4:base+8], fieldLen)
	be.PutUint16(pmf[base+12:base+14], ccsid)
	// scale/precision/paramType and the variable-info offsets are left
	// zero -- no field name, no LOB locator.

	// CP 0x3813 wrapper: 4-byte LL + 2-byte CP + pmf bytes.
	cp := make([]byte, 6+len(pmf))
	be.PutUint32(cp[0:4], uint32(len(cp)))
	be.PutUint16(cp[4:6], 0x3813)
	copy(cp[6:], pmf)

	// 20-byte DSS header + 20-byte reply template + CP list.
	frame := make([]byte, 40+len(cp))
	be.PutUint32(frame[0:4], uint32(len(frame)))
	be.PutUint16(frame[6:8], uint16(ServerDatabase))
	be.PutUint32(frame[12:16], corr)
	be.PutUint16(frame[16:18], 20)
	be.PutUint16(frame[18:20], RepDBReply)
	copy(frame[40:], cp)
	return frame
}

// TestSelectPathReconcilesGraphicBitDataBind verifies the SELECT/Query
// prepared path applies the graphic-bit-data bind fixup (issue #32).
//
// A []byte predicate against a CCSID-65535 GRAPHIC column arrives from
// the driver as a VARCHAR-FOR-BIT-DATA shape (SQL type 449). Before the
// fix, reconcileGraphicBitDataBindShapes ran only on the Exec/IUD path,
// so the SELECT path shipped the un-reconciled 449 shape in
// CHANGE_DESCRIPTOR and the server rejected the bind (SQL-332 / 57017).
// After the fix the shape adopts the column's declared GRAPHIC type
// (469) from the PREPARE_DESCRIBE parameter-marker format.
//
// We drive SelectPreparedSQL against a fake conn that replays a single
// PREPARE_DESCRIBE reply carrying a graphic CCSID-65535 PMF, then read
// back the CHANGE_DESCRIPTOR frame (the third frame written) and assert
// the encoded input SQL type is the reconciled 469, not 449. The
// subsequent OPEN read has no matching reply and errors out, but the
// CHANGE_DESCRIPTOR bytes are already on the wire by then, so the
// assertion is unaffected.
func TestSelectPathReconcilesGraphicBitDataBind(t *testing.T) {
	const (
		nextCorrelation = 3 // CREATE_RPB=3, PREPARE_DESCRIBE=4, CHANGE_DESCRIPTOR=5, OPEN=6
		graphicSQLType  = uint16(469)
		graphicFieldLen = uint32(4) // one 2-byte graphic character, bit data
	)

	// PREPARE_DESCRIBE reply correlation is nextCorrelation+1 (4).
	prepReply := makeGraphicPMFReply(nextCorrelation+1, graphicSQLType, graphicFieldLen, ccsidBinary)
	conn := newFakeConn(prepReply)

	// Driver default for a []byte bind: VARCHAR FOR BIT DATA (449),
	// CCSID 65535. FieldLength 6 leaves room for the 2-byte SL + 4
	// payload bytes so the value also encodes cleanly on the no-fix
	// path; the fixup later overrides it with the PMF's length.
	shapes := []PreparedParam{{
		SQLType:     449,
		FieldLength: 6,
		CCSID:       ccsidBinary,
	}}
	values := []any{[]byte{0x00, 0x41, 0x00, 0x42}} // two graphic chars

	// SelectPreparedSQL errors on the missing OPEN reply; we only care
	// about the bytes it wrote up to and including CHANGE_DESCRIPTOR.
	_, _ = SelectPreparedSQL(conn, "SELECT 1 FROM SYSIBM.SYSDUMMY1 WHERE G = ?", shapes, values, nextCorrelation)

	// Find the CHANGE_DESCRIPTOR frame among the written DSS frames and
	// read back the input SQL type it encoded.
	var descPayload []byte
	for _, fv := range splitDSSFrames(t, conn.written.Bytes()) {
		if fv.reqRepID == ReqDBSQLChangeDescriptor {
			descPayload = fv.payload
			break
		}
	}
	if descPayload == nil {
		t.Fatalf("no CHANGE_DESCRIPTOR frame written")
	}

	got := changeDescriptorSQLType(t, descPayload)
	if got != graphicSQLType {
		t.Errorf("CHANGE_DESCRIPTOR input SQL type = %d, want %d (graphic fixup not applied on SELECT path)", got, graphicSQLType)
	}
}

// changeDescriptorSQLType extracts the first input parameter's SQL type
// from a CHANGE_DESCRIPTOR request payload (the bytes after the 20-byte
// request template, as returned by splitDSSFrames). The shape descriptor
// rides in CP 0x381E whose data is the EncodeDBExtendedDataFormat layout
// (16-byte header + 64-byte field records, SQL type at record offset 2).
func changeDescriptorSQLType(t *testing.T, payload []byte) uint16 {
	t.Helper()
	cps, err := parseCPs(payload)
	if err != nil {
		t.Fatalf("parseCPs: %v", err)
	}
	data, ok := cps[cpDBExtendedDataFormat]
	if !ok {
		t.Fatalf("CHANGE_DESCRIPTOR payload had no CP 0x%04X", cpDBExtendedDataFormat)
	}
	const fmtHeaderLen = 16
	if len(data) < fmtHeaderLen+4 {
		t.Fatalf("CP 0x381E data too short: %d bytes", len(data))
	}
	return binary.BigEndian.Uint16(data[fmtHeaderLen+2 : fmtHeaderLen+4])
}
