package hostserver

import (
	"encoding/binary"
	"testing"
)

// TestParseExtendedResultDataTwoVarcharFixedSlot reproduces the
// `SELECT AUTHORIZATION_NAME, STATUS FROM QSYS2.USER_INFO` failure
// captured live against IBM i V7R6M0:
//
//	hostserver: parse row data: row 0: column 1 ("STATUS",
//	  sql_type=449): varchar declared length 16448 exceeds column max 12
//
// Two nullable VARCHAR(10) columns; col.Length=12 (slot incl 2 SL
// bytes); rowSize=24 (2*12); response ORS bitmap has the VLF echo
// bit clear; row data fixed-slot. Pre-fix the row decoder advanced
// by `2+actualLen` per VARCHAR (the VLF rule), so after the first
// column ("GOTEST", 6 chars -> consumed 8 bytes) the next column
// started 4 bytes too early -- inside the EBCDIC-space (0x40) padding
// of the previous slot. The decoder then read 0x40 0x40 = 16448 as
// the next length prefix and bailed out.
func TestParseExtendedResultDataTwoVarcharFixedSlot(t *testing.T) {
	be := binary.BigEndian
	// 20-byte header + 4-byte indicators + 24-byte row data.
	data := make([]byte, 48)

	// Header.
	be.PutUint32(data[0:4], 1)   // consistency token
	be.PutUint32(data[4:8], 1)   // rowCount
	be.PutUint16(data[8:10], 2)  // colCount
	be.PutUint16(data[10:12], 2) // indicatorSize
	// 12..15 reserved
	be.PutUint32(data[16:20], 24) // rowSize = 2 * col.Length = 24

	// Indicators: 1 row * 2 cols * 2 bytes = 4 bytes; both non-null.
	// data[20..23] already zero.

	// Row data offset 24, 24 bytes total. Each VARCHAR(10) slot is
	// 12 bytes: 2 SL + payload (EBCDIC) + 0x40 (space) padding.
	//
	// AUTHORIZATION_NAME = "GOTEST" (6 EBCDIC chars + 4 space pad)
	row := []byte{
		0x00, 0x06,
		0xC7, 0xD6, 0xE3, 0xC5, 0xE2, 0xE3, // GOTEST
		0x40, 0x40, 0x40, 0x40, // 4 space pad to col.Length=12
		// STATUS = "*ENABLED" (8 EBCDIC chars + 2 space pad)
		0x00, 0x08,
		0x5C, 0xC5, 0xD5, 0xC1, 0xC2, 0xD3, 0xC5, 0xC4, // *ENABLED
		0x40, 0x40, // 2 space pad to col.Length=12
	}
	copy(data[24:], row)

	cols := []SelectColumn{
		{SQLType: 449, Length: 12, CCSID: 37, Name: "AUTHORIZATION_NAME"},
		{SQLType: 449, Length: 12, CCSID: 37, Name: "STATUS"},
	}

	rows, err := parseExtendedResultData(data, cols, false /* vlfCompressed */)
	if err != nil {
		t.Fatalf("parseExtendedResultData: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d, want 1", len(rows))
	}
	got0, _ := rows[0][0].(string)
	got1, _ := rows[0][1].(string)
	if got0 != "GOTEST" {
		t.Errorf("col 0 = %q, want %q", got0, "GOTEST")
	}
	if got1 != "*ENABLED" {
		t.Errorf("col 1 = %q, want %q", got1, "*ENABLED")
	}
}

// TestParseExtendedResultDataNonVLFNullableMix exercises the mixed
// null/non-null case in non-VLF: one column null, the next a
// non-null VARCHAR. The null skip path advances by col.wireLength()
// and must agree with the slot width the row-data block reserves.
// Pre-fix wireLength() returned `col.Length+2` for VARCHAR even when
// col.Length already counted the SL bytes, so a leading-null VARCHAR
// would shift subsequent columns by 2 bytes per null and corrupt
// them just like the all-non-null case did.
func TestParseExtendedResultDataNonVLFNullableMix(t *testing.T) {
	be := binary.BigEndian
	// 20 + 4 indicators + 24 row = 48 bytes.
	data := make([]byte, 48)
	be.PutUint32(data[0:4], 1)
	be.PutUint32(data[4:8], 1)
	be.PutUint16(data[8:10], 2)
	be.PutUint16(data[10:12], 2)
	be.PutUint32(data[16:20], 24)

	// Indicator block: col 0 null (-1 / 0xFFFF), col 1 non-null (0).
	be.PutUint16(data[20:22], 0xFFFF)
	be.PutUint16(data[22:24], 0x0000)

	// Row data: col 0 slot is 12 bytes of don't-care (server still
	// reserves the slot even when null); col 1 has "*ENABLED".
	row := []byte{
		// col 0 (null) -- 12 zero bytes
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		// col 1 (non-null VARCHAR(10)) -- "*ENABLED" + 2 space pad
		0x00, 0x08,
		0x5C, 0xC5, 0xD5, 0xC1, 0xC2, 0xD3, 0xC5, 0xC4,
		0x40, 0x40,
	}
	copy(data[24:], row)

	cols := []SelectColumn{
		{SQLType: 449, Length: 12, CCSID: 37, Name: "AUTHORIZATION_NAME"},
		{SQLType: 449, Length: 12, CCSID: 37, Name: "STATUS"},
	}
	rows, err := parseExtendedResultData(data, cols, false)
	if err != nil {
		t.Fatalf("parseExtendedResultData: %v", err)
	}
	if rows[0][0] != nil {
		t.Errorf("col 0 = %v, want nil", rows[0][0])
	}
	if got, _ := rows[0][1].(string); got != "*ENABLED" {
		t.Errorf("col 1 = %q, want %q", got, "*ENABLED")
	}
}
