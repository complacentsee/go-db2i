package hostserver

import (
	"encoding/binary"
	"testing"
)

// TestParseExtendedResultDataVLFFromBitFlag covers the case live-
// reproduced against IBM Cloud IBM i 7.6: a one-row, one-column
// VARCHAR result whose VLF total bytes (rowInfoHeader 8 + actual
// row bytes 6 + rowInfoArray 4 = 18) coincides with rowSize*rowCount
// (18*1 = 18) of the non-VLF layout. Pure length-detection picks
// non-VLF and reads SL=0 from the first 2 bytes (which are actually
// the VLF row-info header), returning an empty []byte for what
// should have been a 4-byte payload.
//
// The fix is to honour the response ORS bitmap echo bit
// (ORSVarFieldComp / 0x00010000) -- when set, the data is VLF
// regardless of the size match.
func TestParseExtendedResultDataVLFFromBitFlag(t *testing.T) {
	// Build the bytes seen on the wire from the live probe:
	// header=20, indicators=2, rowInfoHeader=8, rowData=6, rowInfoArray=4
	be := binary.BigEndian
	data := make([]byte, 40)
	be.PutUint32(data[0:4], 1)             // consistency token
	be.PutUint32(data[4:8], 1)             // rowCount
	be.PutUint16(data[8:10], 1)            // colCount
	be.PutUint16(data[10:12], 2)           // indicatorSize
	be.PutUint32(data[16:20], 18)          // rowSize (col.Length=18, matches non-VLF total!)
	// indicators (1*1*2 = 2 bytes): not null
	// 20..21 = 00 00
	// VLF row-info header at offset 22:
	be.PutUint32(data[22:26], 14) // rowInfoArrayOffset (relative to row-info-header start)
	be.PutUint32(data[26:30], 1)  // rowInfoArrayCount
	// Row 0 starts at offset 22+8 = 30: SL=4 + 4 bytes payload.
	be.PutUint16(data[30:32], 4)
	copy(data[32:36], []byte{0xCA, 0xFE, 0xBA, 0xBE})
	// Row info array entry 0 at offset 22+14 = 36: row 0 starts at offset 8.
	be.PutUint32(data[36:40], 8)

	cols := []SelectColumn{{
		SQLType: 449, Length: 18, CCSID: 65535, Name: "VB16",
	}}

	t.Run("vlf bit set -> VLF parse", func(t *testing.T) {
		rows, err := parseExtendedResultData(data, cols, true)
		if err != nil {
			t.Fatalf("parseExtendedResultData(vlf=true): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("rows=%d, want 1", len(rows))
		}
		got, ok := rows[0][0].([]byte)
		if !ok {
			t.Fatalf("col is %T, want []byte", rows[0][0])
		}
		if want := []byte{0xCA, 0xFE, 0xBA, 0xBE}; !bytesEqual(got, want) {
			t.Errorf("payload = % X, want % X", got, want)
		}
	})

	t.Run("vlf bit zero, length match -> falls back to non-VLF (still buggy)", func(t *testing.T) {
		// Documents the legacy fallback. With the bit clear and the
		// VLF total coincidentally matching non-VLF rowSize, the
		// parser picks non-VLF and reads SL=0 from the row-info
		// header bytes -> empty payload. Live servers set the bit
		// when echo is requested, so this path only fires for older
		// request paths that don't request the echo.
		rows, err := parseExtendedResultData(data, cols, false)
		if err != nil {
			t.Fatalf("parseExtendedResultData(vlf=false): %v", err)
		}
		got, _ := rows[0][0].([]byte)
		if len(got) != 0 {
			t.Errorf("legacy non-VLF fallback unexpectedly read %d bytes; expected the documented buggy 0 bytes", len(got))
		}
	})
}

// TestDBReplyVLFCompressedExtractsBit confirms ParseDBReply pulls
// the variable-length-field-compression bit out of the response ORS
// bitmap (payload[0:4]) and surfaces it via VLFCompressed().
func TestDBReplyVLFCompressedExtractsBit(t *testing.T) {
	cases := []struct {
		name string
		ors  uint32
		want bool
	}{
		{"bit set", ORSReturnData | ORSResultData | ORSVarFieldComp, true},
		{"bit clear", ORSReturnData | ORSResultData, false},
		{"all bits set", 0xFFFFFFFF, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := make([]byte, 20)
			binary.BigEndian.PutUint32(payload[0:4], tc.ors)
			rep, err := ParseDBReply(payload)
			if err != nil {
				t.Fatalf("ParseDBReply: %v", err)
			}
			if rep.ORSBitmap != tc.ors {
				t.Errorf("ORSBitmap = 0x%08X, want 0x%08X", rep.ORSBitmap, tc.ors)
			}
			if rep.VLFCompressed() != tc.want {
				t.Errorf("VLFCompressed() = %v, want %v", rep.VLFCompressed(), tc.want)
			}
		})
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
