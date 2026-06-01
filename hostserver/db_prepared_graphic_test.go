package hostserver

import (
	"bytes"
	"testing"
)

// Graphic-PARAMETER encode tests for issue #5: the symmetric
// counterpart to the issue-#3 read path. They drive
// EncodeDBExtendedData with synthetic PreparedParams for the three
// graphic families -- GRAPHIC (468/469), VARGRAPHIC (464/465), and
// LONG VARGRAPHIC (472/473) -- across the CCSIDs the encoder branches
// on (1200 UTF-16 BE, 13488 UCS-2 BE, 65535 FOR BIT DATA) and assert
// the exact data-block wire bytes.
//
// The golden bytes mirror the read-side goldens in
// db_result_data_graphic_test.go so the encode/decode pair stays
// self-consistent:
//
//	VARGRAPHIC(n) 'ABC' -> 00 03 | 00 41 00 42 00 43   (SL = char count)
//	GRAPHIC(5)    'ABC' -> 00 41 00 42 00 43 00 20 00 20 (no SL, U+0020 pad)

// graphicDataBlock encodes a single-param, single-row
// EncodeDBExtendedData payload and returns just the per-row data
// bytes: everything past the 20-byte header and the 2-byte indicator.
// Lets the wire-byte assertions focus on the encoder output without
// restating the fixed framing each time (the framing itself is pinned
// by TestEncodeDBExtendedData* in db_prepared_bind_test.go).
func graphicDataBlock(t *testing.T, p PreparedParam, value any) []byte {
	t.Helper()
	got, err := EncodeDBExtendedData([]PreparedParam{p}, []any{value})
	if err != nil {
		t.Fatalf("EncodeDBExtendedData: %v", err)
	}
	const headerLen, indicatorLen = 20, 2
	if len(got) < headerLen+indicatorLen {
		t.Fatalf("output too short: %d bytes", len(got))
	}
	return got[headerLen+indicatorLen:]
}

// TestEncodeGraphicWireBytes pins the exact data-block bytes for the
// variable and fixed graphic families across every CCSID branch. SL is
// asserted to count GRAPHIC CHARACTERS (= payload bytes / 2) for the
// variable forms, and the fixed form is asserted to carry NO SL with
// U+0020 (0x00 0x20) graphic-space padding out to FieldLength.
func TestEncodeGraphicWireBytes(t *testing.T) {
	cases := []struct {
		name  string
		param PreparedParam
		value any
		want  []byte
	}{
		{
			// VARGRAPHIC(3) CCSID 1200, "Hi": SL=2 chars, 4 payload
			// bytes (UTF-16 BE), padded to FieldLength 8.
			name:  "vargraphic 1200 Hi",
			param: PreparedParam{SQLType: 465, FieldLength: 8, CCSID: 1200},
			value: "Hi",
			want:  []byte{0x00, 0x02, 0x00, 0x48, 0x00, 0x69, 0x00, 0x00},
		},
		{
			// LONG VARGRAPHIC (473) shares the VARGRAPHIC wire shape;
			// FieldLength 10 leaves 4 trailing zero pad bytes.
			name:  "long vargraphic 1200 Hi",
			param: PreparedParam{SQLType: 473, FieldLength: 10, CCSID: 1200},
			value: "Hi",
			want:  []byte{0x00, 0x02, 0x00, 0x48, 0x00, 0x69, 0x00, 0x00, 0x00, 0x00},
		},
		{
			// Non-BMP rune U+1D11E (musical treble clef) is the
			// surrogate pair D834 DD1E -- two UTF-16 code units, so
			// SL=2 with a 4-byte payload. Confirms CCSID 1200 keeps
			// surrogate pairs intact rather than substituting.
			name:  "vargraphic 1200 non-BMP surrogate",
			param: PreparedParam{SQLType: 465, FieldLength: 8, CCSID: 1200},
			value: "\U0001D11E",
			want:  []byte{0x00, 0x02, 0xD8, 0x34, 0xDD, 0x1E, 0x00, 0x00},
		},
		{
			// CCSID 13488 (strict UCS-2 BE) encodes BMP code points
			// identically to 1200.
			name:  "vargraphic 13488 BMP",
			param: PreparedParam{SQLType: 465, FieldLength: 8, CCSID: 13488},
			value: "AB",
			want:  []byte{0x00, 0x02, 0x00, 0x41, 0x00, 0x42, 0x00, 0x00},
		},
		{
			// CCSID 13488 forbids surrogate pairs: the non-BMP rune is
			// substituted with U+003F ('?') -> a single code unit, SL=1.
			name:  "vargraphic 13488 non-BMP substitute",
			param: PreparedParam{SQLType: 465, FieldLength: 8, CCSID: 13488},
			value: "\U0001D11E",
			want:  []byte{0x00, 0x01, 0x00, 0x3F, 0x00, 0x00, 0x00, 0x00},
		},
		{
			// CCSID 65535 (FOR BIT DATA): raw bytes pass through
			// verbatim; SL is still char count = len/2.
			name:  "vargraphic bit data 65535",
			param: PreparedParam{SQLType: 465, FieldLength: 8, CCSID: ccsidBinary},
			value: []byte{0xDE, 0xAD, 0xBE, 0xEF},
			want:  []byte{0x00, 0x02, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00},
		},
		{
			// GRAPHIC(5) CCSID 1200, "ABC": fixed, no SL, U+0020
			// graphic-space padding out to FieldLength 10.
			name:  "graphic fixed 1200 ABC padded",
			param: PreparedParam{SQLType: 468, FieldLength: 10, CCSID: 1200},
			value: "ABC",
			want:  []byte{0x00, 0x41, 0x00, 0x42, 0x00, 0x43, 0x00, 0x20, 0x00, 0x20},
		},
		{
			// GRAPHIC fixed, empty value: the whole slot is U+0020
			// graphic spaces (no SL).
			name:  "graphic fixed 1200 empty",
			param: PreparedParam{SQLType: 469, FieldLength: 10, CCSID: 1200},
			value: "",
			want:  []byte{0x00, 0x20, 0x00, 0x20, 0x00, 0x20, 0x00, 0x20, 0x00, 0x20},
		},
		{
			// GRAPHIC fixed FOR BIT DATA: raw bytes then 0x00 pad
			// (binary has no graphic-space concept).
			name:  "graphic fixed bit data 65535",
			param: PreparedParam{SQLType: 468, FieldLength: 4, CCSID: ccsidBinary},
			value: []byte{0xCA, 0xFE},
			want:  []byte{0xCA, 0xFE, 0x00, 0x00},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := graphicDataBlock(t, tc.param, tc.value)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("data block mismatch:\n got=% X\nwant=% X", got, tc.want)
			}
		})
	}
}

// TestEncodeGraphicSLCountsCharacters locks in the one asymmetry most
// likely to regress: the VARGRAPHIC SL is a GRAPHIC CHARACTER count,
// not a byte count. A 3-character value occupies 6 payload bytes but
// must ship SL=3 (matching the read path's `nbytes = chars * 2`).
func TestEncodeGraphicSLCountsCharacters(t *testing.T) {
	p := PreparedParam{SQLType: 465, FieldLength: 22, CCSID: 1200}
	got := graphicDataBlock(t, p, "ABC")
	if sl := int(got[0])<<8 | int(got[1]); sl != 3 {
		t.Errorf("SL = %d, want 3 (char count, not byte count)", sl)
	}
	wantPayload := []byte{0x00, 0x41, 0x00, 0x42, 0x00, 0x43}
	if !bytes.Equal(got[2:2+len(wantPayload)], wantPayload) {
		t.Errorf("payload = % X, want % X", got[2:2+len(wantPayload)], wantPayload)
	}
}

// TestEncodeGraphicOutEmptySlot covers the stored-procedure OUT-only
// path: the driver binds an empty string for a *string OUT slot, and
// after the PMF fixup the slot is a VARGRAPHIC(20) CCSID 1200
// (FieldLength 42). The encoder must emit SL=0 and leave the rest of
// the field as zero pad -- the server overwrites it on return.
func TestEncodeGraphicOutEmptySlot(t *testing.T) {
	p := PreparedParam{SQLType: 465, FieldLength: 42, CCSID: 1200, ParamType: 0xF1}
	got := graphicDataBlock(t, p, "")
	if len(got) != 42 {
		t.Fatalf("data block = %d bytes, want 42 (FieldLength)", len(got))
	}
	if got[0] != 0x00 || got[1] != 0x00 {
		t.Errorf("SL = % X, want 00 00 (empty OUT slot)", got[:2])
	}
	for i, b := range got {
		if b != 0x00 {
			t.Errorf("byte %d = 0x%02X, want 0x00 (empty OUT slot fully zero)", i, b)
			break
		}
	}
}

// TestEncodeGraphicRejectsTooLong confirms the length guards fire for
// both families when the payload would overrun the declared
// FieldLength budget (FieldLength-2 for variable, FieldLength for
// fixed). Mirrors TestEncodeDBExtendedDataBinaryVarcharRejectsTooLong.
func TestEncodeGraphicRejectsTooLong(t *testing.T) {
	t.Run("vargraphic overflow", func(t *testing.T) {
		// FieldLength 4 -> budget 2 bytes = 1 char; "AB" is 4 bytes.
		p := PreparedParam{SQLType: 465, FieldLength: 4, CCSID: 1200}
		if _, err := EncodeDBExtendedData([]PreparedParam{p}, []any{"AB"}); err == nil {
			t.Error("expected error for oversized VARGRAPHIC payload, got nil")
		}
	})
	t.Run("graphic fixed overflow", func(t *testing.T) {
		// FieldLength 4 = 2 chars; "ABC" is 6 bytes.
		p := PreparedParam{SQLType: 468, FieldLength: 4, CCSID: 1200}
		if _, err := EncodeDBExtendedData([]PreparedParam{p}, []any{"ABC"}); err == nil {
			t.Error("expected error for oversized GRAPHIC payload, got nil")
		}
	})
}

// TestEncodeGraphicNullSlot confirms a NULL bind for a graphic param
// advances the data offset by FieldLength without writing payload
// bytes (the indicator block already flagged NULL). A trailing INTEGER
// param must land correctly aligned -- the encode-side mirror of the
// read-side TestDecodeRowGraphicAlignment null cases.
func TestEncodeGraphicNullSlot(t *testing.T) {
	params := []PreparedParam{
		{SQLType: 465, FieldLength: 8, CCSID: 1200}, // VARGRAPHIC(3)
		{SQLType: 496, FieldLength: 4},              // INTEGER NN
	}
	got, err := EncodeDBExtendedData(params, []any{nil, int32(0x01020304)})
	if err != nil {
		t.Fatalf("EncodeDBExtendedData: %v", err)
	}
	// header(20) + indicators(2 cols * 2) + data(8 + 4).
	const headerLen = 20
	indicatorLen := 2 * 2
	data := got[headerLen+indicatorLen:]
	if len(data) != 12 {
		t.Fatalf("data block = %d bytes, want 12", len(data))
	}
	// NULL graphic slot: 8 zero bytes, then the INTEGER.
	wantInt := []byte{0x01, 0x02, 0x03, 0x04}
	if !bytes.Equal(data[8:12], wantInt) {
		t.Errorf("trailing INTEGER = % X, want % X (NULL graphic slot misaligned)", data[8:12], wantInt)
	}
	// Indicator for the graphic slot must be 0xFFFF (NULL).
	if got[headerLen] != 0xFF || got[headerLen+1] != 0xFF {
		t.Errorf("graphic indicator = % X, want FF FF (NULL)", got[headerLen:headerLen+2])
	}
}
