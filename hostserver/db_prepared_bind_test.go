package hostserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
	"testing"
)

// TestEncodeDBExtendedDataBinaryVarchar pins the byte layout of the
// VARCHAR FOR BIT DATA branch (CCSID 65535) added when the M6 driver
// learnt to bind []byte values. Wire-validated round trip exists in
// the live param-bind probe; this offline test guards against
// accidental regressions to the encoder.
//
// Layout for a single VARCHAR(N) FOR BIT DATA param row:
//
//	header (20)        = 0x00000001 0x00000001 NN 0002 00000000 RRRRRRRR
//	indicator (2)      = 0x0000          (not null)
//	data (FieldLength) = SL(2)=N + N raw payload bytes
func TestEncodeDBExtendedDataBinaryVarchar(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	params := []PreparedParam{{
		SQLType:     449, // VARCHAR nullable
		FieldLength: uint32(len(payload)) + 2,
		Precision:   uint16(len(payload)),
		CCSID:       65535, // FOR BIT DATA
	}}
	got, err := EncodeDBExtendedData(params, []any{payload})
	if err != nil {
		t.Fatalf("EncodeDBExtendedData: %v", err)
	}

	wantHeader := []byte{
		0x00, 0x00, 0x00, 0x01, // consistency token
		0x00, 0x00, 0x00, 0x01, // row count
		0x00, 0x01, // column count
		0x00, 0x02, // indicator size
		0x00, 0x00, 0x00, 0x00, // reserved
		0x00, 0x00, 0x00, 0x06, // row size = 2 (SL) + 4 (payload)
	}
	wantIndicator := []byte{0x00, 0x00}                    // not null
	wantData := []byte{0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF} // SL=4 + raw bytes
	want := append(append(append([]byte{}, wantHeader...), wantIndicator...), wantData...)

	if !bytes.Equal(got, want) {
		t.Fatalf("byte mismatch:\n got=% X\nwant=% X", got, want)
	}
}

// TestEncodeDBExtendedDataBinaryVarcharRejectsTooLong confirms the
// length guard fires when the supplied payload would overrun the
// declared FieldLength budget.
func TestEncodeDBExtendedDataBinaryVarcharRejectsTooLong(t *testing.T) {
	params := []PreparedParam{{
		SQLType: 449, FieldLength: 4, CCSID: 65535, // budget = 4-2 = 2
	}}
	_, err := EncodeDBExtendedData(params, []any{[]byte{0x01, 0x02, 0x03}})
	if err == nil {
		t.Fatal("expected error for oversized payload, got nil")
	}
}

// TestEncodeDBExtendedDataUTF8Varchar covers the CCSID 1208 branch.
// JT400 uses 1208 when the server advertises it as a tagged-string
// CCSID; the encoder writes the UTF-8 bytes verbatim and lets the
// server transcode to the column CCSID.
func TestEncodeDBExtendedDataUTF8Varchar(t *testing.T) {
	s := "café" // 5 bytes in UTF-8: 0x63 0x61 0x66 0xC3 0xA9
	params := []PreparedParam{{
		SQLType:     449,
		FieldLength: uint32(len(s)) + 2,
		Precision:   uint16(len(s)),
		CCSID:       1208,
	}}
	got, err := EncodeDBExtendedData(params, []any{s})
	if err != nil {
		t.Fatalf("EncodeDBExtendedData: %v", err)
	}
	wantPayload := []byte{0x00, 0x05, 0x63, 0x61, 0x66, 0xC3, 0xA9}
	if !bytes.HasSuffix(got, wantPayload) {
		t.Errorf("UTF-8 payload not at end of output: got % X, want suffix % X", got, wantPayload)
	}
}

// TestEncodeVarcharCCSIDTagMatchesBytes is the offline acceptance
// guard for issue #24: the descriptor CCSID tag and the encoded
// VARCHAR payload bytes must agree for every SBCS override the
// encoder claims to honour. The bug was that ebcdicForCCSID silently
// fell back to the CCSID-37 codec for unmodelled pages, so a param
// tagged (say) 1140 shipped CCSID-37 bytes under a 1140 tag. We use
// '@', which diverges between CCSID 37 (0x7C) and CCSID 273 (0xB5),
// so a fallback-to-37 regression would make the 273 case emit 0x7C
// and fail this test.
func TestEncodeVarcharCCSIDTagMatchesBytes(t *testing.T) {
	const s = "A@B" // '@' diverges between CCSID 37 and 273
	for _, ccsid := range []uint16{37, 273} {
		ccsid := ccsid
		t.Run("ccsid="+strconv.Itoa(int(ccsid)), func(t *testing.T) {
			params := []PreparedParam{{
				SQLType:     449, // VARCHAR nullable
				FieldLength: uint32(len(s)) + 2,
				Precision:   uint16(len(s)),
				CCSID:       ccsid,
			}}

			// Descriptor tag: EncodeDBExtendedDataFormat writes the
			// CCSID at offset base+12 of the field descriptor.
			format := EncodeDBExtendedDataFormat(params)
			gotTag := binary.BigEndian.Uint16(format[16+12 : 16+14])
			if gotTag != ccsid {
				t.Fatalf("descriptor CCSID tag = %d, want %d", gotTag, ccsid)
			}

			// Encoded bytes: must equal the requested CCSID's codec
			// output, not a silent CCSID-37 fallback.
			wantBytes, err := ebcdicForCCSID(ccsid).Encode(s)
			if err != nil {
				t.Fatalf("encode reference: %v", err)
			}
			if int(ebcdicForCCSID(ccsid).CCSID()) != int(ccsid) {
				t.Fatalf("ebcdicForCCSID(%d) codec reports CCSID %d -- tag/byte mismatch", ccsid, ebcdicForCCSID(ccsid).CCSID())
			}
			data, err := EncodeDBExtendedData(params, []any{s})
			if err != nil {
				t.Fatalf("EncodeDBExtendedData: %v", err)
			}
			wantPayload := append([]byte{0x00, byte(len(wantBytes))}, wantBytes...)
			if !bytes.HasSuffix(data, wantPayload) {
				t.Fatalf("ccsid=%d payload mismatch: got % X, want suffix % X", ccsid, data, wantPayload)
			}
		})
	}
}

// cachedRowData slices the data region (after the 20-byte header and the
// single 2-byte indicator) out of a one-row, one-param CP 0x381F payload.
func cachedRowData(t *testing.T, payload []byte, fieldLength int) []byte {
	t.Helper()
	const dataOff = 20 + 2 // header + one indicator
	if len(payload) < dataOff+fieldLength {
		t.Fatalf("payload too short: %d bytes, need >= %d", len(payload), dataOff+fieldLength)
	}
	return payload[dataOff : dataOff+fieldLength]
}

// TestEncodeCachedVarbinary pins the native VARBINARY (908) bind shape: a
// 2-byte BE actual-length prefix, the raw payload, then 0x00 slot-padding
// out to FieldLength-2. This is the wire shape reconcileBindShapesFromPMF
// and the package-cache fast path deliver for a []byte into a native
// VARBINARY column (issue #40). Mirror of db_result_data.go case 908/909.
func TestEncodeCachedVarbinary(t *testing.T) {
	t.Run("exact", func(t *testing.T) {
		payload := []byte{0x11, 0x22, 0x33, 0x44}
		params := []PreparedParam{{
			SQLType:     908,
			FieldLength: uint32(len(payload)) + 2,
			CCSID:       65535,
		}}
		got, err := EncodeDBExtendedData(params, []any{payload})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		want := []byte{0x00, 0x04, 0x11, 0x22, 0x33, 0x44}
		if data := cachedRowData(t, got, 6); !bytes.Equal(data, want) {
			t.Fatalf("varbinary data:\n got=% X\nwant=% X", data, want)
		}
	})
	t.Run("short_slot_padded", func(t *testing.T) {
		// VARBINARY(8) carrying a 4-byte value: SL=4 + payload + 4 zero pad.
		params := []PreparedParam{{SQLType: 908, FieldLength: 10, CCSID: 65535}}
		got, err := EncodeDBExtendedData(params, []any{[]byte{0x11, 0x22, 0x33, 0x44}})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		want := []byte{0x00, 0x04, 0x11, 0x22, 0x33, 0x44, 0x00, 0x00, 0x00, 0x00}
		if data := cachedRowData(t, got, 10); !bytes.Equal(data, want) {
			t.Fatalf("varbinary short data:\n got=% X\nwant=% X", data, want)
		}
	})
	t.Run("empty", func(t *testing.T) {
		// VARBINARY(4) carrying a 0-byte value: SL=0 + 4 zero pad.
		params := []PreparedParam{{SQLType: 908, FieldLength: 6, CCSID: 65535}}
		got, err := EncodeDBExtendedData(params, []any{[]byte{}})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		want := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		if data := cachedRowData(t, got, 6); !bytes.Equal(data, want) {
			t.Fatalf("varbinary empty data:\n got=% X\nwant=% X", data, want)
		}
	})
}

// TestEncodeCachedBinaryFixed pins the native BINARY (912) bind shape: NO
// SL prefix, the raw payload, then 0x00 right-padding to exactly
// FieldLength. JT400's SQLBinary.set zero-pads a short value; the read
// mirror (db_result_data.go case 912/913) reads back exactly col.Length
// bytes (issue #40).
func TestEncodeCachedBinaryFixed(t *testing.T) {
	t.Run("short_zero_padded", func(t *testing.T) {
		// BINARY(8) carrying a 2-byte value: payload + 6 zero pad.
		params := []PreparedParam{{SQLType: 912, FieldLength: 8, CCSID: 65535}}
		got, err := EncodeDBExtendedData(params, []any{[]byte{0xAA, 0xBB}})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		want := []byte{0xAA, 0xBB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		if data := cachedRowData(t, got, 8); !bytes.Equal(data, want) {
			t.Fatalf("binary short data:\n got=% X\nwant=% X", data, want)
		}
	})
	t.Run("exact_verbatim", func(t *testing.T) {
		params := []PreparedParam{{SQLType: 912, FieldLength: 4, CCSID: 65535}}
		got, err := EncodeDBExtendedData(params, []any{[]byte{0xDE, 0xAD, 0xBE, 0xEF}})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		want := []byte{0xDE, 0xAD, 0xBE, 0xEF}
		if data := cachedRowData(t, got, 4); !bytes.Equal(data, want) {
			t.Fatalf("binary exact data:\n got=% X\nwant=% X", data, want)
		}
	})
	t.Run("empty_all_zero", func(t *testing.T) {
		// BINARY(4) carrying a 0-byte value: a full-width 0x00 field.
		params := []PreparedParam{{SQLType: 912, FieldLength: 4, CCSID: 65535}}
		got, err := EncodeDBExtendedData(params, []any{[]byte{}})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		want := []byte{0x00, 0x00, 0x00, 0x00}
		if data := cachedRowData(t, got, 4); !bytes.Equal(data, want) {
			t.Fatalf("binary empty data:\n got=% X\nwant=% X", data, want)
		}
	})
}

// TestEncodeCachedBinaryRejectsTooLong confirms an over-length binary bind
// is a HARD error, NOT a recoverable ErrUnsupportedCachedParamType
// fallback. This matches JT400 (which throws DataTruncation by default for
// an over-length binary write) and the VARCHAR/VARGRAPHIC over-length
// guards in encodeRowData. Wrapping ErrUnsupportedCachedParamType would
// wrongly route the value to the VARCHAR FOR BIT DATA fallback, which has
// no safe handling for a value that exceeds the column width.
func TestEncodeCachedBinaryRejectsTooLong(t *testing.T) {
	cases := []struct {
		name        string
		sqlType     uint16
		fieldLength uint32
	}{
		{"binary_fixed", 912, 2}, // BINARY(2)
		{"varbinary", 908, 4},    // VARBINARY(2): FieldLength = 2 + 2
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := []PreparedParam{{SQLType: tc.sqlType, FieldLength: tc.fieldLength, CCSID: 65535}}
			_, err := EncodeDBExtendedData(params, []any{[]byte{0x01, 0x02, 0x03}})
			if err == nil {
				t.Fatal("expected error for over-length binary, got nil")
			}
			if errors.Is(err, ErrUnsupportedCachedParamType) {
				t.Errorf("error must NOT wrap ErrUnsupportedCachedParamType (no fallback for over-length): %v", err)
			}
		})
	}
}

// TestEncodeCachedCharFixed pins the byte layout of the fixed-length
// CHAR (452/453) encode arm added for issue #68 Item 2 -- the arm a
// CHAR array element routes through, and which also gives scalar
// fixed-CHAR binds their first encoder. NO SL prefix; the payload is
// right-padded to FieldLength with the EBCDIC space 0x40 (text) or the
// 0x00 zero-init pad (FOR BIT DATA). EBCDIC: 'A'=0xC1 'B'=0xC2 'C'=0xC3.
func TestEncodeCachedCharFixed(t *testing.T) {
	t.Run("ebcdic_text_pad_40", func(t *testing.T) {
		// CHAR(10) CCSID 37, value "ABC" -> C1 C2 C3 then 7x 0x40.
		params := []PreparedParam{{SQLType: 452, FieldLength: 10, CCSID: 37}}
		got, err := EncodeDBExtendedData(params, []any{"ABC"})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		wantHeader := []byte{
			0x00, 0x00, 0x00, 0x01, // consistency token
			0x00, 0x00, 0x00, 0x01, // row count
			0x00, 0x01, // column count
			0x00, 0x02, // indicator size
			0x00, 0x00, 0x00, 0x00, // reserved
			0x00, 0x00, 0x00, 0x0A, // row size = FieldLength = 10
		}
		wantIndicator := []byte{0x00, 0x00}
		wantData := []byte{0xC1, 0xC2, 0xC3, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40}
		want := append(append(append([]byte{}, wantHeader...), wantIndicator...), wantData...)
		if !bytes.Equal(got, want) {
			t.Fatalf("byte mismatch:\n got=% X\nwant=% X", got, want)
		}
	})

	t.Run("for_bit_data_pad_00", func(t *testing.T) {
		// CHAR(8) FOR BIT DATA (CCSID 65535), []byte{01,02,03} -> raw
		// bytes then 5x 0x00 (no blank for binary).
		params := []PreparedParam{{SQLType: 453, FieldLength: 8, CCSID: 65535}}
		got, err := EncodeDBExtendedData(params, []any{[]byte{0x01, 0x02, 0x03}})
		if err != nil {
			t.Fatalf("EncodeDBExtendedData: %v", err)
		}
		wantData := []byte{0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00}
		// Data region is the last FieldLength bytes of the frame.
		gotData := got[len(got)-8:]
		if !bytes.Equal(gotData, wantData) {
			t.Fatalf("data mismatch:\n got=% X\nwant=% X", gotData, wantData)
		}
	})

	t.Run("rejects_too_long", func(t *testing.T) {
		// CHAR(2), value "ABC" (3 EBCDIC bytes) overruns the width.
		params := []PreparedParam{{SQLType: 452, FieldLength: 2, CCSID: 37}}
		if _, err := EncodeDBExtendedData(params, []any{"ABC"}); err == nil {
			t.Fatal("expected error for over-length char, got nil")
		}
	})
}
