package hostserver

import (
	"bytes"
	"encoding/binary"
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
