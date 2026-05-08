package hostserver

import (
	"bytes"
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
	wantIndicator := []byte{0x00, 0x00}                              // not null
	wantData := []byte{0x00, 0x04, 0xDE, 0xAD, 0xBE, 0xEF}           // SL=4 + raw bytes
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
