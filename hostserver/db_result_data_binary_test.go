package hostserver

import (
	"bytes"
	"testing"
)

// TestDecodeColumnBinaryChar confirms a CHAR column with CCSID 65535
// (FOR BIT DATA) returns the raw bytes as []byte instead of running
// them through an EBCDIC decode that would lose any byte > 0xFE
// (no glyph in CCSID 37) and silently fail.
func TestDecodeColumnBinaryChar(t *testing.T) {
	// 8-byte FOR BIT DATA column with arbitrary bytes including
	// values that have no EBCDIC mapping (0x00, 0xFF).
	wire := []byte{0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0x42, 0x01}
	col := SelectColumn{
		SQLType: SQLTypeChar,
		Length:  8,
		CCSID:   65535,
	}
	got, n, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if n != 8 {
		t.Errorf("consumed = %d, want 8", n)
	}
	gotBytes, ok := got.([]byte)
	if !ok {
		t.Fatalf("got type %T, want []byte", got)
	}
	if !bytes.Equal(gotBytes, wire) {
		t.Errorf("decoded bytes mismatch\n got: %x\nwant: %x", gotBytes, wire)
	}
}

// TestDecodeColumnBinaryVarChar mirrors the CHAR test for VARCHAR.
// The 2-byte length prefix tells the decoder how many bytes follow;
// CCSID 65535 means those bytes pass through verbatim instead of
// EBCDIC-decoded.
func TestDecodeColumnBinaryVarChar(t *testing.T) {
	// Wire: LL=5 (2 bytes BE), then 5 binary bytes.
	wire := []byte{0x00, 0x05, 0xCA, 0xFE, 0xBA, 0xBE, 0x42}
	col := SelectColumn{
		SQLType: SQLTypeVarChar,
		Length:  16, // declared max
		CCSID:   65535,
	}
	got, n, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if n != 7 { // 2-byte header + 5-byte payload
		t.Errorf("consumed = %d, want 7", n)
	}
	gotBytes, ok := got.([]byte)
	if !ok {
		t.Fatalf("got type %T, want []byte", got)
	}
	want := []byte{0xCA, 0xFE, 0xBA, 0xBE, 0x42}
	if !bytes.Equal(gotBytes, want) {
		t.Errorf("decoded bytes mismatch\n got: %x\nwant: %x", gotBytes, want)
	}
}

// TestDecodeColumnTextCharStillStringForRegularCCSID confirms the
// 65535 short-circuit doesn't accidentally fire for regular text
// columns (CCSID 37). Same wire bytes as the binary test, but with
// a normal CCSID -- should come back as a Go string (whatever the
// EBCDIC decode produces, garbage or not, but typed as string).
func TestDecodeColumnTextCharStillStringForRegularCCSID(t *testing.T) {
	wire := []byte{0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8} // EBCDIC "ABCDEFGH"
	col := SelectColumn{
		SQLType: SQLTypeChar,
		Length:  8,
		CCSID:   37,
	}
	got, _, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if _, ok := got.([]byte); ok {
		t.Errorf("regular CCSID 37 column returned []byte instead of string")
	}
	gotStr, ok := got.(string)
	if !ok {
		t.Fatalf("got type %T, want string", got)
	}
	if gotStr != "ABCDEFGH" {
		t.Errorf("decoded string = %q, want %q", gotStr, "ABCDEFGH")
	}
}
