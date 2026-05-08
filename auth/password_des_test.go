package auth

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// TestXorWith0x55AndLshift pins the password-pre-DES transform JT400
// uses: XOR each byte with 0x55, then shift the whole 8-byte word
// left by 1 bit. Hand-computed for the input
// {0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8} (EBCDIC "ABCDEFGH"):
//
//	XOR with 0x55: {0x94, 0x97, 0x96, 0x91, 0x90, 0x93, 0x92, 0x9D}
//	(0xC1 ^ 0x55 = 0x94, etc.)
//	Then left shift by 1 bit treating the 8 bytes as a single
//	big-endian word, with each byte's high bit cascading into the
//	previous byte's low bit:
//	  b[0] = (0x94 << 1) | (0x97 >> 7) = 0x28 | 0x01 = 0x29
//	  b[1] = (0x97 << 1) | (0x96 >> 7) = 0x2E | 0x01 = 0x2F
//	  b[2] = (0x96 << 1) | (0x91 >> 7) = 0x2C | 0x01 = 0x2D
//	  b[3] = (0x91 << 1) | (0x90 >> 7) = 0x22 | 0x01 = 0x23
//	  b[4] = (0x90 << 1) | (0x93 >> 7) = 0x20 | 0x01 = 0x21
//	  b[5] = (0x93 << 1) | (0x92 >> 7) = 0x26 | 0x01 = 0x27
//	  b[6] = (0x92 << 1) | (0x9D >> 7) = 0x24 | 0x01 = 0x25
//	  b[7] = (0x9D << 1)               = 0x3A
func TestXorWith0x55AndLshift(t *testing.T) {
	in := []byte{0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8}
	want := []byte{0x29, 0x2F, 0x2D, 0x23, 0x21, 0x27, 0x25, 0x3A}
	xorWith0x55AndLshift(in)
	if !bytes.Equal(in, want) {
		t.Errorf("xorWith0x55AndLshift\n got: %x\nwant: %x", in, want)
	}
}

// TestAddArray8WithCarry confirms the 8-byte big-endian add wraps
// correctly across byte boundaries.
func TestAddArray8WithCarry(t *testing.T) {
	// 0xFFFE + 0x0005 = 0x10003. Carry propagates: byte 7 = 0x03,
	// byte 6 wraps to 0x00 (0xFF + carry), byte 5 picks up the
	// final 0x01.
	a := []byte{0, 0, 0, 0, 0, 0, 0xFF, 0xFE}
	b := []byte{0, 0, 0, 0, 0, 0, 0x00, 0x05}
	dst := make([]byte, 8)
	addArray8(a, b, dst)
	want := []byte{0, 0, 0, 0, 0, 0x01, 0x00, 0x03}
	if !bytes.Equal(dst, want) {
		t.Errorf("addArray8 carry\n got: %x\nwant: %x", dst, want)
	}
}

// TestAddArray8Wraparound confirms full 64-bit wrap is a clean
// modular add (no panic, top byte's carry is dropped).
func TestAddArray8Wraparound(t *testing.T) {
	a := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	b := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	dst := make([]byte, 8)
	addArray8(a, b, dst)
	want := make([]byte, 8) // all zero (wraps)
	if !bytes.Equal(dst, want) {
		t.Errorf("addArray8 wrap\n got: %x\nwant: %x", dst, want)
	}
}

// TestEbcdicStrLen confirms the JTOpen "string ends at first 0x40
// or 0x00" semantic. EBCDIC 0x40 == ASCII space.
func TestEbcdicStrLen(t *testing.T) {
	cases := []struct {
		in   []byte
		want int
	}{
		{[]byte{0xC1, 0xC2, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40}, 2},
		{[]byte{0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xD1}, 10},
		{[]byte{0x00, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40}, 0},
	}
	for _, tc := range cases {
		if got := ebcdicStrLen(tc.in); got != tc.want {
			t.Errorf("ebcdicStrLen(%x) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestEncodeEBCDICPadded10 confirms ASCII strings encode + pad
// correctly to the 10-byte field IBM i expects. "AFTRAEGE1" is 9
// EBCDIC chars (0xC1 0xC6 0xE3 0xD9 0xC1 0xC5 0xC7 0xC5 0xF1) + 1
// trailing 0x40.
func TestEncodeEBCDICPadded10(t *testing.T) {
	got, err := encodeEBCDICPadded10("AFTRAEGE1")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := []byte{0xC1, 0xC6, 0xE3, 0xD9, 0xC1, 0xC5, 0xC7, 0xC5, 0xF1, 0x40}
	if !bytes.Equal(got, want) {
		t.Errorf("encodeEBCDICPadded10\n got: %x\nwant: %x", got, want)
	}
}

// TestEncodeEBCDICPadded10TooLong rejects inputs > 10 EBCDIC bytes.
func TestEncodeEBCDICPadded10TooLong(t *testing.T) {
	if _, err := encodeEBCDICPadded10("ABCDEFGHIJK"); err == nil {
		t.Error("expected error for 11-char input")
	}
}

// TestEncryptPasswordDESDeterministic is the same regression net we
// have for PBKDF2: same input -> same output. End-to-end coverage of
// the whole token+substitute chain.
func TestEncryptPasswordDESDeterministic(t *testing.T) {
	clientSeed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	serverSeed := []byte{8, 7, 6, 5, 4, 3, 2, 1}
	a, err := EncryptPasswordDES("AFTRAEGE1", "random2025", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	b, err := EncryptPasswordDES("AFTRAEGE1", "random2025", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Errorf("non-deterministic\n a: %x\n b: %x", a, b)
	}
	if len(a) != 8 {
		t.Errorf("output length = %d, want 8", len(a))
	}
}

// TestEncryptPasswordDESLongPassword exercises the > 8-char password
// branch (split + double-encrypt + XOR token). Self-consistent;
// without a live IBM i to validate against we can only confirm the
// output is non-zero, length 8, and stable.
func TestEncryptPasswordDESLongPassword(t *testing.T) {
	clientSeed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	serverSeed := []byte{8, 7, 6, 5, 4, 3, 2, 1}
	// Password capped at 10 EBCDIC bytes; pick a 10-char one that
	// still exercises the > 8-char branch in generateDESToken.
	out, err := EncryptPasswordDES("USER", "long10pass", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if len(out) != 8 {
		t.Errorf("length = %d, want 8", len(out))
	}
	if bytes.Equal(out, make([]byte, 8)) {
		t.Errorf("output is all zeros -- chained DES likely not running")
	}
}

// TestEncryptPasswordDESShortPassword exercises the <= 8-char branch
// (single DES on the password block).
func TestEncryptPasswordDESShortPassword(t *testing.T) {
	clientSeed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	serverSeed := []byte{8, 7, 6, 5, 4, 3, 2, 1}
	out, err := EncryptPasswordDES("USER", "secret", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if len(out) != 8 {
		t.Errorf("length = %d, want 8", len(out))
	}
	if bytes.Equal(out, make([]byte, 8)) {
		t.Errorf("output is all zeros")
	}
}

// TestEncryptPasswordDESPinnedShort pins one specific output so any
// future change to the algorithm shows up here. NOT a wire-validated
// vector -- this is a self-test snapshot. Replace with a real
// IBM-i-captured vector if/when one becomes available.
func TestEncryptPasswordDESPinnedShort(t *testing.T) {
	clientSeed, _ := hex.DecodeString("0102030405060708")
	serverSeed, _ := hex.DecodeString("0807060504030201")
	out, err := EncryptPasswordDES("USER", "secret", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	// Pinned snapshot from the first clean run of this code; if
	// the algorithm is later fixed against a live IBM i vector,
	// this should be REPLACED with the wire-validated vector and
	// not silently updated to whatever the new output is.
	want, _ := hex.DecodeString("245b19af657167a4")
	if !bytes.Equal(out, want) {
		// Print the actual so anyone re-pinning has it ready.
		t.Errorf("DES output mismatch (regression snapshot)\n got: %x\nwant: %x", out, want)
	}
}
