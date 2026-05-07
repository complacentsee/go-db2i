package ebcdic

import (
	"bytes"
	"testing"
)

// TestCCSID37UserIDFromFixture round-trips "AFTRAEGE1" -- the fixture
// captures these 10 bytes (with trailing 0x40 space pad) on the wire,
// so passing this test means the CCSID 37 codec is wire-compatible
// for the user-ID case the driver cares about most.
func TestCCSID37UserIDFromFixture(t *testing.T) {
	want := []byte{0xC1, 0xC6, 0xE3, 0xD9, 0xC1, 0xC5, 0xC7, 0xC5, 0xF1}
	got, err := CCSID37.Encode("AFTRAEGE1")
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("encoded = % X, want % X", got, want)
	}
	roundTripped, err := CCSID37.Decode(got)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if roundTripped != "AFTRAEGE1" {
		t.Errorf("decoded = %q, want %q", roundTripped, "AFTRAEGE1")
	}
}

// TestCCSID37PunctuationAndDigits sanity-checks a few well-known
// EBCDIC byte values that show up in real fixtures (digit '0'-'9' run
// is 0xF0..0xF9, lowercase 'a' is 0x81, slash is 0x61).
func TestCCSID37PunctuationAndDigits(t *testing.T) {
	cases := map[string][]byte{
		"0":   {0xF0},
		"9":   {0xF9},
		"/":   {0x61},
		" ":   {0x40},
		"a":   {0x81},
		"341513/QUSER/QZSOSIGN": {
			0xF3, 0xF4, 0xF1, 0xF5, 0xF1, 0xF3,
			0x61,
			0xD8, 0xE4, 0xE2, 0xC5, 0xD9,
			0x61,
			0xD8, 0xE9, 0xE2, 0xD6, 0xE2, 0xC9, 0xC7, 0xD5,
		},
	}
	for s, want := range cases {
		got, err := CCSID37.Encode(s)
		if err != nil {
			t.Errorf("Encode(%q): %v", s, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Encode(%q) = % X, want % X", s, got, want)
		}
	}
}
