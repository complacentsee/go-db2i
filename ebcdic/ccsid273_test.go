package ebcdic

import (
	"bytes"
	"testing"
)

// TestCCSID273RoundTripASCII confirms the German EBCDIC codec
// preserves the ASCII printable subset (digits, A-Z, a-z, common
// punctuation) -- the bulk of the byte mapping where CCSID 37 and
// CCSID 273 happen to coincide.
func TestCCSID273RoundTripASCII(t *testing.T) {
	const sample = "AFTRAEGE1 PUB400 SELECT * FROM SYSIBM.SYSDUMMY1 0123456789"
	encoded, err := CCSID273.Encode(sample)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := CCSID273.Decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded != sample {
		t.Errorf("round-trip = %q, want %q", decoded, sample)
	}
}

// TestCCSID273DivergentChars walks the byte positions where CCSID
// 273 differs from CCSID 37. Each position is asserted to encode
// and decode to the German variant character (e.g. byte 0xC0 ->
// 'ä', 0x4A -> 'Ä'), proving the codec really is 273 and not the
// CCSID 37 stand-in we shipped previously.
func TestCCSID273DivergentChars(t *testing.T) {
	cases := []struct {
		ebcdic byte
		want   rune
	}{
		// Canonical CCSID 273 mappings (verified against Python's
		// cp273 codec). The byte<->rune pairs below cover the
		// German-specific positions that diverge from CCSID 37.
		{0x40, ' '},
		{0x4A, 'Ä'},
		{0x4F, '!'},
		{0x5A, 'Ü'},
		{0x5F, '^'},
		{0x7B, '#'},
		{0x7C, '§'},
		{0x90, '°'}, // degree sign sits at 0x90 in CCSID 273
		{0xA0, 'µ'}, // micro sign at 0xA0
		{0xA1, 'ß'},
		{0xB0, '¢'}, // cent sign at 0xB0 in CCSID 273
		{0xB5, '@'},
		{0xC0, 'ä'},
		{0xD0, 'ü'},
		{0xE0, 'Ö'},
		{0xFC, ']'},
	}
	for _, tc := range cases {
		t.Run(string(tc.want), func(t *testing.T) {
			got, err := CCSID273.Decode([]byte{tc.ebcdic})
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if r := []rune(got); len(r) != 1 || r[0] != tc.want {
				t.Errorf("decode 0x%02X = %q, want %q", tc.ebcdic, got, string(tc.want))
			}
			// Round-trip the rune back through Encode.
			back, err := CCSID273.Encode(string(tc.want))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if !bytes.Equal(back, []byte{tc.ebcdic}) {
				t.Errorf("encode %q = %X, want %02X", string(tc.want), back, tc.ebcdic)
			}
		})
	}
}

// TestCCSID273MatchesCCSID37OnSharedBytes asserts that for byte
// positions where the two CCSIDs coincide (most of 0x00-0xFF),
// CCSID273 decodes to the same rune CCSID37 does. Catches
// hand-typo regressions in the 256-entry forward table.
func TestCCSID273MatchesCCSID37OnSharedBytes(t *testing.T) {
	// Bytes that DIFFER between CCSID 37 and 273 (a superset of
	// the divergent set above; we just exclude these from the
	// equality check rather than try to enumerate every shared
	// byte).
	// Canonical superset of CCSID 37 vs 273 differences (verified
	// byte-by-byte against Python cp273 / cp37 codecs).
	differ := map[byte]bool{
		0x43: true, 0x4A: true, 0x4F: true,
		0x59: true, 0x5A: true, 0x5F: true,
		0x63: true, 0x6A: true,
		0x7B: true, 0x7C: true,
		0xA1: true,
		0xB0: true, 0xB1: true, 0xB2: true, 0xB5: true,
		0xBA: true, 0xBB: true, 0xBC: true,
		0xC0: true, 0xCC: true,
		0xD0: true, 0xDC: true,
		0xE0: true, 0xEC: true,
		0xFC: true,
	}
	for b := 0; b < 256; b++ {
		if differ[byte(b)] {
			continue
		}
		s37, _ := CCSID37.Decode([]byte{byte(b)})
		s273, _ := CCSID273.Decode([]byte{byte(b)})
		if s37 != s273 {
			t.Errorf("byte 0x%02X: CCSID37 -> %q, CCSID273 -> %q (expected same)", b, s37, s273)
		}
	}
}
