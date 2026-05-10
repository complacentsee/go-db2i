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

// TestCCSID273ByteRoundTripIsBijective walks every wire byte
// (0..255) through Decode-then-Encode and asserts the round-trip
// lands on the original byte. Pins the codec as a bijection at
// the wire boundary -- the invariant the []byte-bind path in the
// conformance suite (TestLOBClob/[]byte_pre-encoded_CCSID_273)
// depends on.
//
// The bug-#15 investigation suspected codec asymmetry; it turned
// out to be a CCSID mismatch between the test's pre-encoded
// payload and the CLOB column's job-default CCSID (37 on the
// English-locale LPAR), not a codec bug. The test was fixed by
// pinning the column to `CCSID 273`; this offline test is the
// cheap regression net for the codec half of the contract.
//
// Wire bytes whose forward-table rune is U+001A SUBSTITUTE are
// exempt from the strict-bijection assertion: several IBM EBCDIC
// code-page bytes designate distinct wire codes that all map to
// U+001A on decode (the "no mapping in destination charset"
// sentinel), and there's no information-preserving reverse
// mapping for that one-to-many case.
func TestCCSID273ByteRoundTripIsBijective(t *testing.T) {
	const substitute = rune(0x001A)
	for b := 0; b < 256; b++ {
		s, err := CCSID273.Decode([]byte{byte(b)})
		if err != nil {
			t.Fatalf("decode 0x%02X: %v", b, err)
		}
		runes := []rune(s)
		if len(runes) != 1 {
			t.Errorf("byte 0x%02X decoded to %d-rune string %q (want 1)", b, len(runes), s)
			continue
		}
		if runes[0] == substitute {
			continue
		}
		enc, err := CCSID273.Encode(s)
		if err != nil {
			t.Fatalf("encode %q (from byte 0x%02X): %v", s, b, err)
		}
		if len(enc) != 1 || enc[0] != byte(b) {
			t.Errorf("byte 0x%02X -> rune U+%04X -> encode %x (want [%02X])", b, runes[0], enc, b)
		}
	}
}
