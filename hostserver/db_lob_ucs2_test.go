package hostserver

import (
	"bytes"
	"errors"
	"testing"
	"unicode/utf16"
)

// TestEncodeUCS2BE_BMP_HappyPath confirms BMP-only input produces
// byte-for-byte the same wire bytes as the surrogate-aware
// encodeUTF16BE helper. CCSID 13488 (strict UCS-2 BE) and CCSID 1200
// (UTF-16 BE) only diverge on non-BMP runes; for any string of
// codepoints <= 0xFFFF the byte streams must agree.
func TestEncodeUCS2BE_BMP_HappyPath(t *testing.T) {
	// ASCII + Latin-1 + an em-dash (U+2014) — all BMP.
	cases := []string{
		"",
		"hello",
		"DBCLOB Test — Hello, IBM i! ",
		"Latin: café · ñ · π · Ω",
	}
	for _, s := range cases {
		want := encodeUTF16BE(s)
		got := encodeUCS2BE(s)
		if !bytes.Equal(got, want) {
			t.Errorf("encodeUCS2BE(%q) = % x; want % x", s, got, want)
		}
		// Also confirm strict mode does not error on BMP-only input.
		gotStrict, err := encodeUCS2BEStrict(s)
		if err != nil {
			t.Errorf("encodeUCS2BEStrict(%q) BMP-only returned error: %v", s, err)
			continue
		}
		if !bytes.Equal(gotStrict, want) {
			t.Errorf("encodeUCS2BEStrict(%q) = % x; want % x", s, gotStrict, want)
		}
	}
}

// TestEncodeUCS2BE_NonBMP_Substitute exercises the default substitute
// path. A non-BMP rune (𝄞 = U+1D11E, treble clef) must NOT emit a
// surrogate pair; CCSID 13488 rejects surrogates server-side with
// SQL-330. JT400 substitutes U+003F ("?") in the same situation, so
// we mirror that — the byte stream for 𝄞 alone must be 0x00 0x3F.
func TestEncodeUCS2BE_NonBMP_Substitute(t *testing.T) {
	const trebleClef = "𝄞"
	got := encodeUCS2BE(trebleClef)
	want := []byte{0x00, 0x3F}
	if !bytes.Equal(got, want) {
		t.Errorf("encodeUCS2BE(treble clef) = % x; want % x (UCS-2 BE for '?')", got, want)
	}
	// Belt and braces: confirm no surrogate-pair byte sequence
	// snuck out. UTF-16 BE for U+1D11E is D8 34 DD 1E, which is
	// what encodeUTF16BE would have emitted.
	utf16Bytes := encodeUTF16BE(trebleClef)
	if bytes.Equal(got, utf16Bytes) {
		t.Errorf("encodeUCS2BE leaked a surrogate pair: % x (CCSID 13488 forbids surrogates)", got)
	}

	// Mixed BMP + non-BMP: every BMP rune should stay verbatim, and
	// every non-BMP rune should turn into 0x00 0x3F. "A𝄞B" has a
	// BMP-NonBMP-BMP layout, giving 6 wire bytes total.
	got = encodeUCS2BE("A𝄞B")
	want = []byte{0x00, 0x41, 0x00, 0x3F, 0x00, 0x42}
	if !bytes.Equal(got, want) {
		t.Errorf("encodeUCS2BE(\"A𝄞B\") = % x; want % x", got, want)
	}
}

// TestEncodeUCS2BE_NonBMP_StrictError confirms the opt-in strict
// helper returns a typed *NonBMPRuneError on the first non-BMP rune
// and emits no bytes — callers who would rather know than silently
// corrupt data can route through this path.
func TestEncodeUCS2BE_NonBMP_StrictError(t *testing.T) {
	const trebleClef = "Music: 𝄞 — done."
	got, err := encodeUCS2BEStrict(trebleClef)
	if err == nil {
		t.Fatalf("encodeUCS2BEStrict(non-BMP input) returned nil error; want *NonBMPRuneError")
	}
	if got != nil {
		t.Errorf("encodeUCS2BEStrict on error returned %d bytes; want nil", len(got))
	}
	var nbm *NonBMPRuneError
	if !errors.As(err, &nbm) {
		t.Fatalf("error %v is not *NonBMPRuneError", err)
	}
	if nbm.CCSID != 13488 {
		t.Errorf("NonBMPRuneError.CCSID = %d; want 13488", nbm.CCSID)
	}
	if nbm.Rune != 0x1D11E {
		t.Errorf("NonBMPRuneError.Rune = U+%04X; want U+1D11E (treble clef)", nbm.Rune)
	}
}

// TestEncodeUCS2BE_BoundaryRunes pins the BMP/non-BMP boundary so a
// future tweak to the helper (e.g. switching from `utf16.IsSurrogate`
// to a hand-rolled check) doesn't quietly drift the cutoff. U+FFFF
// is the last legal BMP codepoint and must round-trip; U+10000 is
// the first non-BMP codepoint and must substitute.
func TestEncodeUCS2BE_BoundaryRunes(t *testing.T) {
	// U+FFFF — last BMP codepoint. Note: U+FFFE/U+FFFF are
	// "noncharacters" per Unicode but UCS-2 still encodes them as
	// the literal code unit. Confirm the encoder doesn't substitute.
	got := encodeUCS2BE(string(rune(0xFFFF)))
	want := []byte{0xFF, 0xFF}
	if !bytes.Equal(got, want) {
		t.Errorf("encodeUCS2BE(U+FFFF) = % x; want % x", got, want)
	}

	// U+10000 — first non-BMP codepoint. Must substitute.
	got = encodeUCS2BE(string(rune(0x10000)))
	want = []byte{0x00, 0x3F}
	if !bytes.Equal(got, want) {
		t.Errorf("encodeUCS2BE(U+10000) = % x; want % x (substitute)", got, want)
	}

	// utf16.IsSurrogate is informational here — used to make sure
	// the helper isn't accidentally treating lone surrogate code
	// points (which can appear in the runes of a malformed Go
	// string, though Go's source-level strings can't carry them) as
	// BMP. A lone surrogate code unit isn't representable as a Go
	// rune via `string(rune(0xD800))` — Go converts it to U+FFFD.
	// Confirm that path.
	got = encodeUCS2BE(string(rune(0xD800)))
	if utf16.IsSurrogate(rune(0xD800)) {
		// Go's `string(rune(0xD800))` encodes as U+FFFD (replacement
		// char) which is BMP and round-trips as 0xFFFD = FF FD.
		want = []byte{0xFF, 0xFD}
		if !bytes.Equal(got, want) {
			t.Errorf("encodeUCS2BE(string(rune(0xD800))) = % x; want % x (Go replacement)", got, want)
		}
	}
}
