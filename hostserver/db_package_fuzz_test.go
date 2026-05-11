package hostserver

import (
	"strings"
	"testing"
)

// FuzzSuffixFromOptions feeds adversarial PackageOptions integers
// through the suffix formula. Contracts:
//   - The returned suffix is always exactly 4 chars long.
//   - Every char is a member of SUFFIX_INVARIANT_.
//   - SuffixFromOptions never panics on out-of-range values (the
//     clipSuffixIndex helper is supposed to clamp).
//
// We do NOT assert any specific output here -- the only reference for
// "correct" output is JT400, and that's covered by
// TestSuffixFromOptions_FixtureMatch and the table-driven test.
func FuzzSuffixFromOptions(f *testing.F) {
	// Seed corpus covers the documented enum extremes plus the
	// COMMIT_MODE_RR overflow branches that get the trickiest math.
	seeds := []PackageOptions{
		{},                                      // defaults
		{TranslateHex: 1},                       // 1-bit slot pin
		{CommitMode: 4, DateSeparator: 0},       // RR overflow case 1
		{CommitMode: 4, DateSeparator: 3},       // RR overflow case 2
		{DateFormat: 7, DateSeparator: 4},       // upper-end enum values
		{DecimalSeparator: 1, Naming: 1},        // both top bits of char3
		{TimeFormat: 4, TimeSeparator: 3},       // top of char4
		{CommitMode: 100, DateFormat: -50},      // adversarial out-of-range
		{TranslateHex: 1 << 30},                 // huge
		{TimeFormat: -1, TimeSeparator: -1},     // negative
	}
	for _, s := range seeds {
		f.Add(s.TranslateHex, s.CommitMode, s.DateFormat,
			s.DateSeparator, s.DecimalSeparator, s.Naming,
			s.TimeFormat, s.TimeSeparator)
	}

	f.Fuzz(func(t *testing.T,
		translateHex, commitMode, dateFormat, dateSep,
		decSep, naming, timeFormat, timeSep int) {
		opts := PackageOptions{
			TranslateHex:     translateHex,
			CommitMode:       commitMode,
			DateFormat:       dateFormat,
			DateSeparator:    dateSep,
			DecimalSeparator: decSep,
			Naming:           naming,
			TimeFormat:       timeFormat,
			TimeSeparator:    timeSep,
		}
		got := SuffixFromOptions(opts)
		if len(got) != 4 {
			t.Fatalf("SuffixFromOptions(%+v) len=%d, want 4", opts, len(got))
		}
		for i := 0; i < 4; i++ {
			if !strings.ContainsRune(suffixInvariant, rune(got[i])) {
				t.Fatalf("SuffixFromOptions(%+v) char %d=%q not in SUFFIX_INVARIANT_",
					opts, i, string(got[i]))
			}
		}
		// Determinism: same input -> same output.
		got2 := SuffixFromOptions(opts)
		if got != got2 {
			t.Fatalf("SuffixFromOptions non-deterministic: %q vs %q for %+v", got, got2, opts)
		}
	})
}

// FuzzBuildPackageName runs random base strings + arbitrary options
// through the composer. Contract: result is always 10 ASCII chars,
// every char from the IBM-i object-name charset (A-Z 0-9 _ # @ $)
// plus the 36-char SUFFIX_INVARIANT_ alphabet for the 4-char tail.
// Trailing-space padding (the 6-char base may be padded with ' ') is
// allowed in chars 1..6 only.
//
// We do NOT call validatePackageIdent here because BuildPackageName
// is documented to accept the canonicalised form -- DSN parsing
// gates the validation. The fuzzer focuses on "no panic, length
// invariant holds" for inputs the validator already let through.
func FuzzBuildPackageName(f *testing.F) {
	f.Add("APP", 0, 0, 1, 0, 0, 0, 0, 0)
	f.Add("MY_PKG", 1, 2, 3, 1, 0, 1, 2, 1)
	f.Add("PK", 0, 4, 0, 0, 0, 0, 0, 0)
	f.Add("", 0, 0, 0, 0, 0, 0, 0, 0)
	f.Add("123456", 0, 0, 0, 0, 0, 0, 0, 0)
	f.Add("OVERSIXCHARS", 0, 0, 0, 0, 0, 0, 0, 0)

	f.Fuzz(func(t *testing.T,
		base string,
		translateHex, commitMode, dateFormat, dateSep,
		decSep, naming, timeFormat, timeSep int) {
		// Limit input size; the fuzzer would otherwise spend cycles
		// generating mega-strings the composer just truncates.
		if len(base) > 256 {
			t.Skip()
		}
		opts := PackageOptions{
			TranslateHex:     translateHex,
			CommitMode:       commitMode,
			DateFormat:       dateFormat,
			DateSeparator:    dateSep,
			DecimalSeparator: decSep,
			Naming:           naming,
			TimeFormat:       timeFormat,
			TimeSeparator:    timeSep,
		}
		got := BuildPackageName(base, opts)
		if len(got) != 10 {
			t.Fatalf("BuildPackageName(%q,%+v) len=%d, want 10", base, opts, len(got))
		}
		// The 4-char suffix tail MUST be in SUFFIX_INVARIANT_.
		for i := 6; i < 10; i++ {
			if !strings.ContainsRune(suffixInvariant, rune(got[i])) {
				t.Fatalf("BuildPackageName(%q,%+v) suffix char %d=%q not in SUFFIX_INVARIANT_",
					base, opts, i, string(got[i]))
			}
		}
	})
}
