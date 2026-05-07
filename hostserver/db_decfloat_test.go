package hostserver

import (
	"strings"
	"testing"
)

// TestDecFloatRoundTrip exercises encodeDecimal64/encodeDecimal128
// against decodeDecimal64/decodeDecimal128. Round-tripping through
// the DPD codec is the cheapest assurance the encoder preserves
// every (sign, coefficient, exponent) triple within the type's
// dynamic range.
func TestDecFloatRoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		decimal128 bool
		negative   bool
		coefDigits string
		exponent   int
		want       string // expected formatDecimalFloat output
	}{
		// decimal64 (16 digits).
		{name: "d64_pos_simple", coefDigits: "1234567890123456", exponent: -10, want: "123456.7890123456"},
		{name: "d64_neg_simple", negative: true, coefDigits: "1234567890123456", exponent: -10, want: "-123456.7890123456"},
		{name: "d64_zero", coefDigits: "0", exponent: 0, want: "0"},
		{name: "d64_one", coefDigits: "1", exponent: 0, want: "1"},
		{name: "d64_nine_times", coefDigits: "9999999999999999", exponent: 0, want: "9999999999999999"},
		{name: "d64_scientific", coefDigits: "1", exponent: 100, want: "1E+100"},
		{name: "d64_negative_exp", coefDigits: "1", exponent: -100, want: "1E-100"},

		// decimal128 (34 digits).
		{name: "d128_full", decimal128: true, coefDigits: "1234567890123456789012345678901234", exponent: 67, want: "1.234567890123456789012345678901234E+100"},
		{name: "d128_neg_full", decimal128: true, negative: true, coefDigits: "1234567890123456789012345678901234", exponent: -33, want: "-1.234567890123456789012345678901234"},
		{name: "d128_zero", decimal128: true, coefDigits: "0", exponent: 0, want: "0"},
		{name: "d128_max_digits", decimal128: true, coefDigits: "9999999999999999999999999999999999", exponent: 0, want: "9999999999999999999999999999999999"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b []byte
			var err error
			if tc.decimal128 {
				b, err = encodeDecimal128(tc.negative, []byte(tc.coefDigits), tc.exponent)
			} else {
				b, err = encodeDecimal64(tc.negative, []byte(tc.coefDigits), tc.exponent)
			}
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			expectedLen := 8
			if tc.decimal128 {
				expectedLen = 16
			}
			if len(b) != expectedLen {
				t.Fatalf("encoded length = %d, want %d", len(b), expectedLen)
			}
			var got string
			if tc.decimal128 {
				got, err = decodeDecimal128(b)
			} else {
				got, err = decodeDecimal64(b)
			}
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != tc.want {
				t.Errorf("round-trip = %q, want %q (encoded = %X)", got, tc.want, b)
			}
		})
	}
}

// TestParseDecFloatString sanity-checks the decimal-string parser
// that feeds the encoder. The encoder takes (sign, digits, exp);
// the parser is what most callers will use to get there.
func TestParseDecFloatString(t *testing.T) {
	cases := []struct {
		in       string
		wantNeg  bool
		wantDigs string
		wantExp  int
	}{
		{"0", false, "0", 0},
		{"123", false, "123", 0},
		{"-123", true, "123", 0},
		{"3.14", false, "314", -2},
		{"-3.14", true, "314", -2},
		{"0.001", false, "1", -3},
		{"100", false, "100", 0},
		{"1.5e10", false, "15", 9},
		{"1.234567890123456789012345678901234E+100", false, "1234567890123456789012345678901234", 67},
		{"+42", false, "42", 0},
		{"-1E-10", true, "1", -10},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			neg, digs, exp, err := parseDecFloatString(tc.in)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if neg != tc.wantNeg || string(digs) != tc.wantDigs || exp != tc.wantExp {
				t.Errorf("parse(%q) = (%v, %q, %d), want (%v, %q, %d)",
					tc.in, neg, digs, exp, tc.wantNeg, tc.wantDigs, tc.wantExp)
			}
		})
	}
}

// TestParseDecFloatStringRejectsGarbage makes sure the parser
// surfaces malformed input rather than silently returning zero.
func TestParseDecFloatStringRejectsGarbage(t *testing.T) {
	for _, in := range []string{"", "abc", "1.2.3", "1e", "1.5e", "1.5eX"} {
		t.Run(in, func(t *testing.T) {
			if _, _, _, err := parseDecFloatString(in); err == nil {
				t.Errorf("expected error for %q, got nil", in)
			}
		})
	}
}

// suppress unused import warning if other helpers vanish during
// future trims; keep the strings import live deliberately.
var _ = strings.Repeat
