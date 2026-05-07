package hostserver

import "testing"

// TestPackedBCDRoundTrip confirms encodePackedBCD followed by
// decodePackedBCD preserves the canonical decimal-string form for
// every (precision, scale, value) combination we currently support
// as M4 parameter binding. We pick representative cases that
// exercise the sign nibble, leading-zero pad nibble (odd precision),
// max-precision overflow boundary, and zero / fractional-only forms.
func TestPackedBCDRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		precision int
		scale     int
		value     string
		want      string
	}{
		{name: "small_positive", precision: 5, scale: 2, value: "123.45", want: "123.45"},
		{name: "small_negative", precision: 5, scale: 2, value: "-123.45", want: "-123.45"},
		{name: "zero", precision: 5, scale: 2, value: "0", want: "0.00"},
		{name: "all_fractional", precision: 5, scale: 5, value: ".12345", want: "0.12345"},
		{name: "max31_5_pos", precision: 31, scale: 5, value: "99999999999999999999999999.12345", want: "99999999999999999999999999.12345"},
		{name: "max31_5_neg", precision: 31, scale: 5, value: "-99999999999999999999999999.12345", want: "-99999999999999999999999999.12345"},
		{name: "leading_plus_trim", precision: 5, scale: 2, value: "+12.34", want: "12.34"},
		{name: "scale_zero", precision: 7, scale: 0, value: "1234567", want: "1234567"},
		{name: "odd_precision", precision: 7, scale: 2, value: "12345.67", want: "12345.67"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			packed, err := encodePackedBCD(tc.value, tc.precision, tc.scale)
			if err != nil {
				t.Fatalf("encodePackedBCD: %v", err)
			}
			expectedBytes := (tc.precision + 1 + 1) / 2
			if len(packed) != expectedBytes {
				t.Errorf("len(packed) = %d, want %d", len(packed), expectedBytes)
			}
			got, err := decodePackedBCD(packed, tc.precision, tc.scale)
			if err != nil {
				t.Fatalf("decodePackedBCD: %v", err)
			}
			if got != tc.want {
				t.Errorf("round-trip = %q, want %q (packed = %X)", got, tc.want, packed)
			}
		})
	}
}

// TestZonedBCDRoundTrip mirrors TestPackedBCDRoundTrip for the
// NUMERIC encoder (one byte per digit, sign in last byte's high
// nibble).
func TestZonedBCDRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		precision int
		scale     int
		value     string
		want      string
	}{
		{name: "small_positive", precision: 5, scale: 2, value: "123.45", want: "123.45"},
		{name: "small_negative", precision: 5, scale: 2, value: "-123.45", want: "-123.45"},
		{name: "zero", precision: 5, scale: 2, value: "0", want: "0.00"},
		{name: "max31_5_pos", precision: 31, scale: 5, value: "12345678901234567890123456.78901", want: "12345678901234567890123456.78901"},
		{name: "scale_zero", precision: 7, scale: 0, value: "1234567", want: "1234567"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			zoned, err := encodeZonedBCD(tc.value, tc.precision, tc.scale)
			if err != nil {
				t.Fatalf("encodeZonedBCD: %v", err)
			}
			if len(zoned) != tc.precision {
				t.Errorf("len(zoned) = %d, want %d", len(zoned), tc.precision)
			}
			got, err := decodeZonedBCD(zoned, tc.precision, tc.scale)
			if err != nil {
				t.Fatalf("decodeZonedBCD: %v", err)
			}
			if got != tc.want {
				t.Errorf("round-trip = %q, want %q (zoned = %X)", got, tc.want, zoned)
			}
		})
	}
}

// TestPackedBCDRejectsOverflow makes sure we surface an explicit
// error when a caller hands us a value that doesn't fit the column
// shape, rather than silently truncating.
func TestPackedBCDRejectsOverflow(t *testing.T) {
	cases := []struct {
		name      string
		precision int
		scale     int
		value     string
	}{
		{name: "int_too_big", precision: 5, scale: 2, value: "9999.99"},   // 4 int digits > 3 allowed
		{name: "frac_too_long", precision: 5, scale: 2, value: "1.234"},   // 3 frac digits > 2 allowed
		{name: "non_digit", precision: 5, scale: 2, value: "12a.34"},      // bad char
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := encodePackedBCD(tc.value, tc.precision, tc.scale); err == nil {
				t.Errorf("expected error for %q DECIMAL(%d,%d), got nil", tc.value, tc.precision, tc.scale)
			}
		})
	}
}
