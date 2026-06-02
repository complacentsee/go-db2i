package hostserver

import (
	"errors"
	"strings"
	"testing"
)

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
		{name: "int_too_big", precision: 5, scale: 2, value: "9999.99"}, // 4 int digits > 3 allowed
		{name: "frac_too_long", precision: 5, scale: 2, value: "1.234"}, // 3 frac digits > 2 allowed
		{name: "non_digit", precision: 5, scale: 2, value: "12a.34"},    // bad char
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := encodePackedBCD(tc.value, tc.precision, tc.scale); err == nil {
				t.Errorf("expected error for %q DECIMAL(%d,%d), got nil", tc.value, tc.precision, tc.scale)
			}
		})
	}
}

// TestToDecimalStringFloat64NoScientific pins the #22 fix: a float64
// bound into a DECIMAL/NUMERIC column must serialise in plain decimal
// form across magnitudes, never the %g scientific notation (1e+06,
// 1e-05, ...) that the packed/zoned BCD encoders reject as non-digit
// input. Pre-fix this failed for every magnitude that %g rendered with
// an exponent.
func TestToDecimalStringFloat64NoScientific(t *testing.T) {
	cases := []struct {
		name string
		in   float64
		want string
	}{
		{name: "million", in: 1e6, want: "1000000"},
		{name: "ten_million", in: 12345678, want: "12345678"},
		{name: "small_fraction", in: 1e-5, want: "0.00001"},
		{name: "negative_large", in: -2.5e8, want: "-250000000"},
		{name: "mixed", in: 1234.5, want: "1234.5"},
		{name: "tiny", in: 0.0001, want: "0.0001"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toDecimalString(tc.in)
			if err != nil {
				t.Fatalf("toDecimalString(%v): %v", tc.in, err)
			}
			if strings.ContainsAny(got, "eE") {
				t.Fatalf("toDecimalString(%v) = %q contains exponent notation", tc.in, got)
			}
			if got != tc.want {
				t.Errorf("toDecimalString(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestEncodeCachedFloat64Decimal exercises the cache-hit code path
// end to end: on a cache hit the *PGM-stored shape is the native
// DECIMAL type (484/485), so a bound float64 reaches the packed-BCD
// encoder via toDecimalString. Before #22 the %g rendering of large
// magnitudes (1e+06) made encodePackedBCD reject the value; here we
// confirm the encoded CP 0x381F payload's packed field round-trips
// back to the expected decimal string.
func TestEncodeCachedFloat64Decimal(t *testing.T) {
	const (
		precision = 10
		scale     = 2
	)
	// DECIMAL(10,2) packs to ceil((10+1)/2) = 6 bytes.
	fieldLen := uint32((precision + 1 + 1) / 2)
	shapes := []PreparedParam{{
		SQLType:     484, // DECIMAL (NN)
		FieldLength: fieldLen,
		Precision:   precision,
		Scale:       scale,
	}}
	cases := []struct {
		name string
		in   float64
		want string
	}{
		{name: "million", in: 1e6, want: "1000000.00"},
		{name: "with_fraction", in: 1234.5, want: "1234.50"},
		{name: "negative", in: -9999.99, want: "-9999.99"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := EncodeDBExtendedData(shapes, []any{tc.in})
			if err != nil {
				t.Fatalf("EncodeDBExtendedData(%v): %v", tc.in, err)
			}
			// header (20) + indicator (1 col * 2) + data (fieldLen).
			dataOff := 20 + 2
			packed := payload[dataOff : dataOff+int(fieldLen)]
			got, err := decodePackedBCD(packed, precision, scale)
			if err != nil {
				t.Fatalf("decodePackedBCD: %v", err)
			}
			if got != tc.want {
				t.Errorf("round-trip = %q, want %q (packed = %X)", got, tc.want, packed)
			}
		})
	}
}

// TestEncodeCachedDecimalOverflowFallsBack confirms the #22 fallback
// routing: a value that won't fit the column shape (magnitude or scale
// overflow) surfaces an error wrapping ErrUnsupportedCachedParamType,
// so cache-hit dispatch falls back to the DOUBLE PREPARE_DESCRIBE path
// instead of propagating a hard failure to the caller.
func TestEncodeCachedDecimalOverflowFallsBack(t *testing.T) {
	cases := []struct {
		name    string
		sqlType uint16
	}{
		{name: "decimal", sqlType: 484},
		{name: "numeric", sqlType: 488},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// DECIMAL/NUMERIC(5,2): 3 integer digits max. 1e6 overflows.
			shapes := []PreparedParam{{
				SQLType:     tc.sqlType,
				FieldLength: fieldLenFor(tc.sqlType, 5),
				Precision:   5,
				Scale:       2,
			}}
			_, err := EncodeDBExtendedData(shapes, []any{1e6})
			if err == nil {
				t.Fatalf("expected overflow error, got nil")
			}
			if !errors.Is(err, ErrUnsupportedCachedParamType) {
				t.Errorf("error %v does not wrap ErrUnsupportedCachedParamType", err)
			}
		})
	}
}

// fieldLenFor returns the on-wire FieldLength for a DECIMAL (484/485)
// or NUMERIC (488/489) column of the given precision: packed BCD is
// ceil((precision+1)/2) bytes, zoned is one byte per digit.
func fieldLenFor(sqlType uint16, precision int) uint32 {
	switch sqlType {
	case 484, 485:
		return uint32((precision + 1 + 1) / 2)
	default: // 488, 489 zoned
		return uint32(precision)
	}
}
