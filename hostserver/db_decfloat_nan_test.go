package hostserver

import (
	"encoding/binary"
	"testing"
)

// Offline tests for issue #39 item 5: DECFLOAT NaN / SNaN decode must
// preserve the sign bit ("-NaN" for negative) and distinguish
// signaling ("SNaN"/"-SNaN") from quiet NaN, matching JT400's
// AS400DecFloat.toObject. The combination field 11111 marks NaN; the
// bit right after it (bit 57 of the hi word, the MSB of the exponent
// continuation = DEC_FLOAT_*_SIGNAL_MASK) is 1 for signaling. Infinity
// (combination 11110) already carried its sign; these tests pin that
// too as a regression guard.

// decFloat64Special builds the 8 wire bytes for a decimal64 special
// value: sign bit (bit 63), the 5-bit combination field (bits 58..62),
// and the signaling bit (bit 57). All coefficient/exponent bits stay
// zero -- per IEEE 754 they're unused for Inf/NaN.
func decFloat64Special(negative bool, combo byte, signaling bool) []byte {
	var hi uint64
	if negative {
		hi |= 1 << 63
	}
	hi |= uint64(combo&0x1F) << 58
	if signaling {
		hi |= 1 << 57
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, hi)
	return out
}

// decFloat128Special is the decimal128 analogue. Combination and
// signal bits sit at the same positions in the hi word; the lo word
// stays zero.
func decFloat128Special(negative bool, combo byte, signaling bool) []byte {
	var hi uint64
	if negative {
		hi |= 1 << 63
	}
	hi |= uint64(combo&0x1F) << 58
	if signaling {
		hi |= 1 << 57
	}
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[0:8], hi)
	// out[8:16] left zero.
	return out
}

func TestDecodeDecFloatNaNAndInfinity(t *testing.T) {
	const (
		comboNaN = 0x1F // 11111
		comboInf = 0x1E // 11110
	)
	cases := []struct {
		name      string
		negative  bool
		combo     byte
		signaling bool
		want      string
	}{
		{"qNaN", false, comboNaN, false, "NaN"},
		{"neg_qNaN", true, comboNaN, false, "-NaN"},
		{"sNaN", false, comboNaN, true, "SNaN"},
		{"neg_sNaN", true, comboNaN, true, "-SNaN"},
		{"pos_Inf", false, comboInf, false, "Infinity"},
		{"neg_Inf", true, comboInf, false, "-Infinity"},
	}
	for _, tc := range cases {
		t.Run("d64_"+tc.name, func(t *testing.T) {
			got, err := decodeDecimal64(decFloat64Special(tc.negative, tc.combo, tc.signaling))
			if err != nil {
				t.Fatalf("decodeDecimal64: %v", err)
			}
			if got != tc.want {
				t.Errorf("decodeDecimal64 = %q, want %q", got, tc.want)
			}
		})
		t.Run("d128_"+tc.name, func(t *testing.T) {
			got, err := decodeDecimal128(decFloat128Special(tc.negative, tc.combo, tc.signaling))
			if err != nil {
				t.Fatalf("decodeDecimal128: %v", err)
			}
			if got != tc.want {
				t.Errorf("decodeDecimal128 = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNanString pins the four-way sign/signaling fan-out in isolation.
func TestNanString(t *testing.T) {
	cases := []struct {
		negative  bool
		signaling bool
		want      string
	}{
		{false, false, "NaN"},
		{true, false, "-NaN"},
		{false, true, "SNaN"},
		{true, true, "-SNaN"},
	}
	for _, tc := range cases {
		if got := nanString(tc.negative, tc.signaling); got != tc.want {
			t.Errorf("nanString(%v, %v) = %q, want %q", tc.negative, tc.signaling, got, tc.want)
		}
	}
}
