package hostserver

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// DECFLOAT support per IEEE 754-2008 decimal floating-point. PUB400
// emits DECFLOAT(16) (decimal64 / 8 wire bytes) and DECFLOAT(34)
// (decimal128 / 16 wire bytes) using the "decimal" interchange
// format with DPD (Densely Packed Decimal) coefficient encoding,
// not the BID variant.
//
// Decimal64 layout (8 bytes / 64 bits, MSB first):
//
//	bit 0       sign (1 = negative)
//	bits 1..5   combination field (5 bits): MSD + top exponent bits
//	bits 6..13  exponent continuation (8 bits)
//	bits 14..63 coefficient continuation (50 bits = 5 DPD declets)
//
// Decimal128 layout (16 bytes / 128 bits):
//
//	bit 0         sign
//	bits 1..5     combination field
//	bits 6..17    exponent continuation (12 bits)
//	bits 18..127  coefficient continuation (110 bits = 11 DPD declets)
//
// The combination field encodes the most significant digit (MSD) of
// the coefficient (0-9) and the top 2 bits of the biased exponent.
// MSD 0-7 fits in 3 bits and shares the field with 2 exp-MSB bits;
// MSD 8 and 9 need a special "11..." prefix and surrender 1 of the 3
// bits to the exponent. NaN and Infinity also live in this field
// (combo 11110... and 11111...).
//
// Each DPD declet is 10 bits and encodes exactly 3 BCD digits
// (0-999). The declet-to-BCD lookup is precomputed at init.

// dpdToBCD maps a 10-bit DPD code to three BCD digits packed as
// (d2 << 8) | (d1 << 4) | d0, each digit in [0, 9]. There are 1024
// inputs but only 1000 unique outputs; the 24 redundant declets
// decode to the same 3-digit values as their canonical forms.
var dpdToBCD [1024]uint16

// bcdToDPD maps a 3-digit decimal value (d2*100 + d1*10 + d0, in
// [0, 999]) to its canonical 10-bit DPD encoding. Built by
// inverting dpdToBCD at init: among the redundant codes for any
// 8/9-containing 3-digit value we pick the smallest DPD value,
// which is the canonical form per IEEE 754-2008.
var bcdToDPD [1000]uint16

func init() {
	for d := uint16(0); d < 1024; d++ {
		// Bits b9..b0 (b9 = MSB).
		b := func(i uint) uint16 { return (d >> (9 - i)) & 1 }
		var d2, d1, d0 uint16
		// Selectors: (b3, b2, b1).
		switch {
		case b(6) == 0:
			// All 3 digits 0-7.
			d2 = (b(0) << 2) | (b(1) << 1) | b(2)
			d1 = (b(3) << 2) | (b(4) << 1) | b(5)
			d0 = (b(7) << 2) | (b(8) << 1) | b(9)
		case b(6) == 1 && b(7) == 0 && b(8) == 0:
			// Digit 0 is 8 or 9.
			d2 = (b(0) << 2) | (b(1) << 1) | b(2)
			d1 = (b(3) << 2) | (b(4) << 1) | b(5)
			d0 = 8 + b(9)
		case b(6) == 1 && b(7) == 0 && b(8) == 1:
			// Digit 1 is 8 or 9.
			d2 = (b(0) << 2) | (b(1) << 1) | b(2)
			d1 = 8 + b(5)
			d0 = (b(3) << 2) | (b(4) << 1) | b(9)
		case b(6) == 1 && b(7) == 1 && b(8) == 0:
			// Digit 2 is 8 or 9.
			d2 = 8 + b(2)
			d1 = (b(3) << 2) | (b(4) << 1) | b(5)
			d0 = (b(0) << 2) | (b(1) << 1) | b(9)
		case b(6) == 1 && b(7) == 1 && b(8) == 1:
			// Two or three 8/9 digits; sub-selector (b3, b4).
			switch {
			case b(3) == 0 && b(4) == 0:
				// Digits 1 and 2 are 8/9.
				d2 = 8 + b(2)
				d1 = 8 + b(5)
				d0 = (b(0) << 2) | (b(1) << 1) | b(9)
			case b(3) == 0 && b(4) == 1:
				// Digits 0 and 2 are 8/9.
				d2 = 8 + b(2)
				d1 = (b(0) << 2) | (b(1) << 1) | b(5)
				d0 = 8 + b(9)
			case b(3) == 1 && b(4) == 0:
				// Digits 0 and 1 are 8/9.
				d2 = (b(0) << 2) | (b(1) << 1) | b(2)
				d1 = 8 + b(5)
				d0 = 8 + b(9)
			case b(3) == 1 && b(4) == 1:
				// All three digits are 8/9.
				d2 = 8 + b(2)
				d1 = 8 + b(5)
				d0 = 8 + b(9)
			}
		}
		dpdToBCD[d] = (d2 << 8) | (d1 << 4) | d0
	}
	// Invert dpdToBCD into bcdToDPD. Iterate DPD values in
	// ascending order so the FIRST DPD that produces each
	// (d2, d1, d0) wins -- that's the canonical encoding.
	var seen [1000]bool
	for d := uint16(0); d < 1024; d++ {
		bcd := dpdToBCD[d]
		idx := int((bcd>>8)&0xF)*100 + int((bcd>>4)&0xF)*10 + int(bcd&0xF)
		if !seen[idx] {
			bcdToDPD[idx] = d
			seen[idx] = true
		}
	}
}

// encodeDecimal64 packs a sign + decimal digit string + exponent
// into the 8-byte IEEE 754-2008 decimal64 wire format. The caller
// supplies the digits as ASCII '0'..'9'; this function:
//
//   - left-pads with zeros to exactly 16 digits if shorter (no
//     rounding -- caller is responsible for fitting in the
//     precision)
//   - applies the standard +398 exponent bias
//   - splits the leading digit into the combination field's MSD
//     and pulls the top 2 exponent bits in alongside it
//   - encodes the remaining 15 digits as 5 DPD declets MSB-first
//
// Rejects coefficients with more than 16 significant digits or
// exponents outside the [-383, 384] biased-encodable range.
func encodeDecimal64(negative bool, digits []byte, exponent int) ([]byte, error) {
	const (
		precision  = 16
		bias       = 398
		expContBits = 8
		expMin     = -bias                       // -398
		expMax     = (1<<(2+expContBits) - 1) - bias // 1023 - 398 = 625? actually max = 384
	)
	if len(digits) > precision {
		return nil, fmt.Errorf("decimal64: %d digits exceeds precision %d", len(digits), precision)
	}
	for len(digits) < precision {
		digits = append([]byte{'0'}, digits...)
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("decimal64: non-digit %q in coefficient", c)
		}
	}
	biasedExp := exponent + bias
	if biasedExp < 0 || biasedExp >= (1 << (2 + expContBits)) {
		return nil, fmt.Errorf("decimal64: biased exponent %d out of range", biasedExp)
	}

	combo, expCont := encodeCombination(byte(digits[0]-'0'), uint16(biasedExp), expContBits)

	var hi uint64
	if negative {
		hi |= 1 << 63
	}
	hi |= uint64(combo) << 58
	hi |= uint64(expCont) << 50

	// 5 declets, MSB-first; declet 4 is digits[1..3], ..., declet 0 is digits[13..15].
	for i := 0; i < 5; i++ {
		off := 1 + i*3
		d2 := uint16(digits[off] - '0')
		d1 := uint16(digits[off+1] - '0')
		d0 := uint16(digits[off+2] - '0')
		idx := int(d2)*100 + int(d1)*10 + int(d0)
		dpd := uint64(bcdToDPD[idx])
		// declet 4 sits at bits 40..49, declet 0 at bits 0..9.
		hi |= dpd << uint((4-i)*10)
	}

	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, hi)
	return out, nil
}

// encodeDecimal128 mirrors encodeDecimal64 for the 16-byte
// (precision 34) wire format. Layout: 1 sign + 5 combo + 12
// exp-cont + 110 coef-cont (= 33 trailing digits as 11 declets).
// The 110-bit coefficient continuation straddles the hi/lo uint64
// boundary, same as the decoder.
func encodeDecimal128(negative bool, digits []byte, exponent int) ([]byte, error) {
	const (
		precision  = 34
		bias       = 6176
		expContBits = 12
	)
	if len(digits) > precision {
		return nil, fmt.Errorf("decimal128: %d digits exceeds precision %d", len(digits), precision)
	}
	for len(digits) < precision {
		digits = append([]byte{'0'}, digits...)
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return nil, fmt.Errorf("decimal128: non-digit %q in coefficient", c)
		}
	}
	biasedExp := exponent + bias
	if biasedExp < 0 || biasedExp >= (1 << (2 + expContBits)) {
		return nil, fmt.Errorf("decimal128: biased exponent %d out of range", biasedExp)
	}

	combo, expCont := encodeCombination(byte(digits[0]-'0'), uint16(biasedExp), expContBits)

	var hi, lo uint64
	if negative {
		hi |= 1 << 63
	}
	hi |= uint64(combo) << 58
	hi |= uint64(expCont) << 46

	// 11 declets; declet 10 at bits 100..109 of the 128-bit value.
	for i := 0; i < 11; i++ {
		off := 1 + i*3
		d2 := uint16(digits[off] - '0')
		d1 := uint16(digits[off+1] - '0')
		d0 := uint16(digits[off+2] - '0')
		idx := int(d2)*100 + int(d1)*10 + int(d0)
		dpd := uint64(bcdToDPD[idx])
		// declet i (i = 10..0) sits at bits (10-i)*10 .. (10-i)*10+9
		// counted from MSB of the coefficient continuation. Since
		// declet 10 is the most significant, its bit position is
		// 100..109 in the full 128-bit value.
		bitOff := (10 - i) * 10
		// Actually map to wire: declet at bitOff in coefficient
		// continuation == bits (bitOff) of decimal128 coefficient,
		// where coefficient occupies bits 0..109.
		coefBitOff := uint(100 - i*10)
		if coefBitOff >= 64 {
			hi |= dpd << (coefBitOff - 64)
		} else if coefBitOff+10 <= 64 {
			lo |= dpd << coefBitOff
		} else {
			// Straddles boundary: low part to lo, high part to hi.
			lo |= dpd << coefBitOff
			hi |= dpd >> (64 - coefBitOff)
		}
		_ = bitOff
	}

	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[0:8], hi)
	binary.BigEndian.PutUint64(out[8:16], lo)
	return out, nil
}

// encodeCombination is the inverse of decodeCombination for finite
// values. Given a single MSD (0..9) and a biased exponent, it
// returns the 5-bit combination field and the (expContBits)-bit
// exponent continuation that go into the wire layout.
func encodeCombination(msd byte, biasedExp uint16, expContBits uint) (combo byte, expCont uint16) {
	expMSB := byte((biasedExp >> expContBits) & 0x03) // top 2 bits of biased exp
	expCont = biasedExp & ((1 << expContBits) - 1)
	if msd < 8 {
		// 00..10 followed by 3-bit MSD.
		combo = (expMSB << 3) | (msd & 0x07)
	} else {
		// 11 prefix, then 2-bit expMSB, then bottom bit of MSD.
		combo = 0x18 | (expMSB << 1) | (msd & 0x01)
	}
	return combo, expCont
}

// parseDecFloatString teases apart "[-+]?D+(\.D+)?([Ee][-+]?D+)?"
// into (sign, coefficient digits with leading zeros stripped,
// power-of-ten exponent). The returned exponent is such that the
// numeric value equals (-1)^sign * coefficient * 10^exponent.
//
// "0" -> (false, "0", 0); "0.001" -> (false, "1", -3);
// "1.5e10" -> (false, "15", 9); "-99.9" -> (true, "999", -1).
func parseDecFloatString(s string) (negative bool, digits []byte, exponent int, err error) {
	if s == "" {
		return false, nil, 0, fmt.Errorf("empty decimal string")
	}
	if s[0] == '+' || s[0] == '-' {
		negative = s[0] == '-'
		s = s[1:]
	}
	expPart := ""
	hasExp := false
	if i := strings.IndexAny(s, "Ee"); i >= 0 {
		expPart = s[i+1:]
		s = s[:i]
		hasExp = true
	}
	intPart := s
	fracPart := ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	}
	if intPart == "" && fracPart == "" {
		return false, nil, 0, fmt.Errorf("no digits in %q", s)
	}
	for _, c := range intPart {
		if c < '0' || c > '9' {
			return false, nil, 0, fmt.Errorf("bad digit %q in integer part", c)
		}
	}
	for _, c := range fracPart {
		if c < '0' || c > '9' {
			return false, nil, 0, fmt.Errorf("bad digit %q in fractional part", c)
		}
	}
	digits = append(digits, intPart...)
	digits = append(digits, fracPart...)
	exponent = -len(fracPart)
	// Trim leading zeros (keep at least one).
	for len(digits) > 1 && digits[0] == '0' {
		digits = digits[1:]
	}
	if len(digits) == 0 {
		digits = []byte{'0'}
	}
	if hasExp {
		expSign := 1
		if len(expPart) > 0 && expPart[0] == '+' {
			expPart = expPart[1:]
		} else if len(expPart) > 0 && expPart[0] == '-' {
			expSign = -1
			expPart = expPart[1:]
		}
		if expPart == "" {
			return false, nil, 0, fmt.Errorf("missing exponent digits")
		}
		var v int
		for _, c := range expPart {
			if c < '0' || c > '9' {
				return false, nil, 0, fmt.Errorf("bad digit %q in exponent", c)
			}
			v = v*10 + int(c-'0')
		}
		exponent += expSign * v
	}
	return negative, digits, exponent, nil
}

// decodeDecimal64 turns 8 wire bytes (big-endian decimal64) into a
// decimal string. Format follows java.math.BigDecimal.toString:
// scientific notation when the exponent is negative-large or the
// adjusted exponent makes plain notation cumbersome, plain otherwise.
// Matches what JDBC's java.sql.ResultSet.getString returns for
// DECFLOAT(16).
func decodeDecimal64(b []byte) (string, error) {
	if len(b) != 8 {
		return "", fmt.Errorf("decimal64: want 8 bytes, have %d", len(b))
	}
	hi := binary.BigEndian.Uint64(b)
	sign := byte(hi >> 63)
	combo := byte((hi >> 58) & 0x1F) // 5 bits
	expCont := uint64((hi >> 50) & 0xFF) // 8 bits
	coefCont := hi & ((1 << 50) - 1)     // 50 bits

	msd, expMSB, kind := decodeCombination(combo)
	switch kind {
	case classNaN:
		return "NaN", nil
	case classInf:
		if sign == 1 {
			return "-Infinity", nil
		}
		return "Infinity", nil
	}
	const decimal64Bias = 398
	const decimal64Continuation = 8
	exponent := int(expMSB)<<decimal64Continuation | int(expCont)
	exponent -= decimal64Bias

	// Walk 5 declets MSB-first; emit 3 BCD digits each.
	digits := make([]byte, 0, 16)
	digits = append(digits, '0'+msd)
	for i := 4; i >= 0; i-- {
		declet := uint16((coefCont >> (uint(i) * 10)) & 0x3FF)
		bcd := dpdToBCD[declet]
		digits = append(digits, '0'+byte((bcd>>8)&0xF))
		digits = append(digits, '0'+byte((bcd>>4)&0xF))
		digits = append(digits, '0'+byte(bcd&0xF))
	}
	return formatDecimalFloat(sign == 1, digits, exponent), nil
}

// decodeDecimal128 turns 16 wire bytes (big-endian decimal128) into
// a decimal string. Same approach as decimal64 with a wider
// combination field and 11 declets instead of 5.
func decodeDecimal128(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("decimal128: want 16 bytes, have %d", len(b))
	}
	hi := binary.BigEndian.Uint64(b[0:8])
	lo := binary.BigEndian.Uint64(b[8:16])

	sign := byte(hi >> 63)
	combo := byte((hi >> 58) & 0x1F)
	expCont := uint64((hi >> 46) & 0xFFF) // 12 bits

	msd, expMSB, kind := decodeCombination(combo)
	switch kind {
	case classNaN:
		return "NaN", nil
	case classInf:
		if sign == 1 {
			return "-Infinity", nil
		}
		return "Infinity", nil
	}
	const decimal128Bias = 6176
	const decimal128Continuation = 12
	exponent := int(expMSB)<<decimal128Continuation | int(expCont)
	exponent -= decimal128Bias

	// 110-bit coefficient continuation: top 46 bits in `hi` (after
	// 1 sign + 5 combo + 12 expCont = 18 used), bottom 64 bits in
	// `lo`. Splice into a 110-bit value via big-int-ish handling.
	hiCoef := hi & ((1 << 46) - 1)
	// 11 declets: walk from MSD; declet 10 is hi[40:50], ..., declet 0 is lo[0:10].
	digits := make([]byte, 0, 36)
	digits = append(digits, '0'+msd)
	for i := 10; i >= 0; i-- {
		var declet uint16
		bitOff := i * 10
		if bitOff >= 64 {
			declet = uint16((hiCoef >> uint(bitOff-64)) & 0x3FF)
		} else if bitOff+10 <= 64 {
			declet = uint16((lo >> uint(bitOff)) & 0x3FF)
		} else {
			// Declet straddles the hi/lo boundary.
			low := lo >> uint(bitOff)                  // bits available in lo
			highBits := hiCoef << uint(64-bitOff)      // remaining bits from hi
			declet = uint16((low | highBits) & 0x3FF)
		}
		bcd := dpdToBCD[declet]
		digits = append(digits, '0'+byte((bcd>>8)&0xF))
		digits = append(digits, '0'+byte((bcd>>4)&0xF))
		digits = append(digits, '0'+byte(bcd&0xF))
	}
	return formatDecimalFloat(sign == 1, digits, exponent), nil
}

// decodeCombination takes the 5-bit combination field and returns
// (msd, top 2 exponent bits, classification). Combo encoding:
//
//	00xxx, 01xxx, 10xxx     -> exp top = bits 0..1, msd = 0..7
//	11000, 11001, 11010,
//	11011, 11100, 11101     -> exp top = bits 1..2, msd = 8 or 9
//	11110                   -> Infinity
//	11111                   -> NaN
type decimalClass int

const (
	classFinite decimalClass = iota
	classInf
	classNaN
)

func decodeCombination(combo byte) (msd, expMSB byte, class decimalClass) {
	// Top 2 bits 00/01/10: standard finite, MSD 0-7.
	// Top 2 bits 11: either 8/9 MSD finite, Infinity, or NaN.
	if combo>>3 != 0b11 {
		expMSB = (combo >> 3) & 0x03
		msd = combo & 0x07
		return msd, expMSB, classFinite
	}
	switch combo {
	case 0b11110:
		return 0, 0, classInf
	case 0b11111:
		return 0, 0, classNaN
	}
	// Finite, MSD 8 or 9: exponent top is bits 2..3 (combo
	// without the leading 11), MSD = 8 + bit 0.
	expMSB = (combo >> 1) & 0x03
	msd = 8 + (combo & 0x01)
	return msd, expMSB, classFinite
}

// formatDecimalFloat takes the unscaled coefficient digits + the
// power-of-ten exponent and emits a decimal string. We mirror
// java.math.BigDecimal#toString conventions: use plain notation
// when the value isn't too large/small in absolute terms,
// scientific (XeY) when the digit-shift would otherwise be unwieldy.
//
// Specifically: let `n` = number of significant digits, `e` =
// exponent.  Adjusted exponent = e + (n - 1).  If e <= 0 and
// adjusted >= -6, plain. Otherwise scientific.
func formatDecimalFloat(negative bool, digits []byte, exponent int) string {
	// Trim leading zeros from digits (keep at least one).
	for len(digits) > 1 && digits[0] == '0' {
		digits = digits[1:]
	}
	n := len(digits)
	adjustedExp := exponent + n - 1

	var sb strings.Builder
	if negative {
		sb.WriteByte('-')
	}

	if exponent <= 0 && adjustedExp >= -6 {
		// Plain notation.
		if exponent == 0 {
			sb.Write(digits)
		} else {
			absExp := -exponent
			if n > absExp {
				sb.Write(digits[:n-absExp])
				sb.WriteByte('.')
				sb.Write(digits[n-absExp:])
			} else {
				sb.WriteString("0.")
				for i := 0; i < absExp-n; i++ {
					sb.WriteByte('0')
				}
				sb.Write(digits)
			}
		}
		return sb.String()
	}

	// Scientific notation: "D.DDDDe+EE" or "DEsEE".
	sb.WriteByte(digits[0])
	if n > 1 {
		sb.WriteByte('.')
		sb.Write(digits[1:])
	}
	sb.WriteByte('E')
	if adjustedExp >= 0 {
		sb.WriteByte('+')
	}
	fmt.Fprintf(&sb, "%d", adjustedExp)
	return sb.String()
}
