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
