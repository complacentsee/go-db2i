package hostserver

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// ccsidBinary is the IBM i sentinel CCSID for "no conversion /
// binary" -- the FOR BIT DATA flag on CHAR/VARCHAR columns. Columns
// with this CCSID are returned as []byte without EBCDIC decoding;
// passing them through CCSID 37 would silently corrupt arbitrary
// binary content (anything outside the 256-char EBCDIC table maps
// to U+FFFD or worse).
const ccsidBinary uint16 = 65535

// findExtendedResultData locates CP 0x380E in the reply and decodes
// the rows it carries against the column descriptors in cols. The
// fixed result-data layout (mirroring DBExtendedData) is:
//
//	0..3   consistency token
//	4..7   row count
//	8..9   column count
//	10..11 indicator size (0 or 2; 2 means each column has a NULL
//	       indicator short)
//	12..15 reserved (or "use VLF compression" flag in some traces)
//	16..19 row size (uncompressed bytes per row)
//	20..   indicators (col_count * row_count * indicator_size bytes)
//
// After indicators, the layout depends on whether VLF (variable-
// length-field) compression is on. Both PUB400 and the captured
// fixture turn it on (we requested 0x3833=0xE8 in the OPEN
// request), so we always parse the VLF-compressed form:
//
//	row info header:   row-info-array-offset (uint32 BE)
//	                  + rows-fetched          (uint32 BE)
//	rows:             concatenated, each at the offset given by
//	                  the row info array
//	row info array:    rowCount * uint32 BE offset, relative to the
//	                  row-info-header start.
//
// Returns nil rows (not an error) if CP 0x380E is absent -- some
// reply flavours (e.g. PREPARE_DESCRIBE without OPEN) carry only
// the format CP, no row data.
func (r *DBReply) findExtendedResultData(cols []SelectColumn) ([]SelectRow, error) {
	for _, p := range r.Params {
		if p.CodePoint == 0x380E {
			return parseExtendedResultData(p.Data, cols)
		}
	}
	return nil, nil
}

func parseExtendedResultData(data []byte, cols []SelectColumn) ([]SelectRow, error) {
	const fixedLen = 20
	if len(data) < fixedLen {
		return nil, fmt.Errorf("hostserver: extended-result-data too short: %d bytes", len(data))
	}
	be := binary.BigEndian
	rowCount := int(be.Uint32(data[4:8]))
	colCount := int(be.Uint16(data[8:10]))
	indicatorSize := int(be.Uint16(data[10:12]))
	rowSize := int(be.Uint32(data[16:20]))
	// Bytes 12..15 are reserved/compression-flag in JTOpen but
	// PUB400 leaves them zero in both VLF and non-VLF replies, so
	// we detect format by length instead.

	if colCount != len(cols) {
		return nil, fmt.Errorf("hostserver: result data column count %d != format column count %d", colCount, len(cols))
	}
	if rowCount == 0 {
		return nil, nil
	}

	indicatorBytes := indicatorSize * colCount * rowCount
	if fixedLen+indicatorBytes > len(data) {
		return nil, fmt.Errorf("hostserver: indicators (%d bytes) past end of result data (%d bytes)", indicatorBytes, len(data))
	}
	indicators := data[fixedLen : fixedLen+indicatorBytes]

	// Detect VLF vs non-VLF by total size. Non-VLF stores rows
	// concatenated immediately after indicators with each row
	// taking exactly rowSize bytes; VLF additionally inserts a
	// row-info header (8 bytes) and a row-info array (4 bytes per
	// row) so rows can be variable-length packed. PUB400 picks
	// non-VLF for fixed-width single-row results (e.g. SELECT of
	// a single INTEGER) and VLF when any column is variable-length.
	expectedNonVLF := fixedLen + indicatorBytes + rowSize*rowCount
	if len(data) == expectedNonVLF {
		return parseNonVLF(data[fixedLen+indicatorBytes:], cols, indicators, indicatorSize, rowSize, rowCount)
	}

	// VLF path.
	rowInfoHeaderStart := fixedLen + indicatorBytes
	if rowInfoHeaderStart+8 > len(data) {
		return nil, fmt.Errorf("hostserver: row info header overruns result data (have %d, need %d for VLF; non-VLF expected %d)",
			len(data), rowInfoHeaderStart+8, expectedNonVLF)
	}
	rowInfoArrayOffset := int(be.Uint32(data[rowInfoHeaderStart : rowInfoHeaderStart+4]))

	rows := make([]SelectRow, rowCount)
	for i := 0; i < rowCount; i++ {
		offEntry := rowInfoHeaderStart + rowInfoArrayOffset + i*4
		if offEntry+4 > len(data) {
			return nil, fmt.Errorf("hostserver: row info array entry %d overruns result data", i)
		}
		rowOff := rowInfoHeaderStart + int(be.Uint32(data[offEntry:offEntry+4]))
		row, _, err := decodeRow(data[rowOff:], cols, indicators[i*colCount*indicatorSize:(i+1)*colCount*indicatorSize], indicatorSize)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		rows[i] = row
	}
	return rows, nil
}

// parseNonVLF decodes the plain (no row info header / array) result
// data layout PUB400 ships when the row size is fixed and small.
// rowsBytes points at the start of the row data block (right after
// indicators); each row consumes exactly rowSize bytes.
func parseNonVLF(rowsBytes []byte, cols []SelectColumn, indicators []byte, indicatorSize, rowSize, rowCount int) ([]SelectRow, error) {
	rows := make([]SelectRow, rowCount)
	for i := 0; i < rowCount; i++ {
		start := i * rowSize
		end := start + rowSize
		if end > len(rowsBytes) {
			return nil, fmt.Errorf("non-VLF row %d (offset %d..%d) overruns row block (%d bytes)", i, start, end, len(rowsBytes))
		}
		colCount := len(cols)
		row, _, err := decodeRow(rowsBytes[start:end], cols, indicators[i*colCount*indicatorSize:(i+1)*colCount*indicatorSize], indicatorSize)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		rows[i] = row
	}
	return rows, nil
}

// decodeRow walks the column descriptors in order, slicing bytes
// off the start of rowBytes per column. Returns the populated row
// plus the number of bytes consumed (so the caller can advance to
// the next row in non-VLF layouts).
func decodeRow(rowBytes []byte, cols []SelectColumn, indicators []byte, indicatorSize int) (SelectRow, int, error) {
	row := make(SelectRow, len(cols))
	off := 0
	be := binary.BigEndian
	for i, col := range cols {
		// NULL indicator -- if non-zero, the column value is NULL.
		// JTOpen treats indicator -1 (0xFFFF) as null; some servers
		// use -2 (default). Anything non-zero we treat as null.
		isNull := false
		if indicatorSize > 0 {
			ind := be.Uint16(indicators[i*indicatorSize : (i+1)*indicatorSize])
			isNull = (ind != 0)
		}
		if isNull {
			row[i] = nil
			off += int(col.wireLength())
			continue
		}
		val, n, err := decodeColumn(rowBytes[off:], col)
		if err != nil {
			return nil, 0, fmt.Errorf("column %d (%q, sql_type=%d): %w", i, col.Name, col.SQLType, err)
		}
		row[i] = val
		off += n
	}
	return row, off, nil
}

// wireLength returns the column's max wire length per its SQL type.
// For most types this matches col.Length; for VARCHAR it adds the
// 2-byte length prefix.
func (c SelectColumn) wireLength() uint32 {
	switch c.SQLType {
	case SQLTypeVarChar, SQLTypeVarCharNonBlank, 449:
		return c.Length + 2
	}
	return c.Length
}

// decodeColumn returns the decoded value and the number of wire
// bytes it consumed. For variable-length types (VARCHAR), the
// "consumed" count is the length declared on the wire (i.e. it
// matches what a non-VLF row layout would store for that column);
// in the VLF-compressed result-data path the actual stored bytes
// can be shorter than that, but we always advance by the declared
// width because the next column's offset is computed from it.
func decodeColumn(b []byte, col SelectColumn) (any, int, error) {
	switch col.SQLType {

	case SQLTypeTimestamp, SQLTypeTimestampNN:
		// 26 EBCDIC bytes in wire format: "YYYY-MM-DD-HH.MM.SS.ffffff"
		// We translate to ISO 8601 ("YYYY-MM-DDTHH:MM:SS.ffffff")
		// for golden-file equality with JTOpen's java.sql.Timestamp
		// .toString().
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("timestamp wants %d bytes, have %d", col.Length, len(b))
		}
		s, err := ebcdic.CCSID37.Decode(b[:col.Length])
		if err != nil {
			return nil, 0, fmt.Errorf("decode timestamp ebcdic: %w", err)
		}
		return ibmTimestampToISO(s), int(col.Length), nil

	case SQLTypeDate, SQLTypeDateNN:
		// PUB400 default date format is YMD ("YY-MM-DD" = 8 chars);
		// JDBC always returns ISO ("YYYY-MM-DD"). Translate
		// inline using the 1940 century boundary JTOpen uses
		// (YY 00..39 -> 20YY, 40..99 -> 19YY). When we wire up
		// SET_SQL_ATTRIBUTES date-format negotiation in M5, this
		// fall-through will land iff the server still picks YMD.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("date wants %d bytes, have %d", col.Length, len(b))
		}
		s, err := ebcdic.CCSID37.Decode(b[:col.Length])
		if err != nil {
			return nil, 0, fmt.Errorf("decode date ebcdic: %w", err)
		}
		return ymdToISODate(s), int(col.Length), nil

	case SQLTypeTime, SQLTypeTimeNN:
		// ISO time format on the wire: "HH:MM:SS" (8 EBCDIC chars).
		// IBM-format ("HH.MM.SS") shows up if the connection asked
		// for it via SET_SQL_ATTRIBUTES; not currently exposed.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("time wants %d bytes, have %d", col.Length, len(b))
		}
		s, err := ebcdic.CCSID37.Decode(b[:col.Length])
		if err != nil {
			return nil, 0, fmt.Errorf("decode time ebcdic: %w", err)
		}
		return s, int(col.Length), nil

	case SQLTypeChar, SQLTypeCharNonBlank:
		// Fixed-length CHAR. Normally EBCDIC; CCSID 65535 means
		// "no conversion / binary" (the IBM i FOR BIT DATA path)
		// and the column's bytes are returned as []byte verbatim.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("char wants %d bytes, have %d", col.Length, len(b))
		}
		if col.CCSID == ccsidBinary {
			out := make([]byte, col.Length)
			copy(out, b[:col.Length])
			return out, int(col.Length), nil
		}
		s, err := ebcdic.CCSID37.Decode(b[:col.Length])
		if err != nil {
			return nil, 0, fmt.Errorf("decode char ebcdic: %w", err)
		}
		return s, int(col.Length), nil

	case SQLTypeVarChar, SQLTypeVarCharNonBlank, 449:
		// 2-byte BE length prefix followed by N bytes EBCDIC.
		// The slot occupies col.Length+2 bytes on the wire (in
		// non-VLF layouts), but in VLF-compressed result data
		// the row only contains 2+actual-length bytes for this
		// column (no padding to col.Length). CCSID 65535 routes
		// the payload bytes to []byte without EBCDIC decode --
		// see the CHAR case above for the FOR BIT DATA rationale.
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("varchar header wants 2 bytes, have %d", len(b))
		}
		n := int(binary.BigEndian.Uint16(b[:2]))
		if n > int(col.Length) {
			return nil, 0, fmt.Errorf("varchar declared length %d exceeds column max %d", n, col.Length)
		}
		if len(b) < 2+n {
			return nil, 0, fmt.Errorf("varchar wants %d bytes (header+data), have %d", 2+n, len(b))
		}
		if col.CCSID == ccsidBinary {
			out := make([]byte, n)
			copy(out, b[2:2+n])
			return out, 2 + n, nil
		}
		s, err := ebcdic.CCSID37.Decode(b[2 : 2+n])
		if err != nil {
			return nil, 0, fmt.Errorf("decode varchar ebcdic: %w", err)
		}
		return s, 2 + n, nil

	case SQLTypeInteger, 497: // 496 NN, 497 nullable
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("integer wants 4 bytes, have %d", len(b))
		}
		return int32(binary.BigEndian.Uint32(b[:4])), 4, nil

	case SQLTypeSmallInt, 501: // 500 NN, 501 nullable
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("smallint wants 2 bytes, have %d", len(b))
		}
		return int16(binary.BigEndian.Uint16(b[:2])), 2, nil

	case SQLTypeBigInt, 493: // 492 NN, 493 nullable
		if len(b) < 8 {
			return nil, 0, fmt.Errorf("bigint wants 8 bytes, have %d", len(b))
		}
		return int64(binary.BigEndian.Uint64(b[:8])), 8, nil

	case 996, 997: // DECFLOAT -- type 996/997 covers BOTH precision-16
		// (decimal64, 8 bytes) and precision-34 (decimal128, 16
		// bytes). JT400 distinguishes by column Length, not by
		// SQL type code.
		switch col.Length {
		case 8:
			s, err := decodeDecimal64(b[:8])
			if err != nil {
				return nil, 0, err
			}
			return s, 8, nil
		case 16:
			s, err := decodeDecimal128(b[:16])
			if err != nil {
				return nil, 0, err
			}
			return s, 16, nil
		default:
			return nil, 0, fmt.Errorf("decfloat: unexpected column length %d (want 8 or 16)", col.Length)
		}

	case SQLTypeNumeric, 489: // 488 NN, 489 nullable -- NUMERIC(p, s) zoned decimal
		// Zoned BCD: one byte per digit; high nibble is zone (0xF
		// for plain digit), low nibble is the digit 0-9. The
		// last byte's high nibble carries the sign: 0xC = +,
		// 0xD = -, 0xF = unsigned. Bytes per value = precision.
		nbytes := int(col.Precision)
		if len(b) < nbytes {
			return nil, 0, fmt.Errorf("numeric(%d,%d) wants %d bytes, have %d", col.Precision, col.Scale, nbytes, len(b))
		}
		s, err := decodeZonedBCD(b[:nbytes], int(col.Precision), int(col.Scale))
		if err != nil {
			return nil, 0, err
		}
		return s, nbytes, nil

	case SQLTypeDecimal, 485: // 484 NN, 485 nullable -- DECIMAL(p, s) packed BCD
		// Packed BCD: ceil((precision+1)/2) bytes; each byte
		// holds two BCD digits (high then low nibble); the
		// final nibble is sign (0xC/0xF = positive, 0xD =
		// negative). We return a decimal string ("-123.45")
		// because DECIMAL(31,5) overflows int64/float64; the
		// caller can lift to math/big or shopspring/decimal.
		nbytes := int(col.Precision+1) / 2
		if (int(col.Precision)+1)%2 != 0 {
			nbytes = (int(col.Precision) + 2) / 2
		}
		if len(b) < nbytes {
			return nil, 0, fmt.Errorf("decimal(%d,%d) wants %d bytes, have %d", col.Precision, col.Scale, nbytes, len(b))
		}
		s, err := decodePackedBCD(b[:nbytes], int(col.Precision), int(col.Scale))
		if err != nil {
			return nil, 0, err
		}
		return s, nbytes, nil

	case SQLTypeFloat8, 481: // 480 NN (REAL or DOUBLE), 481 nullable
		// IEEE 754 big-endian; REAL is 4 bytes (float32),
		// DOUBLE is 8 bytes (float64). The SQL type is the same
		// (480) for both -- column length distinguishes them.
		switch col.Length {
		case 4:
			if len(b) < 4 {
				return nil, 0, fmt.Errorf("real wants 4 bytes, have %d", len(b))
			}
			bits := binary.BigEndian.Uint32(b[:4])
			return math.Float32frombits(bits), 4, nil
		case 8:
			if len(b) < 8 {
				return nil, 0, fmt.Errorf("double wants 8 bytes, have %d", len(b))
			}
			bits := binary.BigEndian.Uint64(b[:8])
			return math.Float64frombits(bits), 8, nil
		default:
			return nil, 0, fmt.Errorf("float type 480 has unexpected length %d (want 4 or 8)", col.Length)
		}
	}
	return nil, 0, fmt.Errorf("unsupported SQL type %d (col len=%d, ccsid=%d)", col.SQLType, col.Length, col.CCSID)
}

// decodePackedBCD turns DB2 for i's packed-BCD bytes into a decimal
// string ("[-]integer[.fraction]"). precision is the total digit
// count and scale is the number of fractional digits, both as
// declared by the column descriptor. The byte stream contains
// 2*len(b) nibbles; the last nibble is the sign (0xC/0xF = positive,
// 0xD = negative); the leading 2*len(b) - 1 nibbles are the digits
// in big-endian order.
func decodePackedBCD(b []byte, precision, scale int) (string, error) {
	totalNibbles := 2 * len(b)
	if totalNibbles < 2 {
		return "", fmt.Errorf("decimal: byte count %d too small", len(b))
	}
	// Sign nibble lives in the low half of the last byte.
	signNibble := b[len(b)-1] & 0x0F
	negative := false
	switch signNibble {
	case 0x0A, 0x0C, 0x0E, 0x0F: // 0xA, 0xC, 0xE, 0xF = positive (per IBM packed BCD spec)
		// positive
	case 0x0B, 0x0D: // 0xB, 0xD = negative
		negative = true
	default:
		return "", fmt.Errorf("decimal: bad sign nibble 0x%X in last byte 0x%02X", signNibble, b[len(b)-1])
	}

	// Walk all digit nibbles (everything except the sign).
	digits := make([]byte, 0, totalNibbles-1)
	for i, by := range b {
		hi := (by >> 4) & 0x0F
		lo := by & 0x0F
		// First nibble of the byte is always a digit.
		if hi > 9 {
			return "", fmt.Errorf("decimal: byte %d high nibble 0x%X > 9", i, hi)
		}
		digits = append(digits, '0'+hi)
		// Last byte's low nibble is sign, not a digit.
		if i == len(b)-1 {
			break
		}
		if lo > 9 {
			return "", fmt.Errorf("decimal: byte %d low nibble 0x%X > 9", i, lo)
		}
		digits = append(digits, '0'+lo)
	}
	// Right-trim to the declared precision in case the wire
	// happened to carry a leading zero pad (it does for
	// odd-precision DECIMALs where the high nibble of byte 0 is a
	// pad zero).
	if len(digits) > precision {
		// Strip leading pad nibble(s).
		digits = digits[len(digits)-precision:]
	}
	// Insert the decimal point if scale > 0.
	var out []byte
	if negative {
		out = append(out, '-')
	}
	if scale == 0 {
		out = append(out, trimLeadingZeros(digits)...)
	} else if scale >= len(digits) {
		// "0.00...digits" -- pad zeros after the dot.
		out = append(out, '0', '.')
		for i := 0; i < scale-len(digits); i++ {
			out = append(out, '0')
		}
		out = append(out, digits...)
	} else {
		intPart := trimLeadingZeros(digits[:len(digits)-scale])
		fracPart := digits[len(digits)-scale:]
		out = append(out, intPart...)
		out = append(out, '.')
		out = append(out, fracPart...)
	}
	return string(out), nil
}

// decodeZonedBCD turns DB2 for i's zoned-BCD bytes (NUMERIC(p, s))
// into a decimal string. One byte per digit; low nibble is the
// digit, high nibble is the zone (0xF for plain digits, 0xC/0xD/0xF
// for sign on the last byte).
func decodeZonedBCD(b []byte, precision, scale int) (string, error) {
	if len(b) != precision {
		return "", fmt.Errorf("zoned: byte count %d != precision %d", len(b), precision)
	}
	digits := make([]byte, len(b))
	for i, by := range b {
		lo := by & 0x0F
		if lo > 9 {
			return "", fmt.Errorf("zoned: byte %d low nibble 0x%X > 9", i, lo)
		}
		digits[i] = '0' + lo
	}
	negative := false
	switch (b[len(b)-1] >> 4) & 0x0F {
	case 0x0A, 0x0C, 0x0E, 0x0F:
		// positive / unsigned
	case 0x0B, 0x0D:
		negative = true
	default:
		return "", fmt.Errorf("zoned: bad sign nibble in last byte 0x%02X", b[len(b)-1])
	}

	var out []byte
	if negative {
		out = append(out, '-')
	}
	if scale == 0 {
		out = append(out, trimLeadingZeros(digits)...)
	} else if scale >= len(digits) {
		out = append(out, '0', '.')
		for i := 0; i < scale-len(digits); i++ {
			out = append(out, '0')
		}
		out = append(out, digits...)
	} else {
		intPart := trimLeadingZeros(digits[:len(digits)-scale])
		fracPart := digits[len(digits)-scale:]
		out = append(out, intPart...)
		out = append(out, '.')
		out = append(out, fracPart...)
	}
	return string(out), nil
}

// trimLeadingZeros strips leading '0' bytes from b but keeps at
// least one digit (so "0000" -> "0", "0123" -> "123"). Used to
// undo the precision-pad zeros decodePackedBCD emits before
// inserting the decimal point.
func trimLeadingZeros(b []byte) []byte {
	for len(b) > 1 && b[0] == '0' {
		b = b[1:]
	}
	return b
}

// ymdToISODate normalises whatever date format the server sent into
// ISO "YYYY-MM-DD". Recognises:
//
//	"YYYY-MM-DD"  ISO/JIS  (10 chars) -> as-is
//	"YY-MM-DD"    YMD      (8 chars)  -> "20YY-..." or "19YY-..." (1940 boundary)
//	"MM/DD/YYYY"  USA      (10 chars) -> "YYYY-MM-DD"
//	"DD.MM.YYYY"  EUR      (10 chars) -> "YYYY-MM-DD"
//	"MM/DD/YY"    MDY      (8 chars)  -> "20YY-MM-DD" or "19YY-MM-DD"
//	"DD/MM/YY"    DMY      (8 chars)  -- ambiguous with MDY; prefer MDY
//
// MDY/DMY collide on shape so we default to MDY (US convention); a
// caller that wants DMY can negotiate ISO via DBAttributesOptions
// .DateFormat = DateFormatISO and skip this function entirely. Any
// shape we don't recognise falls through unchanged.
func ymdToISODate(s string) string {
	switch {
	case len(s) == 10 && s[4] == '-' && s[7] == '-':
		return s // ISO / JIS
	case len(s) == 8 && s[2] == '-' && s[5] == '-':
		// YMD: "YY-MM-DD"
		century := "20"
		if s[0] >= '4' {
			century = "19"
		}
		return century + s
	case len(s) == 10 && s[2] == '/' && s[5] == '/':
		// USA: "MM/DD/YYYY"
		return s[6:10] + "-" + s[0:2] + "-" + s[3:5]
	case len(s) == 10 && s[2] == '.' && s[5] == '.':
		// EUR: "DD.MM.YYYY"
		return s[6:10] + "-" + s[3:5] + "-" + s[0:2]
	case len(s) == 8 && s[2] == '/' && s[5] == '/':
		// MDY (US): "MM/DD/YY"
		century := "20"
		if s[6] >= '4' {
			century = "19"
		}
		return century + s[6:8] + "-" + s[0:2] + "-" + s[3:5]
	}
	return s
}

// ibmTimestampToISO converts IBM i's wire timestamp string
// "YYYY-MM-DD-HH.MM.SS.ffffff" to ISO 8601
// "YYYY-MM-DDTHH:MM:SS.ffffff". Both forms are 26 chars; the
// only differences are the date/time delimiter ('-' -> 'T') and
// the time-component separator ('.' -> ':') in HH.MM.SS.
//
// If s doesn't look like a wire timestamp (length or character
// sentinels off), it's returned unchanged so the caller can still
// see the raw value rather than panic on bad input.
func ibmTimestampToISO(s string) string {
	if len(s) != 26 || s[10] != '-' || s[13] != '.' || s[16] != '.' || s[19] != '.' {
		return s
	}
	b := []byte(s)
	b[10] = 'T'
	b[13] = ':'
	b[16] = ':'
	// b[19] stays '.' -- that's the seconds-vs-fractional separator
	// and ISO uses '.' there.
	return string(b)
}
