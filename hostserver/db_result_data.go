package hostserver

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

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

	case SQLTypeChar, SQLTypeCharNonBlank:
		// Fixed-length CHAR. EBCDIC.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("char wants %d bytes, have %d", col.Length, len(b))
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
		// column (no padding to col.Length).
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
