package hostserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode/utf16"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// ErrUnsupportedResultType is the sentinel wrapped by every
// UnsupportedResultTypeError, so a caller can classify "the row
// decoder has no branch for this column's SQL type" with
// errors.Is(err, ErrUnsupportedResultType) without reaching for the
// concrete struct. The concrete error (reachable via errors.As)
// names the offending SQL type, length, and CCSID.
var ErrUnsupportedResultType = errors.New("hostserver: unsupported result SQL type")

// UnsupportedResultTypeError is returned by decodeColumn when a
// column's SQL type has no decode branch (e.g. XML 988/989, ARRAY,
// or a DATALINK whose payload shape we won't guess at). It carries
// the descriptor fields a caller needs to report the gap or special-
// case the type, and wraps ErrUnsupportedResultType so both
// errors.Is(err, ErrUnsupportedResultType) and
// errors.As(err, &UnsupportedResultTypeError{}) work.
//
// Before this typed error existed the decoder returned a bare
// fmt.Errorf, so a single un-decodable column turned the whole-row
// decode into an opaque, unclassifiable failure. The message keeps
// the same informative shape (SQL type + column length + CCSID) as
// the old default so existing text-matching callers still recognise
// it.
type UnsupportedResultTypeError struct {
	SQLType uint16
	Length  uint32
	CCSID   uint16
}

func (e *UnsupportedResultTypeError) Error() string {
	return fmt.Sprintf("unsupported SQL type %d (col len=%d, ccsid=%d)", e.SQLType, e.Length, e.CCSID)
}

// Unwrap lets errors.Is(err, ErrUnsupportedResultType) succeed for
// the typed error.
func (e *UnsupportedResultTypeError) Unwrap() error { return ErrUnsupportedResultType }

// decodeGraphicLOB renders a UCS-2 BE / UTF-16 BE byte slice as a
// Go UTF-8 string. Used for inline DBCLOB columns (SQL types
// 412/413) whose CCSID is 13488 (strict UCS-2 BE) or 1200
// (UTF-16 BE). Non-BMP runes only appear in CCSID 1200 -- the 13488
// encoder substitutes them with U+003F on write, so the inverse
// round-trip is lossy but consistent.
func decodeGraphicLOB(b []byte) string {
	if len(b) < 2 {
		return ""
	}
	codes := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		codes = append(codes, uint16(b[i])<<8|uint16(b[i+1]))
	}
	return string(utf16.Decode(codes))
}

// decodeGraphicStrict is decodeGraphicLOB with an explicit reject for
// an odd payload length. A GRAPHIC / VARGRAPHIC / DBCLOB payload is
// UTF-16 / UCS-2, exactly 2 bytes per code unit, so an odd byte count
// is malformed (a misaligned column slot or a corrupt SL). The lenient
// decodeGraphicLOB silently drops the trailing odd byte; for the
// row-decode path that drop would mask a slot shift, so the
// decodeColumn graphic cases route through here and surface the error
// instead. The inline-LOB streaming path keeps the lenient helper.
func decodeGraphicStrict(b []byte) (string, error) {
	if len(b)%2 != 0 {
		return "", fmt.Errorf("graphic payload %d bytes is odd (UTF-16/UCS-2 needs an even byte count)", len(b))
	}
	return decodeGraphicLOB(b), nil
}

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
//
// vlfCompressed must be the value of DBReply.VLFCompressed() for the
// reply this CP came from -- the byte layout differs depending on
// whether the server compressed variable-length fields (variable-
// width row-info-array layout) or padded them to declared width
// (fixed rowSize per row). Length-based heuristics are unreliable
// because a single-row VARCHAR's VLF overhead can land on the same
// total as a non-VLF row of declared rowSize.
func (r *DBReply) findExtendedResultData(cols []SelectColumn) ([]SelectRow, error) {
	for _, p := range r.Params {
		if p.CodePoint == 0x380E {
			// Empty CP 0x380E means "no rows" -- the server still
			// emitted the CP wrapper but with zero-byte data. JT400's
			// parser short-circuits the same way (DBBaseReplyDS.java
			// guards on `parmLength != 6` -- 6 = LL+CP overhead).
			// Without this guard, SELECTs that return zero rows trip
			// the "extended-result-data too short" error.
			if len(p.Data) == 0 {
				return nil, nil
			}
			return parseExtendedResultData(p.Data, cols, r.VLFCompressed())
		}
	}
	return nil, nil
}

func parseExtendedResultData(data []byte, cols []SelectColumn, vlfCompressed bool) ([]SelectRow, error) {
	const fixedLen = 20
	if len(data) < fixedLen {
		return nil, fmt.Errorf("hostserver: extended-result-data too short: %d bytes", len(data))
	}
	be := binary.BigEndian
	rowCount := int(be.Uint32(data[4:8]))
	colCount := int(be.Uint16(data[8:10]))
	indicatorSize := int(be.Uint16(data[10:12]))
	rowSize := int(be.Uint32(data[16:20]))

	// indicatorSize is a per-column NULL-indicator short: the server
	// sends either 0 (no indicators) or 2 (one BE int16 per column).
	// decodeRow reads exactly 2 bytes per indicator (be.Uint16), so an
	// odd or oversized value would slice a 2-byte window out of a
	// 1-byte (or misaligned) slot and panic. JT400's DBExtendedData
	// only ever writes 0 or 2 here; reject anything else up front.
	if indicatorSize != 0 && indicatorSize != 2 {
		return nil, fmt.Errorf("hostserver: result data indicator size %d (want 0 or 2)", indicatorSize)
	}
	if colCount != len(cols) {
		return nil, fmt.Errorf("hostserver: result data column count %d != format column count %d", colCount, len(cols))
	}
	if rowCount == 0 {
		return nil, nil
	}
	// rowCount, colCount, and rowSize are all server-controlled and can
	// each be up to ~4 billion off a hostile or corrupt reply. Cap them
	// before the indicator/row arithmetic below: the products feed both
	// a make([]SelectRow, rowCount) and a slice bound, so an unchecked
	// rowCount fatal-OOMs the process and the int multiplication can
	// overflow into a small/negative value that defeats the
	// past-end-of-data check. maxResultRows mirrors the 64 MiB ceiling
	// the compressed-reply path uses (db_reply.go) -- two orders of
	// magnitude above any real fetch block.
	const maxResultRows = 64 * 1024 * 1024
	if rowCount < 0 || rowCount > maxResultRows {
		return nil, fmt.Errorf("hostserver: result data row count %d exceeds cap %d", rowCount, maxResultRows)
	}
	if rowSize < 0 {
		return nil, fmt.Errorf("hostserver: result data negative row size %d", rowSize)
	}

	// Compute the indicator-block size in int64 so the colCount * rowCount
	// * indicatorSize product can't wrap a 32-bit-ish int; only commit to
	// an int once we know it fits inside the actual payload.
	indicatorBytes64 := int64(indicatorSize) * int64(colCount) * int64(rowCount)
	if int64(fixedLen)+indicatorBytes64 > int64(len(data)) {
		return nil, fmt.Errorf("hostserver: indicators (%d bytes) past end of result data (%d bytes)", indicatorBytes64, len(data))
	}
	indicatorBytes := int(indicatorBytes64)
	indicators := data[fixedLen : fixedLen+indicatorBytes]

	// Pick layout. The reliable signal is the response ORS bitmap
	// bit 0x00010000 (ORSVarFieldComp), which the server sets when
	// it actually used VLF. When the request didn't ask for the
	// echo (older request paths) the bit comes back zero and we
	// fall back to size-matching: if the data is exactly the
	// fixed-row-padding size, it's non-VLF; otherwise VLF.
	//
	// The pure size-match heuristic alone is fragile because a
	// one-row variable-length-only result can produce a VLF total
	// equal to rowSize*rowCount; trust the echo bit first when it's
	// set.
	expectedNonVLF := fixedLen + indicatorBytes + rowSize*rowCount
	useVLF := vlfCompressed || len(data) != expectedNonVLF
	if !useVLF {
		return parseNonVLF(data[fixedLen+indicatorBytes:], cols, indicators, indicatorSize, rowSize, rowCount)
	}

	// VLF path.
	rowInfoHeaderStart := fixedLen + indicatorBytes
	if rowInfoHeaderStart+8 > len(data) {
		return nil, fmt.Errorf("hostserver: row info header overruns result data (have %d, need %d for VLF)",
			len(data), rowInfoHeaderStart+8)
	}
	rowInfoArrayOffset := int(be.Uint32(data[rowInfoHeaderStart : rowInfoHeaderStart+4]))

	rows := make([]SelectRow, rowCount)
	for i := 0; i < rowCount; i++ {
		offEntry := rowInfoHeaderStart + rowInfoArrayOffset + i*4
		if offEntry+4 > len(data) {
			return nil, fmt.Errorf("hostserver: row info array entry %d overruns result data", i)
		}
		rowOff := rowInfoHeaderStart + int(be.Uint32(data[offEntry:offEntry+4]))
		// The row-info array is server-controlled: a hostile or corrupt
		// entry can point anywhere. JT400 leans on the JVM's array bounds
		// check (getRowDataOffset returns a raw offset and the caller's
		// rawBytes[off] throws ArrayIndexOutOfBoundsException on a bad
		// one); Go's data[rowOff:] panics with no recover() between here
		// and database/sql, so bound rowOff into [rowInfoHeaderStart,
		// len(data)] before slicing.
		if rowOff < rowInfoHeaderStart || rowOff > len(data) {
			return nil, fmt.Errorf("hostserver: row %d data offset %d out of range [%d, %d]", i, rowOff, rowInfoHeaderStart, len(data))
		}
		row, _, err := decodeRow(data[rowOff:], cols, indicators[i*colCount*indicatorSize:(i+1)*colCount*indicatorSize], indicatorSize, false)
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
		row, _, err := decodeRow(rowsBytes[start:end], cols, indicators[i*colCount*indicatorSize:(i+1)*colCount*indicatorSize], indicatorSize, true)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		rows[i] = row
	}
	return rows, nil
}

// decodeRow walks the column descriptors in order, slicing bytes
// off the start of rowBytes per column. Returns the populated row
// plus the number of bytes consumed.
//
// fixedSlots picks how to advance the per-column offset:
//
//   - true  — non-VLF layout. Each column occupies exactly its
//     declared wire-slot width (col.wireLength()), with shorter
//     payloads padded out (EBCDIC space 0x40 for char types, zero
//     for binary). Live-confirmed against IBM i V7R6M0 returning
//     `SELECT AUTHORIZATION_NAME, STATUS FROM QSYS2.USER_INFO`,
//     where each VARCHAR(10) slot is 12 bytes wide regardless of
//     the actual string length and the second column's offset is
//     deterministic from the first column's slot, not its content.
//   - false — VLF-compressed layout. Each column occupies only
//     `2 + actualLen` bytes (no padding); decodeColumn's returned
//     consumed-byte count is what advances `off`.
//
// Pre-fix this function always advanced by decodeColumn's return
// value, which silently under-advanced on non-VLF VARCHARs whose
// content was shorter than the declared slot, sliding subsequent
// columns into the previous slot's padding.
func decodeRow(rowBytes []byte, cols []SelectColumn, indicators []byte, indicatorSize int, fixedSlots bool) (SelectRow, int, error) {
	row := make(SelectRow, len(cols))
	off := 0
	be := binary.BigEndian
	for i, col := range cols {
		slotWidth := int(col.wireLength())
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
			// Advance off by the column's on-wire footprint:
			//
			//   - non-VLF (fixedSlots=true): col.Length, the full
			//     declared slot, regardless of null. The slot is
			//     present and zero/space padded.
			//
			//   - VLF (fixedSlots=false): JT400's
			//     JDServerRow.setRowIndex (the @K54 VLF branch)
			//     advances by `2 + SL_value` for VARCHAR-like and
			//     `serverFormat_.getFieldLength(j)` for fixed types,
			//     regardless of null indicator. For null VARCHAR the
			//     SL is zero and the column footprint is exactly 2
			//     bytes; for null fixed-length columns the slot is
			//     still col.Length bytes wide. Pre-fix we always
			//     advanced by col.Length here, which on a null
			//     VARCHAR(50) column tried to skip 52 bytes of an
			//     8-byte VLF row and panicked
			//     ("slice bounds out of range [52:12]").
			off += nullSkipWidth(col, fixedSlots)
			continue
		}
		// off is driven by server-controlled slot widths (and, in the
		// non-VLF path, by a server-controlled rowSize that bounds the
		// rowBytes window). A rowSize smaller than the summed column
		// widths -- or a corrupt slot width -- walks off past the end
		// of the slice and rowBytes[off:] panics. Bound it here; the
		// per-type decoders already length-check from off forward, but
		// the slice expression itself must be in range first.
		if off < 0 || off > len(rowBytes) {
			return nil, 0, fmt.Errorf("column %d (%q, sql_type=%d): row offset %d past end of %d-byte row", i, col.Name, col.SQLType, off, len(rowBytes))
		}
		val, n, err := decodeColumn(rowBytes[off:], col)
		if err != nil {
			return nil, 0, fmt.Errorf("column %d (%q, sql_type=%d): %w", i, col.Name, col.SQLType, err)
		}
		row[i] = val
		if fixedSlots {
			off += slotWidth
		} else {
			off += n
		}
	}
	return row, off, nil
}

// nullSkipWidth returns how many bytes to advance past a null column
// in the row-data block. Non-VLF reserves the full slot
// (col.Length); VLF compresses VARCHAR-like null columns down to
// just their 2-byte SL prefix while leaving fixed-length null slots
// at col.Length. Mirrors JT400's JDServerRow.setRowIndex VLF branch.
func nullSkipWidth(col SelectColumn, fixedSlots bool) int {
	if fixedSlots {
		return int(col.wireLength())
	}
	if isVarLengthSQLType(col.SQLType) {
		return 2
	}
	return int(col.wireLength())
}

// isVarLengthSQLType reports whether the IBM i SQL type code uses a
// 2-byte length prefix on the wire (i.e. a VARCHAR-family type). The
// list mirrors the case labels in JT400's JDServerRow.setRowIndex
// VLF branch (VARCHAR, VARCHAR_FOR_BIT_DATA, LONG_VARCHAR,
// LONG_VARCHAR_FOR_BIT_DATA, VARBINARY, DATALINK, VARGRAPHIC,
// LONG_VARGRAPHIC, NVARCHAR, LONG_NVARCHAR) translated to the
// IBM i wire SQL type numbers (NN/nullable pairs).
//
// NOTE: GRAPHIC (468/469) is deliberately NOT here. It is a
// FIXED-length type with no SL prefix (live-confirmed on PUB400
// V7R5M0 for issue #3 -- a GRAPHIC(5) value ships exactly
// 2*5 payload bytes with no length header). Only the VARGRAPHIC
// (464/465) and LONG VARGRAPHIC (472/473) graphic forms carry a
// 2-byte SL. The decodeColumn graphic cases and this classifier
// must agree, or a NULL GRAPHIC column in a VLF row would null-skip
// 2 bytes instead of col.Length and shift subsequent columns.
//
// DATALINK (396/397) is VARCHAR-family: JT400's SQLDatalink IS named in
// JDServerRow.setRowIndex's variable-length switch, so a NULL DATALINK
// in a VLF row null-skips its 2-byte SL rather than col.Length.
//
// ROWID (904/905) is deliberately NOT here. Although a ROWID VALUE
// carries a 2-byte SL prefix (read by SQLRowID.convertFromRawBytes),
// JT400's setRowIndex steps ROWID by the FIXED getFieldLength -- ROWID
// is not in its var-length switch. Classifying it var-length would make
// a NULL ROWID null-skip 2 bytes (instead of the fixed slot) and a
// non-null ROWID advance by 2+SL (instead of the fixed slot), shifting
// every subsequent column. The decode case 904/905 reads the SL-prefixed
// value from within the fixed slot and advances by col.Length.
func isVarLengthSQLType(sqlType uint16) bool {
	switch sqlType {
	case 448, 449, // VARCHAR (also covers VARCHAR FOR BIT DATA when CCSID=65535)
		456, 457, // LONG_VARCHAR
		460, 461, // LONG_VARCHAR_FOR_BIT_DATA (rare; typically same shape as 456/457)
		464, 465, // VARGRAPHIC (graphic, 2-byte SL of char-count)
		472, 473, // LONG_VARGRAPHIC
		396, 397, // DATALINK (VARCHAR-family, 2-byte SL of bytes)
		908, 909: // VARBINARY (JT400 SQLVarbinary.getNativeType -> 908)
		return true
	}
	return false
}

// wireLength returns the column's wire-slot width (the byte count
// the row-data block reserves for this column in non-VLF layouts).
//
// SuperExtended data format already reports the *full* slot width
// including any SL prefix at offset+4..+7 of the per-field record,
// so we return col.Length verbatim. The earlier `c.Length + 2`
// branch for VARCHAR was wrong: PUB400 V7R5M0 and IBM i V7R6M0
// both report e.g. 12 for a VARCHAR(10), where the 12 already
// counts the 2 SL bytes. The pre-fix wireLength would have null-
// skipped 14 bytes and shifted every subsequent column by 2.
func (c SelectColumn) wireLength() uint32 {
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
		// JDBC always returns ISO ("YYYY-MM-DD"). When the session
		// negotiated a concrete date format via SET_SQL_ATTRIBUTES we
		// decode by that format (col.DateFormat), mirroring JT400's
		// SQLDate.stringToDate switch on settings.getDateFormat() --
		// this is the only way to disambiguate MDY from DMY, which
		// share an 8-char "NN/NN/NN" wire shape. With no negotiated
		// format (DateFormatJOB / zero) we fall back to shape-sniffing
		// via ymdToISODate, which defaults the ambiguous 8-char shape
		// to MDY.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("date wants %d bytes, have %d", col.Length, len(b))
		}
		s, err := ebcdic.CCSID37.Decode(b[:col.Length])
		if err != nil {
			return nil, 0, fmt.Errorf("decode date ebcdic: %w", err)
		}
		return dateStringToISO(s, col.DateFormat), int(col.Length), nil

	case SQLTypeTime, SQLTypeTimeNN:
		// ISO time format on the wire: "HH:MM:SS" (8 EBCDIC chars).
		// When the session negotiated a concrete time format via
		// SET_SQL_ATTRIBUTES we normalise by that format
		// (col.TimeFormat), mirroring JT400's SQLTime.stringToTime
		// switch on settings.getTimeFormat() -- USA ships AM/PM
		// ("HH:MM AM"), EUR/ISO ship "HH.MM.SS" with '.' separators.
		// With no negotiated format (TimeFormat <= 0, i.e. unset or
		// hms) the wire is already canonical "HH:MM:SS" and we return
		// it verbatim.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("time wants %d bytes, have %d", col.Length, len(b))
		}
		s, err := ebcdic.CCSID37.Decode(b[:col.Length])
		if err != nil {
			return nil, 0, fmt.Errorf("decode time ebcdic: %w", err)
		}
		return timeStringToISO(s, col.TimeFormat), int(col.Length), nil

	case SQLTypeChar, 453, SQLTypeCharNonBlank, 461:
		// 452 = CHAR NN, 453 = CHAR nullable, 460/461 = CHAR with
		// terminator-or-binary variants. All four share wire layout.
		// Fixed-length CHAR. Decode based on column CCSID:
		//   65535       FOR BIT DATA: bytes returned as []byte
		//   1208        UTF-8 passthrough: bytes returned as string
		//               (no transcode -- server already encoded as UTF-8)
		//   else        EBCDIC SBCS table picked per-CCSID via
		//               ebcdicForCCSID (CCSID 273 honoured for German
		//               LPARs; CCSID 37 fallback for everything else).
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("char wants %d bytes, have %d", col.Length, len(b))
		}
		if col.CCSID == ccsidBinary {
			out := make([]byte, col.Length)
			copy(out, b[:col.Length])
			return out, int(col.Length), nil
		}
		if col.CCSID == 1208 {
			return string(b[:col.Length]), int(col.Length), nil
		}
		s, err := ebcdicForCCSID(col.CCSID).Decode(b[:col.Length])
		if err != nil {
			return nil, 0, fmt.Errorf("decode char ccsid %d: %w", col.CCSID, err)
		}
		return s, int(col.Length), nil

	case SQLTypeVarChar, 449, SQLTypeVarCharNonBlank, 457:
		// 2-byte BE length prefix followed by N payload bytes.
		// The slot occupies col.Length+2 bytes on the wire (in
		// non-VLF layouts), but in VLF-compressed result data
		// the row only contains 2+actual-length bytes for this
		// column (no padding to col.Length). Decoder picks the
		// payload codec by CCSID:
		//   65535  FOR BIT DATA, []byte verbatim
		//   1208   UTF-8 string, no transcode
		//   else   EBCDIC SBCS via ebcdicForCCSID (CCSID 273 honoured;
		//          CCSID 37 fallback)
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
		if col.CCSID == 1208 {
			return string(b[2 : 2+n]), 2 + n, nil
		}
		s, err := ebcdicForCCSID(col.CCSID).Decode(b[2 : 2+n])
		if err != nil {
			return nil, 0, fmt.Errorf("decode varchar ccsid %d: %w", col.CCSID, err)
		}
		return s, 2 + n, nil

	case 464, 465, // VARGRAPHIC      (NN / nullable)
		472, 473: // LONG VARGRAPHIC (NN / nullable)
		// Variable-length graphic (DBCS / Unicode) columns. Wire
		// format mirrors VARCHAR but with 2-byte code units: a
		// 2-byte BE SL prefix followed by the payload. Live-confirmed
		// against PUB400 V7R5M0 (issue #3) that the SL counts GRAPHIC
		// CHARACTERS (code units), not bytes -- e.g.
		// CAST('ABC' AS VARGRAPHIC(10) CCSID 1200) ships
		// `00 03 | 00 41 00 42 00 43`, SL=3 with 6 payload bytes. So
		// payload bytes = 2 * SL and consumed = 2 + 2*SL. col.Length
		// is the declared slot width (2 + 2*maxchars); in the
		// VLF-compressed result-data path the row carries only the
		// 2+2*SL actual bytes, which is what we advance by.
		//
		// CCSID picks the codec:
		//   65535  FOR BIT DATA -> raw []byte, no transcode
		//   else   UTF-16 BE / UCS-2 BE via decodeGraphicLOB
		//          (1200 = UTF-16 BE, 13488 = UCS-2 BE; the same
		//          helper the inline-DBCLOB case 412/413 uses).
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("vargraphic header wants 2 bytes, have %d", len(b))
		}
		chars := int(binary.BigEndian.Uint16(b[:2]))
		nbytes := chars * 2
		if nbytes > int(col.Length) {
			return nil, 0, fmt.Errorf("vargraphic declared length %d bytes exceeds column max %d", nbytes, col.Length)
		}
		if len(b) < 2+nbytes {
			return nil, 0, fmt.Errorf("vargraphic wants %d bytes (header+data), have %d", 2+nbytes, len(b))
		}
		if col.CCSID == ccsidBinary {
			out := make([]byte, nbytes)
			copy(out, b[2:2+nbytes])
			return out, 2 + nbytes, nil
		}
		s, err := decodeGraphicStrict(b[2 : 2+nbytes])
		if err != nil {
			return nil, 0, err
		}
		return s, 2 + nbytes, nil

	case 468, 469: // GRAPHIC (fixed-length) -- NN / nullable
		// Fixed-length graphic (DBCS / Unicode) columns. Unlike
		// VARGRAPHIC there is NO SL prefix: the payload is exactly
		// col.Length bytes (2 * declared char count), space-padded
		// with U+0020 graphic spaces. Live-confirmed against PUB400
		// V7R5M0 (issue #3): CAST('ABC' AS GRAPHIC(5) CCSID 1200)
		// ships `00 41 00 42 00 43 00 20 00 20` with col.Length=10
		// and no length header. Because 468/469 are fixed-width,
		// isVarLengthSQLType must NOT list them -- otherwise a NULL
		// GRAPHIC column in a VLF row would null-skip 2 bytes instead
		// of col.Length and slide every subsequent column.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("graphic wants %d bytes, have %d", col.Length, len(b))
		}
		if col.CCSID == ccsidBinary {
			out := make([]byte, col.Length)
			copy(out, b[:col.Length])
			return out, int(col.Length), nil
		}
		s, err := decodeGraphicStrict(b[:col.Length])
		if err != nil {
			return nil, 0, err
		}
		return s, int(col.Length), nil

	case 912, 913:
		// BINARY (fixed-length) -- NN / nullable. JT400's
		// SQLBinary.getNativeType returns 912; the column carries
		// CCSID 65535 by definition. Wire format is `col.Length`
		// bytes of raw payload, no length prefix -- shares shape with
		// CHAR FOR BIT DATA (452/453 + CCSID 65535) but ships under a
		// distinct SQL type code on V7R3+ servers that expose the
		// native BINARY type alongside the older CHAR-with-bit-data
		// emulation.
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("binary wants %d bytes, have %d", col.Length, len(b))
		}
		out := make([]byte, col.Length)
		copy(out, b[:col.Length])
		return out, int(col.Length), nil

	case 908, 909:
		// VARBINARY (variable-length) -- NN / nullable. JT400's
		// SQLVarbinary.getNativeType returns 908; column CCSID is
		// always 65535. Wire format mirrors VARCHAR FOR BIT DATA
		// (449 + CCSID 65535): 2-byte BE actual-length prefix
		// followed by `length` bytes of payload, slot-padded to
		// col.Length in non-VLF layouts. Ships under a distinct
		// SQL type code on V7R3+ servers that expose native
		// VARBINARY alongside the older VARCHAR-with-bit-data
		// emulation.
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("varbinary header wants 2 bytes, have %d", len(b))
		}
		n := int(binary.BigEndian.Uint16(b[:2]))
		if n > int(col.Length) {
			return nil, 0, fmt.Errorf("varbinary declared length %d exceeds column max %d", n, col.Length)
		}
		if len(b) < 2+n {
			return nil, 0, fmt.Errorf("varbinary wants %d bytes (header+data), have %d", 2+n, len(b))
		}
		out := make([]byte, n)
		copy(out, b[2:2+n])
		return out, 2 + n, nil

	case 904, 905: // ROWID -- NN / nullable
		// ROWID is the opaque row-identifier type. JT400's
		// SQLRowID.convertFromRawBytes reads a 2-byte BE unsigned-short
		// length prefix then copies `length` payload bytes (max 40) and
		// surfaces them as a byte[] -- getObject returns the raw bytes,
		// getString hexifies them. We mirror the byte[] shape and return
		// the payload as []byte.
		//
		// CRUCIAL: ROWID is a FIXED-width slot for row stepping (JT400's
		// setRowIndex advances by getFieldLength, not by the value's SL),
		// so it is NOT in isVarLengthSQLType and we advance by col.Length
		// -- the value's 2-byte SL is read from WITHIN that fixed slot.
		// Returning 2+n here (as a var-length type would) is what shifts
		// subsequent columns; do not.
		//
		// ASSUMPTION (verified live on PUB400 V7R5): the server reports a
		// fixed field length that budgets the 2-byte SL plus payload. The
		// guards below tolerate either a 40- or 42-byte reported slot.
		if int(col.Length) < 2 {
			return nil, 0, fmt.Errorf("rowid slot %d too small for 2-byte SL", col.Length)
		}
		if len(b) < int(col.Length) {
			return nil, 0, fmt.Errorf("rowid wants %d bytes (fixed slot), have %d", col.Length, len(b))
		}
		n := int(binary.BigEndian.Uint16(b[:2]))
		if 2+n > int(col.Length) {
			return nil, 0, fmt.Errorf("rowid value length %d + 2-byte SL exceeds slot %d", n, col.Length)
		}
		out := make([]byte, n)
		copy(out, b[2:2+n])
		return out, int(col.Length), nil

	case 396, 397: // DATALINK -- NN / nullable
		// DATALINK is a VARCHAR-family link/URL value. JT400's
		// SQLDatalink.convertFromRawBytes reads a 2-byte BE
		// unsigned-short length prefix then decodes `length` payload
		// bytes through the column CCSID converter to a String (which
		// it then wraps in a java.net.URL). We surface the decoded
		// link string directly. CCSID picks the codec exactly like
		// VARCHAR: 65535 -> raw []byte, 1208 -> UTF-8 passthrough,
		// else EBCDIC SBCS via ebcdicForCCSID. Listed in
		// isVarLengthSQLType so a NULL DATALINK null-skips its 2-byte
		// SL in a VLF row.
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("datalink header wants 2 bytes, have %d", len(b))
		}
		n := int(binary.BigEndian.Uint16(b[:2]))
		if n > int(col.Length) {
			return nil, 0, fmt.Errorf("datalink declared length %d exceeds column max %d", n, col.Length)
		}
		if len(b) < 2+n {
			return nil, 0, fmt.Errorf("datalink wants %d bytes (header+data), have %d", 2+n, len(b))
		}
		if col.CCSID == ccsidBinary {
			out := make([]byte, n)
			copy(out, b[2:2+n])
			return out, 2 + n, nil
		}
		if col.CCSID == 1208 {
			return string(b[2 : 2+n]), 2 + n, nil
		}
		s, err := ebcdicForCCSID(col.CCSID).Decode(b[2 : 2+n])
		if err != nil {
			return nil, 0, fmt.Errorf("decode datalink ccsid %d: %w", col.CCSID, err)
		}
		return s, 2 + n, nil

	case 2436, 2437: // BOOLEAN NN / nullable (V7R5+ native BOOLEAN type)
		// One byte on the wire; JT400's SQLBoolean.convertFromRawBytes
		// treats 0xF0 (EBCDIC '0') as false and anything else as
		// true. The encoder writes 0xF1 (EBCDIC '1') for true so
		// the round trip stays inside the {0xF0, 0xF1} pair, but
		// the read side trusts the false-sentinel and treats every
		// other byte as true to match JT400's behaviour.
		if len(b) < 1 {
			return nil, 0, fmt.Errorf("boolean wants 1 byte, have %d", len(b))
		}
		return b[0] != 0xF0, 1, nil

	case SQLTypeInteger, 497: // 496 NN, 497 nullable
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("integer wants 4 bytes, have %d", len(b))
		}
		v := int64(int32(binary.BigEndian.Uint32(b[:4])))
		if col.Scale != 0 {
			return scaledIntegerString(v, int(col.Scale)), 4, nil
		}
		return int32(v), 4, nil

	case SQLTypeSmallInt, 501: // 500 NN, 501 nullable
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("smallint wants 2 bytes, have %d", len(b))
		}
		v := int64(int16(binary.BigEndian.Uint16(b[:2])))
		if col.Scale != 0 {
			return scaledIntegerString(v, int(col.Scale)), 2, nil
		}
		return int16(v), 2, nil

	case SQLTypeBigInt, 493: // 492 NN, 493 nullable
		if len(b) < 8 {
			return nil, 0, fmt.Errorf("bigint wants 8 bytes, have %d", len(b))
		}
		// No scale branch: JT400's SQLBigint has no scale parameter and
		// never applies movePointLeft -- a BIGINT type code always
		// renders as the raw int64 (only SQLInteger/SQLSmallint scale).
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

	case 404, 405, 408, 409, 412, 413:
		// Inline LOB result-data columns. The server ships these
		// instead of locator-typed (960..969) columns when the
		// connection-level LOB threshold (CP 0x3822 in
		// SET_SQL_ATTRIBUTES, "lob threshold") is set and the
		// column's stored value is at or below that threshold.
		// Wire format mirrors JT400's SQLBlob / SQLClob / SQLDBClob
		// convertFromRawBytes: a 4-byte BE actual-length prefix
		// followed by `length` bytes of payload, slot-padded to
		// col.Length so the next column lines up.
		//
		//   404 / 405  BLOB  inline   (NN / nullable)
		//   408 / 409  CLOB  inline   (NN / nullable)
		//   412 / 413  DBCLOB inline  (NN / nullable, graphic CCSID)
		//
		// BLOB returns []byte; CLOB / DBCLOB return string decoded
		// per the column's CCSID. Mirrors the locator-path
		// materialise behaviour in driver/rows.go for the small-LOB
		// inline case. Closes the M7-3 follow-up gap where
		// CLOB(<=32K) CCSID 1208 columns returned zero rows because
		// the server inlined them and the parser refused the
		// unrecognised type code.
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("inline lob header wants 4 bytes, have %d", len(b))
		}
		n := int(binary.BigEndian.Uint32(b[:4]))
		// col.Length carries `max_payload + 4` (the 4-byte length
		// header is part of the slot width). Sanity-check the
		// declared length against the slot so a server bug or a
		// misaligned column descriptor can't drive an out-of-bounds
		// slice.
		if n < 0 || n > int(col.Length)-4 {
			return nil, 0, fmt.Errorf("inline lob declared length %d exceeds column max %d", n, int(col.Length)-4)
		}
		consumed := int(col.Length)
		if len(b) < consumed {
			return nil, 0, fmt.Errorf("inline lob wants %d bytes (header+data+pad), have %d", consumed, len(b))
		}
		payload := b[4 : 4+n]
		switch col.SQLType {
		case 404, 405:
			out := make([]byte, n)
			copy(out, payload)
			return out, consumed, nil
		case 408, 409:
			// CLOB inline -- decode through the column CCSID. Mirrors
			// driver/rows.decodeLOBChars (1208 = UTF-8 passthrough,
			// 273 = German EBCDIC, default = CCSID 37) but lives at
			// the hostserver layer where the row walker can return
			// the string directly.
			switch col.CCSID {
			case ccsidBinary:
				out := make([]byte, n)
				copy(out, payload)
				return out, consumed, nil
			case 1208:
				return string(payload), consumed, nil
			default:
				s, err := ebcdicForCCSID(col.CCSID).Decode(payload)
				if err != nil {
					return nil, 0, fmt.Errorf("decode inline clob ccsid %d: %w", col.CCSID, err)
				}
				return s, consumed, nil
			}
		case 412, 413:
			// DBCLOB inline -- graphic CCSID. Payload is 2-byte
			// codeunits; CCSID 13488 is UCS-2 BE, 1200 is UTF-16 BE.
			// Both decode through the same UTF-16 BE helper because
			// non-BMP runes don't appear in 13488 by definition (the
			// encoder substitutes them with U+003F on write).
			if n%2 != 0 {
				return nil, 0, fmt.Errorf("dbclob inline declared length %d not even", n)
			}
			return decodeGraphicLOB(payload), consumed, nil
		}

	case 960, 961, 964, 965, 968, 969:
		// LOB locators: BLOB(960/961), CLOB(964/965), DBCLOB(968/969).
		// Wire bytes are a 4-byte BE handle that the caller passes
		// to RetrieveLOBData to materialise the actual content. Some
		// V7R5+ replies pack a length prefix ahead of the handle in
		// the same way VARCHAR does (4-byte handle + extras for
		// CCSID/length), but the leading 4 bytes are always the
		// handle. Caller decides whether to expand inline (small
		// LOBs, the database/sql Scan path) or stream via separate
		// RetrieveLOBData calls.
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("lob locator wants 4 bytes, have %d", len(b))
		}
		handle := binary.BigEndian.Uint32(b[:4])
		// col.Length is the locator-record width on the wire (often
		// 16, 20, 28 depending on V*R*M and column-type metadata
		// configuration). Advance by col.Length so the row layout
		// stays in step.
		consumed := int(col.Length)
		if consumed < 4 {
			consumed = 4
		}
		if len(b) < consumed {
			return nil, 0, fmt.Errorf("lob locator wants %d bytes, have %d", consumed, len(b))
		}
		return LOBLocator{
			Handle:    handle,
			SQLType:   col.SQLType,
			MaxLength: col.Length,
			CCSID:     col.CCSID,
		}, consumed, nil

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
	// No decode branch for this SQL type (e.g. XML 988/989, ARRAY, or
	// a DATALINK whose payload shape we won't guess at). Return the
	// typed UnsupportedResultTypeError so the whole-row decode fails
	// classifiably (errors.As / errors.Is) instead of opaquely, while
	// keeping the original informative message shape.
	return nil, 0, &UnsupportedResultTypeError{SQLType: col.SQLType, Length: col.Length, CCSID: col.CCSID}
}

// scaledIntegerString renders a binary integer with a nonzero column
// scale as a fixed-point decimal string. IBM i can store a
// DECIMAL/NUMERIC(p, s) as a binary SMALLINT/INTEGER/BIGINT when the
// precision fits the integer width; the descriptor then carries the
// real type's Scale, and the raw integer is the unscaled value (so a
// stored 12345 with Scale=2 is the number 123.45). JT400's SQLData
// for the binary types divides by 10^scale; we render the same value
// as a decimal string ("[-]int[.frac]") to avoid the float rounding a
// /10^s division would introduce. A scale at or above the digit count
// pads "0." with leading fraction zeros, mirroring decodePackedBCD.
func scaledIntegerString(v int64, scale int) string {
	if scale <= 0 {
		return strconv.FormatInt(v, 10)
	}
	neg := v < 0
	// Format the magnitude; strconv handles math.MinInt64 cleanly via
	// the unsigned-on-overflow path, but take the absolute value as a
	// string to stay correct at the boundary.
	mag := strconv.FormatInt(v, 10)
	if neg {
		mag = mag[1:] // drop the '-'
	}
	digits := []byte(mag)
	var out []byte
	if neg {
		out = append(out, '-')
	}
	if scale >= len(digits) {
		out = append(out, '0', '.')
		for i := 0; i < scale-len(digits); i++ {
			out = append(out, '0')
		}
		out = append(out, digits...)
	} else {
		out = append(out, digits[:len(digits)-scale]...)
		out = append(out, '.')
		out = append(out, digits[len(digits)-scale:]...)
	}
	return string(out)
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

// dateStringToISO normalises a server-formatted DATE string into ISO
// "YYYY-MM-DD" using the session's negotiated date format. It mirrors
// JT400's SQLDate.stringToDate, which switches on
// settings.getDateFormat() rather than guessing from the string shape
// -- the only way to tell MDY ("MM/DD/YY") from DMY ("DD/MM/YY"),
// which are byte-shape-identical.
//
// format is a DateFormat* byte constant. The unset values
// (DateFormatJOB 0xF0 and the zero value) mean the format was not
// negotiated; in that case we fall back to ymdToISODate's
// shape-sniffing, preserving the pre-negotiation behaviour. Any input
// whose length doesn't match the format also falls through to
// shape-sniffing so a misconfigured session degrades gracefully
// rather than slicing out of range.
//
// The 8-char (YMD/MDY/DMY) variants carry a 2-digit year; we apply
// the same 1940 century boundary JT400 uses (00..39 -> 20YY,
// 40..99 -> 19YY).
func dateStringToISO(s string, format byte) string {
	// Each branch checks both length and separators so a wire string
	// that doesn't match the negotiated format degrades to the
	// shape-sniffer rather than slicing the wrong fields.
	switch format {
	case DateFormatISO, DateFormatJIS:
		if len(s) == 10 && s[4] == '-' && s[7] == '-' {
			return s // already YYYY-MM-DD
		}
	case DateFormatUSA:
		if len(s) == 10 && s[2] == '/' && s[5] == '/' { // MM/DD/YYYY
			return s[6:10] + "-" + s[0:2] + "-" + s[3:5]
		}
	case DateFormatEUR:
		if len(s) == 10 && s[2] == '.' && s[5] == '.' { // DD.MM.YYYY
			return s[6:10] + "-" + s[3:5] + "-" + s[0:2]
		}
	case DateFormatMDY:
		if len(s) == 8 && s[2] == '/' && s[5] == '/' { // MM/DD/YY
			return centuryFromYY(s[6:8]) + s[6:8] + "-" + s[0:2] + "-" + s[3:5]
		}
	case DateFormatDMY:
		if len(s) == 8 && s[2] == '/' && s[5] == '/' { // DD/MM/YY
			return centuryFromYY(s[6:8]) + s[6:8] + "-" + s[3:5] + "-" + s[0:2]
		}
	case DateFormatYMD:
		if len(s) == 8 && s[2] == '-' && s[5] == '-' { // YY-MM-DD
			return centuryFromYY(s[0:2]) + s[0:2] + "-" + s[3:5] + "-" + s[6:8]
		}
	}
	// Not negotiated (DateFormatJOB / zero), or a wire shape that
	// doesn't match the negotiated format -- sniff the shape.
	return ymdToISODate(s)
}

// centuryFromYY returns the "19"/"20" century prefix for a 2-digit
// year string using JT400's 1940 cutover (00..39 -> 20YY,
// 40..99 -> 19YY). yy must be 2 bytes; a non-digit or short input
// defaults to "20".
func centuryFromYY(yy string) string {
	if len(yy) == 2 && yy[0] >= '4' {
		return "19"
	}
	return "20"
}

// timeStringToISO normalises a server-formatted TIME string into ISO
// "HH:MM:SS" using the session's negotiated time format. It mirrors
// JT400's SQLTime.stringToTime, which switches on
// settings.getTimeFormat(): the USA format ships "HH:MM AM"/"HH:MM PM"
// (12-hour clock, no seconds) while EUR/JIS/ISO ship "HH.MM.SS" with
// '.' separators. HMS is already canonical "HH:MM:SS".
//
// format is JT400's TIME_FORMAT choice index (hms=0, usa=1, iso=2,
// eur=3, jis=4). Values <= 0 (unset sentinel or hms) mean the wire is
// already canonical and we return it verbatim. An input whose shape
// doesn't match the negotiated format is returned unchanged so a
// misconfigured session degrades gracefully.
func timeStringToISO(s string, format int8) string {
	switch format {
	case 1: // USA: "HH:MM AM" / "HH:MM PM" (12-hour, no seconds)
		if len(s) == 8 && (s[6] == 'A' || s[6] == 'P') {
			hh := (int(s[0]-'0'))*10 + int(s[1]-'0')
			if s[6] == 'A' {
				if hh == 12 {
					hh = 0
				}
			} else if hh != 12 { // PM
				hh += 12
			}
			return twoDigit(hh) + ":" + s[3:5] + ":00"
		}
	case 2, 3, 4: // ISO / EUR / JIS: "HH.MM.SS" (dotted separators)
		if len(s) == 8 && s[2] == '.' && s[5] == '.' {
			return s[0:2] + ":" + s[3:5] + ":" + s[6:8]
		}
	}
	// hms (0) / unset, or a shape that doesn't match -- already
	// canonical "HH:MM:SS" on the wire.
	return s
}

// twoDigit renders 0..99 as a zero-padded 2-char string.
func twoDigit(n int) string {
	return string([]byte{byte('0' + (n/10)%10), byte('0' + n%10)})
}

// ibmTimestampToISO converts IBM i's wire timestamp string
// "YYYY-MM-DD-HH.MM.SS[.ffffff...]" to ISO 8601
// "YYYY-MM-DDTHH:MM:SS[.ffffff...]". The differences are the
// date/time delimiter ('-' -> 'T') and the time-component
// separator ('.' -> ':') in HH.MM.SS.
//
// The fractional tail is precision-dependent: TIMESTAMP(0) sends
// no tail (19 chars, no separator at idx 19), TIMESTAMP(6) sends
// 6 digits (26 chars), and TIMESTAMP(12) sends 12 digits (32
// chars). We only rewrite the fixed date/time prefix (idx 0..18)
// and pass the remaining tail through verbatim, so any precision
// 0..12 round-trips. This mirrors SQLTimestamp.parse in JTOpen,
// which slices the date/time at the same fixed offsets and treats
// everything from idx 20 on as an optional fractional part.
//
// If s doesn't look like a wire timestamp (too short or the fixed
// sentinels are off), it's returned unchanged so the caller can
// still see the raw value rather than panic on bad input.
func ibmTimestampToISO(s string) string {
	// 19 = "YYYY-MM-DD-HH.MM.SS" with no fractional part. When a
	// fraction is present, idx 19 is the '.' separator followed by
	// the precision-many digits.
	if len(s) < 19 || s[10] != '-' || s[13] != '.' || s[16] != '.' {
		return s
	}
	if len(s) > 19 && s[19] != '.' {
		return s
	}
	b := []byte(s)
	b[10] = 'T'
	b[13] = ':'
	b[16] = ':'
	// b[19] (if present) stays '.' -- that's the seconds-vs-fractional
	// separator and ISO uses '.' there. The fractional digits after it
	// are copied unchanged.
	return string(b)
}
