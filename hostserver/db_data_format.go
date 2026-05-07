package hostserver

import (
	"encoding/binary"
	"fmt"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// SQL type codes from JTOpen's SQLData / DBSQLDescriptorDS. Only the
// types we actually decode in M2 are listed; more land as M3+.
const (
	SQLTypeVarChar         uint16 = 448
	SQLTypeChar            uint16 = 452
	SQLTypeVarCharNonBlank uint16 = 456 // VARCHAR with NULL indicator pre-fixed
	SQLTypeCharNonBlank    uint16 = 460
	SQLTypeInteger         uint16 = 496
	SQLTypeSmallInt        uint16 = 500
	SQLTypeBigInt          uint16 = 492
	// JTOpen pairs each SQL type with two adjacent codes: even = NN
	// (not nullable), odd = nullable. The "NN" suffix on legacy
	// constants below is the not-nullable form.
	SQLTypeDateNN      uint16 = 384
	SQLTypeDate        uint16 = 385
	SQLTypeTimeNN      uint16 = 388
	SQLTypeTime        uint16 = 389
	SQLTypeTimestampNN uint16 = 392
	SQLTypeTimestamp   uint16 = 393
	SQLTypeFloat4          uint16 = 480
	SQLTypeFloat8          uint16 = 480 // distinguished by length
	SQLTypeDecimal         uint16 = 484
	SQLTypeNumeric         uint16 = 488
)

// findSuperExtendedDataFormat returns the parsed list of column
// descriptors from the reply, scanning for CP 0x3812. Returns
// (nil, nil) if the reply doesn't contain the CP.
func (r *DBReply) findSuperExtendedDataFormat() ([]SelectColumn, error) {
	for _, p := range r.Params {
		if p.CodePoint == 0x3812 {
			return parseSuperExtendedDataFormat(p.Data)
		}
	}
	return nil, nil
}

// parseSuperExtendedDataFormat decodes the CP 0x3812 payload --
// JTOpen's DBSuperExtendedDataFormat. Layout (per the JTOpen source
// header comment):
//
//	0..3   consistency token
//	4..7   number of fields (uint32 BE)
//	8      date format        (1 byte)
//	9      time format        (1 byte)
//	10     date separator     (1 byte)
//	11     time separator     (1 byte)
//	12..15 record size        (uint32 BE)
//	16+    repeating per-field fixed records, 48 bytes each:
//	  0..1  field description LL
//	  2..3  SQL type
//	  4..7  field length
//	  8..9  scale
//	  10..11 precision
//	  12..13 CCSID
//	  14    parameter type
//	  15..16 join ref position
//	  17..20 reserved
//	  21    flags
//	  22..25 max array cardinality
//	  26..29 LOB max size
//	  30..31 alignment
//	  32..35 offset to variable-length info (relative to start of fixed)
//	  36..39 length of variable info
//	  40..47 reserved
//	  ...   variable info (LL/CP/CCSID/name) reachable via offset+length
func parseSuperExtendedDataFormat(data []byte) ([]SelectColumn, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("hostserver: super-extended data format too short: %d bytes", len(data))
	}
	be := binary.BigEndian
	numFields := int(be.Uint32(data[4:8]))
	if numFields == 0 {
		return nil, nil
	}
	const perFieldFixed = 48
	const headerLen = 16
	if numFields < 0 || numFields > 1<<16 {
		return nil, fmt.Errorf("hostserver: super-extended data format implausible field count: %d", numFields)
	}
	if len(data) < headerLen+numFields*perFieldFixed {
		return nil, fmt.Errorf("hostserver: super-extended data format truncated: have %d bytes, need >= %d for %d fields",
			len(data), headerLen+numFields*perFieldFixed, numFields)
	}

	cols := make([]SelectColumn, 0, numFields)
	for i := 0; i < numFields; i++ {
		base := headerLen + i*perFieldFixed
		col := SelectColumn{
			SQLType:   be.Uint16(data[base+2 : base+4]),
			Length:    be.Uint32(data[base+4 : base+8]),
			Scale:     be.Uint16(data[base+8 : base+10]),
			Precision: be.Uint16(data[base+10 : base+12]),
			CCSID:     be.Uint16(data[base+12 : base+14]),
		}
		// Field name lives in a variable-info LL/CP record reached
		// via the offset at base+32. JTOpen reads the LL from
		// (base + offsetToVar) and the CP from (base + offsetToVar
		// + 4); the CP we want is 0x3840 (field name).
		offToVar := int(be.Uint32(data[base+32 : base+36]))
		varLen := int(be.Uint32(data[base+36 : base+40]))
		if offToVar > 0 && varLen > 0 {
			name, err := readSuperExtendedFieldName(data, base, offToVar, varLen)
			if err != nil {
				// Don't fail the whole parse for a bad name --
				// just leave it empty and continue. M5 column
				// metadata will need stricter handling.
				col.Name = ""
			} else {
				col.Name = name
			}
		}
		// M5: derived metadata. JT400 pairs each SQL type with
		// even = NN, odd = nullable; type name + display size are
		// computed from (SQLType, Length, Precision, Scale).
		col.TypeName, col.DisplaySize, col.Signed = sqlTypeMetadata(col.SQLType, col.Length, col.Precision, col.Scale)
		col.Nullable = col.SQLType%2 == 1
		cols = append(cols, col)
	}
	return cols, nil
}

// sqlTypeMetadata returns the JDBC-style type name, display size,
// and signedness for a given (raw IBM i SQL type, length,
// precision, scale) tuple. Mirrors java.sql.Types names so the
// goldens (typeName field) and database/sql ColumnType.DatabaseTypeName
// match without further translation.
func sqlTypeMetadata(sqlType uint16, length uint32, precision, scale uint16) (typeName string, displaySize int, signed bool) {
	switch sqlType {
	case 384, 385:
		return "DATE", 10, false
	case 388, 389:
		return "TIME", 8, false
	case 392, 393:
		return "TIMESTAMP", 26, false
	case 448, 449:
		// Wire descriptor stuffs VARCHAR's max declared chars in
		// the "scale" slot (offset 8-9 of the per-field record);
		// Length is wire bytes = scale + 2 SL prefix bytes.
		// JDBC ResultSetMetaData exposes the max chars as
		// precision/displaySize, which is what we surface.
		return "VARCHAR", int(scale), false
	case 452, 453, 456, 457, 460, 461:
		return "CHAR", int(length), false
	case 480, 481:
		switch length {
		case 4:
			return "REAL", 13, true
		case 8:
			return "DOUBLE", 22, true
		}
		return "FLOAT", 22, true
	case 484, 485:
		// DECIMAL(p, s) -- worst case "-DDD.DDDD" = precision + 2.
		return "DECIMAL", int(precision) + 2, true
	case 488, 489:
		return "NUMERIC", int(precision) + 2, true
	case 492, 493:
		return "BIGINT", 20, true
	case 496, 497:
		return "INTEGER", 11, true
	case 500, 501:
		return "SMALLINT", 6, true
	case 996, 997:
		switch length {
		case 8:
			return "DECFLOAT", 23, true
		case 16:
			return "DECFLOAT", 42, true
		}
		return "DECFLOAT", int(precision) + 8, true
	}
	return fmt.Sprintf("UNKNOWN(%d)", sqlType), int(length), false
}

// readSuperExtendedFieldName walks the variable-info chain looking
// for CP 0x3840 (field name). Mirrors DBSuperExtendedDataFormat
// .findCodePoint + .getFieldName.
func readSuperExtendedFieldName(data []byte, fieldBase, offToVar, varLen int) (string, error) {
	be := binary.BigEndian
	// "Length of variable info for the first CP" lives at
	// data[fieldBase + offToVar].
	pos := 0
	for pos < varLen {
		entryStart := fieldBase + offToVar + pos
		if entryStart+6 > len(data) {
			return "", fmt.Errorf("variable info ran past payload at field %d", fieldBase)
		}
		ll := int(be.Uint32(data[entryStart : entryStart+4]))
		if ll < 8 || entryStart+ll > len(data) {
			return "", fmt.Errorf("bad variable-info LL %d at offset %d", ll, entryStart)
		}
		cp := be.Uint16(data[entryStart+4 : entryStart+6])
		if cp == 0x3840 {
			// Layout: LL(4) + CP(2) + CCSID(2) + name bytes
			ccsid := be.Uint16(data[entryStart+6 : entryStart+8])
			nameBytes := data[entryStart+8 : entryStart+ll]
			return decodeFieldNameByCCSID(ccsid, nameBytes), nil
		}
		pos += ll
	}
	return "", fmt.Errorf("CP 0x3840 (field name) not found in variable info")
}

// decodeFieldNameByCCSID converts the field-name bytes to a Go
// string. PUB400 sends column names in CCSID 273 (German EBCDIC)
// for German-installed systems; CCSID 37 (US English) for US.
// Both share the basic A-Z / 0-9 / _ characters at the same code
// points, so for our SELECT_DUMMY case CCSID 37 decoding works
// fine for either. Long term M4 should swap in a CCSID-273 codec.
func decodeFieldNameByCCSID(ccsid uint16, b []byte) string {
	// CCSID 37 (US English EBCDIC) is what we have a codec for; if
	// the server tags the name with 273 (German) the basic uppercase
	// + digit + underscore chars are at the same code points so this
	// still works for SELECT_DUMMY-class identifiers. M4 expansion
	// adds proper CCSID 273.
	s, _ := ebcdic.CCSID37.Decode(b)
	return s
}
