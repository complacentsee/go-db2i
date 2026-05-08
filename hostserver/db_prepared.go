package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// SQL function IDs in the descriptor (0x1E__) family. JTOpen splits
// "describe parameters" off from "execute statement": between PREPARE
// and OPEN, the client sends a CHANGE_DESCRIPTOR (0x1E00) frame
// carrying the input-parameter shape via CP 0x381E
// (DBExtendedDataFormat). Without it, the OPEN that ships parameter
// values via CP 0x381F (DBExtendedData) has no shape to bind to.
const (
	ReqDBSQLChangeDescriptor uint16 = 0x1E00
	ReqDBSQLDeleteDescriptor uint16 = 0x1E01

	cpDBExtendedDataFormat uint16 = 0x381E // input parameter shapes
	cpDBExtendedData       uint16 = 0x381F // input parameter values
	cpDBSyncPointDelimiter uint16 = 0x3814 // 1-byte; OPEN trailer in prepared flow
)

// PreparedParam is one bound parameter for SelectPreparedSQL. SQLType,
// FieldLength, Precision, Scale, CCSID describe the wire shape; Value
// is the Go value to bind (currently only int32 is supported -- M3
// scope; M4 widens to int64/float64/string/decimal/time).
//
// For nullable types, set Nullable=true and the encoder sets the JTOpen
// "nullable" SQL type flavour (e.g. 497 for INTEGER nullable rather
// than 496).
type PreparedParam struct {
	SQLType     uint16
	FieldLength uint32
	Precision   uint16
	Scale       uint16
	CCSID       uint16
	ParamType   byte
	Value       any
}

// EncodeDBExtendedDataFormat builds the CP 0x381E payload (parameter
// shape descriptor):
//
//	header (16 bytes):
//	  0..3   consistency token (always 1)
//	  4..7   number of fields
//	  8..11  reserved
//	  12..15 row size (sum of FieldLength across fields)
//	per-field record (64 bytes each):
//	  0..1   field description length (always 64)
//	  2..3   SQL type
//	  4..7   field length
//	  8..9   scale
//	  10..11 precision
//	  12..13 CCSID
//	  14     parameter type
//	  15..16 reserved
//	  17..20 LOB locator
//	  21..29 reserved
//	  30..33 LOB max size
//	  34..45 reserved
//	  46..47 field name length
//	  48..49 field name CCSID
//	  50..63 field name (max 14 bytes)
func EncodeDBExtendedDataFormat(params []PreparedParam) []byte {
	const (
		headerLen      = 16
		perFieldLen    = 64
		fieldDescLen   = 64
		consistencyTok = 1
	)
	rowSize := uint32(0)
	for _, p := range params {
		rowSize += p.FieldLength
	}

	buf := make([]byte, headerLen+perFieldLen*len(params))
	be := binary.BigEndian

	be.PutUint32(buf[0:4], consistencyTok)
	be.PutUint32(buf[4:8], uint32(len(params)))
	// 8..11 reserved zero.
	be.PutUint32(buf[12:16], rowSize)

	for i, p := range params {
		base := headerLen + i*perFieldLen
		be.PutUint16(buf[base+0:base+2], fieldDescLen)
		be.PutUint16(buf[base+2:base+4], p.SQLType)
		be.PutUint32(buf[base+4:base+8], p.FieldLength)
		be.PutUint16(buf[base+8:base+10], p.Scale)
		be.PutUint16(buf[base+10:base+12], p.Precision)
		be.PutUint16(buf[base+12:base+14], p.CCSID)
		buf[base+14] = p.ParamType
		// Remaining bytes (15..63) zero.
	}
	return buf
}

// EncodeDBExtendedData builds the CP 0x381F payload (parameter values
// for one or more bound rows). For M3 we only emit a single row of
// values, since database/sql QueryContext binds one tuple per call.
//
//	header (20 bytes):
//	  0..3   consistency token (always 1)
//	  4..7   row count (always 1 for M3)
//	  8..9   column count
//	  10..11 indicator size (always 2)
//	  12..15 reserved (compression flag in JTOpen; 0 here)
//	  16..19 row size (sum of FieldLength across columns)
//	indicator block (rowCount * colCount * indicatorSize bytes):
//	  one int16 per column-per-row; 0 = not-null, -1 = null
//	data block (rowCount * rowSize bytes):
//	  fields packed in declaration order
//
// values must have the same length as params and supply one Go value
// per parameter slot (currently int32 only).
func EncodeDBExtendedData(params []PreparedParam, values []any) ([]byte, error) {
	if len(params) != len(values) {
		return nil, fmt.Errorf("hostserver: param count mismatch: shape has %d, values has %d", len(params), len(values))
	}
	const (
		headerLen      = 20
		indicatorSize  = 2
		consistencyTok = 1
		rowCount       = 1
	)
	rowSize := uint32(0)
	for _, p := range params {
		rowSize += p.FieldLength
	}
	indicatorBytes := rowCount * len(params) * indicatorSize
	dataBytes := int(rowSize)
	buf := make([]byte, headerLen+indicatorBytes+dataBytes)
	be := binary.BigEndian

	be.PutUint32(buf[0:4], consistencyTok)
	be.PutUint32(buf[4:8], rowCount)
	be.PutUint16(buf[8:10], uint16(len(params)))
	be.PutUint16(buf[10:12], indicatorSize)
	// 12..15 reserved (compression flag) zero.
	be.PutUint32(buf[16:20], rowSize)

	// Indicators: 0 = not null, -1 (0xFFFF) = null per JT400.
	// We pre-fill from the values array so the data-pack loop
	// only handles non-null encoders.
	for i := 0; i < len(params); i++ {
		off := headerLen + i*indicatorSize
		if values[i] == nil {
			be.PutUint16(buf[off:off+2], 0xFFFF)
		} else {
			be.PutUint16(buf[off:off+2], 0)
		}
	}

	// Pack values. Walk params in declaration order, writing each
	// at the running data offset; null params advance the offset
	// without writing (server reads the indicator first).
	dataOff := headerLen + indicatorBytes
	for i, p := range params {
		v := values[i]
		if v == nil {
			dataOff += int(p.FieldLength)
			continue
		}
		switch p.SQLType {
		case 500, 501: // SMALLINT (NN, nullable)
			iv, err := toInt32(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			if iv < -1<<15 || iv > 1<<15-1 {
				return nil, fmt.Errorf("hostserver: param %d: smallint value %d overflows int16", i, iv)
			}
			be.PutUint16(buf[dataOff:dataOff+2], uint16(int16(iv)))
			dataOff += 2
		case 496, 497: // INTEGER (NN, nullable)
			iv, err := toInt32(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			be.PutUint32(buf[dataOff:dataOff+4], uint32(iv))
			dataOff += 4
		case 492, 493: // BIGINT (NN, nullable)
			iv, err := toInt64(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			be.PutUint64(buf[dataOff:dataOff+8], uint64(iv))
			dataOff += 8
		case 384, 385: // DATE
			s, err := toString(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			wire, err := encodeDateString(s, int(p.FieldLength))
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			ebc, err := ebcdic.CCSID37.Encode(wire)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: encode date: %w", i, err)
			}
			copy(buf[dataOff:dataOff+len(ebc)], ebc)
			dataOff += int(p.FieldLength)
		case 388, 389: // TIME
			s, err := toString(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			wire, err := encodeTimeString(s, int(p.FieldLength))
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			ebc, err := ebcdic.CCSID37.Encode(wire)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: encode time: %w", i, err)
			}
			copy(buf[dataOff:dataOff+len(ebc)], ebc)
			dataOff += int(p.FieldLength)
		case 392, 393: // TIMESTAMP
			s, err := toString(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			wire, err := encodeTimestampString(s, int(p.FieldLength))
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			ebc, err := ebcdic.CCSID37.Encode(wire)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: encode timestamp: %w", i, err)
			}
			copy(buf[dataOff:dataOff+len(ebc)], ebc)
			dataOff += int(p.FieldLength)
		case 996, 997: // DECFLOAT -- decimal64 (FieldLength 8) or decimal128 (16)
			s, err := toString(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			negative, digs, exp, err := parseDecFloatString(s)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			var packed []byte
			switch p.FieldLength {
			case 8:
				packed, err = encodeDecimal64(negative, digs, exp)
			case 16:
				packed, err = encodeDecimal128(negative, digs, exp)
			default:
				return nil, fmt.Errorf("hostserver: param %d: decfloat FieldLength %d unsupported (need 8 or 16)", i, p.FieldLength)
			}
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			copy(buf[dataOff:dataOff+len(packed)], packed)
			dataOff += len(packed)
		case 488, 489: // NUMERIC(p,s) zoned decimal
			s, err := toDecimalString(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			zoned, err := encodeZonedBCD(s, int(p.Precision), int(p.Scale))
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d (numeric(%d,%d)): %w", i, p.Precision, p.Scale, err)
			}
			if uint32(len(zoned)) != p.FieldLength {
				return nil, fmt.Errorf("hostserver: param %d (numeric(%d,%d)): zoned bytes %d != FieldLength %d",
					i, p.Precision, p.Scale, len(zoned), p.FieldLength)
			}
			copy(buf[dataOff:dataOff+len(zoned)], zoned)
			dataOff += len(zoned)
		case 484, 485: // DECIMAL(p,s) packed BCD
			s, err := toDecimalString(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			packed, err := encodePackedBCD(s, int(p.Precision), int(p.Scale))
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d (decimal(%d,%d)): %w", i, p.Precision, p.Scale, err)
			}
			if uint32(len(packed)) != p.FieldLength {
				return nil, fmt.Errorf("hostserver: param %d (decimal(%d,%d)): packed bytes %d != FieldLength %d",
					i, p.Precision, p.Scale, len(packed), p.FieldLength)
			}
			copy(buf[dataOff:dataOff+len(packed)], packed)
			dataOff += len(packed)
		case 480, 481: // REAL/DOUBLE (NN, nullable) -- length picks the width
			fv, err := toFloat64(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			switch p.FieldLength {
			case 4:
				be.PutUint32(buf[dataOff:dataOff+4], math.Float32bits(float32(fv)))
				dataOff += 4
			case 8:
				be.PutUint64(buf[dataOff:dataOff+8], math.Float64bits(fv))
				dataOff += 8
			default:
				return nil, fmt.Errorf("hostserver: param %d: float type 480 wants FieldLength 4 or 8, got %d", i, p.FieldLength)
			}
		case 448, 449: // VARCHAR (NN, nullable)
			// VARCHAR wire layout: 2-byte SL + payload bytes,
			// padded out to FieldLength-2 bytes. CCSID determines
			// payload encoding:
			//   65535       -- FOR BIT DATA, binary passthrough
			//   1208        -- UTF-8, passthrough (server transcodes
			//                  to the column CCSID on its side)
			//   else        -- EBCDIC via the SBCS converter
			var payload []byte
			if p.CCSID == ccsidBinary {
				bv, err := toBytes(v)
				if err != nil {
					return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
				}
				payload = bv
			} else if p.CCSID == 1208 {
				sv, err := toString(v)
				if err != nil {
					return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
				}
				payload = []byte(sv)
			} else {
				sv, err := toString(v)
				if err != nil {
					return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
				}
				conv := ebcdicForCCSID(p.CCSID)
				ebc, err := conv.Encode(sv)
				if err != nil {
					return nil, fmt.Errorf("hostserver: param %d: encode varchar: %w", i, err)
				}
				payload = ebc
			}
			maxBytes := int(p.FieldLength) - 2
			if len(payload) > maxBytes {
				return nil, fmt.Errorf("hostserver: param %d: varchar value too long (%d bytes, max %d)", i, len(payload), maxBytes)
			}
			be.PutUint16(buf[dataOff:dataOff+2], uint16(len(payload)))
			copy(buf[dataOff+2:dataOff+2+len(payload)], payload)
			// Remaining bytes left zero (server reads SL).
			dataOff += int(p.FieldLength)
		default:
			return nil, fmt.Errorf("hostserver: param %d: SQL type %d not yet supported (M3 covers int/varchar)", i, p.SQLType)
		}
	}
	return buf, nil
}

// ChangeDescriptorRequest builds the 0x1E00 frame body that uploads
// the input-parameter shape to the RPB created by CREATE_RPB. JTOpen
// sends this between PREPARE_DESCRIBE and OPEN_DESCRIBE_FETCH on every
// prepared SELECT with parameters; without it OPEN's CP 0x381F has no
// matching shape on the server side.
func ChangeDescriptorRequest(params []PreparedParam) (Header, []byte, error) {
	tpl := DBRequestTemplate{
		ORSBitmap:                 0x00040000, // RLE only -- fire and forget like CREATE_RPB
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 1,
	}
	descriptor := EncodeDBExtendedDataFormat(params)
	return BuildDBRequest(ReqDBSQLChangeDescriptor, tpl, []DBParam{
		{CodePoint: cpDBExtendedDataFormat, Data: descriptor},
	})
}

// SelectPreparedSQL runs the JTOpen prepared-SELECT flow:
//
//  1. CREATE_RPB        (1D00)
//  2. PREPARE_DESCRIBE  (1803)  -- with ORSParameterMarkerFmt set
//  3. CHANGE_DESCRIPTOR (1E00)  -- with input-param shapes
//  4. OPEN_DESCRIBE_FETCH (180E) -- with input-param values
//
// nextCorrelation is the starting CorrelationID; the four frames
// consume nextCorrelation through nextCorrelation+3.
//
// M3 scope: int32 and string parameters; null-bind, decimal, and
// time/timestamp land with M4. The function returns the parsed
// SelectResult identical to SelectStaticSQL.
func SelectPreparedSQL(conn io.ReadWriter, sql string, paramShapes []PreparedParam, paramValues []any, nextCorrelation uint32) (*SelectResult, error) {
	cursor, err := OpenSelectPrepared(conn, sql, paramShapes, paramValues, closureFromInt(nextCorrelation))
	if err != nil {
		return nil, err
	}
	return cursor.drainAll()
}

// openPreparedUntilFirstBatch is the shared implementation used by
// SelectPreparedSQL (drain-all) and OpenSelectPrepared (cursor).
// Mirrors openStaticUntilFirstBatch but with the CHANGE_DESCRIPTOR
// + bound-value frames between PREPARE and OPEN. Frees the RPB on
// any error after CREATE_RPB.
func openPreparedUntilFirstBatch(conn io.ReadWriter, sql string, paramShapes []PreparedParam, paramValues []any, nextCorr func() uint32) ([]SelectColumn, []SelectRow, error) {
	if !strings.ContainsRune(sql, '?') {
		return nil, nil, fmt.Errorf("hostserver: OpenSelectPrepared called on SQL without parameter markers; use OpenSelectStatic")
	}
	if len(paramShapes) != len(paramValues) {
		return nil, nil, fmt.Errorf("hostserver: shape/value count mismatch (%d shapes, %d values)", len(paramShapes), len(paramValues))
	}

	// --- 1) CREATE_RPB. ---
	stmtNameBytes, err := ebcdic.CCSID37.Encode("STMT0001")
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: encode stmt name: %w", err)
	}
	cursorNameBytes, err := ebcdic.CCSID37.Encode("CRSR0001")
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: encode cursor name: %w", err)
	}
	{
		tpl := DBRequestTemplate{
			ORSBitmap:                 0x00040000,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLRPBCreate, tpl, []DBParam{
			DBParamVarString(cpDBPrepareStatementName, 273, stmtNameBytes),
			DBParamVarString(cpDBCursorName, 273, cursorNameBytes),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("hostserver: build CREATE_RPB: %w", err)
		}
		hdr.CorrelationID = nextCorr()
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, nil, fmt.Errorf("hostserver: send CREATE_RPB: %w", err)
		}
	}

	// --- 2) PREPARE_DESCRIBE (with ORSParameterMarkerFmt). ---
	stmtBytes := utf16BE(sql)
	prepCorr := nextCorr()
	{
		tpl := DBRequestTemplate{
			ORSBitmap:                 ORSReturnData | ORSDataFormat | ORSSQLCA | ORSParameterMarkerFmt | 0x00040000,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLPrepareDescribe, tpl, []DBParam{
			dbParamExtendedString(cpDBExtendedStmtText, 13488, stmtBytes),
			DBParamShort(cpDBStatementType, 2),
			DBParamByte(cpDBPrepareOption, 0x00),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("hostserver: build PREPARE_DESCRIBE: %w", err)
		}
		hdr.CorrelationID = prepCorr
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, nil, fmt.Errorf("hostserver: send PREPARE_DESCRIBE: %w", err)
		}
	}
	prepRepHdr, prepRepPayload, err := ReadDBReplyMatching(conn, prepCorr, 8)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: read PREPARE_DESCRIBE reply: %w", err)
	}
	if prepRepHdr.ReqRepID != RepDBReply {
		return nil, nil, fmt.Errorf("hostserver: PREPARE_DESCRIBE reply ReqRepID 0x%04X (want 0x%04X)", prepRepHdr.ReqRepID, RepDBReply)
	}
	prepRep, err := ParseDBReply(prepRepPayload)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: parse PREPARE_DESCRIBE reply: %w", err)
	}
	if dbErr := makeDb2Error(prepRep, "PREPARE_DESCRIBE"); dbErr != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, dbErr
	}
	cols, err := prepRep.findSuperExtendedDataFormat()
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fmt.Errorf("hostserver: parse column descriptors: %w", err)
	}

	// --- 3) CHANGE_DESCRIPTOR. ---
	{
		hdr, payload, err := ChangeDescriptorRequest(paramShapes)
		if err != nil {
			_ = deleteRPB(conn, nextCorr())
			return nil, nil, fmt.Errorf("hostserver: build CHANGE_DESCRIPTOR: %w", err)
		}
		hdr.CorrelationID = nextCorr()
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, nil, fmt.Errorf("hostserver: send CHANGE_DESCRIPTOR: %w", err)
		}
	}

	// --- 4) OPEN_DESCRIBE_FETCH with input parameters. ---
	dataPayload, err := EncodeDBExtendedData(paramShapes, paramValues)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fmt.Errorf("hostserver: encode input parameter data: %w", err)
	}
	var fetchCorr uint32
	{
		tpl := DBRequestTemplate{
			ORSBitmap:                 ORSReturnData | ORSResultData | ORSSQLCA | 0x00040000 | 0x00008000,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 1, // referencing the descriptor we just changed
		}
		params := []DBParam{
			DBParamByte(cpDBOpenAttributes, 0x80),
			DBParamByte(cpDBVariableFieldCompr, 0xE8),
			{CodePoint: cpDBBufferSize, Data: []byte{0x00, 0x00, 0x80, 0x00}},
			DBParamShort(cpDBScrollableCursorFlag, 0x0000),
			DBParamByte(cpDBResultSetHoldability, 0xE8),
			DBParamShort(cpDBStatementType, 2), // SELECT
			{CodePoint: cpDBExtendedData, Data: dataPayload},
			// 0x3814 is a 2-byte short in JTOpen's prepared
			// SELECT trailer (LL=8 in the fixture, not LL=7),
			// not the 1-byte field its name might imply.
			DBParamShort(cpDBSyncPointDelimiter, 0x0000),
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLOpenDescribeFetch, tpl, params)
		if err != nil {
			_ = deleteRPB(conn, nextCorr())
			return nil, nil, fmt.Errorf("hostserver: build OPEN_DESCRIBE_FETCH: %w", err)
		}
		fetchCorr = nextCorr()
		hdr.CorrelationID = fetchCorr
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, nil, fmt.Errorf("hostserver: send OPEN_DESCRIBE_FETCH: %w", err)
		}
	}
	fetchRepHdr, fetchRepPayload, err := ReadDBReplyMatching(conn, fetchCorr, 8)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: read OPEN_DESCRIBE_FETCH reply: %w", err)
	}
	if fetchRepHdr.ReqRepID != RepDBReply {
		return nil, nil, fmt.Errorf("hostserver: OPEN_DESCRIBE_FETCH reply ReqRepID 0x%04X (want 0x%04X)", fetchRepHdr.ReqRepID, RepDBReply)
	}
	fetchRep, err := ParseDBReply(fetchRepPayload)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: parse OPEN_DESCRIBE_FETCH reply: %w", err)
	}
	if dbErr := makeDb2Error(fetchRep, "OPEN_DESCRIBE_FETCH"); dbErr != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, dbErr
	}
	rows, err := fetchRep.findExtendedResultData(cols)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fmt.Errorf("hostserver: parse row data: %w", err)
	}
	return cols, rows, nil
}

// toInt32 narrows common Go integer types into int32 for INTEGER
// parameter binding.
func toInt32(v any) (int32, error) {
	switch x := v.(type) {
	case int32:
		return x, nil
	case int:
		if int64(x) < -1<<31 || int64(x) > 1<<31-1 {
			return 0, fmt.Errorf("int value %d overflows int32", x)
		}
		return int32(x), nil
	case int64:
		if x < -1<<31 || x > 1<<31-1 {
			return 0, fmt.Errorf("int64 value %d overflows int32", x)
		}
		return int32(x), nil
	case int16:
		return int32(x), nil
	case int8:
		return int32(x), nil
	default:
		return 0, fmt.Errorf("cannot bind %T as INTEGER (need int/int32)", v)
	}
}

// toInt64 widens common Go integer types into int64 for BIGINT
// parameter binding.
func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int8:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("cannot bind %T as BIGINT (need int/int64)", v)
	}
}

// toFloat64 widens common Go numeric types into float64 for
// REAL/DOUBLE parameter binding. Integer inputs are exact for the
// IEEE 754 double range; pure float32 inputs upcast losslessly.
func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("cannot bind %T as REAL/DOUBLE (need float)", v)
	}
}

// toString narrows common Go string types for VARCHAR binding.
func toString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	default:
		return "", fmt.Errorf("cannot bind %T as VARCHAR (need string)", v)
	}
}

// toBytes narrows common Go byte-slice types for FOR BIT DATA
// (CCSID 65535) binding. Strings are accepted as a convenience but
// reinterpreted as their underlying byte sequence -- callers passing
// arbitrary text into a binary column is a footgun, but matches the
// JDBC behaviour.
func toBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("cannot bind %T as VARCHAR FOR BIT DATA (need []byte)", v)
	}
}

// toDecimalString narrows numeric inputs for DECIMAL binding. We
// accept strings (the canonical form, since DECIMAL(31,5) overflows
// every primitive Go numeric type) plus int/int64/float64 as
// conveniences.
func toDecimalString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case int:
		return fmt.Sprintf("%d", x), nil
	case int32:
		return fmt.Sprintf("%d", x), nil
	case int64:
		return fmt.Sprintf("%d", x), nil
	case float64:
		return fmt.Sprintf("%g", x), nil
	default:
		return "", fmt.Errorf("cannot bind %T as DECIMAL (need string or numeric)", v)
	}
}

// encodeDateString converts an ISO date "YYYY-MM-DD" into the wire
// form the column expects, picking the format from fieldLen alone.
// 10-char output is ISO ("YYYY-MM-DD"); 8-char is YMD ("YY-MM-DD").
// Use encodeDateStringForFormat to emit USA / EUR / JIS / MDY / DMY
// when the session has negotiated a non-default date format.
func encodeDateString(s string, fieldLen int) (string, error) {
	switch fieldLen {
	case 10:
		return encodeDateStringForFormat(s, DateFormatISO)
	case 8:
		return encodeDateStringForFormat(s, DateFormatYMD)
	default:
		return "", fmt.Errorf("date FieldLength %d unsupported (need 8 YMD or 10 ISO)", fieldLen)
	}
}

// encodeDateStringForFormat formats an ISO date "YYYY-MM-DD" into the
// wire bytes for the given target IBM i date format. The format byte
// matches DBAttributesOptions.DateFormat (one of the DateFormat*
// constants); the wire layouts mirror IBM i's documented format
// names:
//
//	*ISO  10 chars  YYYY-MM-DD
//	*USA  10 chars  MM/DD/YYYY
//	*EUR  10 chars  DD.MM.YYYY
//	*JIS  10 chars  YYYY-MM-DD  (identical to ISO on the wire)
//	*MDY  8  chars  MM/DD/YY
//	*DMY  8  chars  DD/MM/YY
//	*YMD  8  chars  YY-MM-DD
//	*JOB  -         server-picks; not directly encodable. Caller must
//	                resolve to one of the above via the session's
//	                negotiated date-format CP before calling.
//
// The 8-char (*MDY/*DMY/*YMD) variants drop the century. Year-2000
// boundary handling is the SERVER's job once the bytes arrive --
// JT400 uses 1940 as the cutover (00..39 -> 20YY, 40..99 -> 19YY)
// per the captured fixtures.
//
// Returns an error if `iso` isn't a valid 10-char ISO date or if
// `format` isn't one of the supported constants. *JUL (Julian
// "YY/DDD") is intentionally not supported -- no captured workload
// uses it.
func encodeDateStringForFormat(iso string, format byte) (string, error) {
	if len(iso) != 10 || iso[4] != '-' || iso[7] != '-' {
		return "", fmt.Errorf("date %q must be ISO YYYY-MM-DD", iso)
	}
	yyyy, mm, dd := iso[0:4], iso[5:7], iso[8:10]
	yy := iso[2:4]
	switch format {
	case DateFormatISO, DateFormatJIS:
		return yyyy + "-" + mm + "-" + dd, nil
	case DateFormatUSA:
		return mm + "/" + dd + "/" + yyyy, nil
	case DateFormatEUR:
		return dd + "." + mm + "." + yyyy, nil
	case DateFormatMDY:
		return mm + "/" + dd + "/" + yy, nil
	case DateFormatDMY:
		return dd + "/" + mm + "/" + yy, nil
	case DateFormatYMD:
		return yy + "-" + mm + "-" + dd, nil
	case DateFormatJOB:
		return "", fmt.Errorf("date format *JOB cannot be encoded directly -- caller must resolve to a concrete format first")
	default:
		return "", fmt.Errorf("unknown date format byte 0x%02X", format)
	}
}

// encodeTimeString converts ISO time "HH:MM:SS" into the wire form.
// fieldLen 8 only; ISO ":" and IBM "." are both accepted as input
// to make caller code tolerant. Wire goes out with ":" since PUB400
// connects in ISO time format by default; switch to "." when we
// negotiate a different format via SET_SQL_ATTRIBUTES (M5 work).
func encodeTimeString(s string, fieldLen int) (string, error) {
	if fieldLen != 8 {
		return "", fmt.Errorf("time FieldLength %d unsupported (need 8)", fieldLen)
	}
	if len(s) != 8 {
		return "", fmt.Errorf("time %q must be 8 chars HH:MM:SS or HH.MM.SS", s)
	}
	// Normalise separators to ':' (ISO).
	out := []byte(s)
	if out[2] == '.' {
		out[2] = ':'
	}
	if out[5] == '.' {
		out[5] = ':'
	}
	return string(out), nil
}

// encodeTimestampString converts an ISO timestamp
// "YYYY-MM-DDTHH:MM:SS.ffffff" (or "YYYY-MM-DD HH:MM:SS.ffffff")
// into IBM wire form "YYYY-MM-DD-HH.MM.SS.ffffff" (26 chars). The
// IBM wire form uses '-' between date and time and '.' between
// time fields; we always emit it because the existing
// SET_SQL_ATTRIBUTES we ship asks for IBM-format timestamps.
func encodeTimestampString(s string, fieldLen int) (string, error) {
	if fieldLen != 26 {
		return "", fmt.Errorf("timestamp FieldLength %d unsupported (need 26)", fieldLen)
	}
	if len(s) != 26 {
		return "", fmt.Errorf("timestamp %q must be 26 chars", s)
	}
	out := []byte(s)
	// Date/time separator: ISO 'T', alt ' ', IBM '-'.
	if out[10] == 'T' || out[10] == ' ' {
		out[10] = '-'
	}
	if out[10] != '-' {
		return "", fmt.Errorf("timestamp %q has unexpected date/time separator %q at offset 10", s, out[10])
	}
	// Time field separators: ISO ':', IBM '.'.
	if out[13] == ':' {
		out[13] = '.'
	}
	if out[16] == ':' {
		out[16] = '.'
	}
	return string(out), nil
}

// encodeZonedBCD turns a decimal string into IBM zoned BCD bytes
// for a NUMERIC(precision, scale) column. One byte per digit; high
// nibble is 0xF (zone) for plain digits and 0xC/0xD on the last
// byte for sign. Returns precision bytes.
func encodeZonedBCD(s string, precision, scale int) ([]byte, error) {
	negative := false
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		negative = s[0] == '-'
		s = s[1:]
	}
	intPart, fracPart := s, ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	}
	for i := 0; i < len(intPart); i++ {
		if intPart[i] < '0' || intPart[i] > '9' {
			return nil, fmt.Errorf("non-digit %q in integer part", intPart[i])
		}
	}
	for i := 0; i < len(fracPart); i++ {
		if fracPart[i] < '0' || fracPart[i] > '9' {
			return nil, fmt.Errorf("non-digit %q in fractional part", fracPart[i])
		}
	}
	for len(intPart) > 1 && intPart[0] == '0' {
		intPart = intPart[1:]
	}
	if len(fracPart) > scale {
		return nil, fmt.Errorf("fractional digit count %d exceeds scale %d", len(fracPart), scale)
	}
	for len(fracPart) < scale {
		fracPart += "0"
	}
	intWidth := precision - scale
	if len(intPart) > intWidth {
		return nil, fmt.Errorf("integer digit count %d exceeds precision-scale = %d", len(intPart), intWidth)
	}
	for len(intPart) < intWidth {
		intPart = "0" + intPart
	}
	digits := intPart + fracPart // exactly `precision` digits

	out := make([]byte, precision)
	for i := 0; i < precision; i++ {
		out[i] = 0xF0 | (digits[i] - '0')
	}
	if negative {
		out[precision-1] = 0xD0 | (digits[precision-1] - '0')
	}
	return out, nil
}

// encodePackedBCD turns a decimal string ("[-]DDD[.DDD]") into IBM
// packed BCD bytes for a DECIMAL(precision, scale) column. The
// returned byte count is ceil((precision+1)/2). Sign nibble in the
// final low nibble: 0xC = positive, 0xD = negative.
//
// Rejects values whose integer or fractional digit counts exceed
// the column declaration; trims a leading '+' for symmetry.
func encodePackedBCD(s string, precision, scale int) ([]byte, error) {
	negative := false
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		negative = s[0] == '-'
		s = s[1:]
	}
	intPart, fracPart := s, ""
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		intPart = s[:dot]
		fracPart = s[dot+1:]
	}
	// Validate digits.
	for i := 0; i < len(intPart); i++ {
		if intPart[i] < '0' || intPart[i] > '9' {
			return nil, fmt.Errorf("non-digit %q in integer part", intPart[i])
		}
	}
	for i := 0; i < len(fracPart); i++ {
		if fracPart[i] < '0' || fracPart[i] > '9' {
			return nil, fmt.Errorf("non-digit %q in fractional part", fracPart[i])
		}
	}
	// Trim leading zeros from int part (keep at least one).
	for len(intPart) > 1 && intPart[0] == '0' {
		intPart = intPart[1:]
	}
	// Right-pad fractional part with zeros to scale; reject overflow.
	if len(fracPart) > scale {
		return nil, fmt.Errorf("fractional digit count %d exceeds scale %d", len(fracPart), scale)
	}
	for len(fracPart) < scale {
		fracPart += "0"
	}
	// Validate integer width.
	intWidth := precision - scale
	if len(intPart) > intWidth {
		return nil, fmt.Errorf("integer digit count %d exceeds precision-scale = %d", len(intPart), intWidth)
	}
	// Left-pad intPart so total digit count == precision.
	for len(intPart) < intWidth {
		intPart = "0" + intPart
	}
	digits := intPart + fracPart // exactly `precision` digits

	// Pack: precision digits + 1 sign nibble = precision+1 nibbles.
	totalNibbles := precision + 1
	nbytes := (totalNibbles + 1) / 2
	out := make([]byte, nbytes)

	// If totalNibbles is odd we need a leading zero pad nibble.
	leadPad := 2*nbytes - totalNibbles // 0 or 1
	cursor := 0 // index into digits
	for i := 0; i < nbytes; i++ {
		var hi, lo byte
		if i == 0 && leadPad == 1 {
			hi = 0
		} else {
			hi = digits[cursor] - '0'
			cursor++
		}
		if i == nbytes-1 {
			// Last low nibble is sign.
			if negative {
				lo = 0x0D
			} else {
				lo = 0x0C
			}
		} else {
			lo = digits[cursor] - '0'
			cursor++
		}
		out[i] = (hi << 4) | lo
	}
	return out, nil
}

// ebcdicForCCSID picks the EBCDIC converter for a parameter's CCSID.
// M3 ships CCSID 37 (US) and 273 (German -- PUB400 default;
// currently a CCSID-37-table stand-in, see ebcdic.CCSID273 docs).
// Other CCSIDs (5026 Japan, 1140 Euro, ...) land with M4.
func ebcdicForCCSID(ccsid uint16) ebcdic.Codec {
	switch ccsid {
	case 273:
		return ebcdic.CCSID273
	default:
		return ebcdic.CCSID37
	}
}
