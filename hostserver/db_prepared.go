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

	// Indicators: 0 = not null. NULL handling lands when the type
	// system widens.
	for i := 0; i < len(params); i++ {
		off := headerLen + i*indicatorSize
		be.PutUint16(buf[off:off+2], 0)
	}

	// Pack values. Walk params in declaration order, writing each
	// at the running data offset.
	dataOff := headerLen + indicatorBytes
	for i, p := range params {
		v := values[i]
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
			sv, err := toString(v)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: %w", i, err)
			}
			// VARCHAR wire layout: 2-byte SL + EBCDIC bytes,
			// padded out to FieldLength-2 bytes.
			conv := ebcdicForCCSID(p.CCSID)
			ebc, err := conv.Encode(sv)
			if err != nil {
				return nil, fmt.Errorf("hostserver: param %d: encode varchar: %w", i, err)
			}
			maxBytes := int(p.FieldLength) - 2
			if len(ebc) > maxBytes {
				return nil, fmt.Errorf("hostserver: param %d: varchar value too long (%d bytes, max %d)", i, len(ebc), maxBytes)
			}
			be.PutUint16(buf[dataOff:dataOff+2], uint16(len(ebc)))
			copy(buf[dataOff+2:dataOff+2+len(ebc)], ebc)
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
	if !strings.ContainsRune(sql, '?') {
		return nil, fmt.Errorf("hostserver: SelectPreparedSQL called on SQL without parameter markers; use SelectStaticSQL")
	}
	if len(paramShapes) != len(paramValues) {
		return nil, fmt.Errorf("hostserver: shape/value count mismatch (%d shapes, %d values)", len(paramShapes), len(paramValues))
	}
	corr := nextCorrelation

	// --- 1) CREATE_RPB. ---
	stmtNameBytes, err := ebcdic.CCSID37.Encode("STMT0001")
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode stmt name: %w", err)
	}
	cursorNameBytes, err := ebcdic.CCSID37.Encode("CRSR0001")
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode cursor name: %w", err)
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
			return nil, fmt.Errorf("hostserver: build CREATE_RPB: %w", err)
		}
		hdr.CorrelationID = corr
		corr++
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send CREATE_RPB: %w", err)
		}
	}

	// --- 2) PREPARE_DESCRIBE (with ORSParameterMarkerFmt). ---
	stmtBytes := utf16BE(sql)
	prepCorr := corr
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
			return nil, fmt.Errorf("hostserver: build PREPARE_DESCRIBE: %w", err)
		}
		hdr.CorrelationID = prepCorr
		corr++
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send PREPARE_DESCRIBE: %w", err)
		}
	}
	prepRepHdr, prepRepPayload, err := ReadDBReplyMatching(conn, prepCorr, 8)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read PREPARE_DESCRIBE reply: %w", err)
	}
	if prepRepHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: PREPARE_DESCRIBE reply ReqRepID 0x%04X (want 0x%04X)", prepRepHdr.ReqRepID, RepDBReply)
	}
	prepRep, err := ParseDBReply(prepRepPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse PREPARE_DESCRIBE reply: %w", err)
	}
	if prepRep.ReturnCode != 0 && !isSQLWarning(prepRep.ReturnCode) {
		return nil, fmt.Errorf("hostserver: PREPARE_DESCRIBE RC=%d (0x%08X) errorClass=0x%04X",
			prepRep.ReturnCode, prepRep.ReturnCode, prepRep.ErrorClass)
	}
	cols, err := prepRep.findSuperExtendedDataFormat()
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse column descriptors: %w", err)
	}

	// --- 3) CHANGE_DESCRIPTOR. ---
	{
		hdr, payload, err := ChangeDescriptorRequest(paramShapes)
		if err != nil {
			return nil, fmt.Errorf("hostserver: build CHANGE_DESCRIPTOR: %w", err)
		}
		hdr.CorrelationID = corr
		corr++
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send CHANGE_DESCRIPTOR: %w", err)
		}
	}

	// --- 4) OPEN_DESCRIBE_FETCH with input parameters. ---
	dataPayload, err := EncodeDBExtendedData(paramShapes, paramValues)
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode input parameter data: %w", err)
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
			return nil, fmt.Errorf("hostserver: build OPEN_DESCRIBE_FETCH: %w", err)
		}
		fetchCorr = corr
		hdr.CorrelationID = fetchCorr
		corr++
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send OPEN_DESCRIBE_FETCH: %w", err)
		}
	}
	fetchRepHdr, fetchRepPayload, err := ReadDBReplyMatching(conn, fetchCorr, 8)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read OPEN_DESCRIBE_FETCH reply: %w", err)
	}
	if fetchRepHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: OPEN_DESCRIBE_FETCH reply ReqRepID 0x%04X (want 0x%04X)", fetchRepHdr.ReqRepID, RepDBReply)
	}
	fetchRep, err := ParseDBReply(fetchRepPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse OPEN_DESCRIBE_FETCH reply: %w", err)
	}
	if fetchRep.ReturnCode != 0 && !isSQLWarning(fetchRep.ReturnCode) {
		return nil, fmt.Errorf("hostserver: OPEN_DESCRIBE_FETCH RC=%d errorClass=0x%04X",
			fetchRep.ReturnCode, fetchRep.ErrorClass)
	}
	rows, err := fetchRep.findExtendedResultData(cols)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse row data: %w", err)
	}
	return &SelectResult{Columns: cols, Rows: rows}, nil
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
