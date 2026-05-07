package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode/utf16"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// SQL function IDs sent over the as-database (server ID 0xE004)
// service. We only define the ones we use right now.
const (
	ReqDBSQLPrepareDescribe   uint16 = 0x1803 // PREPARE + DESCRIBE in one shot
	ReqDBSQLFetch             uint16 = 0x180B // continuation FETCH from existing cursor
	ReqDBSQLOpenDescribeFetch uint16 = 0x180E // OPEN + DESCRIBE + FETCH
	ReqDBSQLClose             uint16 = 0x180A // CLOSE cursor
	ReqDBSQLRPBCreate         uint16 = 0x1D00 // CREATE Request Parameter Block
	ReqDBSQLRPBDelete         uint16 = 0x1D02 // DELETE RPB (frees RPB slot for next CREATE)
	ReqDBSQLDeleteResultsSet  uint16 = 0x1F01 // DELETE_RESULTS_SET
)

// SQLCode constants we treat specially across the result-set
// parser. JT400 + IBM i return positive SQLCODE values in the
// SQLCA template's ReturnCode field (low 32 bits) and signal
// "no more rows" with +100.
const (
	SQLCodeEndOfData int32 = 100
	// SQLCodeCursorNotOpen is what PUB400 returns when we issue a
	// continuation FETCH after the initial OPEN_DESCRIBE_FETCH
	// already drained the cursor (single-batch result). We treat
	// it as "done", not an error.
	SQLCodeCursorNotOpen int32 = -501
)

// fetchMoreRows issues a continuation FETCH (0x180B) on the cursor
// our SelectStaticSQL/SelectPreparedSQL just opened, parses the
// reply, and returns the next batch. On end-of-data the returned
// rows slice is empty and done == true. Caller is expected to keep
// calling until done is true.
//
// nextCorrelation is the correlation ID to stamp on the request;
// caller advances its own counter.
//
// The blocking factor and buffer size mirror what we requested in
// the original OPEN: 32 KB buffer, server-chosen blocking factor.
func fetchMoreRows(conn io.ReadWriter, cols []SelectColumn, nextCorrelation uint32) (rows []SelectRow, done bool, err error) {
	tpl := DBRequestTemplate{
		// 0x86040000: ReturnData + ResultData + SQLCA + RLE.
		// (Bit 17 = 0x00008000 from OPEN is "cursor attributes"
		// which only applies on initial open; FETCH leaves it off.)
		ORSBitmap:                 ORSReturnData | ORSResultData | ORSSQLCA | 0x00040000,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 0,
	}
	params := []DBParam{
		DBParamShort(cpDBScrollableCursorFlag, 0x0000),                          // 0x380D
		{CodePoint: cpDBBufferSize, Data: []byte{0x00, 0x00, 0x80, 0x00}},       // 0x3834: 32KB
		DBParamByte(cpDBVariableFieldCompr, 0xE8),                               // 0x3833: VLF on
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLFetch, tpl, params)
	if err != nil {
		return nil, false, fmt.Errorf("hostserver: build FETCH: %w", err)
	}
	hdr.CorrelationID = nextCorrelation
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, false, fmt.Errorf("hostserver: send FETCH: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorrelation, 4)
	if err != nil {
		return nil, false, fmt.Errorf("hostserver: read FETCH reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, false, fmt.Errorf("hostserver: FETCH reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, false, fmt.Errorf("hostserver: parse FETCH reply: %w", err)
	}
	// SQLCODE +100 = end of data; -501 = cursor not in OPEN state
	// (PUB400 reports this when the initial OPEN already drained
	// the cursor). Both signal "stop iterating" without an error.
	rc := int32(rep.ReturnCode)
	if rc == SQLCodeEndOfData || rc == SQLCodeCursorNotOpen {
		return nil, true, nil
	}
	if rc != 0 && !isSQLWarning(rep.ReturnCode) {
		return nil, false, fmt.Errorf("hostserver: FETCH RC=%d errorClass=0x%04X", rc, rep.ErrorClass)
	}
	rows, err = rep.findExtendedResultData(cols)
	if err != nil {
		return nil, false, fmt.Errorf("hostserver: parse FETCH row data: %w", err)
	}
	// Empty batch with non-+100 RC also signals "end of data" --
	// some PUB400 paths don't set +100 explicitly but stop sending
	// rows. Treat zero rows as done.
	if len(rows) == 0 {
		return nil, true, nil
	}
	return rows, false, nil
}

// deleteRPB sends an RPB DELETE (0x1D02) for the RPB at slot 1, the
// only slot SelectStaticSQL/SelectPreparedSQL ever creates. JTOpen
// emits this frame as the first cleanup step after every SELECT;
// without it, the next SELECT on the same connection trips because
// CREATE_RPB silently fails when slot 1 is occupied (and the
// downstream PREPARE then references stale state). Returns the
// reply parse so callers can surface non-zero error classes.
//
// nextCorrelation is the correlation ID to stamp on the request;
// caller is responsible for advancing its own counter.
func deleteRPB(conn io.ReadWriter, nextCorrelation uint32) error {
	tpl := DBRequestTemplate{
		ORSBitmap:                 ORSReturnData | 0x00040000,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 0,
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLRPBDelete, tpl, nil)
	if err != nil {
		return fmt.Errorf("hostserver: build RPB DELETE: %w", err)
	}
	hdr.CorrelationID = nextCorrelation
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send RPB DELETE: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorrelation, 4)
	if err != nil {
		return fmt.Errorf("hostserver: read RPB DELETE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return fmt.Errorf("hostserver: RPB DELETE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return fmt.Errorf("hostserver: parse RPB DELETE reply: %w", err)
	}
	if rep.ErrorClass != 0 {
		return fmt.Errorf("hostserver: RPB DELETE errorClass=%d returnCode=%d", rep.ErrorClass, rep.ReturnCode)
	}
	return nil
}

// Per-CP semantics for SQL request flavours.
const (
	cpDBPrepareStatementName uint16 = 0x3806 // var-length CCSID-tagged string (RPB)
	cpDBCursorName           uint16 = 0x380B // var-length CCSID-tagged string (RPB)
	cpDBPrepareOption        uint16 = 0x3808 // 1-byte flag
	cpDBOpenAttributes       uint16 = 0x3809 // 1-byte flag
	cpDBScrollableCursorFlag uint16 = 0x380D // short
	cpDBStatementType        uint16 = 0x3812 // short
	cpDBResultSetHoldability uint16 = 0x3830 // byte
	cpDBExtendedStmtText     uint16 = 0x3831 // CCSID(2) + SL(4) + bytes (UCS-2 statement text)
	cpDBVariableFieldCompr   uint16 = 0x3833 // byte (0xE8=compressed, 0xD5=not)
	cpDBBufferSize           uint16 = 0x3834 // 4-byte int
)

// SelectRow is one row decoded from a SELECT result set. Each
// element is a Go value matching the column's SQL type:
//
//	TIMESTAMP -> string ("YYYY-MM-DD-HH.MM.SS.ffffff")
//	VARCHAR   -> string
//	CHAR      -> string (not space-trimmed)
//	INTEGER   -> int32
//	(more types added as M2..M5 fill in)
type SelectRow []any

// SelectColumn describes one column of a SELECT result set, parsed
// out of the super-extended data format CP.
type SelectColumn struct {
	Name      string
	SQLType   uint16 // raw IBM i SQL type (e.g. 392=TIMESTAMP, 448=VARCHAR)
	Length    uint32
	Scale     uint16
	Precision uint16
	CCSID     uint16
}

// SelectResult bundles the column descriptors with the rows
// returned for a single SELECT round trip.
type SelectResult struct {
	Columns []SelectColumn
	Rows    []SelectRow
}

// SelectStaticSQL runs a static (non-parameterised) SELECT through
// the JTOpen-style 3-call sequence:
//
//  1. CREATE_RPB (0x1D00)        -- create a Request Parameter
//                                   Block named "STMT0001" /
//                                   "CRSR0001". Discards reply.
//  2. PREPARE_DESCRIBE (0x1803)  -- send statement text + ask for
//                                   column descriptors. Reply
//                                   carries SQLCA + super-extended
//                                   data format (CP 0x3812).
//  3. OPEN_DESCRIBE_FETCH (0x180E) -- execute + fetch all rows in
//                                   one round trip. Reply carries
//                                   SQLCA + extended result data
//                                   (CP 0x380E) -- the row bytes.
//
// nextCorrelation is the starting CorrelationID to use; the three
// frames consume nextCorrelation, nextCorrelation+1, nextCorrelation+2.
//
// Limitations (M2-scope):
//   - Static SQL only (no parameter markers); when M3 lands a
//     parallel SelectPreparedSQL adds the parameter-marker side.
//   - First-fetch only. The server returns a row buffer that
//     may indicate "more rows available" via the SQLCA but we
//     don't currently issue follow-up FETCHes.
//   - Result-set holdability / cursor scroll are hardcoded to
//     the JTOpen defaults captured in the select_dummy fixture.
func SelectStaticSQL(conn io.ReadWriter, sql string, nextCorrelation uint32) (*SelectResult, error) {
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
			// JTOpen's CREATE_RPB ORS = 0x00040000 (RLE only,
			// no reply requested -- it's fire-and-forget).
			//
			// Handle layout (verified byte-for-byte against
			// select_dummy.trace): ReturnORSHandle=1,
			// FillORSHandle=1, BasedOnORSHandle=0, RPBHandle=1,
			// ParameterMarkerDescriptor=0. The RPB lands in slot
			// 1 and the subsequent PREPARE_DESCRIBE /
			// OPEN_DESCRIBE_FETCH reference RPBHandle=1 to find
			// it. The earlier draft of this code permuted these
			// handles, which left the server with no RPB at the
			// expected slot and made PUB400 return SQL -401.
			ORSBitmap:                 0x00040000,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		params := []DBParam{
			DBParamVarString(cpDBPrepareStatementName, 273, stmtNameBytes),
			DBParamVarString(cpDBCursorName, 273, cursorNameBytes),
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLRPBCreate, tpl, params)
		if err != nil {
			return nil, fmt.Errorf("hostserver: build CREATE_RPB: %w", err)
		}
		hdr.CorrelationID = corr
		corr++
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send CREATE_RPB: %w", err)
		}
	}
	// CREATE_RPB has no reply expected (ORS bit 1 not set).

	// --- 2) PREPARE_DESCRIBE. ---
	stmtBytes := utf16BE(sql)
	if len(stmtBytes) > 0xFFFFFFFF {
		return nil, fmt.Errorf("hostserver: SQL too long: %d bytes", len(stmtBytes))
	}
	// JTOpen toggles ORSParameterMarkerFmt (bit 16) iff the SQL
	// contains '?' markers; with it the reply carries CP 0x3808
	// (parameter-marker format) describing how to bind. Static
	// SELECTs leave the bit clear -- live PUB400 confirms; setting
	// it on a no-marker statement returns malformed SQLDA replies.
	ors := ORSReturnData | ORSDataFormat | ORSSQLCA | 0x00040000
	if strings.ContainsRune(sql, '?') {
		ors |= ORSParameterMarkerFmt
	}
	prepCorr := corr
	{
		tpl := DBRequestTemplate{
			// Handle layout matches CREATE_RPB exactly so the
			// server reuses the RPB it just made (RPBHandle=1).
			ORSBitmap:                 ors,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		params := []DBParam{
			dbParamExtendedString(cpDBExtendedStmtText, 13488, stmtBytes), // UCS-2 BE
			DBParamShort(cpDBStatementType, 2),                            // SELECT
			DBParamByte(cpDBPrepareOption, 0x00),
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLPrepareDescribe, tpl, params)
		if err != nil {
			return nil, fmt.Errorf("hostserver: build PREPARE_DESCRIBE: %w", err)
		}
		hdr.CorrelationID = prepCorr
		corr++
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send PREPARE_DESCRIBE: %w", err)
		}
	}
	// Read PREPARE_DESCRIBE reply -- column descriptors land here.
	// Use ReadDBReplyMatching to drain any trailer replies PUB400
	// may have queued for the SET_SQL_ATTRIBUTES we ran before
	// this call (it ships an extra 40-byte template-only reply on
	// top of the data-bearing one when 0x3821/0x3825 attributes
	// are present).
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
	if len(cols) == 0 {
		// List the CPs that did come back -- helps when the
		// server picks the original (0x3805) or extended (0x380C)
		// format instead of super-extended (0x3812) we expected.
		var present []string
		for _, p := range prepRep.Params {
			present = append(present, fmt.Sprintf("0x%04X(%dB)", p.CodePoint, len(p.Data)))
		}
		return nil, fmt.Errorf("hostserver: PREPARE_DESCRIBE reply missing column descriptors (CPs in reply: %v)", present)
	}

	// --- 3) OPEN_DESCRIBE_FETCH. ---
	var fetchCorr uint32
	{
		tpl := DBRequestTemplate{
			// 0x86048000: return data + result data + SQLCA + RLE +
			// cursor attributes (bit 17 = 0x00008000). Handle
			// layout matches PREPARE_DESCRIBE so OPEN reuses the
			// same RPB (RPBHandle=1).
			ORSBitmap:                 ORSReturnData | ORSResultData | ORSSQLCA | 0x00040000 | 0x00008000,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		params := []DBParam{
			DBParamByte(cpDBOpenAttributes, 0x80),         // read-only cursor
			DBParamByte(cpDBVariableFieldCompr, 0xE8),     // VLF compression on
			DBParam{CodePoint: cpDBBufferSize, Data: []byte{0x00, 0x00, 0x80, 0x00}}, // 32 KB buffer
			DBParamShort(cpDBScrollableCursorFlag, 0x0000),
			DBParamByte(cpDBResultSetHoldability, 0xE8),
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
	// SQL +100 (no rows, end of cursor) and a few warnings come back
	// as non-zero ReturnCode but aren't fatal. The fixture's reply
	// has RC=0x000002BC = 700 because the cursor was opened
	// successfully but no scroll was specified.
	if fetchRep.ReturnCode != 0 && !isSQLWarning(fetchRep.ReturnCode) {
		return nil, fmt.Errorf("hostserver: OPEN_DESCRIBE_FETCH RC=%d errorClass=0x%04X",
			fetchRep.ReturnCode, fetchRep.ErrorClass)
	}
	rows, err := fetchRep.findExtendedResultData(cols)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse row data: %w", err)
	}
	// Continuation FETCH loop: keep pulling batches until the
	// server signals end-of-data (SQLCODE +100 or empty batch).
	// The initial OPEN_DESCRIBE_FETCH reply may already carry all
	// rows when the result fits the 32 KB buffer; in that case
	// fetchMoreRows returns done=true on the first call.
	for {
		more, done, err := fetchMoreRows(conn, cols, corr)
		if err != nil {
			return nil, fmt.Errorf("hostserver: continuation FETCH: %w", err)
		}
		corr++
		if done {
			break
		}
		rows = append(rows, more...)
	}
	// Free the RPB slot so the next SELECT on this connection
	// can CREATE_RPB at slot 1 without colliding. JTOpen always
	// emits this; M2 deferred it, M4 wires it in.
	if err := deleteRPB(conn, corr); err != nil {
		return nil, fmt.Errorf("hostserver: post-fetch cleanup: %w", err)
	}
	return &SelectResult{Columns: cols, Rows: rows}, nil
}

// utf16BE encodes s as UTF-16 big-endian bytes (CCSID 13488 on the
// wire). JTOpen uses this for SQL statement text on V7R5+ systems.
func utf16BE(s string) []byte {
	codes := utf16.Encode([]rune(s))
	out := make([]byte, 2*len(codes))
	for i, c := range codes {
		binary.BigEndian.PutUint16(out[i*2:], c)
	}
	return out
}

// dbParamExtendedString packs an extended statement text param: CCSID(2)
// + StreamLength(4) + bytes. Distinct from DBParamVarString (which
// uses a 2-byte SL) -- JTOpen's setExtendedStatementText overload
// uses 4 bytes so statements up to 4GB are encodable.
func dbParamExtendedString(cp uint16, ccsid uint16, valueBytes []byte) DBParam {
	b := make([]byte, 6+len(valueBytes))
	binary.BigEndian.PutUint16(b[0:2], ccsid)
	binary.BigEndian.PutUint32(b[2:6], uint32(len(valueBytes)))
	copy(b[6:], valueBytes)
	return DBParam{CodePoint: cp, Data: b}
}

// isSQLWarning returns true for SQL +N return codes (0x00 0x00 N N
// in the wire-format ReturnCode). Negative SQLCODE values come back
// as the high bit set, so we treat them as errors. JTOpen's "warning
// vs error" boundary uses sqlcode > 0, which on the wire is any
// 0x0000NNNN with N <= 0x7FFF. Easier check: ReturnCode high bit
// clear AND sub-1000 numeric is a warning.
func isSQLWarning(rc uint32) bool {
	// Treat non-fatal codes (high bit clear) as warnings; SQLERRD
	// errors come back with the high bit set or in errorClass.
	return rc&0x80000000 == 0
}
