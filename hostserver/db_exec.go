package hostserver

import (
	"fmt"
	"io"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// EXECUTE_IMMEDIATE function ID. Per JT400's
// DBSQLRequestDS.FUNCTIONID_EXECUTE_IMMEDIATE -- runs a single SQL
// statement (INSERT / UPDATE / DELETE / DDL) without the
// PREPARE+DESCRIBE+OPEN dance, since there's no result set to
// describe and no cursor to open.
const ReqDBSQLExecuteImmediate uint16 = 0x1806

// EXECUTE function ID. Per JT400's DBSQLRequestDS.FUNCTIONID_EXECUTE
// -- runs an INSERT / UPDATE / DELETE that was previously PREPAREd
// against the same RPB. Differs from EXECUTE_IMMEDIATE in that the
// statement text is not re-sent (the RPB carries it) and parameter
// values arrive via CP 0x381F just like the prepared SELECT path.
const ReqDBSQLExecute uint16 = 0x1805

// ExecResult is what ExecuteImmediate returns -- just a
// rows-affected count for now (decoded from SQLCA when present;
// 0 if the SQLCA didn't carry one).
type ExecResult struct {
	RowsAffected int64
}

// ExecuteImmediate runs INSERT / UPDATE / DELETE / DDL against conn
// using the SQL service's EXECUTE_IMMEDIATE (0x1806) function. The
// statement text is sent UCS-2 BE encoded (matching what
// SelectStaticSQL does for SELECT). No column descriptors come back
// because there's no result set, so this function deliberately
// doesn't try to parse one.
//
// nextCorrelation is the request correlation ID; caller advances
// its own counter. Currently does not loop / paginate; suitable for
// single-statement Exec where the server answers in one frame.
//
// Returns ExecResult with rows-affected when the SQLCA carries it,
// or zero when the server didn't (e.g. DDL).
func ExecuteImmediate(conn io.ReadWriter, sql string, nextCorrelation uint32) (*ExecResult, error) {
	stmtBytes := utf16BE(sql)
	tpl := DBRequestTemplate{
		// ORS bitmap: ReturnData + SQLCA + RLE. We don't need
		// DataFormat (no result columns) or ResultData (no rows).
		ORSBitmap:                 ORSReturnData | ORSSQLCA | 0x00040000,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 0, // no RPB attached for execute-immediate
		ParameterMarkerDescriptor: 0,
	}
	params := []DBParam{
		dbParamExtendedString(cpDBExtendedStmtText, 13488, stmtBytes), // UCS-2 BE
		DBParamShort(cpDBStatementType, statementTypeForSQL(sql)),
		DBParamByte(cpDBPrepareOption, 0x00),
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLExecuteImmediate, tpl, params)
	if err != nil {
		return nil, fmt.Errorf("hostserver: build EXECUTE_IMMEDIATE: %w", err)
	}
	hdr.CorrelationID = nextCorrelation
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send EXECUTE_IMMEDIATE: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorrelation, 4)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read EXECUTE_IMMEDIATE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: EXECUTE_IMMEDIATE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse EXECUTE_IMMEDIATE reply: %w", err)
	}
	rc := int32(rep.ReturnCode)
	// SQL +100 = "no rows found" (DELETE / UPDATE with no match) is
	// not an error -- it's the expected outcome and the affected-
	// row count is correctly 0. The IBM i server still flags it
	// with errorClass=1, so we have to special-case the +100 path.
	// Any other errorClass>0 OR a non-warning return code is real.
	if rc == 100 {
		return &ExecResult{}, nil
	}
	if dbErr := makeDb2Error(rep, "EXECUTE_IMMEDIATE"); dbErr != nil {
		return nil, dbErr
	}
	// TODO(M7): pull rows-affected out of CP 0x3807 (SQLCA);
	// JT400 reads SQLERRD[2]. For now return 0 -- callers that
	// need the count can decode the SQLCA themselves.
	return &ExecResult{}, nil
}

// ExecutePreparedSQL runs a parameterised INSERT / UPDATE / DELETE
// against conn. Mirrors the JT400 prepared-DML flow:
//
//  1. CREATE_RPB        (1D00)  -- allocate request parameter block
//  2. PREPARE_DESCRIBE  (1803)  -- send statement text, ask for SQLCA
//  3. CHANGE_DESCRIPTOR (1E00)  -- upload input parameter shapes
//  4. EXECUTE           (1805)  -- send parameter values, run statement
//
// nextCorrelation is the starting CorrelationID; the four frames
// consume nextCorrelation through nextCorrelation+3.
//
// Returns ExecResult (rows-affected currently always 0; SQLCA
// decoding lands with M7). SQL +100 ("no rows matched") is treated
// as success with rows-affected=0, matching ExecuteImmediate.
//
// Caller is responsible for ensuring the SQL text starts with INSERT,
// UPDATE, or DELETE -- the function does not validate the verb,
// since callers (the driver) already classify with isSelect().
func ExecutePreparedSQL(conn io.ReadWriter, sql string, paramShapes []PreparedParam, paramValues []any, nextCorrelation uint32) (*ExecResult, error) {
	if len(paramShapes) != len(paramValues) {
		return nil, fmt.Errorf("hostserver: shape/value count mismatch (%d shapes, %d values)", len(paramShapes), len(paramValues))
	}
	corr := nextCorrelation

	// --- 1) CREATE_RPB. Re-use STMT0001 / CRSR0001 names for parity
	// with the prepared-SELECT path. The cursor name is harmless on a
	// non-SELECT RPB; JT400 sends it unconditionally.
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
			ORSBitmap:                 0x00040000, // RLE only -- fire and forget
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

	// --- 2) PREPARE_DESCRIBE. Statement type comes from the verb
	// (3=INSERT, 4=UPDATE, 5=DELETE). We still set the
	// ORSParameterMarkerFmt bit so the server validates the marker
	// count against what the parser sees in the SQL text -- catches
	// e.g. "INSERT ... VALUES (?, ?)" called with one argument early.
	stmtBytes := utf16BE(sql)
	prepCorr := corr
	{
		tpl := DBRequestTemplate{
			ORSBitmap:                 ORSReturnData | ORSSQLCA | ORSParameterMarkerFmt | 0x00040000,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLPrepareDescribe, tpl, []DBParam{
			dbParamExtendedString(cpDBExtendedStmtText, 13488, stmtBytes),
			DBParamShort(cpDBStatementType, statementTypeForSQL(sql)),
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
	if dbErr := makeDb2Error(prepRep, "PREPARE_DESCRIBE"); dbErr != nil {
		return nil, dbErr
	}

	// --- 3) CHANGE_DESCRIPTOR. Skip when no parameters -- saves a
	// round trip for callers that pass through ExecutePreparedSQL
	// for symmetry but happen to bind zero arguments.
	if len(paramShapes) > 0 {
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

	// --- 4) EXECUTE with input parameter values.
	dataPayload, err := EncodeDBExtendedData(paramShapes, paramValues)
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode input parameter data: %w", err)
	}
	execCorr := corr
	{
		tpl := DBRequestTemplate{
			// SQLCA only -- INSERT/UPDATE/DELETE returns no rows.
			ORSBitmap:                 ORSReturnData | ORSSQLCA | 0x00040000,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 1,
		}
		params := []DBParam{
			DBParamShort(cpDBStatementType, statementTypeForSQL(sql)),
			{CodePoint: cpDBExtendedData, Data: dataPayload},
			DBParamShort(cpDBSyncPointDelimiter, 0x0000),
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLExecute, tpl, params)
		if err != nil {
			return nil, fmt.Errorf("hostserver: build EXECUTE: %w", err)
		}
		hdr.CorrelationID = execCorr
		corr++
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send EXECUTE: %w", err)
		}
	}
	execRepHdr, execRepPayload, err := ReadDBReplyMatching(conn, execCorr, 8)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read EXECUTE reply: %w", err)
	}
	if execRepHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: EXECUTE reply ReqRepID 0x%04X (want 0x%04X)", execRepHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(execRepPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse EXECUTE reply: %w", err)
	}
	rc := int32(rep.ReturnCode)
	// All exit paths -- success, +100 no-match, hard error -- must
	// drop the RPB. The slot stays occupied otherwise and the next
	// prepared call on this connection fails at PREPARE_DESCRIBE
	// with RC -101 / errorClass 2 because CREATE_RPB silently
	// no-ops on a busy slot. Live-confirmed on IBM Cloud IBM i 7.6:
	// a DELETE that returned +100 ("no rows matched") left slot 1
	// dirty and broke the very next DELETE.
	cleanup := func() error {
		return deleteRPB(conn, corr)
	}
	if rc == 100 {
		// SQL +100: no rows matched (UPDATE/DELETE WHERE that found
		// nothing). Same handling as ExecuteImmediate.
		if err := cleanup(); err != nil {
			return nil, fmt.Errorf("hostserver: cleanup RPB after EXECUTE+100: %w", err)
		}
		return &ExecResult{}, nil
	}
	if dbErr := makeDb2Error(rep, "EXECUTE"); dbErr != nil {
		_ = cleanup()
		return nil, dbErr
	}
	if err := cleanup(); err != nil {
		return nil, fmt.Errorf("hostserver: cleanup RPB after EXECUTE: %w", err)
	}
	// TODO(M7): pull rows-affected out of CP 0x3807 (SQLCA SQLERRD[2]).
	return &ExecResult{}, nil
}

// statementTypeForSQL picks the SQL statement-type code (CP 0x3812
// short) based on the leading keyword. JT400's mapping per
// JDStatement.STMT_TYPE_*; the values here cover the cases the
// driver actually emits today.
func statementTypeForSQL(sql string) int16 {
	// Strip leading whitespace to find the verb.
	start := 0
	for start < len(sql) && (sql[start] == ' ' || sql[start] == '\t' || sql[start] == '\n' || sql[start] == '\r') {
		start++
	}
	end := start
	for end < len(sql) && sql[end] != ' ' && sql[end] != '\t' && sql[end] != '\n' && sql[end] != '\r' && sql[end] != '(' {
		end++
	}
	verb := sql[start:end]
	// Uppercase comparison without strings.ToUpper to keep this
	// allocation-free.
	switch {
	case eqIgnoreCase(verb, "INSERT"):
		return 3
	case eqIgnoreCase(verb, "UPDATE"):
		return 4
	case eqIgnoreCase(verb, "DELETE"):
		return 5
	case eqIgnoreCase(verb, "SELECT"), eqIgnoreCase(verb, "VALUES"), eqIgnoreCase(verb, "WITH"):
		return 2
	default:
		// CALL / SET / CREATE / DROP / ALTER / GRANT / REVOKE /
		// MERGE / etc. all map to "other" in JT400's taxonomy;
		// 0 lets the server figure it out.
		return 0
	}
}

func eqIgnoreCase(s, want string) bool {
	if len(s) != len(want) {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		if c != want[i] {
			return false
		}
	}
	return true
}
