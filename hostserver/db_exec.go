package hostserver

import (
	"fmt"
	"io"
)

// EXECUTE_IMMEDIATE function ID. Per JT400's
// DBSQLRequestDS.FUNCTIONID_EXECUTE_IMMEDIATE -- runs a single SQL
// statement (INSERT / UPDATE / DELETE / DDL) without the
// PREPARE+DESCRIBE+OPEN dance, since there's no result set to
// describe and no cursor to open.
const ReqDBSQLExecuteImmediate uint16 = 0x1806

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
	if rep.ErrorClass != 0 || (rc != 0 && !isSQLWarning(rep.ReturnCode)) {
		return nil, fmt.Errorf("hostserver: EXECUTE_IMMEDIATE RC=%d errorClass=0x%04X",
			rc, rep.ErrorClass)
	}
	// TODO(M7): pull rows-affected out of CP 0x3807 (SQLCA);
	// JT400 reads SQLERRD[2]. For now return 0 -- callers that
	// need the count can decode the SQLCA themselves.
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
