package hostserver

import (
	"fmt"
	"io"
)

// Client-side cache-hit fast path. When a Conn's PackageManager has
// downloaded a CP 0x380B reply at connect (package-cache=true) and a
// caller's SQL byte-equals a cached PackageStatement, the driver can
// skip the CREATE_RPB + PREPARE_DESCRIBE + CHANGE_DESCRIPTOR
// preamble and ship one EXECUTE frame carrying:
//
//   - cpDBPrepareStatementName (0x3806) -- the cached 18-byte
//     server-assigned name, sent verbatim from PackageStatement.NameBytes
//     so the server-side byte equality holds.
//   - cpDBExtendedDataFormat (0x381E) -- the parameter-marker shape
//     descriptor, rebuilt from PackageStatement.ParameterMarkerFormat.
//   - cpDBExtendedData (0x381F) -- the bound parameter values,
//     encoded per the cached marker types.
//
// JT400's AS400JDBCStatement.commonExecute does the equivalent via
// its nameOverride_ field (see JDPackageManager.getCachedStatementName
// -> setPrepareStatementName on the EXECUTE request). v0.7.1 mirrors
// the per-frame wire shape; we don't yet share JT400's per-Statement
// persistent RPB optimisation.
//
// Cache-hit eligibility is enforced by the driver layer
// (driver.packageLookup) and by ExecutePreparedCached itself: any
// shape with ParamType != 0xF0 (i.e. OUT or INOUT) aborts the fast
// path because CALL statements are excluded from the package by the
// default package-criteria filter and we must not silently lose the
// sql.Out destination.

// ExecutePreparedCached runs an INSERT / UPDATE / DELETE against
// conn using only the cached PackageStatement metadata -- no
// CREATE_RPB, no PREPARE_DESCRIBE, no CHANGE_DESCRIPTOR. The caller
// must guarantee that:
//
//   - cached.NameBytes is the verbatim 18-byte EBCDIC server name.
//   - cached.ParameterMarkerFormat has the right length for
//     paramValues (the driver's packageLookup confirms this against
//     the cached SQL text byte-equality).
//   - paramValues are Go values compatible with the cached
//     parameter shapes; encoding is delegated to
//     EncodeDBExtendedData via the synthetic PreparedParam list.
//
// nextCorr is the connection's correlation-ID source.
func ExecutePreparedCached(conn io.ReadWriter, cached *PackageStatement, paramValues []any, nextCorr func() uint32) (*ExecResult, error) {
	if cached == nil {
		return nil, fmt.Errorf("hostserver: ExecutePreparedCached: cached statement nil")
	}
	if len(cached.NameBytes) != 18 {
		return nil, fmt.Errorf("hostserver: ExecutePreparedCached: cached name has %d bytes (want 18)", len(cached.NameBytes))
	}
	if len(cached.ParameterMarkerFormat) != len(paramValues) {
		return nil, fmt.Errorf("hostserver: ExecutePreparedCached: shape/value count mismatch (%d shapes, %d values)",
			len(cached.ParameterMarkerFormat), len(paramValues))
	}
	shapes, err := preparedParamsFromCached(cached.ParameterMarkerFormat)
	if err != nil {
		return nil, err
	}

	hdr, payload, err := buildCachedExecuteFrame(ReqDBSQLExecute, cached, shapes, paramValues)
	if err != nil {
		return nil, err
	}
	corr := nextCorr()
	hdr.CorrelationID = corr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send cached EXECUTE: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, corr, 8)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read cached EXECUTE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: cached EXECUTE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse cached EXECUTE reply: %w", err)
	}
	rc := int32(rep.ReturnCode)
	if rc == 100 {
		// SQL +100 = "no rows matched" -- not an error; rows-affected = 0.
		return &ExecResult{}, nil
	}
	if dbErr := makeDb2Error(rep, "EXECUTE_CACHED"); dbErr != nil {
		return nil, dbErr
	}
	return &ExecResult{}, nil
}

// OpenSelectPreparedCached is the Query-path companion to
// ExecutePreparedCached. It builds an OPEN_DESCRIBE_FETCH (0x180E)
// against the cached statement name + parameter shape, returning a
// streaming *Cursor whose first batch is the rows in the reply.
//
// LIMITATION (v0.7.1): the cached fast path ships RPBHandle=0 on
// the OPEN frame so continuation FETCH is not available -- the
// returned Cursor is pre-marked exhausted=true / rpbActive=false.
// Callers reading more than the OPEN's first batch will see
// truncated results. Result sets that fit in one 32 KB buffer
// (the typical case for packaged statements -- a few rows of a
// small SELECT) are unaffected. v0.7.2 will swap in a RPB-backed
// path so multi-block FETCH continuation works.
func OpenSelectPreparedCached(conn io.ReadWriter, cached *PackageStatement, paramValues []any, nextCorr func() uint32, opts ...SelectOption) (*Cursor, error) {
	if cached == nil {
		return nil, fmt.Errorf("hostserver: OpenSelectPreparedCached: cached statement nil")
	}
	if len(cached.NameBytes) != 18 {
		return nil, fmt.Errorf("hostserver: OpenSelectPreparedCached: cached name has %d bytes (want 18)", len(cached.NameBytes))
	}
	if len(cached.DataFormat) == 0 {
		return nil, fmt.Errorf("hostserver: OpenSelectPreparedCached: cached statement has no result columns")
	}
	if len(cached.ParameterMarkerFormat) != len(paramValues) {
		return nil, fmt.Errorf("hostserver: OpenSelectPreparedCached: shape/value count mismatch (%d shapes, %d values)",
			len(cached.ParameterMarkerFormat), len(paramValues))
	}
	shapes, err := preparedParamsFromCached(cached.ParameterMarkerFormat)
	if err != nil {
		return nil, err
	}

	hdr, payload, err := buildCachedExecuteFrame(ReqDBSQLOpenDescribeFetch, cached, shapes, paramValues)
	if err != nil {
		return nil, err
	}
	corr := nextCorr()
	hdr.CorrelationID = corr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send cached OPEN_DESCRIBE_FETCH: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, corr, 8)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read cached OPEN reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: cached OPEN reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse cached OPEN reply: %w", err)
	}
	if dbErr := makeDb2Error(rep, "OPEN_DESCRIBE_FETCH_CACHED"); dbErr != nil {
		return nil, dbErr
	}
	cols := make([]SelectColumn, len(cached.DataFormat))
	copy(cols, cached.DataFormat)
	rows, err := rep.findExtendedResultData(cols)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse cached OPEN row data: %w", err)
	}
	// Pre-marked exhausted + no RPB. Close() is a no-op on this
	// shape; Next() drains the buffered rows then returns io.EOF.
	return newBufferedCursor(cols, rows), nil
}

// newBufferedCursor returns a *Cursor that owns the given row batch
// and refuses continuation FETCH. Used by the cache-hit fast path
// (and potentially other future buffered paths) where the server
// state we'd need for continuation isn't available.
func newBufferedCursor(cols []SelectColumn, rows []SelectRow) *Cursor {
	return &Cursor{
		cols:         cols,
		pending:      rows,
		exhausted:    true,
		serverClosed: true,
		rpbActive:    false,
	}
}

// buildCachedExecuteFrame assembles the wire frame for both the
// EXECUTE (0x1805) and OPEN_DESCRIBE_FETCH (0x180E) cache-hit paths.
// The two function codes share the same parameter list shape:
// statement-name override + parameter-format descriptor + bound
// values. ORS bitmap and template handles differ slightly between
// the two, and are filled in by the caller's resolvedFrameKnobs.
func buildCachedExecuteFrame(functionID uint16, cached *PackageStatement, shapes []PreparedParam, paramValues []any) (Header, []byte, error) {
	dataPayload, err := EncodeDBExtendedData(shapes, paramValues)
	if err != nil {
		return Header{}, nil, fmt.Errorf("hostserver: encode cached input parameter data: %w", err)
	}
	descriptor := EncodeDBExtendedDataFormat(shapes)

	// Statement-type code for the EXECUTE template. We rely on the
	// type the package stored (2=SELECT, 3=INSERT, 4=UPDATE,
	// 5=DELETE in our wire usage). Falling back to 0
	// (TYPE_UNDETERMINED) keeps us strictly correct if the server
	// ever ships a statement type we haven't enumerated.
	stmtType := int16(cached.StatementType)

	var ors uint32
	var rpbHandle uint16
	switch functionID {
	case ReqDBSQLExecute:
		// EXECUTE: SQLCA + ReturnData + RLE compression. We don't
		// expect a result-data block; OUT/INOUT params are excluded
		// from the package by the driver-side criteria filter.
		ors = ORSReturnData | ORSSQLCA | ORSDataCompression
		rpbHandle = 0
	case ReqDBSQLOpenDescribeFetch:
		// OPEN_DESCRIBE_FETCH: result data + SQLCA. We deliberately
		// don't ask for DataFormat back (CP 0x3812); the cached
		// PackageStatement.DataFormat is authoritative.
		ors = ORSReturnData | ORSResultData | ORSSQLCA | ORSDataCompression
		rpbHandle = 0
	default:
		return Header{}, nil, fmt.Errorf("hostserver: buildCachedExecuteFrame: unsupported function 0x%04X", functionID)
	}

	tpl := DBRequestTemplate{
		ORSBitmap:                 ors,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 rpbHandle,
		ParameterMarkerDescriptor: 1,
	}
	params := []DBParam{
		// CP 0x3806 sent as a CCSID-tagged var-string. CCSID 273 is
		// what the existing CREATE_RPB path uses for statement
		// names; the cached bytes round-trip correctly because the
		// IBM i character set for server-assigned names (A-Z + 0-9)
		// has the same EBCDIC byte values across CCSID 37 / 273 /
		// 277 / 280 / 285 (see project_db2i_m10_jt400_interop.md).
		DBParamVarString(cpDBPrepareStatementName, 273, cached.NameBytes),
		DBParamShort(cpDBStatementType, stmtType),
		{CodePoint: cpDBExtendedDataFormat, Data: descriptor},
		{CodePoint: cpDBExtendedData, Data: dataPayload},
		DBParamShort(cpDBSyncPointDelimiter, 0x0000),
	}
	return BuildDBRequest(functionID, tpl, params)
}

// preparedParamsFromCached converts the SQLDA-derived
// ParameterMarkerField shapes the package decoder produced into the
// PreparedParam shape the existing EncodeDBExtendedDataFormat /
// EncodeDBExtendedData encoders consume. Refuses any non-input
// direction up front (sql.Out / sql.InOut callers must skip the
// cache).
func preparedParamsFromCached(pmf []ParameterMarkerField) ([]PreparedParam, error) {
	out := make([]PreparedParam, 0, len(pmf))
	for i, p := range pmf {
		if p.ParamType != 0x00 && p.ParamType != 0xF0 {
			return nil, fmt.Errorf("hostserver: cached PMF[%d] direction 0x%02X (only IN is cacheable)", i, p.ParamType)
		}
		out = append(out, PreparedParam{
			SQLType:     p.SQLType,
			FieldLength: p.FieldLength,
			Precision:   p.Precision,
			Scale:       p.Scale,
			CCSID:       p.CCSID,
			ParamType:   0xF0,
		})
	}
	return out, nil
}
