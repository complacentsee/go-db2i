package hostserver

import (
	"fmt"
	"io"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// Client-side cache-hit fast path for v0.7.1. When a Conn's
// PackageManager has downloaded a CP 0x380B reply at connect
// (package-cache=true) -- or has populated the cache mid-session
// via the v0.7.4 auto-populate-after-filing path -- and a caller's
// SQL byte-equals a cached PackageStatement, the driver short-
// circuits one wire round-trip by skipping PREPARE_DESCRIBE:
//
//	miss:  CREATE_RPB + PREPARE_DESCRIBE + CHANGE_DESCRIPTOR + EXECUTE/OPEN
//	hit:   CREATE_RPB +                  + CHANGE_DESCRIPTOR + EXECUTE/OPEN
//
// The server resolves the cached plan via two pieces of wire data:
//   - CP 0x3804 (package name) on the EXECUTE / OPEN frame -- tells
//     the server which *PGM to look in.
//   - CP 0x3806 (statement-name override) carrying the 18-char
//     server-assigned name from the cached PackageStatement -- tells
//     the server which entry in that *PGM to dispatch.
//
// This is byte-equivalent to JT400's wire shape; verified against
// IBM Cloud V7R6M0 in prepared_package_cache_hit.trace and
// prepared_package_filing_iud.trace. The earlier comment block
// here referenced JT400's nameOverride_ field as the mechanism --
// that's actually a JT400-internal client cache, not the wire
// protocol; the wire dispatch is package-marker + RPB handle, no
// reply-side rename capture required.
//
// Cache-hit eligibility is enforced by the driver layer
// (driver.packageLookup) and by ExecutePreparedCached itself: any
// shape with ParamType != 0xF0 (i.e. OUT or INOUT) aborts the fast
// path because CALL statements are excluded from the package by the
// default package-criteria filter and we must not silently lose the
// sql.Out destination.

// ExecutePreparedCached runs an INSERT / UPDATE / DELETE against
// conn using the cached PackageStatement metadata. Skips PREPARE_
// DESCRIBE; still does CREATE_RPB + CHANGE_DESCRIPTOR + EXECUTE +
// DELETE_RPB to keep the server's handle state happy.
//
// nextCorr is the connection's per-call correlation-ID closure --
// each frame consumes one ID.
func ExecutePreparedCached(conn io.ReadWriter, cached *PackageStatement, paramValues []any, nextCorr func() uint32, packageName, packageLibrary string, packageCCSID uint16) (*ExecResult, error) {
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

	// --- 1) CREATE_RPB. Same shape as ExecutePreparedSQL: a fresh
	// RPB in slot 1 with STMT0001/CRSR0001 names + package library.
	if err := sendCachedCreateRPB(conn, packageLibrary, nextCorr()); err != nil {
		return nil, err
	}

	// --- 2) CHANGE_DESCRIPTOR. Upload the cached parameter shape
	// to the RPB so the EXECUTE that follows binds correctly.
	if err := sendCachedChangeDescriptor(conn, shapes, nextCorr()); err != nil {
		return nil, err
	}

	// --- 3) EXECUTE with cached statement-name override + bound values.
	dataPayload, err := EncodeDBExtendedData(shapes, paramValues)
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode cached input parameter data: %w", err)
	}
	execCorr := nextCorr()
	tpl := DBRequestTemplate{
		ORSBitmap:                 ORSReturnData | ORSSQLCA | ORSDataCompression,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 1,
	}
	stmtType := int16(cached.StatementType)
	if stmtType == 0 {
		stmtType = 1 // TYPE_OTHER if the package stored 0
	}
	params := []DBParam{
		DBParamVarString(cpDBPrepareStatementName, 273, cached.NameBytes),
		DBParamShort(cpDBStatementType, stmtType),
		{CodePoint: cpDBExtendedData, Data: dataPayload},
		DBParamShort(cpDBSyncPointDelimiter, 0x0000),
	}
	if packageName != "" {
		pkgParam, err := buildPackageMarkerParam(packageName, packageCCSID)
		if err != nil {
			_ = deleteRPB(conn, nextCorr())
			return nil, fmt.Errorf("hostserver: encode cached EXECUTE package marker: %w", err)
		}
		params = append(params, pkgParam)
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLExecute, tpl, params)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: build cached EXECUTE: %w", err)
	}
	hdr.CorrelationID = execCorr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send cached EXECUTE: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, execCorr, 8)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: read cached EXECUTE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: cached EXECUTE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: parse cached EXECUTE reply: %w", err)
	}

	rc := int32(rep.ReturnCode)
	cleanup := func() error { return deleteRPB(conn, nextCorr()) }
	if rc == 100 {
		// SQL +100 = "no rows matched" -- success with 0 rows.
		if err := cleanup(); err != nil {
			return nil, fmt.Errorf("hostserver: cleanup RPB after cached EXECUTE+100: %w", err)
		}
		return &ExecResult{}, nil
	}
	if dbErr := makeDb2Error(rep, "EXECUTE_CACHED"); dbErr != nil {
		_ = cleanup()
		return nil, dbErr
	}
	if err := cleanup(); err != nil {
		return nil, fmt.Errorf("hostserver: cleanup RPB after cached EXECUTE: %w", err)
	}
	return &ExecResult{RowsAffected: rep.RowsAffected()}, nil
}

// OpenSelectPreparedCached is the Query-path companion to
// ExecutePreparedCached. CREATE_RPB + CHANGE_DESCRIPTOR + OPEN_DESCRIBE_FETCH
// (with statement-name override) -- no PREPARE_DESCRIBE round-trip.
// The returned *Cursor owns the RPB and supports continuation FETCH
// the same way the non-cached path does, so multi-block result sets
// just work.
//
// packageLibrary is the on-wire EBCDIC library name; it's attached
// to CREATE_RPB so the server can resolve the package-named
// statement on the RPB's bind side.
func OpenSelectPreparedCached(conn io.ReadWriter, cached *PackageStatement, paramValues []any, nextCorr func() uint32, packageName, packageLibrary string, packageCCSID uint16) (*Cursor, error) {
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

	// --- 1) CREATE_RPB.
	if err := sendCachedCreateRPB(conn, packageLibrary, nextCorr()); err != nil {
		return nil, err
	}

	// --- 2) CHANGE_DESCRIPTOR (only if we have params).
	if len(shapes) > 0 {
		if err := sendCachedChangeDescriptor(conn, shapes, nextCorr()); err != nil {
			_ = deleteRPB(conn, nextCorr())
			return nil, err
		}
	}

	// --- 3) OPEN_DESCRIBE_FETCH with name override.
	dataPayload, err := EncodeDBExtendedData(shapes, paramValues)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: encode cached input parameter data: %w", err)
	}
	stmtType := int16(cached.StatementType)
	if stmtType == 0 {
		stmtType = 2 // TYPE_SELECT
	}
	openCorr := nextCorr()
	pmDesc := uint16(1)
	if len(shapes) == 0 {
		// No CHANGE_DESCRIPTOR sent; reference descriptor 0 so the
		// server doesn't dereference a non-existent descriptor.
		pmDesc = 0
	}
	tpl := DBRequestTemplate{
		ORSBitmap:                 ORSReturnData | ORSResultData | ORSSQLCA | ORSDataCompression | ORSCursorAttributes,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: pmDesc,
	}
	params := []DBParam{
		DBParamByte(cpDBOpenAttributes, 0x80),
	}
	if packageName != "" {
		pkgParam, err := buildPackageMarkerParam(packageName, packageCCSID)
		if err != nil {
			_ = deleteRPB(conn, nextCorr())
			return nil, fmt.Errorf("hostserver: encode cached OPEN package marker: %w", err)
		}
		params = append(params, pkgParam)
	}
	params = append(params,
		DBParamVarString(cpDBPrepareStatementName, 273, cached.NameBytes),
		DBParamByte(cpDBVariableFieldCompr, 0xE8),
		DBParam{CodePoint: cpDBBufferSize, Data: []byte{0x00, 0x00, 0x80, 0x00}},
		DBParamShort(cpDBScrollableCursorFlag, 0x0000),
		DBParamByte(cpDBResultSetHoldability, 0xE8),
		DBParamShort(cpDBStatementType, stmtType),
	)
	// Extended data only when we have parameters to bind.
	// CP 0x381F with a header-only body (zero parameters) makes the
	// server return SQL-603 errorClass=2 on OPEN_DESCRIBE_FETCH for
	// the criteria=select parameterless SELECT path; the static
	// equivalent skips it entirely, and JT400 follows that pattern.
	if len(shapes) > 0 {
		params = append(params, DBParam{CodePoint: cpDBExtendedData, Data: dataPayload})
	}
	params = append(params, DBParamShort(cpDBSyncPointDelimiter, 0x0000))
	hdr, payload, err := BuildDBRequest(ReqDBSQLOpenDescribeFetch, tpl, params)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: build cached OPEN_DESCRIBE_FETCH: %w", err)
	}
	hdr.CorrelationID = openCorr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send cached OPEN_DESCRIBE_FETCH: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, openCorr, 8)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: read cached OPEN reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: cached OPEN reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: parse cached OPEN reply: %w", err)
	}
	if dbErr := makeDb2Error(rep, "OPEN_DESCRIBE_FETCH_CACHED"); dbErr != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, dbErr
	}

	cols := make([]SelectColumn, len(cached.DataFormat))
	copy(cols, cached.DataFormat)
	rows, err := rep.findExtendedResultData(cols)
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, fmt.Errorf("hostserver: parse cached OPEN row data: %w", err)
	}
	outcome := interpretFetchReply(rep)
	return newCursor(conn, cols, rows, outcome, nextCorr, outcome.numberOfResults), nil
}

// sendCachedCreateRPB issues a CREATE_RPB matching the JT400 wire
// shape: STMT0001 + CRSR0001 + package library. Fire-and-forget
// (ORS=DataCompression only).
func sendCachedCreateRPB(conn io.ReadWriter, packageLibrary string, corr uint32) error {
	stmtNameBytes, err := ebcdic.CCSID37.Encode("STMT0001")
	if err != nil {
		return fmt.Errorf("hostserver: encode cached RPB stmt name: %w", err)
	}
	cursorNameBytes, err := ebcdic.CCSID37.Encode("CRSR0001")
	if err != nil {
		return fmt.Errorf("hostserver: encode cached RPB cursor name: %w", err)
	}
	tpl := DBRequestTemplate{
		ORSBitmap:                 ORSDataCompression,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 0,
	}
	params := []DBParam{
		DBParamVarString(cpDBPrepareStatementName, rpbStringCCSID(), stmtNameBytes),
		DBParamVarString(cpDBCursorName, rpbStringCCSID(), cursorNameBytes),
	}
	if packageLibrary != "" {
		libParam, err := buildPackageLibraryParam(packageLibrary)
		if err != nil {
			return fmt.Errorf("hostserver: encode cached RPB library: %w", err)
		}
		params = append(params, libParam)
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLRPBCreate, tpl, params)
	if err != nil {
		return fmt.Errorf("hostserver: build cached CREATE_RPB: %w", err)
	}
	hdr.CorrelationID = corr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send cached CREATE_RPB: %w", err)
	}
	return nil
}

// sendCachedChangeDescriptor uploads the cached parameter shape to
// the RPB allocated by the previous CREATE_RPB. Fire-and-forget --
// JT400's wire shape sends ORS=DataCompression only here too.
func sendCachedChangeDescriptor(conn io.ReadWriter, shapes []PreparedParam, corr uint32) error {
	hdr, payload, err := ChangeDescriptorRequest(shapes)
	if err != nil {
		return fmt.Errorf("hostserver: build cached CHANGE_DESCRIPTOR: %w", err)
	}
	hdr.CorrelationID = corr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send cached CHANGE_DESCRIPTOR: %w", err)
	}
	return nil
}

// preparedParamsFromCached converts the SQLDA-derived
// ParameterMarkerField shapes the package decoder produced into the
// PreparedParam shape the existing EncodeDBExtendedDataFormat /
// EncodeDBExtendedData encoders consume. Refuses any non-input
// direction up front (sql.Out / sql.InOut callers must skip the
// cache).
//
// Precision/Scale handling: the package SQLDA encodes Precision in
// the high byte of the per-field "length" field and Scale in the
// low byte (see db_package.go:474-475). For numeric types this is
// useful (DECIMAL(5,2) stores 0x0502, decoded as precision=5,
// scale=2). For non-numeric types the same bytes redundantly carry
// the storage width, so the decoded Precision/Scale are garbage
// (an INTEGER with FieldLength=4 would otherwise propagate as
// Precision=0, Scale=4 -- enough to make the server silently drop
// the bound value at EXECUTE).
//
// JT400's wire emits Precision/Scale of 0 for non-decimal SQLTypes
// on cache-hit EXECUTE; we mirror that here. ParamType is forced
// to 0x00 (input-only, no direction tag) to match JT400's
// cache-hit byte sequence verified on V7R6M0 2026-05-11.
func preparedParamsFromCached(pmf []ParameterMarkerField) ([]PreparedParam, error) {
	out := make([]PreparedParam, 0, len(pmf))
	for i, p := range pmf {
		if p.ParamType != 0x00 && p.ParamType != 0xF0 {
			return nil, fmt.Errorf("hostserver: cached PMF[%d] direction 0x%02X (only IN is cacheable)", i, p.ParamType)
		}
		precision := uint16(0)
		scale := uint16(0)
		if isDecimalNumericSQLType(p.SQLType) {
			// Trust the SQLDA's high/low-byte split for packed/
			// zoned decimal -- precision and scale are part of the
			// type identity for these.
			precision = p.Precision
			scale = p.Scale
		}
		out = append(out, PreparedParam{
			SQLType:     p.SQLType,
			FieldLength: p.FieldLength,
			Precision:   precision,
			Scale:       scale,
			CCSID:       p.CCSID,
			ParamType:   0x00,
		})
	}
	return out, nil
}

// isDecimalNumericSQLType reports whether the SQLType is one of the
// packed/zoned decimal variants where Precision and Scale carry
// semantic meaning beyond storage width. For every other type
// (INTEGER, VARCHAR, DATE, ...) Precision/Scale of zero match
// JT400's cache-hit wire bytes.
func isDecimalNumericSQLType(t uint16) bool {
	switch t {
	case 484, 485, // DECIMAL (NN, nullable)
		488, 489, // NUMERIC (zoned decimal)
		996, 997: // DECFLOAT
		return true
	}
	return false
}
