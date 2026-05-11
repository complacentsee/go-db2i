package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode/utf16"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// SQL function IDs sent over the as-database (server ID 0xE004)
// service. We only define the ones we use right now.
const (
	ReqDBSQLPrepareDescribe   uint16 = 0x1803 // PREPARE + DESCRIBE in one shot
	ReqDBSQLOpenDescribe      uint16 = 0x1804 // OPEN + DESCRIBE (no inline FETCH)
	ReqDBSQLFetch             uint16 = 0x180B // continuation FETCH from existing cursor
	ReqDBSQLOpenDescribeFetch uint16 = 0x180E // OPEN + DESCRIBE + FETCH
	ReqDBSQLClose             uint16 = 0x180A // CLOSE cursor
	ReqDBSQLRPBCreate         uint16 = 0x1D00 // CREATE Request Parameter Block
	ReqDBSQLRPBDelete         uint16 = 0x1D02 // DELETE RPB (frees RPB slot for next CREATE)
	ReqDBSQLDeleteResultsSet  uint16 = 0x1F01 // DELETE_RESULTS_SET
)

// reuseResultSet is the CP 0x3810 (cpDBReuseIndicator) byte that asks
// the server to close the cursor but PRESERVE the prepared statement
// so a follow-up OPEN_DESCRIBE on the same RPB can attach to the
// next dynamic result set the proc returns. Mirrors JT400's
// JDCursor.REUSE_RESULT_SET (= 0xF2) -- the close JT400 calls before
// FUNCTIONID_OPEN_DESCRIBE in getMoreResults
// (AS400JDBCStatement.java:3420).
const reuseResultSet byte = 0xF2

// findResultSetCount returns the dynamic-result-set count the server
// advertised in SQLERRD(2) of the EXECUTE / OPEN_DESCRIBE_FETCH
// reply's SQLCA (CP 0x3807). For SELECT statements ERRD(2) is zero
// (JT400 explicitly sets numberOfResults_=0 for non-CALL); for CALL
// procs that declare DYNAMIC RESULT SETS N the server stamps N here.
// Returns 0 when the SQLCA is absent or too short -- the cursor's
// state machine treats that as "this is the only result set".
func findResultSetCount(rep *DBReply) int {
	for _, p := range rep.Params {
		if p.CodePoint != cpDBSQLCA {
			continue
		}
		// SQLERRD[2] at SQLCA-internal offset 100 (4 bytes BE int32).
		// JT400's DBReplySQLCA.locationFromOffset_ = {96, 100, 104, ...}
		// indexes ERRD(1..6); we read index 2.
		if len(p.Data) < 104 {
			return 0
		}
		v := int32(binary.BigEndian.Uint32(p.Data[100:104]))
		if v < 0 {
			return 0
		}
		return int(v)
	}
	return 0
}

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

// fetchOutcome captures the cursor-state signals embedded in the
// (ErrorClass, ReturnCode) tuple of any OPEN_DESCRIBE_FETCH /
// continuation FETCH reply. JT400's JDServerRowCache.fetch (Java
// source ~line 343) authoritatively interprets the tuple; the
// values mirror it byte-for-byte:
//
//	ErrorClass=1, ReturnCode=100  -> end-of-data (SQL +100)
//	ErrorClass=2, ReturnCode=700  -> "fetch/close": all rows
//	                                 delivered AND server already
//	                                 closed the cursor on its own
//	                                 (JT400's @pda perf2 path)
//	ErrorClass=2, ReturnCode=701  -> end-of-data variant
//	SQLCode -501                  -> cursor not open: server closed
//	                                 the cursor between OPEN and our
//	                                 next FETCH (we treat it like
//	                                 700: exhausted + auto-closed)
//
// Anything else means "more rows possible; issue continuation
// FETCH" (and any error/warning surfaces through makeDb2Error
// upstream).
type fetchOutcome struct {
	exhausted    bool // no more rows; don't issue continuation FETCH
	serverClosed bool // server auto-closed the cursor; skip explicit CLOSE

	// numberOfResults is the dynamic-result-set count the server
	// stamped in SQLCA SQLERRD(2) on the EXECUTE-bearing reply
	// (OPEN_DESCRIBE_FETCH for CALL queries through OpenSelectPrepared,
	// 0 for plain SELECT). Populated by interpretFetchReply via
	// findResultSetCount; cursors consume it through newCursor.
	// Zero means "this is the only result set" (or the server didn't
	// populate it).
	numberOfResults int
}

// interpretFetchReply applies JT400's (ErrorClass, ReturnCode)
// dispatch table to a parsed fetch-bearing reply (initial OPEN or
// continuation FETCH). Caller is responsible for separately
// surfacing real errors via makeDb2Error -- this function only
// reports "is the cursor done?" and "did the server already close
// it?".
func interpretFetchReply(rep *DBReply) fetchOutcome {
	rc := int32(rep.ReturnCode)
	ec := rep.ErrorClass
	switch {
	case ec == 1 && rc == SQLCodeEndOfData:
		return fetchOutcome{exhausted: true}
	case ec == 2 && rc == 700:
		// "fetch/close" -- the canonical JT400 single-batch path.
		return fetchOutcome{exhausted: true, serverClosed: true}
	case ec == 2 && rc == 701:
		return fetchOutcome{exhausted: true}
	case rc == SQLCodeCursorNotOpen:
		// Continuation FETCH after server-side auto-close.
		return fetchOutcome{exhausted: true, serverClosed: true}
	}
	return fetchOutcome{}
}

// fetchMoreRows issues a continuation FETCH (0x180B) on the cursor
// our SelectStaticSQL/SelectPreparedSQL just opened, parses the
// reply, and returns the next batch. The returned fetchOutcome
// reports whether the cursor is exhausted and whether the server
// auto-closed it -- callers use these to decide whether to issue
// a follow-up FETCH and whether to send an explicit CLOSE. Caller
// is expected to keep calling until outcome.exhausted is true.
//
// nextCorrelation is the correlation ID to stamp on the request;
// caller advances its own counter.
//
// The blocking factor and buffer size mirror what we requested in
// the original OPEN: 32 KB buffer, server-chosen blocking factor.
func fetchMoreRows(conn io.ReadWriter, cols []SelectColumn, nextCorrelation uint32) (rows []SelectRow, outcome fetchOutcome, err error) {
	tpl := DBRequestTemplate{
		// 0x86040000: ReturnData + ResultData + SQLCA + RLE.
		// (Bit 17 = 0x00008000 from OPEN is "cursor attributes"
		// which only applies on initial open; FETCH leaves it off.)
		ORSBitmap:                 ORSReturnData | ORSResultData | ORSSQLCA | ORSDataCompression,
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
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: build FETCH: %w", err)
	}
	hdr.CorrelationID = nextCorrelation
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: send FETCH: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorrelation, 4)
	if err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: read FETCH reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: FETCH reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: parse FETCH reply: %w", err)
	}
	outcome = interpretFetchReply(rep)
	if outcome.exhausted {
		return nil, outcome, nil
	}
	if dbErr := makeDb2Error(rep, "FETCH"); dbErr != nil {
		return nil, fetchOutcome{}, dbErr
	}
	rows, err = rep.findExtendedResultData(cols)
	if err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: parse FETCH row data: %w", err)
	}
	// Empty batch with no end-of-data signal also means "done" --
	// some PUB400 paths don't set the JT400 codes explicitly but
	// stop sending rows. Treat zero rows as done.
	if len(rows) == 0 {
		outcome.exhausted = true
	}
	return rows, outcome, nil
}

// Per-CP semantics for FETCH on a CALL-opened cursor. JT400's
// getMoreResults flow uses these instead of the BufferSize +
// VarFieldCompr set we use on SELECT cursors:
//   - 0x380E Fetch Scroll Option (short; 0 = FETCH_NEXT)
//   - 0x380C Blocking Factor (uint32; matches JT400's 2048-row
//     default per JDProperties.BLOCK_SIZE)
// The CALL cursor needs the BlockingFactor explicitly (the proc-
// opened cursor's server-side state doesn't carry a default block
// size like a fresh SELECT cursor does).
const (
	cpDBFetchScrollOption uint16 = 0x380E
	cpDBBlockingFactor    uint16 = 0x380C
)

// fetchCallRows pulls rows from a cursor JT400 attached via
// OPEN_DESCRIBE on a CALL-opened dynamic result set. Differs from
// fetchMoreRows in the param set: the SELECT-cursor path's
// BufferSize + VarFieldCompr combo doesn't work for CALL cursors
// (server returns empty batches); JT400's CALL FETCH uses
// FetchScrollOption + BlockingFactor instead. Mirrors
// prepared_call_multi_set.trace sent #18 byte-for-byte modulo
// correlation IDs.
func fetchCallRows(conn io.ReadWriter, cols []SelectColumn, nextCorrelation uint32) (rows []SelectRow, outcome fetchOutcome, err error) {
	tpl := DBRequestTemplate{
		// 0x84040000 = ReturnData + ResultData + RLE.
		ORSBitmap:                 ORSReturnData | ORSResultData | ORSDataCompression,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 0,
	}
	params := []DBParam{
		DBParamShort(cpDBFetchScrollOption, 0x0000), // FETCH_NEXT
		{CodePoint: cpDBBlockingFactor, Data: []byte{0x00, 0x00, 0x08, 0x00}}, // 2048 rows / block
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLFetch, tpl, params)
	if err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: build CALL FETCH: %w", err)
	}
	hdr.CorrelationID = nextCorrelation
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: send CALL FETCH: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorrelation, 4)
	if err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: read CALL FETCH reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: CALL FETCH reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: parse CALL FETCH reply: %w", err)
	}
	outcome = interpretFetchReply(rep)
	// CALL-cursor FETCH replies routinely carry rows AND an end-of-
	// data signal (EC=2 RC=701) when the proc's result set fits in
	// one block. The SELECT-side fetchMoreRows early-returns on
	// exhausted without parsing rows, but for CALL cursors we have
	// to parse the row payload before honouring the exhaustion
	// flag -- otherwise the rows in CP 0x380E get silently dropped
	// on the way out and Rows.Next sees an empty result set.
	// makeDb2Error treats EC=1 RC=100 and EC=2 RC=701 as warnings,
	// not errors, so we can call it before parsing.
	if dbErr := makeDb2Error(rep, "CALL FETCH"); dbErr != nil {
		return nil, fetchOutcome{}, dbErr
	}
	rows, err = rep.findExtendedResultData(cols)
	if err != nil {
		return nil, fetchOutcome{}, fmt.Errorf("hostserver: parse CALL FETCH row data: %w", err)
	}
	if len(rows) == 0 {
		outcome.exhausted = true
	}
	return rows, outcome, nil
}

// cpDBReuseIndicator is the CP for the close-time "what should the
// server do with the prepared statement" flag, per JT400's
// DBSQLRequestDS.setReuseIndicator (offset 0x3810). Values per
// JDCursor.REUSE_*:
//
//	0xF0 REUSE_NO          -- close cursor, drop prepared statement
//	0xF1 REUSE_YES         -- close cursor, keep prepared statement
//	0xF2 REUSE_RESULT_SET  -- preserve cursor for re-fetch from start
//
// We use REUSE_NO (0xF0) on early Close because we don't cache
// prepared statements between Stmt invocations -- each Query opens
// a fresh PREPARE.
const cpDBReuseIndicator uint16 = 0x3810
const reuseNo byte = 0xF0

// closeCursor sends a CLOSE (0x180A) for cursor CRSR0001 -- the
// cursor name we always use, matching the CREATE_RPB invocation.
//
// SQL-501 / SQLSTATE 24501 ("cursor is not open") is treated as
// success: the server may have auto-closed the cursor on the FETCH
// that returned the last batch, in which case our follow-up CLOSE
// is redundant but harmless. We still need to send it because the
// server-side state machine sometimes leaves the cursor in a
// "between batches" state where the next PREPARE on the same RPB
// would otherwise fail with SQL-519.
//
// Uses REUSE_NO (0xF0): close cursor AND drop the prepared statement.
// For the multi-result-set advance path (M9-3) callers want to
// preserve the prepared statement -- use closeCursorReuse with
// reuseResultSet (0xF2) instead.
func closeCursor(conn io.ReadWriter, nextCorrelation uint32) error {
	return closeCursorReuse(conn, nextCorrelation, reuseNo)
}

// closeCursorReuse is the parameterised form of closeCursor. reuse
// is one of 0xF0 (REUSE_NO -- drop prepared statement), 0xF1
// (REUSE_YES -- keep prepared statement), or 0xF2 (REUSE_RESULT_SET
// -- preserve cursor for OPEN_DESCRIBE re-attach). M9-3's multi-
// result-set advance passes reuseResultSet so the next 0x1804 frame
// can grab the proc's next dynamic result set on the same RPB.
func closeCursorReuse(conn io.ReadWriter, nextCorrelation uint32, reuse byte) error {
	tpl := DBRequestTemplate{
		ORSBitmap:                 ORSReturnData | ORSSQLCA | ORSDataCompression,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 0,
	}
	params := []DBParam{
		DBParamByte(cpDBReuseIndicator, reuse),
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLClose, tpl, params)
	if err != nil {
		return fmt.Errorf("hostserver: build CLOSE: %w", err)
	}
	hdr.CorrelationID = nextCorrelation
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send CLOSE: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorrelation, 4)
	if err != nil {
		return fmt.Errorf("hostserver: read CLOSE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return fmt.Errorf("hostserver: CLOSE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return fmt.Errorf("hostserver: parse CLOSE reply: %w", err)
	}
	dbErr := makeDb2Error(rep, "CLOSE")
	if dbErr == nil {
		return nil
	}
	// SQL-501 / 24501 = "cursor not open". The server already
	// closed it on its own (typical after the cursor's own FETCH
	// drained to end-of-data); CLOSE is a no-op in that state.
	if dbErr.SQLCode == -501 || dbErr.SQLState == "24501" {
		return nil
	}
	return dbErr
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
		ORSBitmap:                 ORSReturnData | ORSDataCompression,
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
// out of the super-extended data format CP. Mirrors the fields a
// JDBC ResultSetMetaData / database/sql ColumnType caller will
// reach for, so the M6 driver wrapper can answer ColumnTypes
// without re-deriving anything.
type SelectColumn struct {
	Name      string
	SQLType   uint16 // raw IBM i SQL type (e.g. 392=TIMESTAMP, 448=VARCHAR)
	Length    uint32
	Scale     uint16
	Precision uint16
	CCSID     uint16

	// Derived metadata. TypeName mirrors java.sql's
	// JDBCType.getName conventions ("INTEGER", "VARCHAR",
	// "DECIMAL", ...). DisplaySize is the maximum number of
	// characters needed to print the value; Nullable reflects the
	// type code's NN/nullable parity (even = not nullable, odd =
	// nullable per JT400 convention). Signed is true for numeric
	// types that can be negative.
	TypeName    string
	DisplaySize int
	Nullable    bool
	Signed      bool

	// Extended-metadata fields (populated when the request set the
	// ORSExtendedColumnDescrs bit; otherwise empty). Sourced from
	// the CP 0x3811 reply parameter and decoded per JT400's
	// DBColumnDescriptorsDataFormat: 0x3900 = base column name,
	// 0x3901 = base table name, 0x3902 = column label (carries its
	// own CCSID), 0x3904 = schema name.
	//
	// Schema, Table, BaseColumnName are EBCDIC bytes pre-decoded
	// through the column's server-job CCSID; Label is decoded
	// through the CCSID the server stamped on its 0x3902 record.
	// All four are empty strings when the server didn't include
	// them, the caller didn't ask for extended metadata, or the
	// query target doesn't have a single base table (e.g. computed
	// columns, joins, expressions).
	Schema         string
	Table          string
	BaseColumnName string
	Label          string
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
	cursor, err := OpenSelectStatic(conn, sql, closureFromInt(nextCorrelation))
	if err != nil {
		return nil, err
	}
	return cursor.drainAll()
}

// SelectOption tweaks request-side behaviour of OpenSelectStatic /
// OpenSelectPrepared. Compose with the variadic tail of each:
//
//	hostserver.OpenSelectStatic(conn, sql, nextCorr,
//	    hostserver.WithExtendedMetadata(true))
//
// Zero options reproduces the historical behaviour byte-for-byte.
type SelectOption func(*selectOpts)

type selectOpts struct {
	// extendedMetadata, when true, ORs the
	// ORSExtendedColumnDescrs bit (0x00020000) into the
	// PREPARE_DESCRIBE / OPEN_DESCRIBE_FETCH ORS bitmaps. The
	// server then includes a CP 0x3811 parameter in the reply
	// carrying per-column schema / table / base column name /
	// label. enrichWithExtendedColumnDescriptors overlays those
	// fields onto the SelectColumn slice once the data format CP
	// has been parsed.
	extendedMetadata bool

	// extendedDynamic, when true, appends a CP 0x3804
	// (cpPackageName) parameter carrying the package name to
	// PREPARE_DESCRIBE requests. The parameter tells the server
	// to add the prepared statement to the package the connection
	// registered via CREATE_PACKAGE at connect time. Without it
	// the prepare runs in normal (non-package) mode. Mirrors
	// JT400's "extended dynamic=true" JDBC URL knob.
	//
	// JT400's wire shape (verified against captured fixtures) is
	// the full 10-char EBCDIC package name as a CCSID-tagged
	// var-string, NOT an empty marker -- the earlier M10
	// implementation got this wrong and the server silently failed
	// to file statements into the *PGM. packageName / packageCCSID
	// carry the bytes WithPackageName attaches.
	extendedDynamic bool
	packageName     string
	packageCCSID    uint16
	packageLibrary  string
}

// rpbStringCCSID returns the CCSID the CREATE_RPB statement-name
// / cursor-name var-strings are tagged with. Our M0-M9 captures
// (prepared_int_param.trace, prepared_call_*.trace, etc.) used
// CCSID 273; JT400's newer extended-dynamic captures (first_use,
// cache_download, cache_hit) use CCSID 37 throughout. Both work
// on V7R6M0 because the IBM-i-object-name charset (A-Z + 0-9 + _)
// round-trips byte-identical between SBCS EBCDIC pages. We keep
// 273 for back-compat with the older fixtures' byte-level tests;
// future fixture re-captures may flip this.
func rpbStringCCSID() uint16 { return 273 }

// WithExtendedMetadata asks the server to ship per-column schema,
// table, base column name, and label in the PREPARE_DESCRIBE
// reply. Mirrors JT400's `extended metadata=true` JDBC URL knob.
//
// The default-OFF behaviour keeps the wire shape byte-identical to
// pre-M4-deferred captures so existing fixtures still replay
// cleanly; callers opt in per-statement when they need
// `Rows.ColumnTypeSchemaName` / `Rows.ColumnTypeTableName`.
func WithExtendedMetadata(b bool) SelectOption {
	return func(o *selectOpts) { o.extendedMetadata = b }
}

// WithExtendedDynamic asks the server to file every PREPAREd
// statement into the SQL package the connection registered at
// connect time. The on-wire change is a CP 0x3804 parameter on
// PREPARE_DESCRIBE carrying the 10-char EBCDIC package name as a
// CCSID-tagged var-string; the server-side effect is that
// repeated PREPAREs of the same SQL (across this and other
// connections targeting the same *PGM) re-use the cached parse
// plan instead of re-parsing. Mirrors JT400's "extended
// dynamic=true" JDBC URL knob.
//
// Callers MUST also call WithPackageName -- without a package
// name the option has no effect (the wire CP requires the bytes
// to point the server at the right *PGM).
func WithExtendedDynamic(b bool) SelectOption {
	return func(o *selectOpts) { o.extendedDynamic = b }
}

// WithPackageName supplies the 10-char EBCDIC package name +
// CCSID the CP 0x3804 parameter carries on PREPARE_DESCRIBE when
// WithExtendedDynamic is on. ccsid=37 matches JT400's V7R6M0
// wire (US EBCDIC); other SBCS EBCDIC pages work because the
// package-name charset (A-Z + 0-9 + _#@$) round-trips identically.
//
// Pass-through: the wire encoder lifts `name` via
// asciiToEBCDIC37 (the same helper CREATE_PACKAGE uses), so the
// caller passes the same ASCII / Go string that BuildPackageName
// produced.
func WithPackageName(name string, ccsid uint16) SelectOption {
	return func(o *selectOpts) {
		o.packageName = name
		o.packageCCSID = ccsid
	}
}

// WithPackageLibrary supplies the package library name (CP 0x3801)
// that CREATE_RPB attaches to the request parameter block when
// extended-dynamic is on. JT400 sends this so the server knows
// which library to look in for the *PGM when the per-statement
// CP 0x3804 arrives on PREPARE_DESCRIBE. Without it the server
// returns SQL-112 errorClass=2 on PREPARE_DESCRIBE even when the
// package name CP is correct.
//
// Encoded the same way as the package name: ASCII bytes lifted
// through asciiToEBCDIC37. Most callers pass the connection's
// Config.PackageLibrary (default "QGPL").
func WithPackageLibrary(lib string) SelectOption {
	return func(o *selectOpts) { o.packageLibrary = lib }
}

func resolveSelectOpts(opts []SelectOption) selectOpts {
	var o selectOpts
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

// openStaticUntilFirstBatch is the shared implementation used by
// both SelectStaticSQL (which then drains all rows) and
// OpenSelectStatic (which wraps the result in a *Cursor for lazy
// iteration). It runs CREATE_RPB / PREPARE_DESCRIBE /
// OPEN_DESCRIBE_FETCH and parses the first batch out of the OPEN
// reply -- subsequent batches arrive via fetchMoreRows, which
// callers invoke as appropriate. The returned fetchOutcome
// reports whether the initial OPEN already drained the cursor
// and whether the server auto-closed it (the typical JT400 path
// for results that fit in one block-fetch buffer).
//
// On any error after CREATE_RPB has run, the RPB slot is freed
// before the function returns so the next SELECT on the connection
// can chain cleanly.
func openStaticUntilFirstBatch(conn io.ReadWriter, sql string, nextCorr func() uint32, opts selectOpts) ([]SelectColumn, []SelectRow, fetchOutcome, error) {
	// --- 1) CREATE_RPB. ---
	stmtNameBytes, err := ebcdic.CCSID37.Encode("STMT0001")
	if err != nil {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: encode stmt name: %w", err)
	}
	cursorNameBytes, err := ebcdic.CCSID37.Encode("CRSR0001")
	if err != nil {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: encode cursor name: %w", err)
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
		if opts.extendedDynamic && opts.packageLibrary != "" {
			libParam, err := buildPackageLibraryParam(opts.packageLibrary)
			if err != nil {
				return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: encode package library: %w", err)
			}
			params = append(params, libParam)
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLRPBCreate, tpl, params)
		if err != nil {
			return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: build CREATE_RPB: %w", err)
		}
		hdr.CorrelationID = nextCorr()
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: send CREATE_RPB: %w", err)
		}
	}
	// CREATE_RPB has no reply expected (ORS bit 1 not set).

	// --- 2) PREPARE_DESCRIBE. ---
	stmtBytes := utf16BE(sql)
	if len(stmtBytes) > 0xFFFFFFFF {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: SQL too long: %d bytes", len(stmtBytes))
	}
	// JTOpen toggles ORSParameterMarkerFmt (bit 16) iff the SQL
	// contains '?' markers; with it the reply carries CP 0x3808
	// (parameter-marker format) describing how to bind. Static
	// SELECTs leave the bit clear -- live PUB400 confirms; setting
	// it on a no-marker statement returns malformed SQLDA replies.
	ors := ORSReturnData | ORSDataFormat | ORSSQLCA | ORSDataCompression
	if strings.ContainsRune(sql, '?') {
		ors |= ORSParameterMarkerFmt
	}
	if opts.extendedMetadata {
		ors |= ORSExtendedColumnDescrs
	}
	prepCorr := nextCorr()
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
			DBParamByte(cpDBPrepareOption, prepareOptionByte(opts.extendedDynamic && opts.packageName != "")),
		}
		if opts.extendedMetadata {
			// CP 0x3829 (ExtendedColumnDescriptorOption) = 0xF1
			// asks the server to populate CP 0x3811 with per-column
			// schema / table / base column / label. Without it the
			// server ships CP 0x3811 with zero data, even when the
			// request's ORS bit asks for it. Per JT400's
			// DBSQLRequestDS.setExtendedColumnDescriptorOption.
			params = append(params, DBParamByte(0x3829, 0xF1))
		}
		if opts.extendedDynamic {
			// JT400's commonPrepare emits two wire shapes for the
			// CP 0x3804 package marker:
			//   - sqlStatement.isPackaged() true  -> full var-string with
			//     the package name, prepare-option byte = 0x01
			//   - isPackaged() false              -> empty LL=6 marker,
			//     prepare-option byte = 0x00
			// The first form is the one that actually files the
			// statement into the *PGM (we use sqlStatement.isPackaged
			// equivalent via WithPackageName being set by the driver
			// when packageEligibleFor returned true). The empty
			// marker is a no-op the server tolerates but doesn't
			// store. See AS400JDBCStatement.java:1955-1965 in the
			// IBM/JTOpen source for the original conditional.
			pkgParam, err := buildPackageMarkerParam(opts.packageName, opts.packageCCSID)
			if err != nil {
				return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: encode package marker: %w", err)
			}
			params = append(params, pkgParam)
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLPrepareDescribe, tpl, params)
		if err != nil {
			return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: build PREPARE_DESCRIBE: %w", err)
		}
		hdr.CorrelationID = prepCorr
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: send PREPARE_DESCRIBE: %w", err)
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
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: read PREPARE_DESCRIBE reply: %w", err)
	}
	if prepRepHdr.ReqRepID != RepDBReply {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: PREPARE_DESCRIBE reply ReqRepID 0x%04X (want 0x%04X)", prepRepHdr.ReqRepID, RepDBReply)
	}
	prepRep, err := ParseDBReply(prepRepPayload)
	if err != nil {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: parse PREPARE_DESCRIBE reply: %w", err)
	}
	if dbErr := makeDb2Error(prepRep, "PREPARE_DESCRIBE"); dbErr != nil {
		// Free the RPB so the next SELECT on this connection
		// can chain. Don't fail on cleanup -- the original error
		// is what the caller cares about.
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fetchOutcome{}, dbErr
	}
	cols, err := prepRep.findSuperExtendedDataFormat()
	if err != nil {
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: parse column descriptors: %w", err)
	}
	if opts.extendedMetadata {
		// CP 0x3811 carries the extended descriptors. Best-effort
		// overlay -- ParseDBReply already validated the LL/CP shape,
		// so we just walk the parsed parameter list looking for our
		// CP. Skipping silently on absence keeps the path resilient
		// against servers that ignore the ORS bit on certain
		// statement shapes (computed columns, joins, etc.) or that
		// ship the CP with empty data when the per-statement option
		// CP 0x3829 wasn't accepted.
		for _, p := range prepRep.Params {
			if p.CodePoint == 0x3811 && len(p.Data) > 0 {
				enrichWithExtendedColumnDescriptors(cols, p.Data)
				break
			}
		}
	}
	if len(cols) == 0 {
		// List the CPs that did come back -- helps when the
		// server picks the original (0x3805) or extended (0x380C)
		// format instead of super-extended (0x3812) we expected.
		var present []string
		for _, p := range prepRep.Params {
			present = append(present, fmt.Sprintf("0x%04X(%dB)", p.CodePoint, len(p.Data)))
		}
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: PREPARE_DESCRIBE reply missing column descriptors (CPs in reply: %v)", present)
	}

	// --- 3) OPEN_DESCRIBE_FETCH. ---
	var fetchCorr uint32
	{
		tpl := DBRequestTemplate{
			// 0x86048000: return data + result data + SQLCA + RLE +
			// cursor attributes (bit 17 = 0x00008000). Handle
			// layout matches PREPARE_DESCRIBE so OPEN reuses the
			// same RPB (RPBHandle=1).
			ORSBitmap:                 ORSReturnData | ORSResultData | ORSSQLCA | ORSDataCompression | ORSCursorAttributes,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		params := []DBParam{
			DBParamByte(cpDBOpenAttributes, 0x80), // read-only cursor
		}
		if opts.extendedDynamic && opts.packageName != "" {
			pkgParam, err := buildPackageMarkerParam(opts.packageName, opts.packageCCSID)
			if err != nil {
				return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: encode package marker: %w", err)
			}
			params = append(params, pkgParam)
		}
		params = append(params,
			DBParamByte(cpDBVariableFieldCompr, 0xE8),                                  // VLF compression on
			DBParam{CodePoint: cpDBBufferSize, Data: []byte{0x00, 0x00, 0x80, 0x00}},   // 32 KB buffer
			DBParamShort(cpDBScrollableCursorFlag, 0x0000),
			DBParamByte(cpDBResultSetHoldability, 0xE8),
		)
		hdr, payload, err := BuildDBRequest(ReqDBSQLOpenDescribeFetch, tpl, params)
		if err != nil {
			return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: build OPEN_DESCRIBE_FETCH: %w", err)
		}
		fetchCorr = nextCorr()
		hdr.CorrelationID = fetchCorr
		if err := WriteFrame(conn, hdr, payload); err != nil {
			return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: send OPEN_DESCRIBE_FETCH: %w", err)
		}
	}
	fetchRepHdr, fetchRepPayload, err := ReadDBReplyMatching(conn, fetchCorr, 8)
	if err != nil {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: read OPEN_DESCRIBE_FETCH reply: %w", err)
	}
	if fetchRepHdr.ReqRepID != RepDBReply {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: OPEN_DESCRIBE_FETCH reply ReqRepID 0x%04X (want 0x%04X)", fetchRepHdr.ReqRepID, RepDBReply)
	}
	fetchRep, err := ParseDBReply(fetchRepPayload)
	if err != nil {
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: parse OPEN_DESCRIBE_FETCH reply: %w", err)
	}
	// JT400's "fetch/close" path -- ErrorClass=2, RC=700 -- means
	// the server delivered the entire result in one block AND
	// closed the cursor on its own. interpretFetchReply covers
	// that plus SQL +100 / EC=2 RC=701 (end of data without auto-
	// close). All three are warnings, not errors; makeDb2Error
	// returns nil for any positive RC so the data still flows.
	outcome := interpretFetchReply(fetchRep)
	if dbErr := makeDb2Error(fetchRep, "OPEN_DESCRIBE_FETCH"); dbErr != nil {
		// Server-reported error during OPEN. If the server already
		// auto-closed the cursor (outcome.serverClosed), skip the
		// explicit CLOSE so we don't trip SQL-501 / 24501; otherwise
		// CLOSE first to drop both cursor and prepared statement so
		// the next PREPARE_DESCRIBE on this conn doesn't trip
		// SQL-519 / 24506 against an orphaned cursor.
		if !outcome.serverClosed {
			_ = closeCursor(conn, nextCorr())
		}
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fetchOutcome{}, dbErr
	}
	rows, err := fetchRep.findExtendedResultData(cols)
	if err != nil {
		if !outcome.serverClosed {
			_ = closeCursor(conn, nextCorr())
		}
		_ = deleteRPB(conn, nextCorr())
		return nil, nil, fetchOutcome{}, fmt.Errorf("hostserver: parse row data: %w", err)
	}
	return cols, rows, outcome, nil
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
