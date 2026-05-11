package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// Stmt holds the SQL string for later Exec/Query. We don't currently
// PREPARE on the server until execute time, so Stmt is essentially a
// closure over (conn, query). NumInput returns -1 (unknown) so the
// runtime won't try to count parameter markers itself; the bind
// path validates count when it builds the wire format.
type Stmt struct {
	conn  *Conn
	query string
}

// NumInput: -1 means "let the driver figure it out" -- we don't
// currently bind parameters via this path (M3 deferred work).
func (s *Stmt) NumInput() int { return -1 }

// Close releases any client-side resources tied to the prepared
// statement. The IBM i Db2 prepared-statement handle is bound to the
// underlying connection's RPB slot, so the server-side cleanup
// happens when the next PREPARE rebinds the same slot rather than on
// statement Close. Implements database/sql/driver.Stmt.Close.
func (s *Stmt) Close() error { return nil }

// CheckNamedValue lets database/sql forward our LOB bind type
// (*LOBValue) through the driver boundary without the default
// parameter converter rejecting it for not being one of the six
// driver.Value flavours. Returning nil tells database/sql "leave
// the value alone, the driver knows what to do with it"; returning
// driver.ErrSkip tells it to fall back to its default conversion
// (int -> int64, etc.).
//
// Implements database/sql/driver.NamedValueChecker.
func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(*LOBValue); ok {
		return nil
	}
	return driver.ErrSkip
}

// ExecContext implements driver.StmtExecContext. Plumbs ctx through
// to the underlying net.Conn via SetDeadline so a canceled / timed-
// out request unblocks the in-flight wire read instead of hanging
// for the connection's full timeout. Returns ctx.Err() (Canceled /
// DeadlineExceeded) when the cancellation is the actual cause,
// regardless of which I/O step bailed.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	cleanup := withContextDeadline(ctx, s.conn.conn)
	defer cleanup()
	res, err := s.Exec(namedToValues(args))
	if err != nil {
		return nil, resolveCtxErr(ctx, err)
	}
	return res, nil
}

// QueryContext implements driver.StmtQueryContext. Same plumbing as
// ExecContext.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	cleanup := withContextDeadline(ctx, s.conn.conn)
	defer cleanup()
	rows, err := s.Query(namedToValues(args))
	if err != nil {
		return nil, resolveCtxErr(ctx, err)
	}
	return rows, nil
}

// namedToValues drops the parameter names (we don't use them; IBM i
// SQL is positional). Order is preserved.
func namedToValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

// Exec runs INSERT / UPDATE / DELETE / DDL. With no args it uses the
// single-frame ExecuteImmediate; with args it goes through the
// prepared-DML flow (CREATE_RPB / PREPARE_DESCRIBE / CHANGE_DESCRIPTOR
// / EXECUTE). Both paths return the affected-row count via Result --
// today always 0 since SQLCA decoding is M7 work.
//
// Errors flow through classifyConnErr so a TCP-level failure marks
// the parent Conn dead; the next checkout from the database/sql
// pool then sees driver.ErrBadConn and gets a fresh connection.
// Statement-level errors (syntax, constraint, etc.) flow through
// unchanged as *Db2Error.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if isSelect(s.query) {
		return nil, fmt.Errorf("gojtopen: Exec called with SELECT; use Query")
	}
	start := time.Now()
	logger := s.conn.log
	if len(args) == 0 {
		res, err := hostserver.ExecuteImmediate(s.conn.conn, s.query, s.conn.nextCorr())
		s.logExec(logger, "EXECUTE_IMMEDIATE", 0, start, res, err)
		if err != nil {
			return nil, s.conn.classifyConnErr(err)
		}
		return &Result{rowsAffected: res.RowsAffected, conn: s.conn}, nil
	}
	shapes, values, err := bindArgsToPreparedParams(args, s.conn.preferredStringCCSID())
	if err != nil {
		return nil, err
	}
	res, err := hostserver.ExecutePreparedSQL(s.conn.conn, s.query, shapes, values, s.conn.nextCorr())
	s.logExec(logger, "EXECUTE_PREPARED", len(args), start, res, err)
	if err != nil {
		return nil, s.conn.classifyConnErr(err)
	}
	return &Result{rowsAffected: res.RowsAffected, conn: s.conn}, nil
}

// logExec emits one DEBUG line per Exec call. SQL text is gated on
// Config.LogSQL; parameter values are never logged.
func (s *Stmt) logExec(logger *slog.Logger, op string, paramCount int, start time.Time, res *hostserver.ExecResult, err error) {
	if logger == nil {
		return
	}
	attrs := []slog.Attr{
		slog.String("op", op),
		slog.Int("params", paramCount),
		slog.Duration("elapsed", time.Since(start)),
	}
	if res != nil {
		attrs = append(attrs, slog.Int64("rows_affected", res.RowsAffected))
	}
	if s.conn.cfg != nil && s.conn.cfg.LogSQL {
		attrs = append(attrs, slog.String("sql", s.query))
	}
	if err != nil {
		attrs = append(attrs, slog.String("err", err.Error()))
		logger.LogAttrs(context.Background(), slog.LevelDebug, "gojtopen: exec failed", attrs...)
		return
	}
	logger.LogAttrs(context.Background(), slog.LevelDebug, "gojtopen: exec", attrs...)
}

// Query runs a SELECT (or VALUES / WITH). With no args it opens a
// streaming cursor via OpenSelectStatic; with args it opens via
// OpenSelectPrepared. The cursor pulls subsequent batches lazily as
// the caller's Rows.Next iterates -- a million-row SELECT pays one
// 32 KB-buffer FETCH round-trip per batch instead of one per row.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if !isSelect(s.query) {
		return nil, fmt.Errorf("gojtopen: Query called with non-SELECT; use Exec")
	}
	selectOpts := s.conn.selectOptions()
	start := time.Now()
	if len(args) == 0 {
		cursor, err := hostserver.OpenSelectStatic(s.conn.conn, s.query, s.conn.nextCorrFunc(), selectOpts...)
		s.logQuery("OPEN_SELECT_STATIC", 0, start, err)
		if err != nil {
			return nil, s.conn.classifyConnErr(err)
		}
		return &Rows{cursor: cursor, conn: s.conn}, nil
	}
	shapes, values, err := bindArgsToPreparedParams(args, s.conn.preferredStringCCSID())
	if err != nil {
		return nil, err
	}
	cursor, err := hostserver.OpenSelectPrepared(s.conn.conn, s.query, shapes, values, s.conn.nextCorrFunc(), selectOpts...)
	s.logQuery("OPEN_SELECT_PREPARED", len(args), start, err)
	if err != nil {
		return nil, s.conn.classifyConnErr(err)
	}
	return &Rows{cursor: cursor, conn: s.conn}, nil
}

// logQuery emits one DEBUG line per Query call. SQL text is gated
// on Config.LogSQL.
func (s *Stmt) logQuery(op string, paramCount int, start time.Time, err error) {
	logger := s.conn.log
	if logger == nil {
		return
	}
	attrs := []slog.Attr{
		slog.String("op", op),
		slog.Int("params", paramCount),
		slog.Duration("elapsed", time.Since(start)),
	}
	if s.conn.cfg != nil && s.conn.cfg.LogSQL {
		attrs = append(attrs, slog.String("sql", s.query))
	}
	if err != nil {
		attrs = append(attrs, slog.String("err", err.Error()))
		logger.LogAttrs(context.Background(), slog.LevelDebug, "gojtopen: query failed", attrs...)
		return
	}
	logger.LogAttrs(context.Background(), slog.LevelDebug, "gojtopen: query", attrs...)
}

// bindArgsToPreparedParams maps each driver.Value to a typed
// PreparedParam shape + matching value. The shape describes the
// parameter's SQL type / length / CCSID so the server knows how to
// interpret the value bytes; the value goes into the data CP.
//
// driver.Value is a restricted union: int64, float64, bool, []byte,
// string, time.Time, nil. We pick the smallest IBM i SQL type that
// covers each Go type:
//
//	int64        -> BIGINT  (493)            8 bytes
//	float64      -> DOUBLE  (481)            8 bytes
//	bool         -> SMALLINT (501)           2 bytes (0/1)
//	[]byte       -> VARCHAR FOR BIT DATA     2-byte SL + bytes;
//	                (449 + CCSID 65535)      uses nullable-flavour
//	                                         448+1 so server allows
//	                                         the indicator
//	string       -> VARCHAR  (449 + stringCCSID)
//	                                         2-byte SL + payload bytes
//	time.Time    -> TIMESTAMP (393)         26 bytes ISO format
//	nil          -> INTEGER nullable (497)  flagged via indicator
//
// stringCCSID is supplied by the caller (typically the Conn's
// preferredStringCCSID()): CCSID 1208 (UTF-8) on V7R3+ servers --
// preserves the full Unicode repertoire by writing the UTF-8 bytes
// verbatim and letting the server transcode -- and CCSID 37
// (US English EBCDIC) on older servers that don't accept tagged
// CCSIDs on parameter binds.
//
// The nullable flavour (odd SQL type) is used for every bind so a
// future caller can pass NULL through the same shape without changing
// the request frame; the indicator block decides null vs not-null.
func bindArgsToPreparedParams(args []driver.Value, stringCCSID uint16) ([]hostserver.PreparedParam, []any, error) {
	if stringCCSID == 0 {
		stringCCSID = 37
	}
	shapes := make([]hostserver.PreparedParam, len(args))
	values := make([]any, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case int64:
			shapes[i] = hostserver.PreparedParam{SQLType: 493, FieldLength: 8}
			values[i] = v
		case float64:
			shapes[i] = hostserver.PreparedParam{SQLType: 481, FieldLength: 8}
			values[i] = v
		case bool:
			shapes[i] = hostserver.PreparedParam{SQLType: 501, FieldLength: 2}
			if v {
				values[i] = int32(1)
			} else {
				values[i] = int32(0)
			}
		case []byte:
			// FieldLength must include the 2-byte length prefix.
			// CCSID 65535 routes the encoder through the binary
			// passthrough branch (no EBCDIC conversion).
			//
			// Note: when this value targets a BLOB column the
			// hostserver layer detects that during PREPARE_DESCRIBE
			// reply parsing and overrides the shape to the LOB
			// locator path; the user's []byte then ships via
			// WRITE_LOB_DATA instead of inline. See bindLOBParameters
			// in hostserver/db_lob.go.
			shapes[i] = hostserver.PreparedParam{
				SQLType:     449,
				FieldLength: uint32(len(v)) + 2,
				Precision:   uint16(len(v)),
				CCSID:       65535,
			}
			values[i] = v
		case *LOBValue:
			// Explicit LOB bind. Bytes go through the same []byte
			// path inside bindLOBParameters; Reader-driven values
			// go through the hostserver.LOBStream interface that
			// *LOBValue satisfies.
			//
			// The placeholder VARCHAR shape here is just a
			// best-effort starting point that gets overwritten by
			// bindLOBParameters once the server's parameter marker
			// format is parsed. We size the placeholder at 4 bytes
			// (the locator handle width) so the descriptor's
			// row-size column total stays accurate even before
			// the override runs.
			rv, err := resolveLOBValue(v)
			if err != nil {
				return nil, nil, fmt.Errorf("gojtopen: param %d: %w", i, err)
			}
			shapes[i] = hostserver.PreparedParam{
				SQLType:     961, // BLOB locator NN; bindLOBParameters fixes up SQLType + CCSID
				FieldLength: 4,
				CCSID:       65535,
			}
			values[i] = rv
		case string:
			// FieldLength sizes the field as 2-byte SL + payload
			// length. UTF-8 strings can encode 1-4 bytes per rune;
			// for CCSID 1208 the byte length IS len(v). For CCSID 37
			// (EBCDIC SBCS) the encoded bytes are also 1:1 with len(v)
			// for the ASCII subset we currently support.
			shapes[i] = hostserver.PreparedParam{
				SQLType:     449,
				FieldLength: uint32(len(v)) + 2,
				Precision:   uint16(len(v)),
				CCSID:       stringCCSID,
			}
			values[i] = v
		case time.Time:
			// IBM i timestamp: 26 chars "YYYY-MM-DD-HH.MM.SS.ffffff".
			// encodeTimestampString maps this to wire form via
			// CCSID-37 EBCDIC.
			s := v.UTC().Format("2006-01-02-15.04.05.000000")
			shapes[i] = hostserver.PreparedParam{SQLType: 393, FieldLength: 26}
			values[i] = s
		case nil:
			// NULL: SQLType picks the fixed-length INTEGER nullable
			// shape so FieldLength is meaningful (4 bytes); the
			// server reads the indicator first and ignores the data
			// slot. Column type mismatches are handled by the
			// server's implicit cast.
			shapes[i] = hostserver.PreparedParam{SQLType: 497, FieldLength: 4}
			values[i] = nil
		default:
			return nil, nil, fmt.Errorf("gojtopen: param %d: unsupported Go type %T (driver.Value union: int64/float64/bool/[]byte/string/time.Time/nil)", i, a)
		}
	}
	return shapes, values, nil
}

// isSelect returns true iff the SQL begins with SELECT, VALUES, WITH,
// or any other read-only verb. Used to dispatch Exec vs Query without
// requiring the caller to pre-classify.
func isSelect(sql string) bool {
	for i, r := range sql {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		head := sql[i:]
		// 6 is the longest read-only verb we look for ("SELECT")
		if len(head) >= 6 && strings.EqualFold(head[:6], "SELECT") {
			return true
		}
		if len(head) >= 6 && strings.EqualFold(head[:6], "VALUES") {
			return true
		}
		if len(head) >= 4 && strings.EqualFold(head[:4], "WITH") {
			return true
		}
		return false
	}
	return false
}
