package driver

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/complacentsee/go-db2i/hostserver"
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
// (*LOBValue) AND the stdlib sql.Out wrapper (for stored-procedure
// OUT / INOUT parameters) through the driver boundary without the
// default parameter converter rejecting them for not being one of
// the six driver.Value flavours. Returning nil tells database/sql
// "leave the value alone, the driver knows what to do with it";
// returning driver.ErrSkip tells it to fall back to its default
// conversion (int -> int64, etc.).
//
// Implements database/sql/driver.NamedValueChecker.
func (s *Stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(*LOBValue); ok {
		return nil
	}
	if _, ok := nv.Value.(stdsql.Out); ok {
		// database/sql passes sql.Out{Dest: &x} through as a value
		// (not a pointer); the bind path in
		// bindArgsToPreparedParams reads Dest/In via reflect.
		// Returning nil here keeps Go's default converter from
		// rejecting Out with "unsupported type ...struct".
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

	ctx, span := s.startSpan(ctx, "EXEC", len(args))
	defer span.End()

	res, err := s.Exec(namedToValues(args))
	if err != nil {
		err = resolveCtxErr(ctx, err)
		s.recordSpanError(span, err)
		return nil, err
	}
	if r, ok := res.(*Result); ok {
		span.SetAttributes(attribute.Int64("db.response.returned_rows", r.rowsAffected))
	}
	return res, nil
}

// QueryContext implements driver.StmtQueryContext. Same plumbing as
// ExecContext.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	cleanup := withContextDeadline(ctx, s.conn.conn)
	defer cleanup()

	ctx, span := s.startSpan(ctx, "QUERY", len(args))
	defer span.End()

	rows, err := s.Query(namedToValues(args))
	if err != nil {
		err = resolveCtxErr(ctx, err)
		s.recordSpanError(span, err)
		return nil, err
	}
	return rows, nil
}

// startSpan starts a span on the conn's tracer following OpenTelemetry
// database semantic conventions. Returns the derived ctx (caller
// passes it on so child spans nest correctly) and the started span
// (caller is responsible for ending it via defer).
//
// Span name is the operation verb ("EXEC", "QUERY") since the driver
// can't reliably parse the SQL into a target table/procedure without
// a query parser; the convention's "operation name" guidance allows
// a free-form operation when the underlying API is statement-oriented.
func (s *Stmt) startSpan(ctx context.Context, op string, paramCount int) (context.Context, trace.Span) {
	tracer := s.conn.tracer
	if tracer == nil {
		tracer = noopTracer
	}
	attrs := []attribute.KeyValue{
		// db.system.name uses the dialect form introduced in the May
		// 2025 conventions refresh. Older collectors that key off the
		// historical "db.system" attribute key still recognise the
		// value via wildcard matchers.
		attribute.String("db.system.name", "ibm_db2_for_i"),
		attribute.String("db.operation.name", op),
		attribute.Int("db.statement.parameters.count", paramCount),
	}
	if s.conn.cfg != nil {
		if s.conn.cfg.Library != "" {
			attrs = append(attrs, attribute.String("db.namespace", s.conn.cfg.Library))
		}
		if s.conn.cfg.User != "" {
			attrs = append(attrs, attribute.String("db.user", s.conn.cfg.User))
		}
		if s.conn.cfg.Host != "" {
			attrs = append(attrs, attribute.String("server.address", s.conn.cfg.Host))
		}
		if s.conn.cfg.DBPort != 0 {
			attrs = append(attrs, attribute.Int("server.port", s.conn.cfg.DBPort))
		}
		if s.conn.cfg.LogSQL {
			attrs = append(attrs, attribute.String("db.statement", s.query))
		}
	}
	return tracer.Start(ctx, op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...),
	)
}

// recordSpanError sets the span status to Error and, for *Db2Error,
// attaches the structured SQLSTATE / SQLCODE / MessageID attributes
// so consumers can route alerts off them instead of regexing the
// span event's free-form message.
func (s *Stmt) recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.SetStatus(codes.Error, err.Error())
	var dbErr *hostserver.Db2Error
	if errors.As(err, &dbErr) {
		span.SetAttributes(
			attribute.String("db.response.status_code", dbErr.SQLState),
			attribute.Int("db.ibm_db2_for_i.sqlcode", int(dbErr.SQLCode)),
			attribute.String("db.ibm_db2_for_i.message_id", dbErr.MessageID),
		)
	}
	span.RecordError(err)
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
		return nil, fmt.Errorf("db2i: Exec called with SELECT; use Query")
	}
	start := time.Now()
	logger := s.conn.log
	// CALL always goes through CREATE_RPB + PREPARE_DESCRIBE +
	// EXECUTE on the wire (matching JT400's CallableStatement flow,
	// captured in prepared_call_in_only.trace) even when there are
	// no driver-side args -- the SQL text may still carry literal
	// arguments inside the CALL parens, and the server expects
	// statement-type TYPE_CALL=3 on PREPARE for correct dispatch.
	// ExecuteImmediate's single-frame path collapses PREPARE +
	// EXECUTE and the server doesn't populate SQLERRD(2) for
	// procs invoked through it, which would break M9-3's multi-
	// result-set count when the caller routes a multi-set CALL
	// through Exec by mistake.
	useImmediate := len(args) == 0 && !isCall(s.query)
	if useImmediate {
		res, err := hostserver.ExecuteImmediate(s.conn.conn, s.query, s.conn.nextCorr())
		s.logExec(logger, "EXECUTE_IMMEDIATE", 0, start, res, err)
		if err != nil {
			return nil, s.conn.classifyConnErr(err)
		}
		return &Result{rowsAffected: res.RowsAffected, conn: s.conn}, nil
	}
	shapes, values, outDests, err := bindArgsToPreparedParams(args, s.conn.preferredStringCCSID())
	if err != nil {
		return nil, err
	}

	// Cache-hit fast path: when this connection's package cache
	// holds a statement whose stored SQL byte-equals s.query AND
	// no caller argument is sql.Out (callable statements can't
	// take the fast path because their OUT destinations aren't
	// representable on the wire without the CHANGE_DESCRIPTOR
	// pre-step), skip CREATE_RPB / PREPARE_DESCRIBE / CHANGE_DESCRIPTOR
	// entirely. Falls through to ExecutePreparedSQL on miss; that
	// path still adds the CP 0x3804 marker that POPULATES the
	// cache for subsequent calls.
	if cached := s.conn.packageLookup(s.query); cached != nil && len(outDests) == 0 {
		res, err := hostserver.ExecutePreparedCached(s.conn.conn, cached, values, s.conn.nextCorrFunc())
		s.logExecCached(logger, len(args), start, res, cached.Name, err)
		if err != nil {
			return nil, s.conn.classifyConnErr(err)
		}
		return &Result{rowsAffected: res.RowsAffected, conn: s.conn}, nil
	}
	res, err := hostserver.ExecutePreparedSQL(s.conn.conn, s.query, shapes, values, s.conn.nextCorr(), s.conn.selectOptionsFor(s.query, len(args) > 0)...)
	s.logExec(logger, "EXECUTE_PREPARED", len(args), start, res, err)
	if err != nil {
		return nil, s.conn.classifyConnErr(err)
	}
	if err := writeBackOutParams(outDests, res.OutValues); err != nil {
		return nil, err
	}
	return &Result{rowsAffected: res.RowsAffected, conn: s.conn}, nil
}

// logExecCached is the Exec-path equivalent of logExec for cache-hit
// dispatches. Tagged with op="EXECUTE_PREPARED_CACHED" and carries
// the cached statement's 18-char server name so operators can grep
// for fast-path activity and cross-reference QSYS2.SYSPACKAGE entries.
func (s *Stmt) logExecCached(logger *slog.Logger, paramCount int, start time.Time, res *hostserver.ExecResult, cachedName string, err error) {
	if logger == nil {
		return
	}
	attrs := []slog.Attr{
		slog.String("op", "EXECUTE_PREPARED_CACHED"),
		slog.Int("params", paramCount),
		slog.Duration("elapsed", time.Since(start)),
		slog.String("cached_name", cachedName),
	}
	if res != nil {
		attrs = append(attrs, slog.Int64("rows_affected", res.RowsAffected))
	}
	if s.conn.cfg != nil && s.conn.cfg.LogSQL {
		attrs = append(attrs, slog.String("sql", s.query))
	}
	if err != nil {
		attrs = append(attrs, slog.String("err", err.Error()))
		logger.LogAttrs(context.Background(), slog.LevelDebug, "db2i: exec cache-hit failed", attrs...)
		return
	}
	logger.LogAttrs(context.Background(), slog.LevelDebug, "db2i: exec cache-hit", attrs...)
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
		logger.LogAttrs(context.Background(), slog.LevelDebug, "db2i: exec failed", attrs...)
		return
	}
	logger.LogAttrs(context.Background(), slog.LevelDebug, "db2i: exec", attrs...)
}

// Query runs a SELECT (or VALUES / WITH / CALL). With no args it
// opens a streaming cursor via OpenSelectStatic; with args it opens
// via OpenSelectPrepared. The cursor pulls subsequent batches lazily
// as the caller's Rows.Next iterates -- a million-row SELECT pays
// one 32 KB-buffer FETCH round-trip per batch instead of one per row.
//
// CALL routing: stored procedures that DECLARE cursors WITH RETURN
// surface their result sets through Query + Rows.Next; the
// PREPARE+OPEN+FETCH dance is identical to a SELECT once the server
// has dispatched on the CALL statement type (M9-1). Procedures that
// return multiple result sets advance through Rows.NextResultSet
// (M9-3 -- pending).
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if !isSelect(s.query) && !isCall(s.query) {
		return nil, fmt.Errorf("db2i: Query called with non-SELECT/CALL; use Exec")
	}
	hasParams := len(args) > 0
	selectOpts := s.conn.selectOptionsFor(s.query, hasParams)
	start := time.Now()
	if !hasParams {
		cursor, err := hostserver.OpenSelectStatic(s.conn.conn, s.query, s.conn.nextCorrFunc(), selectOpts...)
		s.logQuery("OPEN_SELECT_STATIC", 0, start, err)
		if err != nil {
			return nil, s.conn.classifyConnErr(err)
		}
		return &Rows{cursor: cursor, conn: s.conn}, nil
	}
	shapes, values, _, err := bindArgsToPreparedParams(args, s.conn.preferredStringCCSID())
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
		logger.LogAttrs(context.Background(), slog.LevelDebug, "db2i: query failed", attrs...)
		return
	}
	logger.LogAttrs(context.Background(), slog.LevelDebug, "db2i: query", attrs...)
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
// bindArgsToPreparedParams produces the wire shapes + bind values
// AND returns a parallel slice of OUT-destination pointers (one per
// arg slot; nil for non-OUT slots). For stored-procedure OUT / INOUT
// parameters callers wrap their destination in `sql.Out{Dest: &x,
// In: bool}`; the bind layer translates that into a PreparedParam
// with ParamType=0xF1 (OUT) or 0xF2 (INOUT), and the caller hangs
// onto the returned outDests so the EXECUTE reply's OUT row can be
// reflect-assigned back to the user's variables.
//
// The OUT shapes here are placeholders: the proc's declared parameter
// types come from the server in the PREPARE_DESCRIBE reply
// (CP 0x3813), and hostserver.ExecutePreparedSQL fixes up the
// OUT-slot shapes from that PMF before sending CHANGE_DESCRIPTOR.
// The placeholder still needs a sensible default so paramShapes
// passes the shape-count validation in EncodeDBExtendedDataFormat.
func bindArgsToPreparedParams(args []driver.Value, stringCCSID uint16) ([]hostserver.PreparedParam, []any, []*stdsql.Out, error) {
	if stringCCSID == 0 {
		stringCCSID = 37
	}
	shapes := make([]hostserver.PreparedParam, len(args))
	values := make([]any, len(args))
	outDests := make([]*stdsql.Out, len(args))
	for i, a := range args {
		// Stored-procedure OUT / INOUT parameter. CheckNamedValue
		// admits sql.Out (value type) through the boundary; here we
		// set the direction byte and stash a pointer to the original
		// (so the wrapper's Dest pointer is reachable from the
		// post-EXECUTE write-back path). INOUT (In=true) carries an
		// IN value derived from the current value of *Dest;
		// OUT-only (In=false) sends a zero placeholder since the
		// server ignores the bind value for that direction.
		if out, ok := a.(stdsql.Out); ok {
			if out.Dest == nil {
				return nil, nil, nil, fmt.Errorf("db2i: param %d: sql.Out.Dest must not be nil", i)
			}
			// Heap-allocate so the write-back path has a stable
			// pointer (the loop variable `out` goes out of scope
			// after this iteration; we need it to outlive the
			// loop). Dest is itself a pointer to the caller's
			// variable, so even if `out` itself was copied
			// elsewhere, the write through Dest still hits the
			// original.
			outCopy := out
			outDests[i] = &outCopy
			direction := byte(0xF1) // PARAMETER_TYPE_OUTPUT
			if out.In {
				direction = 0xF2 // PARAMETER_TYPE_INPUT_OUTPUT
			}
			placeholderShape, placeholderValue, err := outBindShape(&out, stringCCSID, direction)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("db2i: param %d: %w", i, err)
			}
			shapes[i] = placeholderShape
			values[i] = placeholderValue
			continue
		}
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
				return nil, nil, nil, fmt.Errorf("db2i: param %d: %w", i, err)
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
			return nil, nil, nil, fmt.Errorf("db2i: param %d: unsupported Go type %T (driver.Value union: int64/float64/bool/[]byte/string/time.Time/nil)", i, a)
		}
	}
	return shapes, values, outDests, nil
}

// outBindShape returns a placeholder PreparedParam + bind value for
// a stored-procedure OUT / INOUT slot. The shape is derived from the
// Go type of out.Dest -- it's a best-effort starting point;
// hostserver.ExecutePreparedSQL overrides every OUT/INOUT slot's
// SQL type / length / CCSID with the server's declared types from
// the PREPARE_DESCRIBE reply's parameter-marker format (CP 0x3813)
// before the descriptor goes out on the wire. The placeholder still
// has to (a) carry the direction byte and (b) supply a non-zero
// FieldLength so EncodeDBExtendedDataFormat's row-size accumulator
// doesn't underflow.
//
// For INOUT (direction 0xF2), the IN side of the bind value is the
// dereferenced *Dest. For OUT-only (0xF1) the bind value is irrelevant
// per JT400's behaviour -- the server ignores it -- but we send a
// type-appropriate zero so EncodeDBExtendedData doesn't trip on a
// nil where it expects a typed value.
func outBindShape(out *stdsql.Out, stringCCSID uint16, direction byte) (hostserver.PreparedParam, any, error) {
	destVal := reflect.ValueOf(out.Dest)
	if destVal.Kind() != reflect.Pointer {
		return hostserver.PreparedParam{}, nil, fmt.Errorf("sql.Out.Dest must be a pointer, got %T", out.Dest)
	}
	elem := destVal.Elem()
	// IN side of an INOUT binds the current value at the destination.
	var inValue any
	if out.In {
		inValue = elem.Interface()
	}
	switch elem.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		shape := hostserver.PreparedParam{
			SQLType:     497, // INTEGER nullable; PMF fixup overrides
			FieldLength: 4,
			ParamType:   direction,
		}
		if out.In {
			inValue = int32Of(elem)
		} else {
			inValue = int32(0)
		}
		return shape, inValue, nil
	case reflect.Int64:
		shape := hostserver.PreparedParam{
			SQLType:     493, // BIGINT nullable
			FieldLength: 8,
			ParamType:   direction,
		}
		if out.In {
			inValue = elem.Int()
		} else {
			inValue = int64(0)
		}
		return shape, inValue, nil
	case reflect.Float32, reflect.Float64:
		shape := hostserver.PreparedParam{
			SQLType:     481, // DOUBLE nullable
			FieldLength: 8,
			ParamType:   direction,
		}
		if out.In {
			inValue = elem.Float()
		} else {
			inValue = float64(0)
		}
		return shape, inValue, nil
	case reflect.String:
		// VARCHAR(2000) placeholder. The PMF fixup sets the real
		// CCSID + length from the proc's declared parameter type;
		// here we just need a non-zero FieldLength so the row-size
		// accumulator in EncodeDBExtendedDataFormat is correct
		// before fixup.
		shape := hostserver.PreparedParam{
			SQLType:     449,
			FieldLength: 2002, // 2-byte SL + 2000 max bytes
			Precision:   2000,
			CCSID:       stringCCSID,
			ParamType:   direction,
		}
		if out.In {
			inValue = elem.String()
		} else {
			inValue = ""
		}
		return shape, inValue, nil
	case reflect.Bool:
		shape := hostserver.PreparedParam{
			SQLType:     501, // SMALLINT nullable
			FieldLength: 2,
			ParamType:   direction,
		}
		if out.In && elem.Bool() {
			inValue = int32(1)
		} else {
			inValue = int32(0)
		}
		return shape, inValue, nil
	default:
		return hostserver.PreparedParam{}, nil, fmt.Errorf("sql.Out.Dest unsupported type *%s", elem.Type().String())
	}
}

// int32Of narrows the reflect.Value of any int/int8/int16/int32 to
// the int32 the wire encoder for INTEGER expects.
func int32Of(v reflect.Value) int32 {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return int32(v.Int())
	}
	return 0
}

// writeBackOutParams reflect-assigns each decoded OUT value into the
// caller's *sql.Out.Dest. Called after ExecutePreparedSQL with
// outDests[i] = the original *sql.Out for slot i (nil if slot i was
// IN-only) and outValues[i] = the decoded value from the EXECUTE
// reply's result-data row (nil if slot i wasn't OUT/INOUT, or the
// server returned no row).
//
// Conversion follows the same conventions database/sql.Rows.Scan
// uses, but limited to the destination kinds outBindShape accepts:
//
//	*string                      <- string / []byte
//	*int / *int8 / *int16 / *int32 <- int32 / int64 (narrow with range check)
//	*int64                       <- int32 / int64
//	*float32 / *float64          <- float32 / float64
//	*bool                        <- non-zero int32 / bool
//
// Mismatches surface as db2i errors with the param index so the
// caller knows which slot misaligned.
func writeBackOutParams(outDests []*stdsql.Out, outValues []any) error {
	if outDests == nil {
		return nil
	}
	for i, out := range outDests {
		if out == nil {
			continue
		}
		if i >= len(outValues) {
			return fmt.Errorf("db2i: OUT param %d: EXECUTE reply had no value (got %d slots)", i, len(outValues))
		}
		v := outValues[i]
		// Nil from the server means a SQL NULL came back for the
		// OUT slot. database/sql's Scan rejects nil into a non-
		// pointer-pointer; we mirror that by treating it as the
		// zero value of the destination type.
		destVal := reflect.ValueOf(out.Dest).Elem()
		if v == nil {
			destVal.SetZero()
			continue
		}
		if err := assignOutParam(destVal, v); err != nil {
			return fmt.Errorf("db2i: OUT param %d: %w", i, err)
		}
	}
	return nil
}

// assignOutParam handles the type coercion from the server-decoded
// value (int32 / int64 / float64 / string / []byte / bool /
// time.Time) into the destination's Kind. Out of scope for M9-2:
// time.Time, DECIMAL, []byte; those need separate decoder paths.
func assignOutParam(dest reflect.Value, v any) error {
	switch dest.Kind() {
	case reflect.String:
		switch x := v.(type) {
		case string:
			dest.SetString(x)
			return nil
		case []byte:
			dest.SetString(string(x))
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch x := v.(type) {
		case int32:
			dest.SetInt(int64(x))
			return nil
		case int64:
			if dest.Kind() != reflect.Int64 && dest.Kind() != reflect.Int {
				// Range-check before narrowing.
				bits := dest.Type().Bits()
				if bits < 64 {
					lo := int64(-1) << (bits - 1)
					hi := int64(1)<<(bits-1) - 1
					if x < lo || x > hi {
						return fmt.Errorf("int64 value %d overflows %s", x, dest.Type().String())
					}
				}
			}
			dest.SetInt(x)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch x := v.(type) {
		case float64:
			dest.SetFloat(x)
			return nil
		case float32:
			dest.SetFloat(float64(x))
			return nil
		}
	case reflect.Bool:
		switch x := v.(type) {
		case bool:
			dest.SetBool(x)
			return nil
		case int32:
			dest.SetBool(x != 0)
			return nil
		}
	}
	return fmt.Errorf("cannot assign %T into *%s", v, dest.Type().String())
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

// isCall returns true iff the SQL begins with CALL. Stored procedures
// invoke through this verb in standard SQL; the database/sql idiom is
// to route them through Exec when no result set is expected (M9-1 /
// M9-2 OUT-only) or Query when result sets matter (M9-3). The driver
// permits both routes by recognising CALL alongside isSelect's
// read-only verbs in Query, and by NOT rejecting it in Exec.
//
// JDBC escape syntax {call proc(...)} is deferred to M10+ -- JT400
// strips the braces in JDSQLToken before PREPARE, but go-db2i
// expects callers to pass the literal CALL statement.
func isCall(sql string) bool {
	for i, r := range sql {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			continue
		}
		head := sql[i:]
		if len(head) >= 4 && strings.EqualFold(head[:4], "CALL") {
			// Guard against verbs that *contain* CALL but aren't
			// it (none in standard SQL, but cheap to be careful).
			if len(head) == 4 {
				return true
			}
			next := head[4]
			return next == ' ' || next == '\t' || next == '\n' || next == '\r' || next == '('
		}
		return false
	}
	return false
}
