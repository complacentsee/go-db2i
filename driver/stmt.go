package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
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

func (s *Stmt) Close() error { return nil }

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
	if len(args) == 0 {
		res, err := hostserver.ExecuteImmediate(s.conn.conn, s.query, s.conn.nextCorr())
		if err != nil {
			return nil, s.conn.classifyConnErr(err)
		}
		return &Result{rowsAffected: res.RowsAffected, conn: s.conn}, nil
	}
	shapes, values, err := bindArgsToPreparedParams(args)
	if err != nil {
		return nil, err
	}
	res, err := hostserver.ExecutePreparedSQL(s.conn.conn, s.query, shapes, values, s.conn.nextCorr())
	if err != nil {
		return nil, s.conn.classifyConnErr(err)
	}
	return &Result{rowsAffected: res.RowsAffected, conn: s.conn}, nil
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
	if len(args) == 0 {
		cursor, err := hostserver.OpenSelectStatic(s.conn.conn, s.query, s.conn.nextCorrFunc())
		if err != nil {
			return nil, s.conn.classifyConnErr(err)
		}
		return &Rows{cursor: cursor, conn: s.conn}, nil
	}
	shapes, values, err := bindArgsToPreparedParams(args)
	if err != nil {
		return nil, err
	}
	cursor, err := hostserver.OpenSelectPrepared(s.conn.conn, s.query, shapes, values, s.conn.nextCorrFunc())
	if err != nil {
		return nil, s.conn.classifyConnErr(err)
	}
	return &Rows{cursor: cursor, conn: s.conn}, nil
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
//	int64        -> BIGINT  (492)            8 bytes
//	float64      -> DOUBLE  (480)            8 bytes
//	bool         -> SMALLINT (500)           2 bytes (0/1)
//	[]byte       -> VARCHAR FOR BIT DATA     2-byte SL + bytes;
//	                (449 + CCSID 65535)      uses nullable-flavour
//	                                         448+1 so server allows
//	                                         the indicator
//	string       -> VARCHAR  (449 + CCSID 37)
//	                                         2-byte SL + EBCDIC bytes
//	time.Time    -> TIMESTAMP (392)         26 bytes ISO format
//	nil          -> INTEGER nullable (497)  flagged via indicator
//
// String binds use CCSID 37 (US English EBCDIC) -- the IBM i job
// default for unspecified-locale jobs and the only single-byte CCSID
// the encoder currently emits. Strings outside the CCSID-37 repertoire
// will land when the encoder learns CCSID 1208 (UTF-8) passthrough.
//
// The nullable flavour (odd SQL type) is used for every bind so a
// future caller can pass NULL through the same shape without changing
// the request frame; the indicator block decides null vs not-null.
func bindArgsToPreparedParams(args []driver.Value) ([]hostserver.PreparedParam, []any, error) {
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
			shapes[i] = hostserver.PreparedParam{
				SQLType:     449,
				FieldLength: uint32(len(v)) + 2,
				Precision:   uint16(len(v)),
				CCSID:       65535,
			}
			values[i] = v
		case string:
			// Encoder wants room for the 2-byte SL + EBCDIC bytes.
			// EBCDIC is byte-for-byte from ASCII for the SBCS
			// repertoire we support, so len(v) sizes the field.
			shapes[i] = hostserver.PreparedParam{
				SQLType:     449,
				FieldLength: uint32(len(v)) + 2,
				Precision:   uint16(len(v)),
				CCSID:       37,
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
