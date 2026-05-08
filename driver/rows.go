package driver

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"
	"unicode/utf16"

	"github.com/complacentsee/goJTOpen/ebcdic"
	"github.com/complacentsee/goJTOpen/hostserver"
)

// Rows iterates a SELECT result lazily. Underneath it holds an open
// server-side cursor that pulls the next batch via continuation
// FETCH on demand -- callers walking a million rows pay one
// 32 KB-buffer round-trip per batch, not one per row.
//
// The streaming variant is the public API; the buffered
// hostserver.SelectResult shape is still used for offline tests and
// the cmd/smoketest harness via SelectStaticSQL / SelectPreparedSQL.
type Rows struct {
	cursor    *hostserver.Cursor
	conn      *Conn
	closeErr  error // sticky -- so repeated Close calls return the same value
	closed    bool
}

func (r *Rows) Columns() []string {
	cols := r.cursor.Columns()
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

// Close releases the server-side cursor and the RPB slot. Idempotent
// per the database/sql contract -- repeated calls return the same
// (cached) error.
func (r *Rows) Close() error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.closeErr = r.cursor.Close()
	if r.closeErr != nil && r.conn != nil {
		// If RPB cleanup itself failed, the wire is in an
		// indeterminate state. Mark the conn dead so the pool
		// retires it.
		_ = r.conn.classifyConnErr(r.closeErr)
	}
	return r.closeErr
}

// Next copies the next row into dest. Returns io.EOF when exhausted
// (the database/sql convention).
//
// hostserver decodes DATE / TIME / TIMESTAMP columns to ISO strings
// because those are the wire's natural shape; here we promote them
// to time.Time so callers can Scan into *time.Time. Strings remain
// strings -- callers that want Time-shaped DATE values can opt back
// into the raw string by scanning into *string instead, since
// time.Time is convertible both ways through database/sql's
// convertAssign.
func (r *Rows) Next(dest []driver.Value) error {
	row, err := r.cursor.Next()
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	if err != nil {
		if r.conn != nil {
			return r.conn.classifyConnErr(err)
		}
		return err
	}
	cols := r.cursor.Columns()
	for i, v := range row {
		col := cols[i]
		switch col.SQLType {
		case 384, 385, 388, 389, 392, 393:
			s, ok := v.(string)
			if !ok {
				dest[i] = v
				continue
			}
			t, terr := parseTemporalISO(col.SQLType, s)
			if terr != nil {
				return fmt.Errorf("gojtopen: col %d (%s): %w", i, col.Name, terr)
			}
			dest[i] = t
		case 960, 961, 964, 965, 968, 969:
			// BLOB / CLOB / DBCLOB locator. Materialise the full
			// content via RETRIEVE_LOB_DATA. Streaming via io.Reader
			// is documented as a follow-up; for now we read the
			// entire LOB into memory at Scan time (matches the
			// "small to medium LOB" common case and avoids exposing
			// a different scan API to callers).
			loc, ok := v.(hostserver.LOBLocator)
			if !ok {
				dest[i] = v
				continue
			}
			out, lerr := r.materialiseLOB(loc, i)
			if lerr != nil {
				return fmt.Errorf("gojtopen: col %d (%s, lob): %w", i, col.Name, lerr)
			}
			dest[i] = out
		default:
			dest[i] = v
		}
	}
	return nil
}

// materialiseLOB pulls the full LOB content via one or more
// RETRIEVE_LOB_DATA round trips on the same connection. BLOB
// columns return []byte; CLOB / DBCLOB return string after
// transcoding from the wire CCSID.
//
// For now we issue a single RETRIEVE call that asks for up to 2 GB
// (the IBM i wire-format max) and rely on the server to honour the
// LOB's actual length. Genuinely huge LOBs that exceed memory
// should use the (deferred) streaming Read API once landed.
func (r *Rows) materialiseLOB(loc hostserver.LOBLocator, colIdx int) (any, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("rows has no connection (offline test path?)")
	}
	const maxRequest = int64(0x7FFFFFFF) // server caps at int32
	data, err := hostserver.RetrieveLOBData(
		r.conn.conn,
		loc.Handle,
		0,           // offset
		maxRequest,  // request the whole thing
		colIdx,      // column index for server-side validation
		r.conn.nextCorr(),
	)
	if err != nil {
		return nil, r.conn.classifyConnErr(err)
	}
	if data == nil || len(data.Bytes) == 0 {
		// Empty LOB: the column had IS NULL semantics or the LOB
		// genuinely has zero length. Either way the result is empty.
		// Pick the type by SQL type so Scan into either *[]byte or
		// *string works.
		if loc.SQLType == 960 || loc.SQLType == 961 {
			return []byte{}, nil
		}
		return "", nil
	}
	switch loc.SQLType {
	case 960, 961:
		// BLOB: bytes are binary regardless of CCSID tag.
		return data.Bytes, nil
	case 964, 965:
		// CLOB: bytes are characters in CCSID. Match the result-
		// decoder VARCHAR convention -- 1208 stays UTF-8, 65535 is
		// raw, anything else goes through the SBCS EBCDIC table.
		return decodeLOBChars(data.Bytes, data.CCSID)
	case 968, 969:
		// DBCLOB: bytes are 16-bit codepoints in CCSID 13488 (UCS-2
		// BE) or 1200 (UTF-16BE). Translate both as UTF-16BE since
		// the LOB content uses the same encoding.
		return decodeUTF16BE(data.Bytes), nil
	default:
		return data.Bytes, nil
	}
}

// decodeLOBChars converts CLOB payload bytes into a Go string per
// the column's CCSID. Mirrors the VARCHAR result-decode convention
// (CCSID 65535 = raw passthrough, 1208 = UTF-8 passthrough, else
// EBCDIC SBCS via CCSID 37).
func decodeLOBChars(b []byte, ccsid uint16) (string, error) {
	switch ccsid {
	case 65535:
		return string(b), nil
	case 1208:
		return string(b), nil
	default:
		s, err := ebcdic.CCSID37.Decode(b)
		if err != nil {
			return "", err
		}
		return s, nil
	}
}

// decodeUTF16BE turns a sequence of 16-bit big-endian codepoints
// into a Go UTF-8 string. Used for DBCLOB content tagged with
// CCSID 13488 (UCS-2 BE) or 1200 (UTF-16 BE).
func decodeUTF16BE(b []byte) string {
	if len(b) < 2 {
		return ""
	}
	codes := make([]uint16, 0, len(b)/2)
	for i := 0; i+1 < len(b); i += 2 {
		codes = append(codes, uint16(b[i])<<8|uint16(b[i+1]))
	}
	return string(utf16.Decode(codes))
}

// parseTemporalISO converts the ISO strings hostserver emits for
// DATE / TIME / TIMESTAMP into time.Time values database/sql can
// scan into *time.Time.
//
//	DATE      -> "YYYY-MM-DD"
//	TIME      -> "HH:MM:SS" (rare to see fractional)
//	TIMESTAMP -> "YYYY-MM-DDTHH:MM:SS[.ffffff]" (ibmTimestampToISO output)
//
// All times are interpreted as UTC -- IBM i timestamps don't carry
// zone information and the eATM treats them as zoneless. Callers that
// know the session zone can adjust on the Go side.
func parseTemporalISO(sqlType uint16, s string) (time.Time, error) {
	switch sqlType {
	case 384, 385:
		return time.ParseInLocation("2006-01-02", s, time.UTC)
	case 388, 389:
		return time.ParseInLocation("15:04:05", s, time.UTC)
	case 392, 393:
		layouts := []string{
			"2006-01-02T15:04:05.999999",
			"2006-01-02T15:04:05",
		}
		for _, layout := range layouts {
			if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unrecognised timestamp format %q", s)
	}
	return time.Time{}, fmt.Errorf("non-temporal SQL type %d", sqlType)
}

// Optional ColumnType* methods -- expose JDBC-style metadata that
// database/sql.Rows.ColumnTypes() consumers expect. The underlying
// SelectColumn already has TypeName, DisplaySize, Nullable, Signed
// from the M5 metadata work; thread them through.

func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.cursor.Columns()[index].TypeName
}

func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.cursor.Columns()[index].Nullable, true
}

func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	c := r.cursor.Columns()[index]
	switch c.SQLType {
	case 448, 449, 452, 453, 456, 457, 460, 461: // CHAR / VARCHAR variants
		return int64(c.Length), true
	}
	return 0, false
}

func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	c := r.cursor.Columns()[index]
	switch c.SQLType {
	case 484, 485, 488, 489: // DECIMAL, NUMERIC
		return int64(c.Precision), int64(c.Scale), true
	case 996, 997: // DECFLOAT(16/34) -- precision encoded in length
		if c.Length == 8 {
			return 16, 0, true
		}
		return 34, 0, true
	}
	return 0, 0, false
}

func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	c := r.cursor.Columns()[index]
	switch c.SQLType {
	case 384, 385, 388, 389, 392, 393:
		// Next promotes DATE / TIME / TIMESTAMP into time.Time so
		// Scan into *time.Time works without a string-detour.
		return reflect.TypeOf(time.Time{})
	case 448, 449, 452, 453, 456, 457, 460, 461:
		if c.CCSID == 65535 {
			return reflect.TypeOf([]byte{})
		}
		return reflect.TypeOf("")
	case 960, 961:
		// BLOB locator -- materialised as []byte by Next.
		return reflect.TypeOf([]byte{})
	case 964, 965, 968, 969:
		// CLOB / DBCLOB locator -- materialised as string by Next.
		return reflect.TypeOf("")
	case 480, 481:
		return reflect.TypeOf(float64(0))
	case 484, 485, 488, 489, 996, 997:
		return reflect.TypeOf("") // decimal/decfloat as string for now
	case 492, 493:
		return reflect.TypeOf(int64(0))
	case 496, 497:
		return reflect.TypeOf(int32(0))
	case 500, 501:
		return reflect.TypeOf(int16(0))
	}
	return reflect.TypeOf((*any)(nil)).Elem()
}
