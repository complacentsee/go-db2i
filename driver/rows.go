package driver

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

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
		default:
			dest[i] = v
		}
	}
	return nil
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
