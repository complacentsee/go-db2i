package driver

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// Rows wraps a buffered SelectResult (the M5 fetch path returns the
// whole result set in memory). Lazy iteration via continuation FETCH
// stays a deferred M6+ enhancement.
type Rows struct {
	result *hostserver.SelectResult
	pos    int
}

func (r *Rows) Columns() []string {
	out := make([]string, len(r.result.Columns))
	for i, c := range r.result.Columns {
		out[i] = c.Name
	}
	return out
}

func (r *Rows) Close() error { return nil }

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
	if r.pos >= len(r.result.Rows) {
		return io.EOF
	}
	row := r.result.Rows[r.pos]
	for i, v := range row {
		col := r.result.Columns[i]
		switch col.SQLType {
		case 384, 385, 388, 389, 392, 393:
			s, ok := v.(string)
			if !ok {
				dest[i] = v
				continue
			}
			t, err := parseTemporalISO(col.SQLType, s)
			if err != nil {
				return fmt.Errorf("gojtopen: row %d col %d (%s): %w", r.pos, i, col.Name, err)
			}
			dest[i] = t
		default:
			dest[i] = v
		}
	}
	r.pos++
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
	return r.result.Columns[index].TypeName
}

func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.result.Columns[index].Nullable, true
}

func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	c := r.result.Columns[index]
	switch c.SQLType {
	case 448, 449, 452, 453, 456, 457, 460, 461: // CHAR / VARCHAR variants
		return int64(c.Length), true
	}
	return 0, false
}

func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	c := r.result.Columns[index]
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
	c := r.result.Columns[index]
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
