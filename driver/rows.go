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

// Columns returns the per-column names parsed from the
// PREPARE_DESCRIBE reply, in SELECT order. Implements
// database/sql/driver.Rows.Columns.
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
			// BLOB / CLOB / DBCLOB locator. Default mode materialises
			// the full content via RETRIEVE_LOB_DATA at Scan time.
			// DSN `?lob=stream` flips this to the streaming path:
			// the column slot becomes a *LOBReader the caller can
			// Read incrementally. Multi-GB LOBs that would blow up
			// memory in materialise mode flow through fine in
			// stream mode.
			loc, ok := v.(hostserver.LOBLocator)
			if !ok {
				dest[i] = v
				continue
			}
			// Server expects 1-based column index per JT400's
			// JDServerRow.newData call (the +1 there). Sending the
			// 0-based Go index landed the locator on the wrong
			// column server-side and produced SQL-818 (consistency
			// tokens do not match) on V7R5+ targets.
			lobColIdx := i + 1
			if r.conn != nil && r.conn.cfg != nil && r.conn.cfg.LOBStream {
				dest[i] = &LOBReader{
					conn:      r.conn,
					loc:       loc,
					colIdx:    lobColIdx,
					chunkSize: DefaultLOBChunkSize,
				}
				continue
			}
			out, lerr := r.materialiseLOB(loc, lobColIdx)
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
// BLOB / CLOB use a single-shot retrieve with a 2-GiB ceiling --
// IBM i caps the requested size at int32 and honours the LOB's
// actual length on top of that. DBCLOB uses a two-step fetch
// (probe with size 0 to learn the current length, then request
// that many characters) because requesting 2 GiB on a graphic LOB
// returns SQL-807: the server tries a CCSID conversion sized off
// the requested length, not the actual length, and chokes.
func (r *Rows) materialiseLOB(loc hostserver.LOBLocator, colIdx int) (any, error) {
	if r.conn == nil {
		return nil, fmt.Errorf("rows has no connection (offline test path?)")
	}
	const maxRequest = int64(0x7FFFFFFF) // server caps at int32

	graphic := loc.SQLType == 968 || loc.SQLType == 969
	wantBytes := maxRequest
	if graphic {
		probe, err := hostserver.RetrieveLOBData(
			r.conn.conn,
			loc.Handle,
			0, 0, // size 0 -> just report current length
			colIdx,
			r.conn.nextCorr(),
		)
		if err != nil {
			return nil, r.conn.classifyConnErr(err)
		}
		// CurrentLength for graphic LOBs is reported in
		// characters; the wire RetrieveLOBData adapter still
		// takes byte count, so multiply by 2.
		wantBytes = int64(probe.CurrentLength) * 2
		if wantBytes == 0 {
			return "", nil
		}
	}
	data, err := hostserver.RetrieveLOBData(
		r.conn.conn,
		loc.Handle,
		0,
		wantBytes,
		colIdx,
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
// EBCDIC SBCS via the per-CCSID codec). Picks the right codec for
// 273 (German) vs 37 (US) so the 17 divergent code points (e.g.
// "!" = 0x5A in CCSID 37 but 0x4F in CCSID 273) round-trip.
func decodeLOBChars(b []byte, ccsid uint16) (string, error) {
	switch ccsid {
	case 65535:
		return string(b), nil
	case 1208:
		return string(b), nil
	case 273:
		return ebcdic.CCSID273.Decode(b)
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

// ColumnTypeDatabaseTypeName returns the server-reported SQL type
// name for the column at the given index (e.g. "DECIMAL", "VARCHAR",
// "TIMESTAMP"). Implements database/sql.RowsColumnTypeDatabaseTypeName.
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.cursor.Columns()[index].TypeName
}

// ColumnTypeSchemaName returns the base schema for the named column,
// or empty when the connection didn't request extended metadata
// (DSN "extended-metadata=true") or the query target doesn't have a
// single underlying table (computed columns, joins, expressions).
// Mirrors JDBC's ResultSetMetaData.getSchemaName.
//
// Not part of the database/sql.RowsColumnType* interface set; reach
// it via type assertion:
//
//	if mr, ok := rows.(interface{ ColumnTypeSchemaName(int) string }); ok {
//	    schema := mr.ColumnTypeSchemaName(0)
//	}
func (r *Rows) ColumnTypeSchemaName(index int) string {
	return r.cursor.Columns()[index].Schema
}

// ColumnTypeTableName returns the base table for the named column,
// or empty under the same conditions as ColumnTypeSchemaName.
// Mirrors JDBC's ResultSetMetaData.getTableName.
func (r *Rows) ColumnTypeTableName(index int) string {
	return r.cursor.Columns()[index].Table
}

// ColumnTypeBaseColumnName returns the underlying base column name
// for an aliased SELECT column, or empty when extended metadata
// wasn't requested or the column isn't a direct projection of a
// base column. Mirrors JT400's DBColumnDescriptorsDataFormat
// .getBaseColumnName.
func (r *Rows) ColumnTypeBaseColumnName(index int) string {
	return r.cursor.Columns()[index].BaseColumnName
}

// ColumnTypeLabel returns the column label (SQL `AS` alias) the
// server reports, or empty when extended metadata wasn't requested
// or the server didn't include a label. Mirrors JT400's
// DBColumnDescriptorsDataFormat.getColumnLabel.
func (r *Rows) ColumnTypeLabel(index int) string {
	return r.cursor.Columns()[index].Label
}

// ColumnTypeNullable reports whether the column at index allows NULLs,
// per the server-side SQLType code (odd-valued SQL types are
// nullable). Implements database/sql.RowsColumnTypeNullable.
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.cursor.Columns()[index].Nullable, true
}

// ColumnTypeLength returns the declared character length for the
// CHAR / VARCHAR family of columns. Returns ok=false for fixed-width
// types where Length isn't a meaningful column attribute. Implements
// database/sql.RowsColumnTypeLength.
func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	c := r.cursor.Columns()[index]
	switch c.SQLType {
	case 448, 449, 452, 453, 456, 457, 460, 461: // CHAR / VARCHAR variants
		return int64(c.Length), true
	}
	return 0, false
}

// ColumnTypePrecisionScale returns DECIMAL / NUMERIC precision and
// scale, or the DECFLOAT precision (16 or 34) when applicable.
// Returns ok=false for non-numeric column types. Implements
// database/sql.RowsColumnTypePrecisionScale.
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

// ColumnTypeScanType returns the Go type Next promotes a column's
// raw bytes into before exposing it on the driver.Value slot --
// time.Time for DATE/TIME/TIMESTAMP, []byte for FOR BIT DATA and
// BLOB, string for CHAR/VARCHAR/CLOB, the appropriate numeric type
// for everything else. Implements
// database/sql.RowsColumnTypeScanType.
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
		// BLOB locator. Default: materialised as []byte at Scan;
		// `?lob=stream` returns a *LOBReader instead.
		if r.conn != nil && r.conn.cfg != nil && r.conn.cfg.LOBStream {
			return reflect.TypeOf((*LOBReader)(nil))
		}
		return reflect.TypeOf([]byte{})
	case 964, 965, 968, 969:
		// CLOB / DBCLOB locator. Default: materialised as string;
		// `?lob=stream` returns a *LOBReader (caller transcodes
		// per LOBReader.CCSID()).
		if r.conn != nil && r.conn.cfg != nil && r.conn.cfg.LOBStream {
			return reflect.TypeOf((*LOBReader)(nil))
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
