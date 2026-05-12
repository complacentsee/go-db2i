package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/complacentsee/go-db2i/hostserver"
)

// MaxBlockedInputRows mirrors JT400's `maximumBlockedInputRows` cap
// (`AS400JDBCPreparedStatementImpl.java:1636-1677`). Larger
// `BatchExec` inputs are split client-side into chunks of this size
// and executed sequentially -- each chunk pays one round-trip,
// total rows-affected is summed across chunks.
const MaxBlockedInputRows = 32000

// BatchExec packs `rows` into one EXECUTE per 32k-row chunk via the
// CP 0x381F multi-row block-insert wire shape and returns the
// total rows affected across all chunks. The SQL text must begin
// with INSERT, UPDATE, DELETE, or MERGE; each row's len(args) must
// match the SQL's parameter-marker count and supply values of
// types the driver's default bind path accepts (int64 / float64 /
// bool / string / []byte / time.Time / nil; `database/sql/driver`'s
// DefaultParameterConverter normalises int / int32 / etc).
//
// MERGE batching (v0.7.10): MERGE batches share the same CP 0x381F
// multi-row shape as IUD on V7R1+ (JT400 sets the same
// `canBeBatched_` flag for both; see
// `JDSQLStatement.java:644-648`). The typical pattern uses a
// `USING (VALUES (?, ?))` source clause so each row supplies the
// match/insert values; parameter markers inside the USING-VALUES
// clause MUST be wrapped in explicit `CAST(? AS <type>)` so IBM
// i's parser can determine the source column types (otherwise
// PREPARE_DESCRIBE fails with SQL-584 / QSQRCHK):
//
//	MERGE INTO target t USING (VALUES (
//	    CAST(? AS INTEGER), CAST(? AS VARCHAR(32))
//	)) AS s(id, val)
//	    ON (t.id = s.id)
//	    WHEN MATCHED THEN UPDATE SET t.val = s.val
//	    WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
//
// Returns the server's combined affected count (matched-updates +
// not-matched-inserts); MERGE's per-clause counts aren't broken
// out on the wire.
//
// Restrictions:
//   - LOB values (`*db2i.LOBValue`) are rejected. JT400 falls back
//     to per-row EXECUTE for LOB batches (locator rule);
//     `BatchExec` rejects with a clear pointer at the per-row path.
//   - sql.Out / sql.InOut destinations are rejected: IUD/MERGE
//     have no OUT params on the wire.
//   - SELECT / CALL / DECLARE are rejected: BatchExec is for
//     IUD+MERGE verbs only.
//
// Access pattern via database/sql:
//
//	conn, _ := db.Conn(ctx); defer conn.Close()
//	var affected int64
//	err := conn.Raw(func(driverConn any) error {
//	    d := driverConn.(*db2i.Conn)
//	    n, err := d.BatchExec(ctx, "INSERT INTO t VALUES (?, ?)", rows)
//	    affected = n
//	    return err
//	})
//
// Returns total rows-affected and any wire / validation error.
// On error the partial-progress count from completed chunks is
// returned alongside the error so callers can decide between
// "retry the remaining chunks" vs "roll back via a transaction".
func (c *Conn) BatchExec(ctx context.Context, sql string, rows [][]any) (int64, error) {
	if c.closed {
		return 0, driver.ErrBadConn
	}
	if len(rows) == 0 {
		return 0, nil
	}

	verb := firstSQLVerb(sql)
	switch strings.ToUpper(verb) {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		// supported -- IUD via CP 0x381F multi-row (v0.7.9);
		// MERGE via the same wire shape (v0.7.10) on V7R1+.
	case "SELECT", "VALUES", "WITH":
		return 0, fmt.Errorf("db2i: BatchExec: SELECT-like verb %q -- use db.QueryContext", verb)
	case "CALL":
		return 0, fmt.Errorf("db2i: BatchExec: CALL is not eligible for block-insert; use db.ExecContext per call")
	case "DECLARE":
		return 0, fmt.Errorf("db2i: BatchExec: DECLARE is not eligible; use db.ExecContext")
	default:
		if verb == "" {
			return 0, fmt.Errorf("db2i: BatchExec: SQL has no recognisable verb")
		}
		return 0, fmt.Errorf("db2i: BatchExec: verb %q not supported (want INSERT / UPDATE / DELETE / MERGE)", verb)
	}

	// Up-front per-row validation. Uniform arity + LOB / sql.Out
	// rejection. We pre-validate the entire input so we don't
	// half-execute on a malformed row N partway through.
	if len(rows[0]) == 0 && len(rows) > 0 {
		return 0, fmt.Errorf("db2i: BatchExec: rows have zero columns; SQL must take at least one parameter")
	}
	firstWidth := len(rows[0])
	for ri, r := range rows {
		if len(r) != firstWidth {
			return 0, fmt.Errorf("db2i: BatchExec: row %d has %d values, want %d (matching row 0)", ri, len(r), firstWidth)
		}
		for ci, v := range r {
			if _, isLOB := v.(*LOBValue); isLOB {
				return 0, fmt.Errorf("db2i: BatchExec: row %d col %d: *LOBValue in a batch is not supported (JT400 falls back to per-row for LOB binds; use db.ExecContext in a loop)", ri, ci)
			}
			// sql.Out is a value-receiver type; we detect by name to
			// avoid pulling in database/sql here.
			if isSQLOut(v) {
				return 0, fmt.Errorf("db2i: BatchExec: row %d col %d: sql.Out destinations are not supported (IUD has no OUT params; use BatchExec only for INSERT/UPDATE/DELETE without sql.Out)", ri, ci)
			}
		}
	}

	// 32k-row split. JT400 caps a single block-insert EXECUTE at
	// 32k rows so the server doesn't have to allocate an unbounded
	// buffer for the data block; we mirror.
	start := time.Now()
	stringCCSID := c.preferredStringCCSID()
	totalAffected := int64(0)
	chunks := 0
	for off := 0; off < len(rows); off += MaxBlockedInputRows {
		end := off + MaxBlockedInputRows
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[off:end]

		// Convert each row's []any to []driver.Value via
		// database/sql/driver's default converter. This normalises
		// int/int32 -> int64, *string -> string, etc., so the
		// downstream bind path sees the same Value flavours it
		// would receive from a regular db.Exec call. Then
		// bindArgsToPreparedParams turns each row into wire shapes
		// + values.
		shapes, rowsAsAny, err := convertAndBindBatch(chunk, stringCCSID)
		if err != nil {
			return totalAffected, fmt.Errorf("db2i: BatchExec: chunk @ row %d: %w", off, err)
		}
		res, err := hostserver.ExecuteBatch(c.conn, sql, shapes, rowsAsAny, c.nextCorr(), c.selectOptionsFor(sql, true)...)
		if err != nil {
			return totalAffected, c.classifyConnErr(err)
		}
		totalAffected += res.RowsAffected
		chunks++
	}

	if c.log != nil {
		c.log.LogAttrs(ctx, slog.LevelDebug, "db2i: batch exec",
			slog.String("op", "EXECUTE_BATCH"),
			slog.Int("rows", len(rows)),
			slog.Int("chunks", chunks),
			slog.Int64("rows_affected", totalAffected),
			slog.Duration("elapsed", time.Since(start)),
		)
	}
	return totalAffected, nil
}

// convertAndBindBatch normalises each row's values via
// `database/sql/driver.DefaultParameterConverter` (so int -> int64,
// *string -> string, etc.) then calls bindArgsToPreparedParams on
// the first row to derive parameter shapes. Subsequent rows go
// through the same bind path and the produced shapes must match
// row 0's exactly (same Go type per column position; mismatched
// types reject up front so the server-side descriptor stays valid
// across all rows of the EXECUTE).
//
// Returns the column shapes and the bound `[][]any` ready for
// hostserver.ExecuteBatch.
func convertAndBindBatch(chunk [][]any, stringCCSID uint16) ([]hostserver.PreparedParam, [][]any, error) {
	cv := driver.DefaultParameterConverter
	rowsOut := make([][]any, len(chunk))
	var firstShapes []hostserver.PreparedParam
	for ri, r := range chunk {
		converted := make([]driver.Value, len(r))
		for ci, v := range r {
			cvv, err := cv.ConvertValue(v)
			if err != nil {
				return nil, nil, fmt.Errorf("row %d col %d: %w", ri, ci, err)
			}
			converted[ci] = cvv
		}
		shapes, values, outDests, err := bindArgsToPreparedParams(converted, stringCCSID)
		if err != nil {
			return nil, nil, fmt.Errorf("row %d: %w", ri, err)
		}
		if hasOutDest(outDests) {
			// Belt-and-braces: the upstream sql.Out reject catches
			// this; reaching here means the converter or a custom
			// type slipped one through. Surface clearly rather than
			// shipping a malformed wire request.
			return nil, nil, fmt.Errorf("row %d: sql.Out destination in batch (unsupported)", ri)
		}
		if ri == 0 {
			firstShapes = shapes
		} else {
			if err := assertShapesMatch(firstShapes, shapes); err != nil {
				return nil, nil, fmt.Errorf("row %d: %w (every row in a batch must have the same Go types per column as row 0)", ri, err)
			}
		}
		rowsOut[ri] = values
	}
	return firstShapes, rowsOut, nil
}

// assertShapesMatch confirms two rows' derived parameter shapes are
// equivalent. The server's CHANGE_DESCRIPTOR uploads the column
// shapes once; subsequent rows in the EXECUTE block must bind into
// the same widths / SQLTypes or the server interprets them
// incorrectly.
func assertShapesMatch(a, b []hostserver.PreparedParam) error {
	if len(a) != len(b) {
		return fmt.Errorf("column count %d differs from row 0 (%d)", len(b), len(a))
	}
	for i := range a {
		if a[i].SQLType != b[i].SQLType || a[i].FieldLength != b[i].FieldLength ||
			a[i].Precision != b[i].Precision || a[i].Scale != b[i].Scale ||
			a[i].CCSID != b[i].CCSID || a[i].ParamType != b[i].ParamType {
			return fmt.Errorf("col %d shape mismatch (got SQLType=%d FieldLength=%d, row 0 SQLType=%d FieldLength=%d)",
				i, b[i].SQLType, b[i].FieldLength, a[i].SQLType, a[i].FieldLength)
		}
	}
	return nil
}

// isSQLOut reports whether v is a sql.Out wrapper. We detect by
// type-name to avoid importing `database/sql` from this file --
// CheckNamedValue already admits sql.Out through the normal path
// via a string-typed reflect check, and stmt.go imports it as
// stdsql; mirroring that import here would expand the symbol set
// the batch file pulls in. The runtime type assertion in stmt.go
// is the load-bearing one; this is a belt-and-braces fallback.
func isSQLOut(v any) bool {
	if v == nil {
		return false
	}
	return fmt.Sprintf("%T", v) == "sql.Out"
}
