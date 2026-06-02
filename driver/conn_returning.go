package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
)

// InsertReturning runs an INSERT and returns the rows it produced --
// a functional equivalent of JT400's Statement.getGeneratedKeys, but
// for the full inserted row(s) including every server-generated column
// (IDENTITY, ROWID, GENERATED ALWAYS expressions, and column
// DEFAULTs), not just the single sticky IDENTITY value.
//
// The mechanism is go-db2i-original and differs from JT400 on the wire:
// JT400 uses the JDBC RETURN_GENERATED_KEYS attribute (a server-built
// keys result set), whereas this rewrites the statement as a
// client-side SELECT ... FROM FINAL TABLE (INSERT ...). There is
// therefore no JT400 trace fixture for this path; parity is functional,
// not byte-for-byte.
//
// It wraps the caller's INSERT in IBM i's `SELECT ... FROM FINAL
// TABLE ( <insert> )` table-reference and runs the whole thing as a
// query, materialising every returned row. The parameter markers stay
// in the INSERT, so `args` binds exactly as it would for a plain
// `db.Exec(insertSQL, args...)` -- the FINAL TABLE wrapper adds no
// markers and shifts no positions.
//
// `returning` names the result columns. When empty, `*` is used and
// the full inserted row comes back in table-column order. When one or
// more names are given, the SELECT projects exactly those (e.g.
// "ID", "ROWID_COL", or an expression alias). `cols` is the result
// column-name list in projection order; `rows` is one []any per
// inserted row. Each element is the driver's NATIVE decode for that
// column's SQL type -- int16 (SMALLINT), int32 (INTEGER), int64
// (BIGINT), float64, bool, string, []byte, or time.Time, and nil for
// SQL NULL. Because Conn.Raw bypasses database/sql's convertAssign
// these are the raw decode types, NOT normalised, so an INTEGER
// IDENTITY column arrives as int32 (not int64) -- type-switch
// accordingly.
//
// Why this exists: database/sql's Result.LastInsertId surfaces only
// the single session IDENTITY value the server stashes after an
// INSERT, and only for INSERTs that touch an IDENTITY column. It
// cannot return a ROWID, a GENERATED ALWAYS AS expression, a DEFAULT,
// or the keys of a multi-row INSERT. FINAL TABLE returns all of them,
// for every inserted row, in one round-trip.
//
// Server requirement: `FROM FINAL TABLE (INSERT ...)` is an IBM i
// V7R3+ feature (the target here is V7R5). Older releases reject the
// table reference at PREPARE time; the wire error is surfaced
// unwrapped so callers can recognise it.
//
// Package caching: the wrapped SELECT is a data-change table reference,
// so every call re-executes the INSERT. Under extended-dynamic package
// caching a cache-hit still issues a server OPEN (it only skips
// PREPARE_DESCRIBE), so the INSERT runs on each call exactly as it does
// without caching -- there is no replay-without-insert hazard.
//
// insertSQL must begin with INSERT (case-insensitive; leading line/
// block comments are skipped via the same verb-dispatch path the rest
// of the driver uses). UPDATE and DELETE also support data-change
// table references on IBM i, but this helper is scoped to INSERT to
// match getGeneratedKeys semantics; non-INSERT verbs are rejected with
// a typed error.
//
// Restrictions:
//   - INSERT only. UPDATE / DELETE / MERGE / SELECT / CALL reject up
//     front (use db.ExecContext or db.QueryContext for those).
//   - One INSERT statement. Multi-statement text is not split; the
//     server sees whatever is inside the FINAL TABLE parentheses.
//   - Result rows are materialised into [][]any. Conn.Raw cannot hand
//     back a streaming *sql.Rows, so very large INSERT...SELECT result
//     sets buffer fully in memory -- size the INSERT accordingly.
//
// Access pattern via database/sql:
//
//	conn, _ := db.Conn(ctx); defer conn.Close()
//	var cols []string
//	var rows [][]any
//	err := conn.Raw(func(driverConn any) error {
//	    d := driverConn.(*db2i.Conn)
//	    c, r, err := d.InsertReturning(ctx,
//	        "INSERT INTO t (LABEL) VALUES (?)", []any{"hi"}, "ID", "LABEL")
//	    cols, rows = c, r
//	    return err
//	})
//	// rows[0][0] is the generated IDENTITY for the inserted row.
//
// Returns the column names, the inserted rows, and any wire /
// validation error. On error cols and rows are nil.
func (c *Conn) InsertReturning(ctx context.Context, insertSQL string, args []any, returning ...string) (cols []string, rows [][]any, err error) {
	if c.closed {
		return nil, nil, driver.ErrBadConn
	}

	wrapped, err := buildFinalTableSQL(insertSQL, returning)
	if err != nil {
		return nil, nil, err
	}

	start := time.Now()
	stmt := &Stmt{conn: c, query: wrapped}
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}

	rowsAny, err := stmt.QueryContext(ctx, named)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rowsAny.Close() }()

	cols = rowsAny.Columns()
	dest := make([]driver.Value, len(cols))
	for {
		nerr := rowsAny.Next(dest)
		if nerr == io.EOF {
			break
		}
		if nerr != nil {
			return nil, nil, nerr
		}
		row := make([]any, len(dest))
		for i, v := range dest {
			row[i] = v
		}
		rows = append(rows, row)
	}

	if c.log != nil {
		c.log.LogAttrs(ctx, slog.LevelDebug, "db2i: insert returning",
			slog.String("op", "FINAL_TABLE"),
			slog.Int("returned_rows", len(rows)),
			slog.Int("columns", len(cols)),
			slog.Duration("elapsed", time.Since(start)),
		)
	}
	return cols, rows, nil
}

// buildFinalTableSQL validates that insertSQL is an INSERT and wraps
// it as `SELECT <projection> FROM FINAL TABLE ( <insertSQL> )`.
//
// The projection is `*` when returning is empty, otherwise the names
// joined by ", ". Names are inserted verbatim -- callers control them
// and they're column identifiers / expressions, not user data, so the
// parameter markers (which carry user data) stay inside the INSERT
// where they were.
//
// The verb is classified with firstSQLVerb, which skips leading line/
// block comments before reading the first token, so a banner-commented
// INSERT still classifies correctly.
func buildFinalTableSQL(insertSQL string, returning []string) (string, error) {
	verb := firstSQLVerb(insertSQL)
	if !eqIgnoreCaseDriver(verb, "INSERT") {
		if verb == "" {
			return "", fmt.Errorf("db2i: InsertReturning: SQL has no recognisable verb (want INSERT)")
		}
		return "", fmt.Errorf("db2i: InsertReturning: verb %q is not INSERT; FINAL TABLE generated-key retrieval is INSERT-only (use db.ExecContext / db.QueryContext for %s)", verb, verb)
	}

	projection := "*"
	if len(returning) > 0 {
		projection = strings.Join(returning, ", ")
	}

	var b strings.Builder
	b.Grow(len("SELECT  FROM FINAL TABLE ( )") + len(projection) + len(insertSQL))
	b.WriteString("SELECT ")
	b.WriteString(projection)
	b.WriteString(" FROM FINAL TABLE ( ")
	b.WriteString(insertSQL)
	b.WriteString(" )")
	return b.String(), nil
}
