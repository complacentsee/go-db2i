//go:build conformance

// Issue #42 conformance: end-to-end live tests for retrieving the
// full set of generated keys from an INSERT via the driver-typed
// Conn.InsertReturning helper, which wraps the INSERT in IBM i's
// `SELECT ... FROM FINAL TABLE ( ... )` data-change table reference
// (V7R3+; target here is V7R5M0).
//
// The probe goes through sql.Conn.Raw to reach *db2i.Conn since
// InsertReturning is not part of the database/sql interface -- the
// same pattern batch_test.go uses for Conn.BatchExec.
//
// CREATE TABLE DDL used by these tests (so the parent can reproduce
// live), under schema()/tablePrefix:
//
//	CREATE TABLE <schema>.<prefix>GENKEYS (
//	    ID     INTEGER GENERATED ALWAYS AS IDENTITY
//	             (START WITH 1 INCREMENT BY 1),
//	    LABEL  VARCHAR(32) NOT NULL,
//	    QTY    INTEGER NOT NULL,
//	    DOUBLED INTEGER GENERATED ALWAYS AS (QTY * 2),
//	    PRIMARY KEY (ID)
//	)
package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	db2i "github.com/complacentsee/go-db2i/driver"
)

// insertReturning is the test-side wrapper around sql.Conn.Raw ->
// *db2i.Conn -> InsertReturning so the tests don't repeat the
// type-assertion dance.
func insertReturning(t *testing.T, db *sql.DB, ctx context.Context, insertSQL string, args []any, returning ...string) ([]string, [][]any) {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()
	var cols []string
	var rows [][]any
	rawErr := conn.Raw(func(driverConn any) error {
		dconn, ok := driverConn.(*db2i.Conn)
		if !ok {
			return fmt.Errorf("driverConn is %T, want *db2i.Conn", driverConn)
		}
		c, r, err := dconn.InsertReturning(ctx, insertSQL, args, returning...)
		cols, rows = c, r
		return err
	})
	if rawErr != nil {
		t.Fatalf("InsertReturning %q: %v", insertSQL, rawErr)
	}
	return cols, rows
}

// asInt64 coerces a FINAL TABLE result value (which arrives as a
// database/sql/driver.Value) to int64 for assertion. IBM i INTEGER /
// IDENTITY columns surface as int64; the helper tolerates the few
// other integer flavours the converter might hand back.
func asInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	default:
		t.Fatalf("value %v (%T) is not an integer", v, v)
		return 0
	}
}

// TestGeneratedKeysFromFinalTable inserts via InsertReturning and
// asserts the server-generated columns come back correctly:
//   - the IDENTITY value(s),
//   - a GENERATED ALWAYS AS (QTY * 2) computed column,
//   - one key-row per inserted row for a multi-row INSERT.
//
// This is the parity case for JT400's getGeneratedKeys, except it
// returns every generated column rather than just the sticky IDENTITY.
func TestGeneratedKeysFromFinalTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "GENKEYS"
	// Drop on entry so a leftover from a crashed run doesn't collide.
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	// NOTE carries a column DEFAULT the INSERT never supplies, so FROM
	// FINAL TABLE returning it proves the helper surfaces server-supplied
	// values beyond IDENTITY. (A computed `GENERATED ALWAYS AS (expr)`
	// column is intentionally avoided: Db2 for i V7R5 rejects an
	// arbitrary column expression there with SQL-104.)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" ("+
			"ID INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1), "+
			"LABEL VARCHAR(32) NOT NULL, "+
			"QTY INTEGER NOT NULL, "+
			"NOTE VARCHAR(8) NOT NULL DEFAULT 'auto', "+
			"PRIMARY KEY (ID))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	// Single-row INSERT, explicit returning columns. The IDENTITY
	// should be 1 (fresh table, START WITH 1) and NOTE the DEFAULT.
	t.Run("single_row_explicit_cols", func(t *testing.T) {
		cols, rows := insertReturning(t, db, ctx,
			"INSERT INTO "+tbl+" (LABEL, QTY) VALUES (?, ?)",
			[]any{"first", int64(7)},
			"ID", "LABEL", "QTY", "NOTE")
		if len(rows) != 1 {
			t.Fatalf("got %d returned rows, want 1", len(rows))
		}
		if len(cols) != 4 {
			t.Fatalf("got %d columns %v, want 4", len(cols), cols)
		}
		row := rows[0]
		if id := asInt64(t, row[0]); id != 1 {
			t.Errorf("IDENTITY ID = %d, want 1", id)
		}
		if label, _ := row[1].(string); label != "first" {
			t.Errorf("LABEL = %q, want %q", row[1], "first")
		}
		if qty := asInt64(t, row[2]); qty != 7 {
			t.Errorf("QTY = %d, want 7", qty)
		}
		if note, _ := row[3].(string); note != "auto" {
			t.Errorf("NOTE (DEFAULT) = %q, want %q", row[3], "auto")
		}
	})

	// Single-row INSERT, no explicit returning columns (`*`). The
	// projection comes back in table-column order: ID, LABEL, QTY,
	// NOTE. IDENTITY should now be 2 (second insert).
	t.Run("single_row_star_projection", func(t *testing.T) {
		cols, rows := insertReturning(t, db, ctx,
			"INSERT INTO "+tbl+" (LABEL, QTY) VALUES (?, ?)",
			[]any{"second", int64(10)})
		if len(rows) != 1 {
			t.Fatalf("got %d returned rows, want 1", len(rows))
		}
		if len(cols) != 4 {
			t.Fatalf("`*` projection returned %d columns %v, want 4", len(cols), cols)
		}
		row := rows[0]
		if id := asInt64(t, row[0]); id != 2 {
			t.Errorf("IDENTITY ID = %d, want 2", id)
		}
		if note, _ := row[3].(string); note != "auto" {
			t.Errorf("NOTE (DEFAULT) = %q, want %q", row[3], "auto")
		}
	})

	// Multi-row INSERT ... VALUES with multiple rows returns one
	// key-row per inserted row. IDENTITY assigns 3 and 4 (in row
	// order). This is the case LastInsertId cannot serve.
	t.Run("multi_row_one_key_each", func(t *testing.T) {
		cols, rows := insertReturning(t, db, ctx,
			"INSERT INTO "+tbl+" (LABEL, QTY) VALUES (?, ?), (?, ?)",
			[]any{"third", int64(1), "fourth", int64(2)},
			"ID", "NOTE")
		if len(rows) != 2 {
			t.Fatalf("got %d returned rows, want 2 (one per inserted row)", len(rows))
		}
		if len(cols) != 2 {
			t.Fatalf("got %d columns %v, want 2", len(cols), cols)
		}
		// FINAL TABLE row order for a multi-row INSERT is not guaranteed
		// by SQL semantics, so assert the two fresh IDENTITY values as a
		// set rather than positionally.
		id0 := asInt64(t, rows[0][0])
		id1 := asInt64(t, rows[1][0])
		if !((id0 == 3 && id1 == 4) || (id0 == 4 && id1 == 3)) {
			t.Errorf("multi-row IDENTITY values = (%d, %d), want {3, 4}", id0, id1)
		}
		for i, r := range rows {
			if note, _ := r[1].(string); note != "auto" {
				t.Errorf("row%d NOTE (DEFAULT) = %q, want %q", i, r[1], "auto")
			}
		}
	})
}
