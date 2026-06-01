//go:build conformance

// errors_matrix_test.go extends the error-path coverage (issue #6) past
// the two cases TestDb2ErrorPredicates pins (dup-key + syntax). It
// triggers a spread of server SQL errors through plain SQL and asserts
// the *hostserver.Db2Error surfaces the right SQLCODE/SQLSTATE pair and
// classification predicate, and that the swallowed-warning path (SQL
// +100 on a no-match UPDATE/DELETE) reports success with zero rows
// affected rather than an error.
//
// SQLSTATE only populates when the failing op carries the SQLCA
// (PREPARE_DESCRIBE / EXECUTE / FETCH all do), so every trigger here
// runs through one of those paths (hostserver/db_error.go).
package conformance

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/complacentsee/go-db2i/hostserver"
)

// asDb2Error unwraps err to *hostserver.Db2Error or fails the test.
func asDb2Error(t *testing.T, err error) *hostserver.Db2Error {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a *hostserver.Db2Error, got nil error")
	}
	var dbErr *hostserver.Db2Error
	if !errors.As(err, &dbErr) {
		t.Fatalf("expected *hostserver.Db2Error, got %T: %v", err, err)
	}
	return dbErr
}

// runFailingQuery runs a SELECT expected to fail and returns the error,
// draining a (hypothetically) successful result so a FETCH-time error
// (e.g. runtime divide-by-zero) still surfaces.
func runFailingQuery(db *sql.DB, sqlText string, args ...any) error {
	rows, err := db.Query(sqlText, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	dest := make([]any, len(cols))
	for i := range dest {
		dest[i] = new(any)
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
	}
	return rows.Err()
}

// TestErrorSQLStateMatrix triggers a representative set of server SQL
// errors and asserts the SQLCODE/SQLSTATE pairing and predicate
// classification. SQLCODEs are exact (stable across VRMs); SQLSTATE is
// exact for the well-known pairs and class-only for the syntax/arith
// codes whose 5-char subcode is less portable.
func TestErrorSQLStateMatrix(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	// A NOT NULL + PRIMARY KEY table backs the -407/-803 cases; the
	// narrow SMALLINT/VARCHAR(2) columns back the data-exception cases.
	tbl := schema() + "." + tablePrefix + "eerr"
	db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec("CREATE TABLE " + tbl + " (id INTEGER NOT NULL PRIMARY KEY, v INTEGER NOT NULL, s SMALLINT, c VARCHAR(2))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec("INSERT INTO "+tbl+" (id, v) VALUES (?, ?)", 1, 100); err != nil {
		t.Fatalf("seed: %v", err)
	}

	notThere := schema() + "." + tablePrefix + "NOPE" // never created

	cases := []struct {
		name       string
		exec       bool // true => Exec (DML), false => failing Query
		sql        string
		args       []any
		wantCode   int32 // asserted exactly when codeExact
		codeExact  bool
		wantState  string // exact 5-char when stateExact, else class prefix
		stateExact bool
		predicate  func(*hostserver.Db2Error) bool
		predName   string
	}{
		{
			// IsNotFound is the no-data (+100/02xxx) predicate, not an
			// object-existence one, so it is deliberately NOT asserted
			// here -- a -204/42704 is correctly not "not found" by that
			// classifier.
			name: "object_not_found", sql: "SELECT * FROM " + notThere,
			wantCode: -204, codeExact: true, wantState: "42704", stateExact: true,
		},
		{
			name: "column_not_found", sql: "SELECT NOSUCHCOL FROM SYSIBM.SYSDUMMY1",
			wantCode: -206, codeExact: true, wantState: "42703", stateExact: true,
		},
		{
			name: "syntax_error", sql: "SELECT * FORM SYSIBM.SYSDUMMY1",
			codeExact: false, wantState: "42", stateExact: false,
		},
		{
			name: "null_into_not_null", exec: true,
			sql: "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)", args: []any{2, nil},
			wantCode: -407, codeExact: true, wantState: "23502", stateExact: true,
		},
		{
			name: "duplicate_key", exec: true,
			sql: "INSERT INTO " + tbl + " (id, v) VALUES (?, ?)", args: []any{1, 200},
			wantCode: -803, codeExact: true, wantState: "23505", stateExact: true,
			predicate: (*hostserver.Db2Error).IsConstraintViolation, predName: "IsConstraintViolation",
		},
		{
			// Numeric value out of range: 999999 binds as BIGINT and the
			// server narrows it into the SMALLINT column (class 22 data
			// exception). Divide-by-zero is intentionally not used -- the
			// job's arithmetic-error handling returns no error for it.
			name: "numeric_overflow", exec: true,
			sql: "INSERT INTO " + tbl + " (id, v, s) VALUES (?, ?, ?)", args: []any{3, 0, 999999},
			wantCode: -406, codeExact: true, wantState: "22003", stateExact: true,
		},
		{
			// String assignment too long for VARCHAR(2): another class 22
			// data exception, distinct SQLSTATE (22001).
			name: "string_truncation", exec: true,
			sql: "INSERT INTO " + tbl + " (id, v, c) VALUES (?, ?, ?)", args: []any{4, 0, "way too long"},
			wantCode: -404, codeExact: true, wantState: "22001", stateExact: true,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			var err error
			if c.exec {
				_, err = db.Exec(c.sql, c.args...)
			} else {
				err = runFailingQuery(db, c.sql, c.args...)
			}
			dbErr := asDb2Error(t, err)

			if c.codeExact {
				if dbErr.SQLCode != c.wantCode {
					t.Errorf("SQLCode = %d, want %d (err: %v)", dbErr.SQLCode, c.wantCode, err)
				}
			} else if dbErr.SQLCode >= 0 {
				t.Errorf("SQLCode = %d, want a negative SQL error code (err: %v)", dbErr.SQLCode, err)
			}

			if c.stateExact {
				if dbErr.SQLState != c.wantState {
					t.Errorf("SQLState = %q, want %q", dbErr.SQLState, c.wantState)
				}
			} else if !strings.HasPrefix(dbErr.SQLState, c.wantState) {
				t.Errorf("SQLState = %q, want class %q*", dbErr.SQLState, c.wantState)
			}

			if c.predicate != nil && !c.predicate(dbErr) {
				t.Errorf("predicate %s() = false for SQLCode=%d SQLState=%q", c.predName, dbErr.SQLCode, dbErr.SQLState)
			}

			// Error() embeds the SQLCODE via "SQL%d" formatting, so a
			// negative code renders as e.g. "SQL-204".
			if !strings.Contains(err.Error(), "SQL") {
				t.Errorf("Error() = %q, want it to contain the SQL code", err.Error())
			}
		})
	}
}

// TestErrorNoDataNotSurfaced pins the warning-swallow contract: an
// UPDATE / DELETE that matches no rows is SQL +100, which the driver
// treats as success with RowsAffected()==0 -- it must NOT surface a
// *hostserver.Db2Error (db_error.go isSQLWarning / db_exec.go +100
// special-case).
func TestErrorNoDataNotSurfaced(t *testing.T) {
	db := openDB(t)
	dropTestTables(t, db)

	tbl := schema() + "." + tablePrefix + "endt"
	db.Exec("DROP TABLE " + tbl)
	if _, err := db.Exec("CREATE TABLE " + tbl + " (id INTEGER NOT NULL, v INTEGER)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE " + tbl)

	t.Run("update no match", func(t *testing.T) {
		res, err := db.Exec("UPDATE "+tbl+" SET v = ? WHERE id = ?", 1, 999999)
		if err != nil {
			t.Fatalf("UPDATE no-match returned error (should swallow SQL +100): %v", err)
		}
		if n, _ := res.RowsAffected(); n != 0 {
			t.Errorf("RowsAffected = %d, want 0", n)
		}
	})

	t.Run("delete no match", func(t *testing.T) {
		res, err := db.Exec("DELETE FROM "+tbl+" WHERE id = ?", 888888)
		if err != nil {
			t.Fatalf("DELETE no-match returned error (should swallow SQL +100): %v", err)
		}
		if n, _ := res.RowsAffected(); n != 0 {
			t.Errorf("RowsAffected = %d, want 0", n)
		}
	})
}

// TestErrorConnSurvivesStatementError confirms a statement-level SQL
// error (non-08xxx SQLSTATE) leaves the pooled connection usable -- the
// driver's classifyConnErr only retires the conn for transport / 08xxx
// failures (driver/health.go). Also exercises hostserver.IsDb2Error.
func TestErrorConnSurvivesStatementError(t *testing.T) {
	db := openDB(t)
	// Pin one connection so the recovery query provably reuses the same
	// socket that just took the error.
	db.SetMaxOpenConns(1)

	err := runFailingQuery(db, "SELECT * FROM "+schema()+"."+tablePrefix+"NOPE")
	if !hostserver.IsDb2Error(err) {
		t.Fatalf("expected a *hostserver.Db2Error, got %T: %v", err, err)
	}

	var one int
	if err := db.QueryRow("SELECT 1 FROM SYSIBM.SYSDUMMY1").Scan(&one); err != nil {
		t.Fatalf("connection unusable after a statement-level error: %v", err)
	}
	if one != 1 {
		t.Errorf("recovery query returned %d, want 1", one)
	}
}
