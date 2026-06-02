package driver

import (
	"context"
	"strings"
	"testing"
)

// TestBuildFinalTableSQL_Wrapping pins the SELECT ... FROM FINAL
// TABLE ( ... ) wrapper construction for both the implicit-`*` form
// (no returning cols) and the explicit-projection form. No wire
// activity -- buildFinalTableSQL is a pure string builder.
func TestBuildFinalTableSQL_Wrapping(t *testing.T) {
	cases := []struct {
		name      string
		insertSQL string
		returning []string
		want      string
	}{
		{
			name:      "no returning cols -> star",
			insertSQL: "INSERT INTO t (LABEL) VALUES (?)",
			returning: nil,
			want:      "SELECT * FROM FINAL TABLE ( INSERT INTO t (LABEL) VALUES (?) )",
		},
		{
			name:      "single returning col",
			insertSQL: "INSERT INTO t (LABEL) VALUES (?)",
			returning: []string{"ID"},
			want:      "SELECT ID FROM FINAL TABLE ( INSERT INTO t (LABEL) VALUES (?) )",
		},
		{
			name:      "multiple returning cols joined by comma-space",
			insertSQL: "INSERT INTO t (LABEL, QTY) VALUES (?, ?)",
			returning: []string{"ID", "LABEL", "CREATED_TS"},
			want:      "SELECT ID, LABEL, CREATED_TS FROM FINAL TABLE ( INSERT INTO t (LABEL, QTY) VALUES (?, ?) )",
		},
		{
			name:      "lowercase insert verb still wraps",
			insertSQL: "insert into t (label) values (?)",
			returning: []string{"ID"},
			want:      "SELECT ID FROM FINAL TABLE ( insert into t (label) values (?) )",
		},
		{
			name:      "leading comment before verb",
			insertSQL: "-- audit banner\nINSERT INTO t (LABEL) VALUES (?)",
			returning: []string{"ID"},
			want:      "SELECT ID FROM FINAL TABLE ( -- audit banner\nINSERT INTO t (LABEL) VALUES (?) )",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildFinalTableSQL(tc.insertSQL, tc.returning)
			if err != nil {
				t.Fatalf("buildFinalTableSQL: unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("wrapper mismatch\n got: %q\nwant: %q", got, tc.want)
			}
		})
	}
}

// TestBuildFinalTableSQL_RejectsNonInsert confirms only INSERT passes
// the verb gate; every other verb (and empty/unrecognisable text)
// returns a typed error. The error names the offending verb so the
// caller knows why it was rejected.
func TestBuildFinalTableSQL_RejectsNonInsert(t *testing.T) {
	cases := []struct {
		sql     string
		wantErr string // substring
	}{
		{"UPDATE t SET x=? WHERE id=?", "is not INSERT"},
		{"DELETE FROM t WHERE id=?", "is not INSERT"},
		{"MERGE INTO t USING s ON ...", "is not INSERT"},
		{"SELECT * FROM t", "is not INSERT"},
		{"CALL mylib.p(?)", "is not INSERT"},
		{"GIBBERISH", "is not INSERT"},
		{"", "no recognisable verb"},
		{"   \n\t  ", "no recognisable verb"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			_, err := buildFinalTableSQL(tc.sql, nil)
			if err == nil {
				t.Fatalf("verb %q should have been rejected", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestInsertReturning_RejectsNonInsert confirms the public entry point
// rejects a non-INSERT statement before any wire activity. With no
// live conn the call would nil-dereference at QueryContext time, so a
// clean typed error proves the verb gate fired first.
func TestInsertReturning_RejectsNonInsert(t *testing.T) {
	c := &Conn{cfg: &Config{}, log: silentLogger}
	_, _, err := c.InsertReturning(context.Background(), "UPDATE t SET x=? WHERE id=?", []any{1, 2})
	if err == nil {
		t.Fatal("expected error for non-INSERT verb")
	}
	if !strings.Contains(err.Error(), "is not INSERT") {
		t.Errorf("error should explain INSERT-only restriction: %v", err)
	}
}

// TestInsertReturning_ArgsPassThroughToWire proves that an accepted
// INSERT carries its args into the wire path: with no live conn the
// QueryContext call panics on the nil net.Conn, which means the verb
// gate passed and the bound args reached the prepared-query dispatch.
// A clean return would mean the wrapper short-circuited and never
// tried to bind -- the negative we're guarding against. End-to-end
// arg correctness lives in the conformance suite's
// TestGeneratedKeysFromFinalTable.
func TestInsertReturning_ArgsPassThroughToWire(t *testing.T) {
	c := &Conn{cfg: &Config{}, log: silentLogger}
	defer func() {
		if r := recover(); r != nil {
			// Panic from the nil net.Conn at QueryContext time means
			// the wrapper accepted the INSERT and pushed the bound
			// args into the wire path -- the success condition.
			return
		}
		t.Fatal("expected wire dispatch to panic on nil conn; got clean return -- did the helper swallow the INSERT before binding?")
	}()
	_, _, err := c.InsertReturning(context.Background(),
		"INSERT INTO t (LABEL) VALUES (?)", []any{"hi"}, "ID", "LABEL")
	if err != nil && strings.Contains(err.Error(), "is not INSERT") {
		t.Errorf("valid INSERT should pass the verb gate; got: %v", err)
	}
}
