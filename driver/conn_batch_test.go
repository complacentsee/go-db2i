package driver

import (
	"context"
	stdsql "database/sql"
	"strings"
	"testing"

	"github.com/complacentsee/go-db2i/hostserver"
)

// TestBatchExec_VerbTruthTable pins the verb-classifier gate at the
// top of BatchExec. INSERT / UPDATE / DELETE pass; everything else
// returns a typed error pointing the caller at the right alternative.
// No wire activity in this path -- the rejection fires before any
// network access, so the conn doesn't need to be live.
func TestBatchExec_VerbTruthTable(t *testing.T) {
	c := &Conn{
		cfg: &Config{},
		log: silentLogger,
	}
	rows := [][]any{{int64(1)}}
	cases := []struct {
		sql     string
		wantErr string // substring; empty = accept (will pass verb gate, fail later on nil conn)
	}{
		// INSERT/UPDATE/DELETE all pass the verb gate.
		{"INSERT INTO t VALUES (?)", ""},
		{"UPDATE t SET x=? WHERE id=?", ""},
		{"DELETE FROM t WHERE id=?", ""},
		{"insert into t values (?)", ""}, // case-insensitive
		// Rejected verbs.
		{"SELECT * FROM t WHERE id=?", "SELECT-like verb"},
		{"VALUES 1", "SELECT-like verb"},
		{"WITH c AS (SELECT 1) SELECT * FROM c", "SELECT-like verb"},
		{"CALL mylib.p(?)", "CALL"},
		{"DECLARE C CURSOR FOR SELECT 1", "DECLARE"},
		{"MERGE INTO t USING s ON ... WHEN MATCHED ...", "MERGE batching not yet supported"},
		{"GIBBERISH", "not supported"},
		{"", "no recognisable verb"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			// Wrap in defer/recover for the accept case: with no
			// live conn, BatchExec will nil-dereference inside
			// WriteFrame after the verb gate accepts. We only
			// care here that the gate passed (i.e., no
			// verb-related error returned before the panic).
			passedGate := false
			defer func() {
				if r := recover(); r != nil {
					if tc.wantErr != "" {
						t.Errorf("verb %q: should have rejected before any wire activity (verb gate must run before wire), got panic: %v", tc.sql, r)
					}
					// Accept case: panic is from the nil conn at
					// wire time -- means the verb gate passed.
					passedGate = true
				}
			}()
			_, err := c.BatchExec(context.Background(), tc.sql, rows)
			if tc.wantErr == "" {
				// No panic AND no verb-related error means the
				// gate let it through to a downstream check.
				if err != nil && (strings.Contains(err.Error(), "not supported") ||
					strings.Contains(err.Error(), "no recognisable verb") ||
					strings.Contains(err.Error(), "SELECT-like") ||
					strings.Contains(err.Error(), "MERGE batching") ||
					strings.Contains(err.Error(), "CALL is not eligible") ||
					strings.Contains(err.Error(), "DECLARE is not eligible")) {
					t.Errorf("verb %q should have passed gate; got verb-gate error: %v", tc.sql, err)
				}
				_ = passedGate
				return
			}
			if err == nil {
				t.Fatalf("verb %q should have been rejected", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("verb %q: error %q does not contain %q", tc.sql, err.Error(), tc.wantErr)
			}
		})
	}
}

// TestBatchExec_ZeroRowsNoop confirms the empty-input fast path
// returns (0, nil) without any wire activity.
func TestBatchExec_ZeroRowsNoop(t *testing.T) {
	c := &Conn{cfg: &Config{}, log: silentLogger}
	n, err := c.BatchExec(context.Background(), "INSERT INTO t VALUES (?)", nil)
	if err != nil {
		t.Fatalf("empty rows should be no-op, got err: %v", err)
	}
	if n != 0 {
		t.Errorf("empty rows: rows-affected = %d, want 0", n)
	}
	// Same for empty slice (vs nil).
	n, err = c.BatchExec(context.Background(), "INSERT INTO t VALUES (?)", [][]any{})
	if err != nil {
		t.Fatalf("empty slice should be no-op, got err: %v", err)
	}
	if n != 0 {
		t.Errorf("empty slice: rows-affected = %d, want 0", n)
	}
}

// TestBatchExec_RowWidthMismatch validates the per-row arity check.
// Wire activity never starts; the error names the offending row.
func TestBatchExec_RowWidthMismatch(t *testing.T) {
	c := &Conn{cfg: &Config{}, log: silentLogger}
	rows := [][]any{
		{int64(1), "a"},
		{int64(2)}, // mismatch
	}
	_, err := c.BatchExec(context.Background(), "INSERT INTO t VALUES (?, ?)", rows)
	if err == nil {
		t.Fatal("expected error for row width mismatch")
	}
	if !strings.Contains(err.Error(), "row 1") {
		t.Errorf("error should name row 1: %v", err)
	}
}

// TestBatchExec_RejectsLOBValue confirms the *LOBValue check fires
// before any wire activity. Documents the v0.7.9 limitation: LOB
// batches must use the per-row path.
func TestBatchExec_RejectsLOBValue(t *testing.T) {
	c := &Conn{cfg: &Config{}, log: silentLogger}
	rows := [][]any{
		{int64(1), &LOBValue{Bytes: []byte{0x01, 0x02}}},
	}
	_, err := c.BatchExec(context.Background(), "INSERT INTO t VALUES (?, ?)", rows)
	if err == nil {
		t.Fatal("expected error for *LOBValue in batch")
	}
	if !strings.Contains(err.Error(), "*LOBValue") {
		t.Errorf("error should mention *LOBValue: %v", err)
	}
}

// TestBatchExec_RejectsSQLOut confirms sql.Out wrappers in batch
// inputs are rejected. IUD has no OUT params on the wire; admitting
// them would silently produce a malformed CHANGE_DESCRIPTOR.
func TestBatchExec_RejectsSQLOut(t *testing.T) {
	var dest string
	c := &Conn{cfg: &Config{}, log: silentLogger}
	rows := [][]any{
		{int64(1), stdsql.Out{Dest: &dest}},
	}
	_, err := c.BatchExec(context.Background(), "INSERT INTO t VALUES (?, ?)", rows)
	if err == nil {
		t.Fatal("expected error for sql.Out in batch")
	}
	if !strings.Contains(err.Error(), "sql.Out") {
		t.Errorf("error should mention sql.Out: %v", err)
	}
}

// TestBatchExec_ZeroColumns rejects the degenerate
// `INSERT INTO t DEFAULT VALUES` shape -- BatchExec requires at
// least one parameter marker.
func TestBatchExec_ZeroColumns(t *testing.T) {
	c := &Conn{cfg: &Config{}, log: silentLogger}
	rows := [][]any{{}, {}}
	_, err := c.BatchExec(context.Background(), "INSERT INTO t DEFAULT VALUES", rows)
	if err == nil {
		t.Fatal("expected error for zero-column rows")
	}
	if !strings.Contains(err.Error(), "zero columns") {
		t.Errorf("error should mention zero columns: %v", err)
	}
}

// TestAssertShapesMatch covers the per-row shape-equality check
// directly so we don't need a wire round-trip to verify it. Drift
// in any of SQLType / FieldLength / Precision / Scale / CCSID /
// ParamType must surface a typed error.
func TestAssertShapesMatch(t *testing.T) {
	base := []hostserver.PreparedParam{
		{SQLType: 493, FieldLength: 8},
		{SQLType: 449, FieldLength: 64, CCSID: 1208},
	}
	if err := assertShapesMatch(base, base); err != nil {
		t.Errorf("identical shapes should match: %v", err)
	}
	// SQLType drift.
	drifted := []hostserver.PreparedParam{
		{SQLType: 493, FieldLength: 8},
		{SQLType: 497, FieldLength: 64, CCSID: 1208}, // INTEGER vs VARCHAR
	}
	if err := assertShapesMatch(base, drifted); err == nil {
		t.Error("SQLType drift should fail")
	}
	// Column-count drift.
	short := []hostserver.PreparedParam{{SQLType: 493, FieldLength: 8}}
	if err := assertShapesMatch(base, short); err == nil {
		t.Error("column-count drift should fail")
	}
}

// TestIsSQLOut covers the type-name detector used as a belt-and-
// braces check in convertAndBindBatch. Confirms the detection
// matches the actual database/sql type.
func TestIsSQLOut(t *testing.T) {
	var dest int
	if !isSQLOut(stdsql.Out{Dest: &dest}) {
		t.Error("sql.Out should be detected")
	}
	if isSQLOut(int64(1)) || isSQLOut("hi") || isSQLOut(nil) {
		t.Error("non-Out values should not be detected as Out")
	}
}
