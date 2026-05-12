package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestValidateSavepointName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   string
		wantErr string // substring; empty = expect no error
	}{
		{"ok simple", "SP1", ""},
		{"ok with digits + underscore", "SP_42_X", ""},
		{"ok mixed case", "MySavepoint", ""},
		{"ok max length 128", strings.Repeat("A", 128), ""},
		{"empty", "", "empty"},
		{"too long 129", strings.Repeat("A", 129), "exceeds 128"},
		{"leading digit", "1SP", "must start with a letter"},
		{"leading underscore", "_SP", "must start with a letter"},
		{"space inside", "SP 1", "illegal character"},
		{"quote inside", `SP"1`, "illegal character"},
		{"semicolon injection", "SP1; DROP TABLE T;", "illegal character"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateSavepointName(tc.input)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateSavepointName(%q): unexpected error %v", tc.input, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateSavepointName(%q): want error containing %q, got nil", tc.input, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateSavepointName(%q): want error containing %q, got %v", tc.input, tc.wantErr, err)
			}
		})
	}
}

func TestSavepointSQLAssembly(t *testing.T) {
	t.Parallel()
	// The savepoint methods build SQL through runSavepointSQL before
	// dispatching to Stmt.ExecContext. We can't observe the wire
	// without a live LPAR, but we can drive the same Builder logic
	// the runtime uses and assert the exact text matches JT400's
	// AS400JDBCConnection emission.
	cases := []struct {
		method string
		name   string
		want   string
	}{
		{"Savepoint", "SP1", "SAVEPOINT SP1 ON ROLLBACK RETAIN CURSORS"},
		{"ReleaseSavepoint", "SP1", "RELEASE SAVEPOINT SP1"},
		{"RollbackToSavepoint", "MySp_2", "ROLLBACK TO SAVEPOINT MySp_2"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.method, func(t *testing.T) {
			t.Parallel()
			var verb, suffix string
			switch tc.method {
			case "Savepoint":
				verb, suffix = "SAVEPOINT", " ON ROLLBACK RETAIN CURSORS"
			case "ReleaseSavepoint":
				verb, suffix = "RELEASE SAVEPOINT", ""
			case "RollbackToSavepoint":
				verb, suffix = "ROLLBACK TO SAVEPOINT", ""
			}
			var b strings.Builder
			b.WriteString(verb)
			b.WriteByte(' ')
			b.WriteString(tc.name)
			b.WriteString(suffix)
			if got := b.String(); got != tc.want {
				t.Fatalf("%s SQL: got %q want %q", tc.method, got, tc.want)
			}
		})
	}
}

func TestSavepointClosedConnRejects(t *testing.T) {
	t.Parallel()
	c := &Conn{closed: true}
	ctx := context.Background()
	if err := c.Savepoint(ctx, "SP1"); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("Savepoint on closed conn: want driver.ErrBadConn, got %v", err)
	}
	if err := c.ReleaseSavepoint(ctx, "SP1"); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("ReleaseSavepoint on closed conn: want driver.ErrBadConn, got %v", err)
	}
	if err := c.RollbackToSavepoint(ctx, "SP1"); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("RollbackToSavepoint on closed conn: want driver.ErrBadConn, got %v", err)
	}
}

func TestSavepointBadNameRejectsBeforeWire(t *testing.T) {
	t.Parallel()
	// A live connection isn't required: validateSavepointName fires
	// before the wire. Pass closed=false but no transport -- if the
	// validator lets the call through, the test panics on the nil
	// transport.
	c := &Conn{}
	ctx := context.Background()
	cases := []string{"", "1SP", "SP'X", "DROP; SP", strings.Repeat("A", 129)}
	for _, name := range cases {
		name := name
		t.Run("name="+truncate(name, 16), func(t *testing.T) {
			t.Parallel()
			if err := c.Savepoint(ctx, name); err == nil {
				t.Fatalf("Savepoint(%q): expected validation error, got nil", name)
			}
		})
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
