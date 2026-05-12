package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

func TestSetSchemaClosedConn(t *testing.T) {
	t.Parallel()
	c := &Conn{closed: true}
	if err := c.SetSchema(context.Background(), "MYLIB"); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("SetSchema on closed conn: want driver.ErrBadConn, got %v", err)
	}
}

func TestSetSchemaNameValidation(t *testing.T) {
	t.Parallel()
	c := &Conn{}
	ctx := context.Background()
	cases := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"empty", "", "empty"},
		{"too long", "TOOLONG12345", "> 10"},
		{"semicolon injection", "X; DROP T", "not in"},
		{"quote injection", "X'OR'1", "not in"},
		{"hyphen", "X-Y", "not in"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := c.SetSchema(ctx, tc.input)
			if err == nil {
				t.Fatalf("SetSchema(%q): expected validation error, got nil", tc.input)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("SetSchema(%q): want error containing %q, got %v", tc.input, tc.wantErr, err)
			}
		})
	}
}

func TestSetSchemaSQLText(t *testing.T) {
	t.Parallel()
	// Drive the canonicalisation path to confirm what SQL we'd emit
	// before any wire activity. Mirrors the assembly inside SetSchema.
	cases := []struct {
		input string
		want  string
	}{
		{"MYLIB", "SET SCHEMA MYLIB"},
		{"mylib", "SET SCHEMA MYLIB"},
		{"My Lib", "SET SCHEMA MY_LIB"}, // canonPackageIdent turns spaces into underscores
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := "SET SCHEMA " + canonPackageIdent(tc.input)
			if got != tc.want {
				t.Fatalf("SET SCHEMA assembly: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestAddLibrariesValidation(t *testing.T) {
	t.Parallel()
	c := &Conn{}
	ctx := context.Background()
	cases := []struct {
		name    string
		input   []string
		wantErr string
	}{
		{"nil", nil, "empty list"},
		{"empty slice", []string{}, "empty list"},
		{"first invalid", []string{"BAD;NAME", "OK"}, "invalid library name"},
		{"middle invalid", []string{"OK", "BAD;", "ALSOOK"}, "invalid library name"},
		{"too long entry", []string{"TOOLONGNAME1"}, "invalid library name"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := c.AddLibraries(ctx, tc.input)
			if err == nil {
				t.Fatalf("AddLibraries(%v): expected error, got nil", tc.input)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("AddLibraries(%v): want error containing %q, got %v", tc.input, tc.wantErr, err)
			}
		})
	}
}

func TestAddLibrariesClosedConn(t *testing.T) {
	t.Parallel()
	c := &Conn{closed: true}
	if err := c.AddLibraries(context.Background(), []string{"OK"}); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("AddLibraries on closed conn: want ErrBadConn, got %v", err)
	}
}

func TestRemoveLibrariesValidation(t *testing.T) {
	t.Parallel()
	c := &Conn{}
	ctx := context.Background()
	cases := []struct {
		name    string
		input   []string
		wantErr string
	}{
		{"nil", nil, "empty list"},
		{"empty slice", []string{}, "empty list"},
		{"invalid name", []string{"BAD;NAME"}, "invalid library name"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := c.RemoveLibraries(ctx, tc.input)
			if err == nil {
				t.Fatalf("RemoveLibraries(%v): expected error, got nil", tc.input)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("RemoveLibraries(%v): want error containing %q, got %v", tc.input, tc.wantErr, err)
			}
		})
	}
}

func TestRemoveLibrariesClosedConn(t *testing.T) {
	t.Parallel()
	c := &Conn{closed: true}
	if err := c.RemoveLibraries(context.Background(), []string{"OK"}); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("RemoveLibraries on closed conn: want ErrBadConn, got %v", err)
	}
}

func TestRemoveLibrariesSQLAssembly(t *testing.T) {
	t.Parallel()
	// Confirm the CL string we'd build for a single library matches
	// the JT400 pattern at testdata/jtopen-fixtures.../Cases.java
	// (QCMDEXC invocation of RMVLIBLE).
	canon := canonPackageIdent("mylib")
	cmd := "RMVLIBLE LIB(" + canon + ")"
	sql := "CALL QSYS2.QCMDEXC('" + cmd + "')"
	want := "CALL QSYS2.QCMDEXC('RMVLIBLE LIB(MYLIB)')"
	if sql != want {
		t.Fatalf("RemoveLibraries SQL: got %q want %q", sql, want)
	}
}

func TestIsBenignRmvlibleErr(t *testing.T) {
	t.Parallel()
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("SQL-443 CPF2104 received from external program"), true},
		{errors.New("CPF9810 library not found"), true},
		{errors.New("Library FOO not removed from library list"), true},
		{errors.New("SQL-204 object not found"), true}, // generic "not found" -- match for forgiving CPF9810 mappings
		{errors.New("SQL-001 syntax error"), false},
		{errors.New("CPF0001 some other CPF"), false},
	}
	for _, tc := range cases {
		got := isBenignRmvlibleErr(tc.err)
		if got != tc.want {
			t.Fatalf("isBenignRmvlibleErr(%v): got %v want %v", tc.err, got, tc.want)
		}
	}
}
