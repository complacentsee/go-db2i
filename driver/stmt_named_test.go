package driver

import (
	"database/sql/driver"
	"strings"
	"testing"
)

func mkArg(ord int, name string, value any) driver.NamedValue {
	return driver.NamedValue{Ordinal: ord, Name: name, Value: value}
}

// TestParseCallProc pins the v0.7.18 CALL-statement parser used to
// route the sql.Named catalog lookup at the right proc. Covers
// qualified (`schema.proc`), system-naming (`schema/proc`), and
// unqualified call forms; rejects non-CALL SQL.
func TestParseCallProc(t *testing.T) {
	cases := []struct {
		name       string
		sql        string
		defaultLib string
		wantSchema string
		wantProc   string
		wantErr    string
	}{
		{
			name:       "qualified period",
			sql:        "CALL GOSPROCS.P_LOOKUP(?, ?, ?)",
			wantSchema: "GOSPROCS",
			wantProc:   "P_LOOKUP",
		},
		{
			name:       "qualified slash (system naming)",
			sql:        "CALL gosprocs/p_lookup(?, ?, ?)",
			wantSchema: "GOSPROCS",
			wantProc:   "P_LOOKUP",
		},
		{
			name:       "unqualified with default schema",
			sql:        "CALL p_lookup(?, ?)",
			defaultLib: "myschema",
			wantSchema: "MYSCHEMA",
			wantProc:   "P_LOOKUP",
		},
		{
			name:    "unqualified without default schema rejects",
			sql:     "CALL p_lookup(?)",
			wantErr: "default schema",
		},
		{
			name:    "non-CALL rejects",
			sql:     "SELECT 1 FROM SYSIBM.SYSDUMMY1",
			wantErr: "not a CALL",
		},
		{
			name:       "leading whitespace tolerated",
			sql:        "   CALL\tlib.proc(?)",
			wantSchema: "LIB",
			wantProc:   "PROC",
		},
		{
			name:       "identifier with $, #, @",
			sql:        "CALL APP.PROC#1(?)",
			wantSchema: "APP",
			wantProc:   "PROC#1",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			schema, name, err := parseCallProc(tc.sql, tc.defaultLib)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("want error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error %q does not contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCallProc: %v", err)
			}
			if schema != tc.wantSchema || name != tc.wantProc {
				t.Errorf("got (schema=%q, proc=%q), want (%q, %q)", schema, name, tc.wantSchema, tc.wantProc)
			}
		})
	}
}

// TestResolveNamedArgs_NoNamedArgsFastPath confirms args without
// any sql.Named tag pass through unchanged without a catalog
// round-trip (the load-bearing constant-cost guarantee).
func TestResolveNamedArgs_NoNamedArgsFastPath(t *testing.T) {
	s := &Stmt{
		conn:  &Conn{cfg: &Config{}, log: silentLogger},
		query: "INSERT INTO t VALUES (?)",
	}
	got, err := s.resolveNamedArgs(nil, []driver.NamedValue{
		mkArg(1, "", 1), mkArg(2, "", 2), mkArg(3, "", 3),
	})
	if err != nil {
		t.Fatalf("resolveNamedArgs (positional): %v", err)
	}
	if len(got) != 3 || got[0].Value != 1 || got[2].Value != 3 {
		t.Errorf("positional args mutated: %v", got)
	}
	if s.paramNamesLoaded {
		t.Error("paramNamesLoaded set on positional path; should be lazy")
	}
}

// TestResolveNamedArgs_RejectsNonCall pins the "named binds only
// for CALL" rule. SELECT / INSERT / UPDATE with named markers
// would silently match the wrong column if we tried to resolve
// them, so we reject up-front.
func TestResolveNamedArgs_RejectsNonCall(t *testing.T) {
	s := &Stmt{
		conn:  &Conn{cfg: &Config{}, log: silentLogger},
		query: "SELECT * FROM t WHERE id = ?",
	}
	_, err := s.resolveNamedArgs(nil, []driver.NamedValue{mkArg(1, "id", 42)})
	if err == nil {
		t.Fatal("expected reject for named binding on SELECT")
	}
	if !strings.Contains(err.Error(), "only supported for CALL") {
		t.Errorf("error %q does not mention CALL-only restriction", err)
	}
}

// TestResolveNamedArgs_RejectsMixedNamedAndPositional confirms
// callers can't pass `db.Exec("CALL p(?, ?)", val1, sql.Named("x",
// val2))` -- the intent is ambiguous (which position does val1
// fill?).
func TestResolveNamedArgs_RejectsMixedNamedAndPositional(t *testing.T) {
	s := &Stmt{
		conn:  &Conn{cfg: &Config{}, log: silentLogger},
		query: "CALL lib.proc(?, ?)",
	}
	_, err := s.resolveNamedArgs(nil, []driver.NamedValue{
		mkArg(1, "", 1), mkArg(2, "X", 2),
	})
	if err == nil {
		t.Fatal("expected reject for mixed named + positional")
	}
	if !strings.Contains(err.Error(), "mixed") &&
		!strings.Contains(err.Error(), "tag every arg or none") {
		t.Errorf("error %q does not mention mixed-binding rule", err)
	}
}