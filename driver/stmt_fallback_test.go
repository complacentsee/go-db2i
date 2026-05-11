package driver

import (
	"errors"
	"fmt"
	"testing"

	"github.com/complacentsee/go-db2i/hostserver"
)

// TestShouldRefallbackToPrepare covers the cache-hit DDL-invalidation
// fallback predicate. Catches the right SQLCodes, rejects everything
// else (including looks-similar-but-not Db2Errors and wrapped errors).
func TestShouldRefallbackToPrepare(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "non-Db2Error",
			err:  errors.New("connection lost"),
			want: false,
		},
		{
			name: "SQL-204 direct",
			err:  &hostserver.Db2Error{SQLCode: -204, SQLState: "42704"},
			want: true,
		},
		{
			name: "SQL-805 direct",
			err:  &hostserver.Db2Error{SQLCode: -805, SQLState: "51002"},
			want: true,
		},
		{
			name: "SQL-204 wrapped via fmt.Errorf",
			err:  fmt.Errorf("EXECUTE_CACHED: %w", &hostserver.Db2Error{SQLCode: -204}),
			want: true,
		},
		{
			name: "SQL-805 wrapped via fmt.Errorf",
			err:  fmt.Errorf("OPEN_CACHED: %w", &hostserver.Db2Error{SQLCode: -805}),
			want: true,
		},
		{
			name: "constraint violation (SQL-803) NOT a refallback",
			err:  &hostserver.Db2Error{SQLCode: -803, SQLState: "23505"},
			want: false,
		},
		{
			name: "lock timeout (SQL-911) NOT a refallback",
			err:  &hostserver.Db2Error{SQLCode: -911, SQLState: "57033"},
			want: false,
		},
		{
			name: "positive SQLCode +100 NOT a refallback",
			err:  &hostserver.Db2Error{SQLCode: 100, SQLState: "02000"},
			want: false,
		},
		{
			name: "permission denied (SQL-551) NOT a refallback",
			err:  &hostserver.Db2Error{SQLCode: -551, SQLState: "42501"},
			want: false,
		},
		{
			name: "ErrUnsupportedCachedParamType direct",
			err:  hostserver.ErrUnsupportedCachedParamType,
			want: true,
		},
		{
			name: "ErrUnsupportedCachedParamType wrapped via fmt.Errorf",
			err:  fmt.Errorf("hostserver: param 1: SQL type 405: %w", hostserver.ErrUnsupportedCachedParamType),
			want: true,
		},
		{
			name: "ErrUnsupportedCachedParamType doubly wrapped",
			err:  fmt.Errorf("encode cached input parameter data: %w", fmt.Errorf("param 1: SQL type 405: %w", hostserver.ErrUnsupportedCachedParamType)),
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldRefallbackToPrepare(tc.err)
			if got != tc.want {
				t.Errorf("shouldRefallbackToPrepare(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestPurgeCachedStatement covers the symmetric delete on
// c.pkg.Cached. Safe to call when the conn has no package context
// or the entry is absent; both no-op.
func TestPurgeCachedStatement(t *testing.T) {
	t.Run("nil pkg", func(t *testing.T) {
		c := &Conn{}
		c.purgeCachedStatement("SELECT 1")
		// (no panic; nothing to assert)
	})
	t.Run("nil Cached map", func(t *testing.T) {
		c := &Conn{pkg: &hostserver.PackageManager{Name: "GOTPKG"}}
		c.purgeCachedStatement("SELECT 1")
		// (no panic)
	})
	t.Run("entry absent", func(t *testing.T) {
		c := &Conn{pkg: &hostserver.PackageManager{
			Name:    "GOTPKG",
			Cached:  map[string]*hostserver.PackageStatement{},
		}}
		c.purgeCachedStatement("SELECT 1")
		if len(c.pkg.Cached) != 0 {
			t.Errorf("cache should remain empty after purge of absent entry; len=%d", len(c.pkg.Cached))
		}
	})
	t.Run("entry present", func(t *testing.T) {
		const sql = "SELECT id FROM t WHERE x = ?"
		c := &Conn{pkg: &hostserver.PackageManager{
			Name: "GOTPKG",
			Cached: map[string]*hostserver.PackageStatement{
				sql:                   {SQLText: sql, NameBytes: make([]byte, 18)},
				"SELECT 2 FROM other": {SQLText: "SELECT 2 FROM other", NameBytes: make([]byte, 18)},
			},
		}}
		c.purgeCachedStatement(sql)
		if _, ok := c.pkg.Cached[sql]; ok {
			t.Errorf("expected entry purged, still present")
		}
		if len(c.pkg.Cached) != 1 {
			t.Errorf("expected 1 remaining entry, got %d", len(c.pkg.Cached))
		}
	})
}
