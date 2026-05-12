package db2iiter

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

// fakeDriver is an in-process database/sql driver that returns the
// rows a test pre-seeded. The point is to exercise ScanAll without
// pulling in the real go-db2i driver (which needs a live LPAR).
type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	return &fakeConn{}, nil
}

type fakeConn struct{}

func (*fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{query: query}, nil
}
func (*fakeConn) Close() error              { return nil }
func (*fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("not supported") }

type fakeStmt struct {
	query string
}

func (*fakeStmt) Close() error  { return nil }
func (*fakeStmt) NumInput() int { return 0 }
func (*fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not supported")
}
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	r, ok := harness.Load(s.query)
	if !ok {
		return nil, errors.New("no canned rows for query")
	}
	src := r.([]row)
	cp := make([]row, len(src))
	copy(cp, src)
	return &fakeRows{rows: cp, errAtEnd: harnessErr.Load(s.query)}, nil
}

type row struct {
	id   int64
	name string
	err  error // if non-nil, Next returns this row but Scan-side helper should error
}

type fakeRows struct {
	rows     []row
	pos      int
	errAtEnd error
}

func (r *fakeRows) Columns() []string { return []string{"id", "name"} }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		if r.errAtEnd != nil {
			err := r.errAtEnd
			r.errAtEnd = nil
			return err
		}
		return io.EOF
	}
	dest[0] = r.rows[r.pos].id
	dest[1] = r.rows[r.pos].name
	r.pos++
	return nil
}

// Shared harness for queries to canned rows. Using package-level
// vars (load-once-per-test pattern) keeps the driver Open path
// pure; tests set/clear before each Run.
var (
	harness    safeMap
	harnessErr errMap
)

type safeMap struct{ m map[string]any }

func (s *safeMap) Load(k string) (any, bool) { v, ok := s.m[k]; return v, ok }
func (s *safeMap) Store(k string, v any) {
	if s.m == nil {
		s.m = map[string]any{}
	}
	s.m[k] = v
}
func (s *safeMap) Clear() { s.m = nil }

type errMap struct{ m map[string]error }

func (s *errMap) Load(k string) error { return s.m[k] }
func (s *errMap) Store(k string, v error) {
	if s.m == nil {
		s.m = map[string]error{}
	}
	s.m[k] = v
}
func (s *errMap) Clear() { s.m = nil }

func init() {
	sql.Register("fakedb2i", fakeDriver{})
}

type Person struct {
	ID   int64
	Name string
}

func scanPerson(rs *sql.Rows) (Person, error) {
	var p Person
	return p, rs.Scan(&p.ID, &p.Name)
}

func TestScanAllHappyPath(t *testing.T) {
	harness.Clear()
	harnessErr.Clear()
	harness.Store("SELECT happy", []row{
		{id: 1, name: "alpha"},
		{id: 2, name: "beta"},
		{id: 3, name: "gamma"},
	})
	db, err := sql.Open("fakedb2i", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT happy")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []Person
	var hadErr error
	for p, err := range ScanAll(rows, scanPerson) {
		if err != nil {
			hadErr = err
			break
		}
		got = append(got, p)
	}
	if hadErr != nil {
		t.Fatalf("ScanAll happy: unexpected err %v", hadErr)
	}
	want := []Person{{1, "alpha"}, {2, "beta"}, {3, "gamma"}}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i, p := range got {
		if p != want[i] {
			t.Fatalf("row %d: got %+v want %+v", i, p, want[i])
		}
	}
}

func TestScanAllEmpty(t *testing.T) {
	harness.Clear()
	harnessErr.Clear()
	harness.Store("SELECT empty", []row{})
	db, err := sql.Open("fakedb2i", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT empty")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	iterations := 0
	for _, err := range ScanAll(rows, scanPerson) {
		iterations++
		if err != nil {
			t.Fatalf("empty result: unexpected err %v", err)
		}
	}
	if iterations != 0 {
		t.Fatalf("empty result: got %d iterations, want 0", iterations)
	}
}

func TestScanAllRowsErr(t *testing.T) {
	harness.Clear()
	harnessErr.Clear()
	harness.Store("SELECT lateErr", []row{
		{id: 1, name: "alpha"},
		{id: 2, name: "beta"},
	})
	harnessErr.Store("SELECT lateErr", errors.New("late driver error"))

	db, err := sql.Open("fakedb2i", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT lateErr")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var seen []Person
	var finalErr error
	for p, err := range ScanAll(rows, scanPerson) {
		if err != nil {
			finalErr = err
			continue
		}
		seen = append(seen, p)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 successful rows, got %d", len(seen))
	}
	if finalErr == nil {
		t.Fatalf("expected late error to be yielded, got nil")
	}
	if finalErr.Error() != "late driver error" {
		t.Fatalf("expected late error verbatim, got %v", finalErr)
	}
}

func TestScanAllScanError(t *testing.T) {
	harness.Clear()
	harnessErr.Clear()
	harness.Store("SELECT scanErr", []row{
		{id: 1, name: "alpha"},
		{id: 2, name: "beta"},
	})

	db, err := sql.Open("fakedb2i", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT scanErr")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// Force a scan-side error by passing the wrong number of dest
	// pointers (only 1, table has 2). sql.Scan returns
	// "sql: expected 2 destination arguments in Scan, not 1".
	failingScan := func(rs *sql.Rows) (Person, error) {
		var p Person
		return p, rs.Scan(&p.ID)
	}

	iter := 0
	var firstErr error
	for _, err := range ScanAll(rows, failingScan) {
		iter++
		if err != nil {
			firstErr = err
			break
		}
	}
	if iter != 1 {
		t.Fatalf("expected 1 iteration before break, got %d", iter)
	}
	if firstErr == nil {
		t.Fatalf("expected scan error, got nil")
	}
}

func TestScanAllEarlyBreak(t *testing.T) {
	harness.Clear()
	harnessErr.Clear()
	harness.Store("SELECT breakEarly", []row{
		{id: 1, name: "a"},
		{id: 2, name: "b"},
		{id: 3, name: "c"},
	})

	db, err := sql.Open("fakedb2i", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT breakEarly")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for p, err := range ScanAll(rows, scanPerson) {
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		count++
		if p.ID == 2 {
			break
		}
	}
	if count != 2 {
		t.Fatalf("early-break: got %d iterations, want 2 (stopped at id=2)", count)
	}
}
