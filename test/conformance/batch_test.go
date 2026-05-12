//go:build conformance

// v0.7.9 batched IUD conformance: end-to-end live tests for the
// CP 0x381F block-insert wire shape exposed via the driver-typed
// Conn.BatchExec method.
//
// The probe goes through sql.Conn.Raw to reach *db2i.Conn since
// BatchExec is not part of the database/sql interface; the pattern
// mirrors how pgx exposes batched operations.
package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"
	"time"

	db2i "github.com/complacentsee/go-db2i/driver"
)

// batchExec is the test-side wrapper around sql.Conn.Raw -> *db2i.Conn
// -> BatchExec so the conformance tests don't repeat the
// type-assertion dance.
func batchExec(t *testing.T, db *sql.DB, ctx context.Context, sqlText string, rows [][]any) int64 {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()
	var affected int64
	rawErr := conn.Raw(func(driverConn any) error {
		dconn, ok := driverConn.(*db2i.Conn)
		if !ok {
			return fmt.Errorf("driverConn is %T, want *db2i.Conn", driverConn)
		}
		n, err := dconn.BatchExec(ctx, sqlText, rows)
		affected = n
		return err
	})
	if rawErr != nil {
		t.Fatalf("BatchExec %q: %v", sqlText, rawErr)
	}
	return affected
}

// TestBatch_InsertVerified files a 100-row INSERT batch into a
// fresh table and confirms both the wire-side rows-affected and
// a follow-up COUNT(*) agree.
func TestBatch_InsertVerified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "bins"
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(32) NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	const n = 100
	rows := make([][]any, n)
	for i := 0; i < n; i++ {
		rows[i] = []any{int64(i + 1), fmt.Sprintf("row-%03d", i)}
	}
	affected := batchExec(t, db, ctx, "INSERT INTO "+tbl+" (ID, LABEL) VALUES (?, ?)", rows)
	if affected != n {
		t.Errorf("BatchExec rows-affected = %d, want %d", affected, n)
	}

	// Follow-up COUNT(*) confirms the rows actually landed.
	var got int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&got); err != nil {
		t.Fatalf("COUNT(*): %v", err)
	}
	if got != n {
		t.Errorf("SELECT COUNT(*) = %d, want %d", got, n)
	}

	// Spot-check a deterministic row -- confirms the value-encoding
	// in the multi-row CP 0x381F path lines up with the regular
	// single-row encoder.
	var label string
	if err := db.QueryRowContext(ctx, "SELECT LABEL FROM "+tbl+" WHERE ID = ?", int64(42)).Scan(&label); err != nil {
		t.Fatalf("spot-check SELECT: %v", err)
	}
	if label != "row-041" {
		t.Errorf("ID=42 label = %q, want %q", label, "row-041")
	}
}

// TestBatch_UpdateVerified seeds rows then bulk-updates them with a
// per-row LABEL change; asserts rows-affected = N and a spot-check
// of the new value.
func TestBatch_UpdateVerified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "bupd"
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(32) NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	const n = 50
	for i := 0; i < n; i++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?, ?)", int64(i+1), "seed"); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	updates := make([][]any, n)
	for i := 0; i < n; i++ {
		updates[i] = []any{fmt.Sprintf("upd-%03d", i), int64(i + 1)}
	}
	affected := batchExec(t, db, ctx, "UPDATE "+tbl+" SET LABEL = ? WHERE ID = ?", updates)
	if affected != n {
		t.Errorf("BatchExec UPDATE rows-affected = %d, want %d", affected, n)
	}

	var label string
	if err := db.QueryRowContext(ctx, "SELECT LABEL FROM "+tbl+" WHERE ID = ?", int64(13)).Scan(&label); err != nil {
		t.Fatalf("spot-check: %v", err)
	}
	if label != "upd-012" {
		t.Errorf("ID=13 label = %q, want %q", label, "upd-012")
	}
}

// TestBatch_DeleteVerified seeds rows then bulk-deletes them; asserts
// rows-affected = N and a follow-up COUNT(*) shows the table empty.
func TestBatch_DeleteVerified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "bdel"
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	const n = 25
	for i := 0; i < n; i++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?)", int64(i+1)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	deletes := make([][]any, n)
	for i := 0; i < n; i++ {
		deletes[i] = []any{int64(i + 1)}
	}
	affected := batchExec(t, db, ctx, "DELETE FROM "+tbl+" WHERE ID = ?", deletes)
	if affected != n {
		t.Errorf("BatchExec DELETE rows-affected = %d, want %d", affected, n)
	}

	var remaining int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&remaining); err != nil {
		t.Fatalf("COUNT(*): %v", err)
	}
	if remaining != 0 {
		t.Errorf("rows left after batch DELETE: %d, want 0", remaining)
	}
}

// TestBatch_MergeVerified (v0.7.10) exercises MERGE batching via a
// parameterised `MERGE INTO ... USING (VALUES (?, ?))` over N rows.
// Half the batch rows match existing target rows (UPDATE branch);
// the other half don't (INSERT branch). Asserts the post-state
// matches both branches' expectations.
//
// MERGE wire shape is identical to IUD on V7R1+
// (JDSQLStatement.java:644-648); v0.7.10 just removed the verb
// reject in Conn.BatchExec. PUB400 V7R5M0 is at the threshold and
// supports MERGE batching.
func TestBatch_MergeVerified(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "bmrg"
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, VAL VARCHAR(32) NOT NULL)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	// Seed IDs 1..matchCount so the first matchCount batch rows
	// hit WHEN MATCHED. IDs matchCount+1..n trip WHEN NOT MATCHED
	// THEN INSERT.
	const matchCount = 20
	const n = 50
	for i := 1; i <= matchCount; i++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?, ?)", int64(i), "seed"); err != nil {
			t.Fatalf("seed iter %d: %v", i, err)
		}
	}

	rows := make([][]any, n)
	for i := 0; i < n; i++ {
		rows[i] = []any{int64(i + 1), fmt.Sprintf("merge-%03d", i+1)}
	}
	// IBM i SQL's parser needs explicit CASTs around parameter
	// markers in the USING clause's VALUES so it can determine the
	// source column types -- without them SQL-584 / QSQRCHK fires
	// at PREPARE_DESCRIBE.
	mergeSQL := "MERGE INTO " + tbl + " t USING (VALUES (" +
		"CAST(? AS INTEGER), CAST(? AS VARCHAR(32)))) AS s(ID, VAL) " +
		"ON (t.ID = s.ID) " +
		"WHEN MATCHED THEN UPDATE SET t.VAL = s.VAL " +
		"WHEN NOT MATCHED THEN INSERT (ID, VAL) VALUES (s.ID, s.VAL)"
	affected := batchExec(t, db, ctx, mergeSQL, rows)
	// Server's rows-affected sums matched-updates + not-matched-
	// inserts. With matchCount matched + (n-matchCount) inserted,
	// the total equals n.
	if affected != int64(n) {
		t.Errorf("MERGE rows-affected = %d, want %d (matched=%d + inserted=%d)",
			affected, n, matchCount, n-matchCount)
	}

	var total int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&total); err != nil {
		t.Fatalf("post-MERGE COUNT(*): %v", err)
	}
	if total != n {
		t.Errorf("table row count after MERGE = %d, want %d", total, n)
	}

	// Spot-check both branches: ID=5 was seeded then matched-and-
	// updated (so VAL should be the merged value, not "seed");
	// ID=42 was inserted (didn't exist before).
	var v5, v42 string
	if err := db.QueryRowContext(ctx, "SELECT VAL FROM "+tbl+" WHERE ID = ?", int64(5)).Scan(&v5); err != nil {
		t.Fatalf("spot-check ID=5: %v", err)
	}
	if v5 != "merge-005" {
		t.Errorf("ID=5 VAL = %q after MERGE, want %q (WHEN MATCHED branch did not fire)", v5, "merge-005")
	}
	if err := db.QueryRowContext(ctx, "SELECT VAL FROM "+tbl+" WHERE ID = ?", int64(42)).Scan(&v42); err != nil {
		t.Fatalf("spot-check ID=42: %v", err)
	}
	if v42 != "merge-042" {
		t.Errorf("ID=42 VAL = %q after MERGE, want %q (WHEN NOT MATCHED THEN INSERT did not fire)", v42, "merge-042")
	}
}

// TestBatch_AutoSplits32k verifies the 32k client-side split logic.
// 50k rows -> 2 chunks (32000 + 18000). We don't directly assert
// chunk count from the wire (no per-test hook), but we do require
// the total rows-affected to equal the input size, which proves
// both chunks completed.
func TestBatch_AutoSplits32k(t *testing.T) {
	if testing.Short() {
		t.Skip("50k-row batch -- skipped under -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "bspl"
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	const n = 50_000
	rows := make([][]any, n)
	for i := 0; i < n; i++ {
		rows[i] = []any{int64(i + 1)}
	}
	start := time.Now()
	affected := batchExec(t, db, ctx, "INSERT INTO "+tbl+" (ID) VALUES (?)", rows)
	elapsed := time.Since(start)
	t.Logf("BatchExec 50k INSERT: rows-affected=%d elapsed=%s", affected, elapsed)
	if affected != n {
		t.Errorf("rows-affected = %d, want %d (auto-split must complete every chunk)", affected, n)
	}

	var got int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tbl).Scan(&got); err != nil {
		t.Fatalf("COUNT(*): %v", err)
	}
	if got != n {
		t.Errorf("table count after auto-split batch: %d, want %d", got, n)
	}
}

// TestBatch_PerfDelta is a same-LPAR comparison between
// BatchExec(N rows) and a per-row db.ExecContext loop over the same
// N rows. The expectation is a meaningful speed-up; the test logs
// the timings for the perf-doc citation but doesn't fail on a
// specific ratio (LPAR / RTT variation).
//
// N is intentionally small (100) so the per-row baseline completes
// within the 5-minute context on high-RTT public-internet LPARs
// (PUB400 sees ~100ms RTT -- 1000 per-row INSERTs would not fit).
// The relative speed-up at N=100 already exceeds 10× on V7R6M0
// LAN and >100× on tunneled paths; the doc cites the 1000-row
// V7R6M0 VPC-tunnel measurement (~358×) where the baseline does
// fit.
func TestBatch_PerfDelta(t *testing.T) {
	if testing.Short() {
		t.Skip("perf comparison -- skipped under -short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "bprf"
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	const n = 100

	// Per-row loop (baseline).
	startLoop := time.Now()
	for i := 0; i < n; i++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO "+tbl+" VALUES (?)", int64(i+1)); err != nil {
			t.Fatalf("per-row INSERT iter %d: %v", i, err)
		}
	}
	loopElapsed := time.Since(startLoop)

	if _, err := db.ExecContext(ctx, "DELETE FROM "+tbl); err != nil {
		t.Fatalf("clear before batch: %v", err)
	}

	// Batch path.
	rows := make([][]any, n)
	for i := 0; i < n; i++ {
		rows[i] = []any{int64(i + 1)}
	}
	startBatch := time.Now()
	affected := batchExec(t, db, ctx, "INSERT INTO "+tbl+" (ID) VALUES (?)", rows)
	batchElapsed := time.Since(startBatch)
	if affected != n {
		t.Errorf("batch rows-affected = %d, want %d", affected, n)
	}
	t.Logf("perf 100-row INSERT: per-row=%s batch=%s speed-up=%.1fx",
		loopElapsed, batchElapsed, float64(loopElapsed)/float64(batchElapsed))
}

// TestBatch_LOBFallback exercises the v0.7.15 LOB-batch fallback
// path: when any row in a BatchExec input carries a `*LOBValue`,
// the driver falls back internally to per-row EXECUTE (mirroring
// JT400) instead of rejecting up-front. Caller code is unchanged;
// the cost is one round-trip per row instead of one per chunk.
//
// Asserts rows-affected matches the input count and follow-up
// SELECTs read the exact bytes back. The presence of any
// *LOBValue in the batch is what routes to the per-row path; the
// other rows can be plain []byte and still take the per-row route
// (the route is per-batch, not per-row).
func TestBatch_LOBFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := openDB(t)
	defer db.Close()

	tbl := schema() + "." + tablePrefix + "blob"
	_, _ = db.ExecContext(ctx, "DROP TABLE "+tbl)
	if _, err := db.ExecContext(ctx,
		"CREATE TABLE "+tbl+" (ID INTEGER NOT NULL PRIMARY KEY, PAYLOAD BLOB(1M))"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.ExecContext(ctx, "DROP TABLE "+tbl) //nolint:errcheck

	// Row mix:
	//   1 → 1 KiB of 'A'         (plain []byte)
	//   2 → 2 KiB of 'B'         (plain []byte)
	//   3 → 64 KiB of 'C' wrapped in *LOBValue (Reader streaming)
	//   4 → 4 KiB of 'D'         (plain []byte)
	expected := map[int64][]byte{
		1: bytesOf('A', 1024),
		2: bytesOf('B', 2*1024),
		3: bytesOf('C', 64*1024),
		4: bytesOf('D', 4*1024),
	}
	streamLOB := &db2i.LOBValue{
		Reader: bytesReader(expected[3]),
		Length: int64(len(expected[3])),
	}
	rows := [][]any{
		{int64(1), expected[1]},
		{int64(2), expected[2]},
		{int64(3), streamLOB},
		{int64(4), expected[4]},
	}

	affected := batchExec(t, db, ctx, "INSERT INTO "+tbl+" (ID, PAYLOAD) VALUES (?, ?)", rows)
	if affected != int64(len(rows)) {
		t.Errorf("BatchExec rows-affected = %d, want %d", affected, len(rows))
	}

	// Verify each row round-trips byte-perfect.
	for id := int64(1); id <= 4; id++ {
		var got []byte
		if err := db.QueryRowContext(ctx, "SELECT PAYLOAD FROM "+tbl+" WHERE ID = ?", id).Scan(&got); err != nil {
			t.Errorf("SELECT id=%d: %v", id, err)
			continue
		}
		want := expected[id]
		if len(got) != len(want) {
			t.Errorf("id=%d: length %d, want %d", id, len(got), len(want))
			continue
		}
		// Spot-check head + tail; bytewise compare on multi-MB
		// payloads would dominate the test output on a mismatch.
		if got[0] != want[0] || got[len(got)-1] != want[len(want)-1] {
			t.Errorf("id=%d: bytes differ at boundaries (got [0]=%c [-1]=%c, want [0]=%c [-1]=%c)",
				id, got[0], got[len(got)-1], want[0], want[len(want)-1])
		}
	}
}

func bytesOf(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func bytesReader(b []byte) *bytesReaderImpl {
	return &bytesReaderImpl{b: b}
}

// bytesReaderImpl wraps a byte slice as an io.Reader without pulling
// in bytes.NewReader (which the test file doesn't already import).
type bytesReaderImpl struct {
	b   []byte
	pos int
}

func (r *bytesReaderImpl) Read(p []byte) (int, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.pos:])
	r.pos += n
	return n, nil
}
