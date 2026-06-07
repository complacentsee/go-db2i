//go:build conformance

// array_param_test.go is the live conformance coverage for
// stored-procedure ARRAY parameters (issue #68): db2i.Array[T] bound IN,
// OUT, and INOUT against real CREATE TYPE ... ARRAY procedures on PUB400
// V7R5M0. On DB2 for i an ARRAY crosses the host-server wire only as a
// procedure parameter (#39 / SQL-20441); the IN/INOUT values ride in
// CP 0x382F and the OUT/INOUT values come back in CP 0x3901. The wire
// codecs are pinned byte-for-byte against the JT400 capture offline
// (hostserver/array_param_wire_test.go); these tests prove the end-to-end
// Go round-trip against a live LPAR.
package conformance

import (
	"bytes"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	db2i "github.com/complacentsee/go-db2i/driver"
)

// createArrayType runs CREATE TYPE ... ARRAY idempotently, skipping
// cleanly on the shared-LPAR storage/auth failures the free-tier PUB400
// profile hits, and registers a DROP TYPE cleanup. DB2 for i has no
// CREATE TYPE IF NOT EXISTS, so SQLSTATE 42710 (already exists) is
// tolerated. Types are created before procs so the LIFO cleanups drop the
// dependent procs first.
func createArrayType(t *testing.T, db *sql.DB, name, def string) {
	t.Helper()
	full := procLibrary + "." + name
	if _, err := db.Exec("CREATE TYPE " + full + " AS " + def); err != nil {
		if isEnvProcErr(err) {
			t.Skipf("CREATE TYPE %s failed (environmental -- shared LPAR storage/auth): %v", name, err)
		}
		if !contains(err.Error(), "42710") {
			t.Fatalf("CREATE TYPE %s: %v", name, err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TYPE " + full) })
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestStoredProcedureArray round-trips db2i.Array[T] across IN / OUT /
// INOUT for INTEGER and VARCHAR element types, including a null element
// and a whole-null array.
func TestStoredProcedureArray(t *testing.T) {
	db := openDB(t)

	tInt := "G" + runToken + "YI" // INTEGER ARRAY[10]
	tVc := "G" + runToken + "YV"  // VARCHAR(20) ARRAY[10]
	pOut := "G" + runToken + "Y1" // OUT INTEGER ARRAY
	pSum := "G" + runToken + "Y2" // IN INTEGER ARRAY + OUT INTEGER
	pIO := "G" + runToken + "Y3"  // INOUT INTEGER ARRAY
	pVc := "G" + runToken + "Y4"  // INOUT VARCHAR ARRAY
	pNE := "G" + runToken + "Y5"  // OUT INTEGER ARRAY with a NULL element
	pWN := "G" + runToken + "Y6"  // OUT INTEGER ARRAY left NULL (whole-null)

	createArrayType(t, db, tInt, "INTEGER ARRAY[10]")
	createArrayType(t, db, tVc, "VARCHAR(20) ARRAY[10]")
	qtInt := procLibrary + "." + tInt
	qtVc := procLibrary + "." + tVc

	createProc(t, db, pOut, "(OUT P_A "+qtInt+") LANGUAGE SQL "+
		"BEGIN SET P_A = ARRAY[11,22,33]; END")
	createProc(t, db, pSum, "(IN P_A "+qtInt+", OUT P_S INTEGER) LANGUAGE SQL "+
		"BEGIN SET P_S = P_A[1] + P_A[2] + P_A[3]; END")
	createProc(t, db, pIO, "(INOUT P_A "+qtInt+") LANGUAGE SQL "+
		"BEGIN SET P_A = ARRAY[P_A[1] + 100, P_A[2] + 100]; END")
	createProc(t, db, pVc, "(INOUT P_A "+qtVc+") LANGUAGE SQL "+
		"BEGIN SET P_A = ARRAY['XX','YYY']; END")
	createProc(t, db, pNE, "(OUT P_A "+qtInt+") LANGUAGE SQL "+
		"BEGIN SET P_A = ARRAY[CAST(1 AS INTEGER), CAST(NULL AS INTEGER), CAST(3 AS INTEGER)]; END")
	createProc(t, db, pWN, "(OUT P_A "+qtInt+") LANGUAGE SQL "+
		"BEGIN DECLARE V INTEGER DEFAULT 0; SET V = 1; END")

	call := func(name string) string { return "CALL " + procLibrary + "." + name + " " }

	t.Run("out_int", func(t *testing.T) {
		var a db2i.Array[int32]
		if _, err := db.Exec(call(pOut)+"(?)", sql.Out{Dest: &a}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if a.Null {
			t.Fatal("array is null, want [11,22,33]")
		}
		if got := a.Elements; len(got) != 3 || got[0] != 11 || got[1] != 22 || got[2] != 33 {
			t.Errorf("OUT array = %v, want [11 22 33]", got)
		}
	})

	t.Run("in_int_sum", func(t *testing.T) {
		var sum int32
		in := db2i.Array[int32]{Elements: []int32{10, 20, 30}}
		if _, err := db.Exec(call(pSum)+"(?, ?)", in, sql.Out{Dest: &sum}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if sum != 60 {
			t.Errorf("sum of IN array = %d, want 60 (10+20+30)", sum)
		}
	})

	t.Run("inout_int", func(t *testing.T) {
		a := db2i.Array[int32]{Elements: []int32{1, 2}}
		if _, err := db.Exec(call(pIO)+"(?)", sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if got := a.Elements; len(got) != 2 || got[0] != 101 || got[1] != 102 {
			t.Errorf("INOUT array = %v, want [101 102]", got)
		}
	})

	t.Run("inout_varchar", func(t *testing.T) {
		a := db2i.Array[string]{Elements: []string{"AB", "CDE", "f"}}
		if _, err := db.Exec(call(pVc)+"(?)", sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if got := a.Elements; len(got) != 2 || got[0] != "XX" || got[1] != "YYY" {
			t.Errorf("INOUT varchar array = %q, want [XX YYY]", got)
		}
	})

	t.Run("null_element_out", func(t *testing.T) {
		var a db2i.Array[*int32]
		if _, err := db.Exec(call(pNE)+"(?)", sql.Out{Dest: &a}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if len(a.Elements) != 3 {
			t.Fatalf("got %d elements, want 3", len(a.Elements))
		}
		if a.Elements[0] == nil || *a.Elements[0] != 1 {
			t.Errorf("element[0] = %v, want 1", a.Elements[0])
		}
		if a.Elements[1] != nil {
			t.Errorf("element[1] = %v, want NULL (nil)", *a.Elements[1])
		}
		if a.Elements[2] == nil || *a.Elements[2] != 3 {
			t.Errorf("element[2] = %v, want 3", a.Elements[2])
		}
	})

	t.Run("whole_null_out", func(t *testing.T) {
		var a db2i.Array[int32]
		if _, err := db.Exec(call(pWN)+"(?)", sql.Out{Dest: &a}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if !a.Null {
			t.Errorf("array Null = false, want true (proc left OUT param unset)")
		}
	})

	t.Run("in_null_element", func(t *testing.T) {
		// IN array with a NULL middle element: P_A[2] is NULL, so
		// P_A[1]+P_A[2]+P_A[3] is NULL and the OUT INTEGER comes back NULL,
		// surfaced by the driver as the zero value of a plain int32 dest.
		// (The IN null-element wire bytes are pinned byte-for-byte against
		// JT400 offline in hostserver/array_param_wire_test.go.)
		two := int32(2)
		in := db2i.Array[*int32]{Elements: []*int32{ptrInt32(1), nil, &two}}
		var sum int32 = -1
		if _, err := db.Exec(call(pSum)+"(?, ?)", in, sql.Out{Dest: &sum}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if sum != 0 {
			t.Errorf("sum with NULL element = %d, want 0 (NULL OUT -> zero)", sum)
		}
	})
}

func ptrInt32(v int32) *int32 { return &v }

func trimSp(s string) string { return strings.TrimRight(s, " ") }

func bytesEq(a, b []byte) bool { return bytes.Equal(a, b) }

// decEq compares two decimal strings numerically so the assertion does
// not depend on the exact decode format (e.g. "12.34" vs "12.340").
func decEq(got, want string) bool {
	g, err1 := strconv.ParseFloat(strings.TrimSpace(got), 64)
	w, err2 := strconv.ParseFloat(strings.TrimSpace(want), 64)
	if err1 != nil || err2 != nil {
		return strings.TrimSpace(got) == strings.TrimSpace(want)
	}
	d := g - w
	return d < 1e-9 && d > -1e-9
}

// TestStoredProcedureArrayElementTypes broadens live array element-type
// coverage beyond INTEGER/VARCHAR (issue #68 Items 2 & 3): CHAR (the
// 452/453 encode arm), BIGINT, SMALLINT, DOUBLE, VARBINARY, and DECIMAL.
// Each case is an echo INOUT (SET P_A = P_A) so a single proc exercises
// the IN encode (CP 0x382F) and the OUT decode (CP 0x3901) for that
// element type in one round-trip.
func TestStoredProcedureArrayElementTypes(t *testing.T) {
	db := openDB(t)

	// echoProc creates a TYPE <elemDef> ARRAY[10] and an INOUT echo proc,
	// returning the qualified CALL prefix.
	echoProc := func(t *testing.T, typeSuffix, procSuffix, elemDef string) string {
		typ := "G" + runToken + typeSuffix
		proc := "G" + runToken + procSuffix
		createArrayType(t, db, typ, elemDef+" ARRAY[10]")
		qt := procLibrary + "." + typ
		createProc(t, db, proc, "(INOUT P_A "+qt+") LANGUAGE SQL BEGIN SET P_A = P_A; END")
		return "CALL " + procLibrary + "." + proc + " (?)"
	}

	t.Run("char", func(t *testing.T) {
		call := echoProc(t, "EC", "C0", "CHAR(5)")
		a := db2i.Array[string]{Elements: []string{"AB", "CDE"}}
		if _, err := db.Exec(call, sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if len(a.Elements) != 2 {
			t.Fatalf("got %d elements, want 2", len(a.Elements))
		}
		// CHAR(5) is blank-padded; compare on the trimmed value.
		if got0, got1 := trimSp(a.Elements[0]), trimSp(a.Elements[1]); got0 != "AB" || got1 != "CDE" {
			t.Errorf("CHAR array = [%q %q], want [AB CDE]", a.Elements[0], a.Elements[1])
		}
	})

	t.Run("bigint", func(t *testing.T) {
		call := echoProc(t, "EB", "B0", "BIGINT")
		in := []int64{1 << 40, -(1 << 40), 7}
		a := db2i.Array[int64]{Elements: in}
		if _, err := db.Exec(call, sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if len(a.Elements) != 3 || a.Elements[0] != in[0] || a.Elements[1] != in[1] || a.Elements[2] != 7 {
			t.Errorf("BIGINT array = %v, want %v", a.Elements, in)
		}
	})

	t.Run("smallint", func(t *testing.T) {
		call := echoProc(t, "ES", "S0", "SMALLINT")
		a := db2i.Array[int16]{Elements: []int16{100, -200, 32767}}
		if _, err := db.Exec(call, sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if len(a.Elements) != 3 || a.Elements[0] != 100 || a.Elements[1] != -200 || a.Elements[2] != 32767 {
			t.Errorf("SMALLINT array = %v, want [100 -200 32767]", a.Elements)
		}
	})

	t.Run("double", func(t *testing.T) {
		call := echoProc(t, "EF", "F0", "DOUBLE")
		a := db2i.Array[float64]{Elements: []float64{1.5, -2.25, 100.0}}
		if _, err := db.Exec(call, sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if len(a.Elements) != 3 || a.Elements[0] != 1.5 || a.Elements[1] != -2.25 || a.Elements[2] != 100.0 {
			t.Errorf("DOUBLE array = %v, want [1.5 -2.25 100]", a.Elements)
		}
	})

	t.Run("varbinary", func(t *testing.T) {
		call := echoProc(t, "EV", "V0", "VARBINARY(8)")
		in := [][]byte{{0x01, 0x02, 0x03}, {0xFF}, {}}
		a := db2i.Array[[]byte]{Elements: in}
		if _, err := db.Exec(call, sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if len(a.Elements) != 3 ||
			!bytesEq(a.Elements[0], in[0]) || !bytesEq(a.Elements[1], in[1]) || !bytesEq(a.Elements[2], in[2]) {
			t.Errorf("VARBINARY array = %v, want %v", a.Elements, in)
		}
	})

	t.Run("timestamp_unsupported", func(t *testing.T) {
		// Temporal array elements are NOT supported (#68 Item 3 gated
		// sub-item, validated 2026-06-07). The driver's time.Time ->
		// 26-char IBM-timestamp conversion is applied to scalar binds in
		// stmt.go, NOT to per-element array encoding, so a time.Time
		// element reaches the element encoder as a raw value and is
		// rejected ("cannot bind time.Time as VARCHAR (need string)").
		// Pin that current behaviour; flip to a positive round-trip if
		// temporal array elements ever land. Bind a string with the IBM
		// 26-char form if you need a temporal array element today.
		call := echoProc(t, "ET", "T0", "TIMESTAMP")
		a := db2i.Array[time.Time]{Elements: []time.Time{time.Date(2026, 6, 7, 12, 34, 56, 0, time.UTC)}}
		_, err := db.Exec(call, sql.Out{Dest: &a, In: true})
		if err == nil {
			t.Skip("temporal array elements now round-trip -- update this test to assert values")
		}
		if !contains(err.Error(), "time.Time") {
			t.Errorf("expected a time.Time element bind error, got: %v", err)
		}
	})

	t.Run("decimal", func(t *testing.T) {
		call := echoProc(t, "ED", "D0", "DECIMAL(7,2)")
		a := db2i.Array[string]{Elements: []string{"12.34", "-5.60", "0.00"}}
		if _, err := db.Exec(call, sql.Out{Dest: &a, In: true}); err != nil {
			t.Fatalf("CALL: %v", err)
		}
		if len(a.Elements) != 3 {
			t.Fatalf("got %d elements, want 3", len(a.Elements))
		}
		// Compare numerically (decode format may be "12.34" or padded).
		for i, want := range []string{"12.34", "-5.60", "0.00"} {
			if !decEq(a.Elements[i], want) {
				t.Errorf("DECIMAL element[%d] = %q, want %q", i, a.Elements[i], want)
			}
		}
	})
}
