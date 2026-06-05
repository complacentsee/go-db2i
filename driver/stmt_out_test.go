package driver

import (
	stdsql "database/sql"
	"math/big"
	"reflect"
	"testing"
	"time"
)

// TestOutBindShapeTypes covers the OUT/INOUT destination kinds added
// for issue #40 -- *[]byte, *time.Time, and the math/big decimal
// carriers (value and pointer forms) -- alongside the int/string
// regression cases. Each placeholder shape only needs the right
// direction byte and a non-zero FieldLength (the hostserver OUT-fixup
// overrides the rest from the proc's declared type); the IN value is
// what an INOUT slot ships, derived from the current *Dest.
func TestOutBindShapeTypes(t *testing.T) {
	ts := time.Date(2026, 5, 7, 14, 23, 45, 123456000, time.UTC)

	bytesIn := []byte{0x01, 0x02, 0x03}
	ratVal := new(big.Rat)
	ratVal.SetString("12.5")
	ratPtr := big.NewRat(5, 2) // 2.5
	intVal := new(big.Int)
	intVal.SetInt64(42)
	floatVal := new(big.Float)
	floatVal.SetFloat64(1.5)
	var nilRat *big.Rat

	cases := []struct {
		name      string
		dest      any
		in        bool
		direction byte
		wantDir   byte
		wantIn    any // expected IN value (nil = SQL NULL for OUT-only slots)
	}{
		{
			name:      "byte slice OUT-only binds NULL",
			dest:      &[]byte{},
			in:        false,
			direction: 0xF1,
			wantDir:   0xF1,
			wantIn:    nil, // OUT-only -> SQL NULL (server ignores the IN value)
		},
		{
			name:      "byte slice INOUT echoes bytes",
			dest:      &bytesIn,
			in:        true,
			direction: 0xF2,
			wantDir:   0xF2,
			wantIn:    []byte{0x01, 0x02, 0x03},
		},
		{
			name:      "time.Time OUT-only binds NULL",
			dest:      &time.Time{},
			in:        false,
			direction: 0xF1,
			wantDir:   0xF1,
			wantIn:    nil,
		},
		{
			name:      "time.Time INOUT formats 26-char",
			dest:      &ts,
			in:        true,
			direction: 0xF2,
			wantDir:   0xF2,
			wantIn:    "2026-05-07-14.23.45.123456",
		},
		{
			name:      "big.Rat value INOUT",
			dest:      ratVal, // *big.Rat (value form: Dest points at a big.Rat)
			in:        true,
			direction: 0xF2,
			wantDir:   0xF2,
			wantIn:    "12.5",
		},
		{
			name:      "big.Rat value OUT-only binds NULL",
			dest:      new(big.Rat),
			in:        false,
			direction: 0xF1,
			wantDir:   0xF1,
			wantIn:    nil,
		},
		{
			name:      "big.Rat pointer INOUT",
			dest:      &ratPtr, // **big.Rat (pointer form)
			in:        true,
			direction: 0xF2,
			wantDir:   0xF2,
			wantIn:    "2.5",
		},
		{
			name:      "big.Rat nil pointer OUT-only binds NULL",
			dest:      &nilRat,
			in:        false,
			direction: 0xF1,
			wantDir:   0xF1,
			wantIn:    nil,
		},
		{
			name:      "big.Int value INOUT",
			dest:      intVal,
			in:        true,
			direction: 0xF2,
			wantDir:   0xF2,
			wantIn:    "42",
		},
		{
			name:      "big.Float value INOUT",
			dest:      floatVal,
			in:        true,
			direction: 0xF2,
			wantDir:   0xF2,
			wantIn:    "1.5",
		},
		{
			name:      "int OUT-only binds NULL",
			dest:      new(int),
			in:        false,
			direction: 0xF1,
			wantDir:   0xF1,
			wantIn:    nil,
		},
		{
			name:      "string INOUT regression",
			dest:      func() *string { s := "hi"; return &s }(),
			in:        true,
			direction: 0xF2,
			wantDir:   0xF2,
			wantIn:    "hi",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := stdsql.Out{Dest: tc.dest, In: tc.in}
			shape, inValue, err := outBindShape(&out, 37, tc.direction)
			if err != nil {
				t.Fatalf("outBindShape: %v", err)
			}
			if shape.ParamType != tc.wantDir {
				t.Errorf("ParamType = 0x%02X, want 0x%02X", shape.ParamType, tc.wantDir)
			}
			if shape.FieldLength == 0 {
				t.Errorf("FieldLength = 0, want non-zero placeholder")
			}
			if !reflect.DeepEqual(inValue, tc.wantIn) {
				t.Errorf("IN value = %#v (%T), want %#v (%T)", inValue, inValue, tc.wantIn, tc.wantIn)
			}
		})
	}
}

// TestOutBindShapeByteCopyIsDefensive confirms the INOUT []byte IN
// value is a copy: mutating the caller's slice after the bind must not
// change what ships.
func TestOutBindShapeByteCopyIsDefensive(t *testing.T) {
	src := []byte{0x0A, 0x0B}
	out := stdsql.Out{Dest: &src, In: true}
	_, inValue, err := outBindShape(&out, 37, 0xF2)
	if err != nil {
		t.Fatalf("outBindShape: %v", err)
	}
	src[0] = 0xFF
	got, ok := inValue.([]byte)
	if !ok {
		t.Fatalf("IN value type = %T, want []byte", inValue)
	}
	if !reflect.DeepEqual(got, []byte{0x0A, 0x0B}) {
		t.Errorf("IN value = % X, want 0A 0B (copy should be unaffected by caller mutation)", got)
	}
}

// TestOutBindShapeRejectsUnsupported keeps the default-arm error firing
// for genuinely unsupported destination types.
func TestOutBindShapeRejectsUnsupported(t *testing.T) {
	cases := []any{
		new(struct{}),       // empty struct -- not time.Time / big.*
		new([]int),          // slice, but not []byte
		new(chan int),       // channel
		new(map[string]int), // map
	}
	for _, dest := range cases {
		out := stdsql.Out{Dest: dest}
		if _, _, err := outBindShape(&out, 37, 0xF1); err == nil {
			t.Errorf("outBindShape(%T) = nil error, want unsupported-type error", dest)
		}
	}
}

// TestOutBindShapeTypedNilDest confirms a typed-nil pointer dest is
// rejected with a clean error rather than panicking on reflect's zero
// Value (regression for the concrete-type prelude).
func TestOutBindShapeTypedNilDest(t *testing.T) {
	cases := []any{
		(*[]byte)(nil),
		(*time.Time)(nil),
		(*big.Rat)(nil),
		(*big.Int)(nil),
		(*string)(nil),
	}
	for _, dest := range cases {
		out := stdsql.Out{Dest: dest}
		_, _, err := outBindShape(&out, 37, 0xF1)
		if err == nil {
			t.Errorf("outBindShape(typed-nil %T) = nil error, want error (must not panic)", dest)
		}
	}
}

// TestOutBindShapeOutOnlyIgnoresUnrepresentable confirms an OUT-only
// big.Rat dest holding a non-terminating value still binds (the server
// ignores the IN value of an OUT slot, so it ships SQL NULL), while the
// same value as INOUT surfaces the rendering error.
func TestOutBindShapeOutOnlyIgnoresUnrepresentable(t *testing.T) {
	oneThird := big.NewRat(1, 3)

	// OUT-only: must succeed, binding SQL NULL (nil) -- the dest value
	// is never rendered, so the non-terminating rational is irrelevant.
	outOnly := stdsql.Out{Dest: oneThird, In: false}
	_, inValue, err := outBindShape(&outOnly, 37, 0xF1)
	if err != nil {
		t.Fatalf("OUT-only non-terminating big.Rat: unexpected error %v", err)
	}
	if inValue != nil {
		t.Errorf("OUT-only IN value = %#v, want nil (SQL NULL)", inValue)
	}

	// INOUT: the IN value matters, so the non-terminating rational errors.
	inOut := stdsql.Out{Dest: oneThird, In: true}
	if _, _, err := outBindShape(&inOut, 37, 0xF2); err == nil {
		t.Errorf("INOUT non-terminating big.Rat = nil error, want rendering error")
	}
}

// TestAssignOutParam drives the value coercion from a server-decoded
// value (+ the parallel post-fixup SQL type) into each supported
// destination Kind/type.
func TestAssignOutParam(t *testing.T) {
	cases := []struct {
		name    string
		newDest func() any // returns a pointer to the destination variable
		v       any
		sqlType uint16
		wantErr bool
		check   func(t *testing.T, dest any)
	}{
		{
			name:    "byte slice from bytes",
			newDest: func() any { return new([]byte) },
			v:       []byte{0x01, 0x02},
			check: func(t *testing.T, dest any) {
				if got := *dest.(*[]byte); !reflect.DeepEqual(got, []byte{0x01, 0x02}) {
					t.Errorf("got % X, want 01 02", got)
				}
			},
		},
		{
			name:    "byte slice from string (FOR BIT DATA)",
			newDest: func() any { return new([]byte) },
			v:       "abc",
			check: func(t *testing.T, dest any) {
				if got := *dest.(*[]byte); string(got) != "abc" {
					t.Errorf("got %q, want abc", got)
				}
			},
		},
		{
			name:    "time.Time from DATE iso",
			newDest: func() any { return new(time.Time) },
			v:       "2026-05-07",
			sqlType: 384,
			check: func(t *testing.T, dest any) {
				want := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
				if got := *dest.(*time.Time); !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:    "time.Time from TIME iso",
			newDest: func() any { return new(time.Time) },
			v:       "14:23:45",
			sqlType: 388,
			check: func(t *testing.T, dest any) {
				want := time.Date(0, 1, 1, 14, 23, 45, 0, time.UTC)
				if got := *dest.(*time.Time); !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:    "time.Time from TIMESTAMP iso",
			newDest: func() any { return new(time.Time) },
			v:       "2026-05-07T14:23:45.123456",
			sqlType: 392,
			check: func(t *testing.T, dest any) {
				want := time.Date(2026, 5, 7, 14, 23, 45, 123456000, time.UTC)
				if got := *dest.(*time.Time); !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			},
		},
		{
			name:    "big.Rat value from fractional decimal",
			newDest: func() any { return new(big.Rat) },
			v:       "12.50",
			sqlType: 485,
			check: func(t *testing.T, dest any) {
				want := big.NewRat(25, 2)
				if got := dest.(*big.Rat); got.Cmp(want) != 0 {
					t.Errorf("got %s, want %s", got.RatString(), want.RatString())
				}
			},
		},
		{
			name: "big.Rat pointer (nil) allocated",
			newDest: func() any {
				var p *big.Rat
				return &p
			},
			v:       "2.5",
			sqlType: 485,
			check: func(t *testing.T, dest any) {
				p := *dest.(**big.Rat)
				if p == nil {
					t.Fatalf("pointer not allocated")
				}
				if p.Cmp(big.NewRat(5, 2)) != 0 {
					t.Errorf("got %s, want 5/2", p.RatString())
				}
			},
		},
		{
			name:    "big.Int value from integer decimal",
			newDest: func() any { return new(big.Int) },
			v:       "42",
			sqlType: 485,
			check: func(t *testing.T, dest any) {
				if got := dest.(*big.Int); got.Int64() != 42 {
					t.Errorf("got %s, want 42", got.String())
				}
			},
		},
		{
			name:    "big.Int rejects fractional decimal",
			newDest: func() any { return new(big.Int) },
			v:       "1.5",
			sqlType: 485,
			wantErr: true,
		},
		{
			name:    "big.Float value from decimal",
			newDest: func() any { return new(big.Float) },
			v:       "3.25",
			sqlType: 485,
			check: func(t *testing.T, dest any) {
				got, _ := dest.(*big.Float).Float64()
				if got != 3.25 {
					t.Errorf("got %v, want 3.25", got)
				}
			},
		},
		{
			name:    "string from string regression",
			newDest: func() any { return new(string) },
			v:       "hello",
			check: func(t *testing.T, dest any) {
				if got := *dest.(*string); got != "hello" {
					t.Errorf("got %q, want hello", got)
				}
			},
		},
		{
			name:    "int from int32 regression",
			newDest: func() any { return new(int) },
			v:       int32(7),
			check: func(t *testing.T, dest any) {
				if got := *dest.(*int); got != 7 {
					t.Errorf("got %d, want 7", got)
				}
			},
		},
		{
			name:    "int8 overflow errors",
			newDest: func() any { return new(int8) },
			v:       int64(9999),
			wantErr: true,
		},
		{
			name:    "float64 from float64 regression",
			newDest: func() any { return new(float64) },
			v:       float64(1.5),
			check: func(t *testing.T, dest any) {
				if got := *dest.(*float64); got != 1.5 {
					t.Errorf("got %v, want 1.5", got)
				}
			},
		},
		{
			name:    "bool from int32 regression",
			newDest: func() any { return new(bool) },
			v:       int32(1),
			check: func(t *testing.T, dest any) {
				if got := *dest.(*bool); !got {
					t.Errorf("got false, want true")
				}
			},
		},
		{
			name:    "byte slice rejects non-bytes",
			newDest: func() any { return new([]byte) },
			v:       int32(5),
			wantErr: true,
		},
		{
			name:    "time.Time rejects non-string",
			newDest: func() any { return new(time.Time) },
			v:       []byte{0x01},
			sqlType: 392,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			destPtr := tc.newDest()
			dest := reflect.ValueOf(destPtr).Elem()
			err := assignOutParam(dest, tc.v, tc.sqlType)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("assignOutParam = nil error, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("assignOutParam: %v", err)
			}
			if tc.check != nil {
				tc.check(t, destPtr)
			}
		})
	}
}

// TestWriteBackOutParamsNullAndTypes exercises the full write-back
// loop: a SQL NULL (nil value) zeroes the destination, and a temporal
// value is parsed with the threaded post-fixup SQL type. IN-only slots
// (nil dest entries) are skipped.
func TestWriteBackOutParamsNullAndTypes(t *testing.T) {
	var dateDest time.Time
	preset := []byte{0x09, 0x09} // should be zeroed by the NULL slot
	bytesDest := preset

	outDests := []*stdsql.Out{
		nil, // IN-only slot 0, skipped
		{Dest: &dateDest},
		{Dest: &bytesDest},
	}
	outValues := []any{
		"echoed-in-value", // slot 0 ignored (nil dest)
		"2026-05-07",      // slot 1 DATE
		nil,               // slot 2 SQL NULL -> zero
	}
	outTypes := []uint16{449, 384, 909}

	if err := writeBackOutParams(outDests, outValues, outTypes); err != nil {
		t.Fatalf("writeBackOutParams: %v", err)
	}

	wantDate := time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)
	if !dateDest.Equal(wantDate) {
		t.Errorf("date dest = %v, want %v", dateDest, wantDate)
	}
	if bytesDest != nil {
		t.Errorf("byte dest = % X, want nil (SQL NULL -> zero value)", bytesDest)
	}
}
