package driver

import (
	"testing"

	"github.com/complacentsee/go-db2i/hostserver"
)

// TestArrayElementsConvert checks db2i.Array[T].db2iArrayElements reduces
// each element to a driver value via the default converter, handling
// integer widening, pointer-as-nullable, and the whole-null flag.
func TestArrayElementsConvert(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		av, err := Array[int32]{Elements: []int32{10, 20, 30}}.db2iArrayElements()
		if err != nil {
			t.Fatalf("db2iArrayElements: %v", err)
		}
		if av.Null || len(av.Elements) != 3 {
			t.Fatalf("got Null=%v len=%d, want 3 elements", av.Null, len(av.Elements))
		}
		for i, want := range []int64{10, 20, 30} {
			if got, ok := av.Elements[i].(int64); !ok || got != want {
				t.Errorf("element %d = %v (%T), want int64 %d", i, av.Elements[i], av.Elements[i], want)
			}
		}
	})

	t.Run("pointer_nullable", func(t *testing.T) {
		x := int32(7)
		av, err := Array[*int32]{Elements: []*int32{&x, nil}}.db2iArrayElements()
		if err != nil {
			t.Fatalf("db2iArrayElements: %v", err)
		}
		if len(av.Elements) != 2 {
			t.Fatalf("len = %d, want 2", len(av.Elements))
		}
		if got, ok := av.Elements[0].(int64); !ok || got != 7 {
			t.Errorf("element 0 = %v, want int64 7", av.Elements[0])
		}
		if av.Elements[1] != nil {
			t.Errorf("element 1 = %v, want nil (NULL element)", av.Elements[1])
		}
	})

	t.Run("whole_null", func(t *testing.T) {
		av, err := Array[int32]{Null: true}.db2iArrayElements()
		if err != nil {
			t.Fatalf("db2iArrayElements: %v", err)
		}
		if !av.Null {
			t.Errorf("Null = false, want true")
		}
	})

	t.Run("string", func(t *testing.T) {
		av, err := Array[string]{Elements: []string{"AB", "CDE"}}.db2iArrayElements()
		if err != nil {
			t.Fatalf("db2iArrayElements: %v", err)
		}
		if av.Elements[0] != "AB" || av.Elements[1] != "CDE" {
			t.Errorf("elements = %v, want [AB CDE]", av.Elements)
		}
	})
}

// TestArrayScan checks *db2i.Array[T].db2iArrayScan stores decoded
// elements (including NULL elements via a pointer element type) and the
// whole-null flag.
func TestArrayScan(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		var a Array[int32]
		if err := a.db2iArrayScan(hostserver.ArrayValue{Elements: []any{int32(11), int32(22), int32(33)}}, 497); err != nil {
			t.Fatalf("db2iArrayScan: %v", err)
		}
		if a.Null || len(a.Elements) != 3 || a.Elements[0] != 11 || a.Elements[2] != 33 {
			t.Errorf("scanned = %+v, want {[11 22 33] false}", a)
		}
	})

	t.Run("null_element_pointer", func(t *testing.T) {
		var a Array[*int32]
		if err := a.db2iArrayScan(hostserver.ArrayValue{Elements: []any{int32(1), nil, int32(3)}}, 497); err != nil {
			t.Fatalf("db2iArrayScan: %v", err)
		}
		if len(a.Elements) != 3 {
			t.Fatalf("len = %d, want 3", len(a.Elements))
		}
		if a.Elements[0] == nil || *a.Elements[0] != 1 {
			t.Errorf("element 0 = %v, want 1", a.Elements[0])
		}
		if a.Elements[1] != nil {
			t.Errorf("element 1 = %v, want nil", a.Elements[1])
		}
		if a.Elements[2] == nil || *a.Elements[2] != 3 {
			t.Errorf("element 2 = %v, want 3", a.Elements[2])
		}
	})

	t.Run("whole_null", func(t *testing.T) {
		a := Array[int32]{Elements: []int32{1, 2}} // pre-populated; scan must clear
		if err := a.db2iArrayScan(hostserver.ArrayValue{Null: true}, 497); err != nil {
			t.Fatalf("db2iArrayScan: %v", err)
		}
		if !a.Null || a.Elements != nil {
			t.Errorf("scanned = %+v, want {nil true}", a)
		}
	})

	t.Run("string", func(t *testing.T) {
		var a Array[string]
		if err := a.db2iArrayScan(hostserver.ArrayValue{Elements: []any{"XX", "YYY"}}, 449); err != nil {
			t.Fatalf("db2iArrayScan: %v", err)
		}
		if len(a.Elements) != 2 || a.Elements[0] != "XX" || a.Elements[1] != "YYY" {
			t.Errorf("scanned = %v, want [XX YYY]", a.Elements)
		}
	})
}
