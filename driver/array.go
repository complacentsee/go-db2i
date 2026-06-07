package driver

import (
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/complacentsee/go-db2i/hostserver"
)

// Array is the bind/scan carrier for a DB2 for i stored-procedure ARRAY
// parameter (issue #68). On DB2 for i an ARRAY crosses the host-server
// wire only as a procedure parameter (IN/OUT/INOUT), never a result-set
// column (#39 / SQL-20441). Use it with a CALL:
//
//	IN:    db.Exec("CALL P(?)", db2i.Array[int32]{Elements: []int32{1, 2, 3}})
//	OUT:   var a db2i.Array[int32]
//	       db.Exec("CALL P(?)", sql.Out{Dest: &a})
//	INOUT: db.Exec("CALL P(?)", sql.Out{Dest: &a, In: true})
//
// For per-element NULLs use a pointer element type; a nil pointer element
// is sent (and decoded back) as a SQL NULL element:
//
//	db2i.Array[*int32]{Elements: []*int32{&x, nil, &z}} // element 1 is NULL
//
// With a non-pointer element type a decoded NULL element becomes the zero
// value of T (a NULL INTEGER scans as 0, indistinguishable from a real 0).
// That is deliberate: it matches how this driver writes a non-pointer
// scalar OUT parameter, and how database/sql resolves a SQL NULL scanned
// into a non-pointer destination. Use a pointer element type whenever you
// must tell a NULL element apart from the zero value.
//
// Set Null for a whole-array SQL NULL, which is distinct from an empty
// Elements (an array with zero elements). The element type T must reduce
// to one of the value kinds the scalar bind path supports: the signed
// integers, float32/float64, string, []byte, and the math/big decimal
// carriers (via their string form). These cover SMALLINT/INTEGER/BIGINT,
// REAL/DOUBLE, DECIMAL/NUMERIC/DECFLOAT, CHAR/VARCHAR, and BINARY/VARBINARY
// elements. Temporal elements (DATE/TIME/TIMESTAMP via time.Time) are NOT
// supported -- bind a string in the IBM 26-char timestamp form instead.
// Element types are taken from the procedure's declared ARRAY element type
// (the server's PREPARE_DESCRIBE), not from T, so T only needs to be
// assignable to/from that element type.
type Array[T any] struct {
	Elements []T
	Null     bool
}

// arrayBinder is the non-generic view the IN bind path uses to read an
// Array[T] without knowing T. Both Array[T] (value) and *Array[T]
// implement it.
type arrayBinder interface {
	db2iArrayElements() (hostserver.ArrayValue, error)
}

// arrayScanner is the non-generic view the OUT write-back path uses to
// store decoded elements into an *Array[T]. Only *Array[T] implements it
// (pointer receiver), so a non-pointer OUT destination is rejected.
type arrayScanner interface {
	db2iArrayScan(av hostserver.ArrayValue, elemSQLType uint16) error
}

// db2iArrayElements converts the bound array into the hostserver wire
// carrier, applying database/sql's default per-element conversion so a
// pointer element (nil -> SQL NULL), an integer width, a Stringer-based
// decimal, etc. all reduce to a driver value the scalar element encoder
// accepts.
func (a Array[T]) db2iArrayElements() (hostserver.ArrayValue, error) {
	if a.Null {
		return hostserver.ArrayValue{Null: true}, nil
	}
	out := make([]any, len(a.Elements))
	for i := range a.Elements {
		v, err := driver.DefaultParameterConverter.ConvertValue(a.Elements[i])
		if err != nil {
			return hostserver.ArrayValue{}, fmt.Errorf("array element %d: %w", i, err)
		}
		out[i] = v
	}
	return hostserver.ArrayValue{Elements: out}, nil
}

// db2iArrayScan stores the decoded OUT/INOUT array into the *Array[T].
// elemSQLType is the post-fixup element SQL type (so a temporal element's
// ISO string is re-parsed to the right kind), shared across the array.
// A NULL element assigns the zero value of T (use a pointer element type
// to observe per-element NULLs).
func (a *Array[T]) db2iArrayScan(av hostserver.ArrayValue, elemSQLType uint16) error {
	if av.Null {
		a.Null = true
		a.Elements = nil
		return nil
	}
	a.Null = false
	out := make([]T, len(av.Elements))
	for i := range av.Elements {
		dst := reflect.ValueOf(&out[i]).Elem()
		if av.Elements[i] == nil {
			dst.SetZero() // pointer element -> nil; value element -> zero
			continue
		}
		// A pointer element type (Array[*int32], for per-element NULLs)
		// needs a fresh pointee to assign into; assignOutParam targets a
		// scalar/concrete kind, not a pointer.
		target := dst
		if dst.Kind() == reflect.Pointer {
			p := reflect.New(dst.Type().Elem())
			dst.Set(p)
			target = p.Elem()
		}
		if err := assignOutParam(target, av.Elements[i], elemSQLType); err != nil {
			return fmt.Errorf("array element %d: %w", i, err)
		}
	}
	a.Elements = out
	return nil
}

// anyArrayShape reports whether any reconciled bind shape is an array,
// which both selects the CP 0x382F EXECUTE path in hostserver and
// diverts the CALL off the extended-dynamic package fast path (array
// cache-hit behaviour is not yet captured -- mirrors the temporal-OUT
// divert).
func anyArrayShape(shapes []hostserver.PreparedParam) bool {
	for i := range shapes {
		if shapes[i].IsArray {
			return true
		}
	}
	return false
}
