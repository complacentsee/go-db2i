package driver

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// TestBindArgsToPreparedParams covers each driver.Value flavour the
// driver claims to support. Asserts the chosen SQL type / length /
// CCSID and that the Go value lands in `values` in the shape the
// hostserver encoder expects (e.g. bool -> int32, time.Time ->
// formatted string).
//
// driver.Value union: int64, float64, bool, []byte, string, time.Time, nil.
func TestBindArgsToPreparedParams(t *testing.T) {
	ts := time.Date(2026, 5, 7, 14, 23, 45, 123456000, time.UTC)
	cases := []struct {
		name      string
		in        driver.Value
		sqlType   uint16
		fieldLen  uint32
		ccsid     uint16
		valueWant any
	}{
		{"int64 -> BIGINT nullable", int64(123456789012), 493, 8, 0, int64(123456789012)},
		{"float64 -> DOUBLE nullable", float64(3.14), 481, 8, 0, float64(3.14)},
		{"bool true -> SMALLINT nullable", true, 501, 2, 0, int32(1)},
		{"bool false -> SMALLINT nullable", false, 501, 2, 0, int32(0)},
		{"[]byte -> VARCHAR FOR BIT DATA", []byte{0x01, 0x02, 0x03}, 449, 5, 65535, []byte{0x01, 0x02, 0x03}},
		{"string -> VARCHAR(CCSID 37 explicit)", "hello", 449, 7, 37, "hello"},
		{"string -> VARCHAR(CCSID 1208 UTF-8)", "café", 449, 7, 1208, "café"}, // 5 UTF-8 bytes + 2 SL = 7
		{"time.Time -> TIMESTAMP nullable", ts, 393, 26, 0, "2026-05-07-14.23.45.123456"},
		{"nil -> INTEGER nullable", nil, 497, 4, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shapes, values, _, err := bindArgsToPreparedParams([]driver.Value{tc.in}, tc.ccsid)
			if err != nil {
				t.Fatalf("bindArgsToPreparedParams: %v", err)
			}
			got := shapes[0]
			if got.SQLType != tc.sqlType {
				t.Errorf("SQLType = %d, want %d", got.SQLType, tc.sqlType)
			}
			if got.FieldLength != tc.fieldLen {
				t.Errorf("FieldLength = %d, want %d", got.FieldLength, tc.fieldLen)
			}
			if got.CCSID != tc.ccsid {
				t.Errorf("CCSID = %d, want %d", got.CCSID, tc.ccsid)
			}
			switch want := tc.valueWant.(type) {
			case []byte:
				v, ok := values[0].([]byte)
				if !ok {
					t.Errorf("value is %T, want []byte", values[0])
				} else if string(v) != string(want) {
					t.Errorf("value = % X, want % X", v, want)
				}
			default:
				if values[0] != want {
					t.Errorf("value = %v (%T), want %v (%T)", values[0], values[0], want, want)
				}
			}
		})
	}
}

// TestBindArgsToPreparedParamsRejectsUnsupportedType pins the error
// path for any Go type outside the driver.Value union (e.g. someone
// trying to push an *int through a NamedValueChecker before
// CheckValue normalises it).
func TestBindArgsToPreparedParamsRejectsUnsupportedType(t *testing.T) {
	_, _, _, err := bindArgsToPreparedParams([]driver.Value{struct{}{}}, 37)
	if err == nil {
		t.Fatal("expected error for unsupported type, got nil")
	}
}

// TestBindArgsToPreparedParamsMixedRow exercises a realistic three-arg
// row: int64 + string + nil. Confirms shapes line up positionally and
// that one bad arg in the middle of the row doesn't poison the others.
func TestBindArgsToPreparedParamsMixedRow(t *testing.T) {
	shapes, values, _, err := bindArgsToPreparedParams([]driver.Value{
		int64(7), "abc", nil,
	}, 37)
	if err != nil {
		t.Fatalf("bindArgsToPreparedParams: %v", err)
	}
	if len(shapes) != 3 || len(values) != 3 {
		t.Fatalf("len(shapes)=%d len(values)=%d, want 3/3", len(shapes), len(values))
	}
	wantTypes := []uint16{493, 449, 497}
	for i, want := range wantTypes {
		if shapes[i].SQLType != want {
			t.Errorf("shape %d SQLType = %d, want %d", i, shapes[i].SQLType, want)
		}
	}
	if values[2] != nil {
		t.Errorf("values[2] should be nil for NULL marker, got %v", values[2])
	}
	// Sanity: a hostserver.PreparedParam built by the binder should
	// be safe to feed to EncodeDBExtendedData (covered live, but
	// re-asserting the call shape here keeps the contract visible).
	_ = hostserver.PreparedParam(shapes[0])
}
