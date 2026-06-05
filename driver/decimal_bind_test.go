package driver

import (
	stdsql "database/sql"
	"database/sql/driver"
	"math/big"
	"testing"
	"time"
)

// structDecimal is a struct-shaped decimal type that implements only
// fmt.Stringer (NOT driver.Valuer). database/sql's default converter rejects
// it ("unsupported type"), so it is exactly the gap the exact-decimal funnel
// fills. A string-kind decimal type (type T string) already converts via the
// default converter's String-kind path, and a driver.Valuer decimal already
// converts via Value(); neither needs (or gets) the funnel.
type structDecimal struct{ s string }

func (d structDecimal) String() string { return d.s }

// valuerStringer implements BOTH driver.Valuer and fmt.Stringer, with a
// Value() that intentionally differs from String(). The funnel must decline
// it so the established Valuer contract is never preempted.
type valuerStringer struct{}

func (valuerStringer) String() string               { return "111.11" }
func (valuerStringer) Value() (driver.Value, error) { return "from-valuer", nil }

// stringerEnum is a named int with a String() label. The funnel must leave it
// to the default converter (which binds it as int64), never read its label as
// a decimal.
type stringerEnum int

func (stringerEnum) String() string { return "MONDAY" }

func TestCanonicalDecimalString(t *testing.T) {
	cases := []struct {
		name    string
		in      any
		wantStr string
		wantOK  bool
		wantErr bool
	}{
		// *big.Rat: exact-or-error.
		{"rat_3_2", big.NewRat(3, 2), "1.5", true, false},
		{"rat_1_4", big.NewRat(1, 4), "0.25", true, false},
		{"rat_1_8", big.NewRat(1, 8), "0.125", true, false},
		{"rat_neg_3_2", big.NewRat(-3, 2), "-1.5", true, false},
		{"rat_6_5", big.NewRat(6, 5), "1.2", true, false},
		{"rat_int_7", big.NewRat(7, 1), "7", true, false},
		{"rat_zero", big.NewRat(0, 1), "0", true, false},
		{"rat_reduces_10_4", big.NewRat(10, 4), "2.5", true, false}, // 10/4 -> 5/2
		{"rat_nonterminating_1_3", big.NewRat(1, 3), "", false, true},
		{"rat_nonterminating_2_7", big.NewRat(2, 7), "", false, true},
		// *big.Int.
		{"int_pos", big.NewInt(12345), "12345", true, false},
		{"int_neg", big.NewInt(-98765), "-98765", true, false},
		// *big.Float (exact binary fractions render exactly via *big.Rat).
		{"float_1_5", big.NewFloat(1.5), "1.5", true, false},
		{"float_0_25", big.NewFloat(0.25), "0.25", true, false},
		{"float_neg_2_75", big.NewFloat(-2.75), "-2.75", true, false},
		{"float_zero", big.NewFloat(0), "0", true, false},
		// Struct Stringer decimal: plain + scientific + sign + whitespace.
		{"stringer_plain", structDecimal{"123.45"}, "123.45", true, false},
		{"stringer_leading_plus", structDecimal{"+12.34"}, "12.34", true, false},
		{"stringer_neg_frac", structDecimal{"-0.0007"}, "-0.0007", true, false},
		{"stringer_sci_upper", structDecimal{"1.5E+5"}, "150000", true, false},
		{"stringer_sci_lower", structDecimal{"2.5e-3"}, "0.0025", true, false},
		{"stringer_whitespace", structDecimal{"  12.5  "}, "12.5", true, false},
		{"stringer_highprec", structDecimal{"3.14159265358979323846264338327"}, "3.14159265358979323846264338327", true, false},
		{"stringer_fraction_rejected", structDecimal{"3/2"}, "", false, true},
		{"stringer_comma_rejected", structDecimal{"1,5"}, "", false, true},
		{"stringer_garbage_rejected", structDecimal{"abc"}, "", false, true},
		{"stringer_empty_rejected", structDecimal{""}, "", false, true},
		// Declined (ok=false, no error): types the default converter handles.
		{"int64_declined", int64(42), "", false, false},
		{"float64_declined", float64(1.5), "", false, false},
		{"string_declined", "123.45", "", false, false},
		{"bool_declined", true, "", false, false},
		{"bytes_declined", []byte{1, 2}, "", false, false},
		{"nil_declined", nil, "", false, false},
		{"time_declined", time.Unix(0, 0), "", false, false},
		{"valuer_declined", valuerStringer{}, "", false, false},
		{"stringer_enum_declined", stringerEnum(1), "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := canonicalDecimalString(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("canonicalDecimalString(%v) = (%q,%v,nil), want error", tc.in, got, ok)
				}
				return
			}
			if err != nil {
				t.Fatalf("canonicalDecimalString(%v): unexpected error %v", tc.in, err)
			}
			if ok != tc.wantOK {
				t.Fatalf("canonicalDecimalString(%v) ok = %v, want %v", tc.in, ok, tc.wantOK)
			}
			if ok && got != tc.wantStr {
				t.Errorf("canonicalDecimalString(%v) = %q, want %q", tc.in, got, tc.wantStr)
			}
		})
	}
}

// TestCheckNamedValueAdmitsDecimalTypes asserts the boundary contract: an
// admitted exact-decimal type is converted and substituted into nv.Value as a
// canonical string (return nil), a non-terminating *big.Rat surfaces an error
// (not ErrSkip), and every type the default converter already handles --
// crucially time.Time and a driver.Valuer -- is declined with ErrSkip and left
// byte-identical.
func TestCheckNamedValueAdmitsDecimalTypes(t *testing.T) {
	t.Run("stringer_substituted", func(t *testing.T) {
		nv := &driver.NamedValue{Ordinal: 1, Value: structDecimal{"123.45"}}
		if err := checkNamedValue(nv); err != nil {
			t.Fatalf("checkNamedValue: %v", err)
		}
		if s, ok := nv.Value.(string); !ok || s != "123.45" {
			t.Fatalf("nv.Value = %#v, want string %q", nv.Value, "123.45")
		}
	})

	t.Run("big_rat_substituted", func(t *testing.T) {
		nv := &driver.NamedValue{Ordinal: 1, Value: big.NewRat(3, 2)}
		if err := checkNamedValue(nv); err != nil {
			t.Fatalf("checkNamedValue: %v", err)
		}
		if s, ok := nv.Value.(string); !ok || s != "1.5" {
			t.Fatalf("nv.Value = %#v, want string %q", nv.Value, "1.5")
		}
	})

	t.Run("big_rat_nonterminating_errors", func(t *testing.T) {
		nv := &driver.NamedValue{Ordinal: 1, Value: big.NewRat(1, 3)}
		err := checkNamedValue(nv)
		if err == nil {
			t.Fatalf("checkNamedValue(1/3) = nil, want error")
		}
		if err == driver.ErrSkip {
			t.Fatalf("checkNamedValue(1/3) = ErrSkip, want a descriptive error")
		}
	})

	// Declined types must return ErrSkip and leave the value untouched. The
	// time.Time and valuer cases are the regression guards against the
	// Stringer arm hijacking them.
	declined := []struct {
		name string
		val  any
	}{
		{"time", time.Now()},
		{"valuer_stringer", valuerStringer{}},
		{"stringer_enum", stringerEnum(2)},
		{"int64", int64(7)},
		{"string", "9.99"},
		{"float64", float64(2.5)},
		{"bool", true},
		{"bytes", []byte("xy")},
	}
	for _, tc := range declined {
		t.Run("declined_"+tc.name, func(t *testing.T) {
			nv := &driver.NamedValue{Ordinal: 1, Value: tc.val}
			if err := checkNamedValue(nv); err != driver.ErrSkip {
				t.Fatalf("checkNamedValue(%T) = %v, want driver.ErrSkip", tc.val, err)
			}
		})
	}
}

// TestCheckNamedValueStillAdmitsLOBAndOut guards the pre-existing custom-type
// admissions next to the new decimal arm.
func TestCheckNamedValueStillAdmitsLOBAndOut(t *testing.T) {
	var dest string
	cases := []struct {
		name string
		val  any
	}{
		{"lob", &LOBValue{Bytes: []byte("x")}},
		{"out", stdsql.Out{Dest: &dest}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nv := &driver.NamedValue{Ordinal: 1, Value: tc.val}
			if err := checkNamedValue(nv); err != nil {
				t.Fatalf("checkNamedValue(%s) = %v, want nil", tc.name, err)
			}
		})
	}
}

// TestExactDecimalRidesVarcharBindShape proves the cache-miss wire contract:
// after the funnel substitutes a canonical string, the value binds as VARCHAR
// (SQLType 449) carrying that string verbatim -- the precision-preserving path
// the server casts to the decimal column. (On cache-hit the same string is
// repacked as native BCD from the *PGM parameter-marker format; see
// hostserver TestEncodeCachedFloat64Decimal.)
func TestExactDecimalRidesVarcharBindShape(t *testing.T) {
	nv := &driver.NamedValue{Ordinal: 1, Value: big.NewRat(3, 2)}
	if err := checkNamedValue(nv); err != nil {
		t.Fatalf("checkNamedValue: %v", err)
	}
	shapes, values, _, err := bindArgsToPreparedParams([]driver.Value{nv.Value}, 1208)
	if err != nil {
		t.Fatalf("bindArgsToPreparedParams: %v", err)
	}
	if len(shapes) != 1 || shapes[0].SQLType != 449 {
		t.Fatalf("shape = %+v, want one VARCHAR(449)", shapes)
	}
	if s, ok := values[0].(string); !ok || s != "1.5" {
		t.Fatalf("bound value = %#v, want string %q", values[0], "1.5")
	}
	// FieldLength = 2-byte SL prefix + payload "1.5" (3 bytes) = 5.
	if shapes[0].FieldLength != 5 {
		t.Errorf("FieldLength = %d, want 5 (2 + len(%q))", shapes[0].FieldLength, "1.5")
	}
}
