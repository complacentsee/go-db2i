package hostserver

import (
	"testing"
)

// TestReconcileBindShapesFromPMFFieldHandling locks the per-arm field intent
// directly on the dispatcher output for the load-bearing fields. Each sub-test
// builds shapes/values/pmf, calls reconcileBindShapesFromPMF, and asserts the
// specific fields the arm is responsible for (SQLType / CCSID / Precision /
// Scale / FieldLength / ParamType / DateFormat / Value / expectOutput). The
// former per-type-isolated reconciles this dispatcher replaced live in git
// history only; these assertions are now the sole offline guard.
func TestReconcileBindShapesFromPMFFieldHandling(t *testing.T) {
	const df = byte(0x07)

	t.Run("datetime_forces_precision_scale_zero", func(t *testing.T) {
		shapes := []PreparedParam{{SQLType: 393, FieldLength: 26, Precision: 6, Scale: 6, DateFormat: df}}
		reconcileBindShapesFromPMF(shapes, []any{"2026-06-05-00.00.00.000000"},
			[]ParameterMarkerField{{SQLType: 384, FieldLength: 10, Precision: 6, Scale: 6, CCSID: 37}})
		got := shapes[0]
		if got.SQLType != 384 || got.FieldLength != 10 || got.Precision != 0 || got.Scale != 0 || got.DateFormat != 0 {
			t.Errorf("datetime adopt = %+v; want SQLType 384, FieldLength 10, Precision/Scale/DateFormat 0", got)
		}
	})

	t.Run("null_preserves_dateformat", func(t *testing.T) {
		shapes := []PreparedParam{{SQLType: 497, FieldLength: 4, DateFormat: df}}
		reconcileBindShapesFromPMF(shapes, []any{nil},
			[]ParameterMarkerField{{SQLType: 909, FieldLength: 34, CCSID: ccsidBinary}})
		if got := shapes[0]; got.SQLType != 909 || got.DateFormat != df {
			t.Errorf("null adopt = %+v; want SQLType 909, DateFormat 0x%02X preserved", got, df)
		}
	})

	t.Run("out_inplace_preserves_value_and_dateformat", func(t *testing.T) {
		val := []byte{0xAB}
		shapes := []PreparedParam{{SQLType: 449, FieldLength: 2000, ParamType: 0xF1, Value: val, DateFormat: df}}
		eo := reconcileBindShapesFromPMF(shapes, []any{nil},
			[]ParameterMarkerField{{SQLType: 384, FieldLength: 10, CCSID: 37}})
		got := shapes[0]
		if !eo {
			t.Error("expectOutput = false; want true for an OUT slot")
		}
		if got.SQLType != 384 || got.FieldLength != 10 || got.ParamType != 0xF1 || got.DateFormat != df {
			t.Errorf("out adopt = %+v; want SQLType 384, FieldLength 10, ParamType 0xF1, DateFormat preserved", got)
		}
		if gotVal, ok := got.Value.([]byte); !ok || len(gotVal) != 1 || gotVal[0] != 0xAB {
			t.Errorf("out Value = %v; want preserved []byte{0xAB}", got.Value)
		}
	})

	t.Run("binary_copies_pmf_shape", func(t *testing.T) {
		shapes := []PreparedParam{{SQLType: 449, FieldLength: 2, CCSID: ccsidBinary}}
		reconcileBindShapesFromPMF(shapes, []any{[]byte{0x01}},
			[]ParameterMarkerField{{SQLType: 908, FieldLength: 34, CCSID: ccsidBinary}})
		if got := shapes[0]; got.SQLType != 908 || got.FieldLength != 34 || got.CCSID != ccsidBinary {
			t.Errorf("binary adopt = %+v; want SQLType 908, FieldLength 34, CCSID 65535", got)
		}
	})

	t.Run("graphic_bytes_adopt", func(t *testing.T) {
		// []byte into a CCSID-65535 VARGRAPHIC (464) adopts the graphic shape.
		shapes := []PreparedParam{{SQLType: 449, FieldLength: 4, CCSID: ccsidBinary}}
		reconcileBindShapesFromPMF(shapes, []any{[]byte{0x00, 0x41}},
			[]ParameterMarkerField{{SQLType: 464, FieldLength: 40, CCSID: ccsidBinary}})
		if got := shapes[0]; got.SQLType != 464 || got.FieldLength != 40 || got.CCSID != ccsidBinary {
			t.Errorf("graphic []byte adopt = %+v; want SQLType 464, FieldLength 40, CCSID 65535", got)
		}
	})

	t.Run("graphic_string_adopt", func(t *testing.T) {
		// string into a CCSID-65535 GRAPHIC (468) adopts the graphic shape.
		shapes := []PreparedParam{{SQLType: 449, FieldLength: 6, CCSID: 37}}
		reconcileBindShapesFromPMF(shapes, []any{"AB"},
			[]ParameterMarkerField{{SQLType: 468, FieldLength: 8, CCSID: ccsidBinary}})
		if got := shapes[0]; got.SQLType != 468 || got.FieldLength != 8 || got.CCSID != ccsidBinary {
			t.Errorf("graphic string adopt = %+v; want SQLType 468, FieldLength 8, CCSID 65535", got)
		}
	})

	t.Run("null_typed_decimal_adopts_precision_scale", func(t *testing.T) {
		shapes := []PreparedParam{{SQLType: 497, FieldLength: 4}}
		reconcileBindShapesFromPMF(shapes, []any{nil},
			[]ParameterMarkerField{{SQLType: 485, FieldLength: 16, Precision: 31, Scale: 7}})
		if got := shapes[0]; got.SQLType != 485 || got.FieldLength != 16 || got.Precision != 31 || got.Scale != 7 {
			t.Errorf("null decimal adopt = %+v; want SQLType 485, FieldLength 16, Precision 31, Scale 7", got)
		}
	})

	t.Run("null_lob_skipped", func(t *testing.T) {
		// A nil bind whose PMF declares a LOB (BLOB 961) is owned by
		// bindLOBParameters, so the NULL arm must skip it -- the shape stays 497.
		shapes := []PreparedParam{{SQLType: 497, FieldLength: 4}}
		reconcileBindShapesFromPMF(shapes, []any{nil},
			[]ParameterMarkerField{{SQLType: 961, FieldLength: 4, CCSID: ccsidBinary}})
		if got := shapes[0]; got.SQLType != 497 || got.FieldLength != 4 {
			t.Errorf("null LOB skip = %+v; want untouched SQLType 497, FieldLength 4", got)
		}
	})

	t.Run("inout_decimal_inplace", func(t *testing.T) {
		shapes := []PreparedParam{{SQLType: 449, FieldLength: 2000, ParamType: 0xF2, Value: int64(5)}}
		eo := reconcileBindShapesFromPMF(shapes, []any{int64(5)},
			[]ParameterMarkerField{{SQLType: 485, FieldLength: 16, Precision: 9, Scale: 2}})
		if !eo {
			t.Error("expectOutput = false; want true for an INOUT slot")
		}
		got := shapes[0]
		if got.SQLType != 485 || got.Precision != 9 || got.Scale != 2 || got.ParamType != 0xF2 {
			t.Errorf("inout decimal adopt = %+v; want SQLType 485, Precision 9, Scale 2, ParamType 0xF2", got)
		}
	})

	t.Run("out_beyond_pmf_sets_expectoutput_only", func(t *testing.T) {
		// An OUT slot past the end of the PMF flips expectOutput but leaves the
		// placeholder shape untouched.
		shapes := []PreparedParam{
			{SQLType: 496, FieldLength: 4, Value: int64(1)},
			{SQLType: 449, FieldLength: 2000, ParamType: 0xF1, Value: []byte{0xAB}, DateFormat: df},
		}
		eo := reconcileBindShapesFromPMF(shapes, []any{int64(1), nil},
			[]ParameterMarkerField{{SQLType: 496, FieldLength: 4}}) // shorter than shapes
		if !eo {
			t.Error("expectOutput = false; want true for the OUT slot beyond the PMF")
		}
		if got := shapes[1]; got.SQLType != 449 || got.FieldLength != 2000 || got.ParamType != 0xF1 || got.DateFormat != df {
			t.Errorf("out-beyond-pmf shape = %+v; want untouched SQLType 449, FieldLength 2000, ParamType 0xF1, DateFormat preserved", got)
		}
	})

	t.Run("string_into_binary_untouched", func(t *testing.T) {
		// A string into a native BINARY column keeps its real-CCSID VARCHAR
		// shape -- only a []byte reaches the binary arm.
		shapes := []PreparedParam{{SQLType: 449, FieldLength: 8, CCSID: 37, Value: "AB"}}
		reconcileBindShapesFromPMF(shapes, []any{"AB"},
			[]ParameterMarkerField{{SQLType: 913, FieldLength: 8, CCSID: ccsidBinary}})
		if got := shapes[0]; got.SQLType != 449 || got.CCSID != 37 {
			t.Errorf("string-into-binary = %+v; want untouched SQLType 449, CCSID 37", got)
		}
	})

	t.Run("bytes_into_plain_varchar_untouched", func(t *testing.T) {
		// A []byte into a real-CCSID VARCHAR (448, not bit-data) keeps its
		// shape -- the binary arm only fires for a CCSID-65535 binary PMF.
		shapes := []PreparedParam{{SQLType: 449, FieldLength: 4, CCSID: 37, Value: []byte("hi")}}
		reconcileBindShapesFromPMF(shapes, []any{[]byte("hi")},
			[]ParameterMarkerField{{SQLType: 448, FieldLength: 40, CCSID: 37}})
		if got := shapes[0]; got.SQLType != 449 || got.CCSID != 37 {
			t.Errorf("bytes-into-varchar = %+v; want untouched SQLType 449, CCSID 37", got)
		}
	})

	t.Run("timestamp_into_timestamp_untouched", func(t *testing.T) {
		// isDateTimeSQLType excludes TIMESTAMP (392/393), so a time.Time bind
		// into a TIMESTAMP column keeps its 393 shape.
		shapes := []PreparedParam{{SQLType: 393, FieldLength: 26, CCSID: 37, DateFormat: df}}
		reconcileBindShapesFromPMF(shapes, []any{"2026-06-05-00.00.00.000000"},
			[]ParameterMarkerField{{SQLType: 393, FieldLength: 26, CCSID: 37}})
		if got := shapes[0]; got.SQLType != 393 || got.FieldLength != 26 || got.DateFormat != df {
			t.Errorf("timestamp-into-timestamp = %+v; want untouched SQLType 393, FieldLength 26, DateFormat preserved", got)
		}
	})

	t.Run("empty_pmf_untouched", func(t *testing.T) {
		// A nil PMF leaves every IN slot exactly as the driver chose it.
		shapes := []PreparedParam{{SQLType: 496, FieldLength: 4, Value: int64(1)}}
		eo := reconcileBindShapesFromPMF(shapes, []any{int64(1)}, nil)
		if eo {
			t.Error("expectOutput = true; want false for an IN-only slot with empty PMF")
		}
		if got := shapes[0]; got.SQLType != 496 || got.FieldLength != 4 {
			t.Errorf("empty-pmf shape = %+v; want untouched SQLType 496, FieldLength 4", got)
		}
	})
}
