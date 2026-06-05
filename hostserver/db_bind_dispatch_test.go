package hostserver

import (
	"reflect"
	"testing"
)

// legacyReconcileSequence reproduces the exact cache-miss reconcile pipeline
// ExecutePreparedSQL runs today: the four IN-side reconciles plus the extracted
// OUT/INOUT fixup, in production order, mutating shapes in place and returning
// the expectOutput flag. It is the parity oracle for reconcileBindShapesFromPMF
// -- it calls the REAL reconcile functions, so the equivalence assertion stays
// honest if any of them changes.
func legacyReconcileSequence(shapes []PreparedParam, values []any, pmf []ParameterMarkerField) (expectOutput bool) {
	reconcileGraphicBitDataBindShapes(shapes, values, pmf)
	reconcileBinaryBindShapes(shapes, values, pmf)
	reconcileDateTimeBindShapes(shapes, values, pmf)
	expectOutput = reconcileOutInoutBindShapes(shapes, pmf)
	reconcileNullBindShapes(shapes, values, pmf)
	return expectOutput
}

func cloneShapes(src []PreparedParam) []PreparedParam {
	return append([]PreparedParam(nil), src...)
}

// TestReconcileBindShapesFromPMFMatchesLegacy proves the unified
// PMF-driven dispatcher is byte-identical (shapes + expectOutput) to the
// current five-reconcile sequence across the driver-realizable input matrix --
// the offline parity oracle for the v0.8.0 migration that will replace the
// per-type reconciles with the dispatcher. Inputs carry distinctive
// Value/DateFormat fields so a divergence in field preservation (e.g. NULL
// preserving DateFormat, OUT preserving Value, the IN arms zeroing both via
// whole-struct replace) is caught, not just SQLType/length.
func TestReconcileBindShapesFromPMFMatchesLegacy(t *testing.T) {
	// Sentinels to detect Value/DateFormat (mis)handling.
	const df = byte(0x07)
	sentinel := []byte{0xAB, 0xCD}

	cases := []struct {
		name   string
		shapes []PreparedParam
		values []any
		pmf    []ParameterMarkerField
	}{
		{
			name:   "binary_varbinary",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 2, CCSID: ccsidBinary, Value: sentinel, DateFormat: df}},
			values: []any{[]byte{0x01, 0x02}},
			pmf:    []ParameterMarkerField{{SQLType: 908, FieldLength: 34, CCSID: ccsidBinary}},
		},
		{
			name:   "binary_fixed_binary",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 2, CCSID: ccsidBinary, Value: sentinel}},
			values: []any{[]byte{0x01, 0x02}},
			pmf:    []ParameterMarkerField{{SQLType: 913, FieldLength: 8, CCSID: ccsidBinary}},
		},
		{
			name:   "graphic_vargraphic_bytes",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 4, CCSID: ccsidBinary, Value: sentinel, DateFormat: df}},
			values: []any{[]byte{0x00, 0x41, 0x00, 0x42}},
			pmf:    []ParameterMarkerField{{SQLType: 464, FieldLength: 40, CCSID: ccsidBinary}},
		},
		{
			name:   "graphic_fixed_graphic_string",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 6, CCSID: 37, Value: "AB"}},
			values: []any{"AB"},
			pmf:    []ParameterMarkerField{{SQLType: 468, FieldLength: 8, CCSID: ccsidBinary}},
		},
		{
			name:   "datetime_date_forces_prec_scale_zero",
			shapes: []PreparedParam{{SQLType: 393, FieldLength: 26, Precision: 6, Scale: 6, CCSID: 37, Value: "x", DateFormat: df}},
			values: []any{"2026-06-05-00.00.00.000000"},
			pmf:    []ParameterMarkerField{{SQLType: 384, FieldLength: 10, Precision: 6, Scale: 6, CCSID: 37}},
		},
		{
			name:   "datetime_time",
			shapes: []PreparedParam{{SQLType: 392, FieldLength: 26, CCSID: 37, DateFormat: df}},
			values: []any{"2026-06-05-12.34.56.000000"},
			pmf:    []ParameterMarkerField{{SQLType: 388, FieldLength: 8, CCSID: 37}},
		},
		{
			name:   "null_varbinary_issue11",
			shapes: []PreparedParam{{SQLType: 497, FieldLength: 4, Value: nil, DateFormat: df}},
			values: []any{nil},
			pmf:    []ParameterMarkerField{{SQLType: 909, FieldLength: 34, CCSID: ccsidBinary}},
		},
		{
			name:   "null_decimal_adopts_prec_scale",
			shapes: []PreparedParam{{SQLType: 497, FieldLength: 4}},
			values: []any{nil},
			pmf:    []ParameterMarkerField{{SQLType: 485, FieldLength: 16, Precision: 31, Scale: 7}},
		},
		{
			name:   "null_into_varchar",
			shapes: []PreparedParam{{SQLType: 497, FieldLength: 4, DateFormat: df}},
			values: []any{nil},
			pmf:    []ParameterMarkerField{{SQLType: 449, FieldLength: 100, CCSID: 37}},
		},
		{
			name:   "null_lob_skipped",
			shapes: []PreparedParam{{SQLType: 497, FieldLength: 4}},
			values: []any{nil},
			pmf:    []ParameterMarkerField{{SQLType: 961, FieldLength: 4, CCSID: ccsidBinary}}, // BLOB
		},
		{
			name:   "out_date_inplace_preserves_value_dateformat",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 2000, ParamType: 0xF1, Value: sentinel, DateFormat: df}},
			values: []any{nil},
			pmf:    []ParameterMarkerField{{SQLType: 384, FieldLength: 10, CCSID: 37}},
		},
		{
			name:   "inout_decimal_copies_prec_scale",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 2000, ParamType: 0xF2, Value: int64(5)}},
			values: []any{int64(5)},
			pmf:    []ParameterMarkerField{{SQLType: 485, FieldLength: 16, Precision: 9, Scale: 2}},
		},
		{
			name: "out_beyond_pmf_sets_expectoutput_only",
			shapes: []PreparedParam{
				{SQLType: 496, FieldLength: 4, Value: int64(1)},
				{SQLType: 449, FieldLength: 2000, ParamType: 0xF1, Value: sentinel, DateFormat: df},
			},
			values: []any{int64(1), nil},
			pmf:    []ParameterMarkerField{{SQLType: 496, FieldLength: 4}}, // shorter than shapes
		},
		{
			name:   "string_into_binary_untouched",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 8, CCSID: 37, Value: "AB"}},
			values: []any{"AB"},
			pmf:    []ParameterMarkerField{{SQLType: 913, FieldLength: 8, CCSID: ccsidBinary}},
		},
		{
			name:   "bytes_into_plain_varchar_untouched",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 4, CCSID: 37, Value: []byte("hi")}},
			values: []any{[]byte("hi")},
			pmf:    []ParameterMarkerField{{SQLType: 448, FieldLength: 40, CCSID: 37}}, // real-CCSID VARCHAR, not bit-data
		},
		{
			name:   "int_untouched",
			shapes: []PreparedParam{{SQLType: 496, FieldLength: 4, Value: int64(42)}},
			values: []any{int64(42)},
			pmf:    []ParameterMarkerField{{SQLType: 496, FieldLength: 4}},
		},
		{
			name:   "timestamp_into_timestamp_untouched",
			shapes: []PreparedParam{{SQLType: 393, FieldLength: 26, CCSID: 37, DateFormat: df}},
			values: []any{"2026-06-05-00.00.00.000000"},
			pmf:    []ParameterMarkerField{{SQLType: 393, FieldLength: 26, CCSID: 37}}, // isDateTimeSQLType excludes 392/393
		},
		{
			name: "empty_pmf_with_out_slot",
			shapes: []PreparedParam{
				{SQLType: 496, FieldLength: 4, Value: int64(1)},
				{SQLType: 449, FieldLength: 2000, ParamType: 0xF1, Value: sentinel, DateFormat: df},
			},
			values: []any{int64(1), nil},
			pmf:    nil,
		},
		{
			name: "multi_slot_mixed",
			shapes: []PreparedParam{
				{SQLType: 449, FieldLength: 2, CCSID: ccsidBinary, Value: []byte{0x09}},        // binary
				{SQLType: 449, FieldLength: 2000, ParamType: 0xF2, Value: int64(7)},            // inout decimal
				{SQLType: 497, FieldLength: 4, DateFormat: df},                                 // null
				{SQLType: 393, FieldLength: 26, Precision: 6, Scale: 6, CCSID: 37, Value: "t"}, // datetime
			},
			values: []any{[]byte{0x09}, int64(7), nil, "2026-06-05-01.02.03.000000"},
			pmf: []ParameterMarkerField{
				{SQLType: 908, FieldLength: 34, CCSID: ccsidBinary},
				{SQLType: 485, FieldLength: 16, Precision: 9, Scale: 2},
				{SQLType: 909, FieldLength: 34, CCSID: ccsidBinary},
				{SQLType: 384, FieldLength: 10, CCSID: 37},
			},
		},
		{
			// A user string into a DATE column keeps its VARCHAR shape (the
			// driver tags only time.Time as TIMESTAMP 392/393); the datetime
			// arm must not fire on a 449 shape.
			name:   "string_into_date_untouched",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 10, CCSID: 37, Value: "2026-06-05"}},
			values: []any{"2026-06-05"},
			pmf:    []ParameterMarkerField{{SQLType: 384, FieldLength: 10, CCSID: 37}},
		},
		{
			name:   "graphic_long_vargraphic_bytes",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 4, CCSID: ccsidBinary, Value: []byte{0x00, 0x41}}},
			values: []any{[]byte{0x00, 0x41}},
			pmf:    []ParameterMarkerField{{SQLType: 472, FieldLength: 80, CCSID: ccsidBinary}},
		},
		{
			name:   "out_into_binary",
			shapes: []PreparedParam{{SQLType: 449, FieldLength: 2000, ParamType: 0xF1, Value: []byte{0x01}}},
			values: []any{nil},
			pmf:    []ParameterMarkerField{{SQLType: 908, FieldLength: 34, CCSID: ccsidBinary}},
		},
		{
			name: "two_out_slots",
			shapes: []PreparedParam{
				{SQLType: 449, FieldLength: 2000, ParamType: 0xF1},
				{SQLType: 449, FieldLength: 2000, ParamType: 0xF2, Value: int64(3)},
			},
			values: []any{nil, int64(3)},
			pmf: []ParameterMarkerField{
				{SQLType: 388, FieldLength: 8, CCSID: 37},
				{SQLType: 496, FieldLength: 4},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := cloneShapes(tc.shapes)
			b := cloneShapes(tc.shapes)
			wantEO := legacyReconcileSequence(a, tc.values, tc.pmf)
			gotEO := reconcileBindShapesFromPMF(b, tc.values, tc.pmf)
			if gotEO != wantEO {
				t.Errorf("expectOutput = %v, legacy = %v", gotEO, wantEO)
			}
			if !reflect.DeepEqual(b, a) {
				t.Errorf("shapes diverge from legacy:\n dispatcher = %+v\n legacy     = %+v", b, a)
			}
		})
	}
}

// TestReconcileBindShapesFromPMFFieldHandling locks the per-arm field intent
// directly on the dispatcher output (independent of the legacy oracle), so a
// future change that keeps new==legacy by breaking BOTH the same way is still
// caught for the load-bearing fields.
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
}
