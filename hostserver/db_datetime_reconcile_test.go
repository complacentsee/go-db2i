package hostserver

import "testing"

// TestReconcileDateTimeBindShapes verifies that a time.Time bind (which the
// driver shapes as TIMESTAMP 393) into a native DATE/TIME column adopts the
// column's declared parameter-marker shape (issue #40), while every other
// slot keeps the shape the driver chose. The 26-char value is untouched; the
// existing encodeRowData DATE/TIME arms reshape it.
func TestReconcileDateTimeBindShapes(t *testing.T) {
	// All temporal binds arrive as TIMESTAMP (393, 26-char) from the driver,
	// except the deliberately-different slots noted below.
	shapes := []PreparedParam{
		{SQLType: 393, FieldLength: 26},                  // 0: time.Time -> DATE column
		{SQLType: 393, FieldLength: 26},                  // 1: time.Time -> TIME column
		{SQLType: 393, FieldLength: 26},                  // 2: time.Time -> TIMESTAMP column (no reshape)
		{SQLType: 449, FieldLength: 12, CCSID: 37},       // 3: string -> DATE column (must NOT reshape)
		{SQLType: 393, FieldLength: 26, ParamType: 0xF1}, // 4: OUT slot
		{SQLType: 497, FieldLength: 4},                   // 5: int -> INTEGER column
	}
	ts := "2026-05-07-14.23.45.123456"
	values := []any{ts, ts, ts, "2026-05-07", []byte(nil), int32(7)}
	pmf := []ParameterMarkerField{
		{SQLType: 384, FieldLength: 10, CCSID: 37},              // 0: native DATE (NN)
		{SQLType: 389, FieldLength: 8, CCSID: 37, Precision: 8}, // 1: native TIME (nullable form)
		{SQLType: 393, FieldLength: 26, CCSID: 37},              // 2: TIMESTAMP (same family, no reshape)
		{SQLType: 384, FieldLength: 10, CCSID: 37},              // 3: native DATE but value is a string
		{SQLType: 384, FieldLength: 10, CCSID: 37},              // 4: native DATE but OUT slot
		{SQLType: 497, FieldLength: 4},                          // 5: INTEGER
	}

	reconcileDateTimeBindShapes(shapes, values, pmf)

	// Slot 0: time.Time into DATE -> adopts 384 / FieldLength 10, Precision &
	// Scale forced to 0 (must match the cache-hit path, which zeroes them for
	// non-decimal types).
	if shapes[0].SQLType != 384 || shapes[0].FieldLength != 10 {
		t.Errorf("slot 0 (date): got %+v, want SQLType 384 FieldLength 10", shapes[0])
	}
	if shapes[0].Precision != 0 || shapes[0].Scale != 0 {
		t.Errorf("slot 0 (date): Precision/Scale = %d/%d, want 0/0", shapes[0].Precision, shapes[0].Scale)
	}
	// Slot 1: time.Time into TIME (nullable 389) -> adopts 389 / FieldLength 8;
	// the nonzero PMF Precision (8) must NOT be carried through.
	if shapes[1].SQLType != 389 || shapes[1].FieldLength != 8 {
		t.Errorf("slot 1 (time): got %+v, want SQLType 389 FieldLength 8", shapes[1])
	}
	if shapes[1].Precision != 0 || shapes[1].Scale != 0 {
		t.Errorf("slot 1 (time): Precision/Scale = %d/%d, want 0/0 (PMF precision not echoed)", shapes[1].Precision, shapes[1].Scale)
	}
	// Slot 2: TIMESTAMP target is not in isDateTimeSQLType -> untouched.
	if shapes[2].SQLType != 393 {
		t.Errorf("slot 2 (timestamp): got SQLType %d, want 393 (untouched)", shapes[2].SQLType)
	}
	// Slot 3: string bind (449) is never reshaped, even into a DATE column.
	if shapes[3].SQLType != 449 {
		t.Errorf("slot 3 (string): got SQLType %d, want 449 (strings not reshaped)", shapes[3].SQLType)
	}
	// Slot 4: OUT slot -> untouched (direction byte preserved).
	if shapes[4].SQLType != 393 || shapes[4].ParamType != 0xF1 {
		t.Errorf("slot 4 (OUT): got SQLType %d ParamType 0x%02X, want 393 / 0xF1 (untouched)", shapes[4].SQLType, shapes[4].ParamType)
	}
	// Slot 5: INTEGER bind -> untouched.
	if shapes[5].SQLType != 497 {
		t.Errorf("slot 5 (int): got SQLType %d, want 497 (untouched)", shapes[5].SQLType)
	}
}

// TestReconcileDateTimeBindShapesEmptyPMF confirms the no-PMF and
// shapes-longer-than-PMF paths are safe no-ops.
func TestReconcileDateTimeBindShapesEmptyPMF(t *testing.T) {
	shapes := []PreparedParam{{SQLType: 393, FieldLength: 26}}
	values := []any{"2026-05-07-14.23.45.123456"}

	reconcileDateTimeBindShapes(shapes, values, nil)
	if shapes[0].SQLType != 393 {
		t.Errorf("nil pmf: shape changed to %d, want 393 untouched", shapes[0].SQLType)
	}

	// shapes longer than pmf: the extra slot is left untouched.
	shapes = append(shapes, PreparedParam{SQLType: 393, FieldLength: 26})
	values = append(values, "2026-05-07-14.23.45.123456")
	reconcileDateTimeBindShapes(shapes, values, []ParameterMarkerField{{SQLType: 384, FieldLength: 10, CCSID: 37}})
	if shapes[0].SQLType != 384 {
		t.Errorf("slot 0: want reshaped to 384, got %d", shapes[0].SQLType)
	}
	if shapes[1].SQLType != 393 {
		t.Errorf("slot 1 (beyond pmf): want untouched 393, got %d", shapes[1].SQLType)
	}
}
