package hostserver

import "testing"

// TestReconcileBinaryBindShapes verifies that a []byte bind into a native
// BINARY/VARBINARY column adopts the column's declared parameter-marker
// shape (issue #40), while every other slot keeps the shape the driver
// chose. The driver maps every []byte to VARCHAR FOR BIT DATA (449); the
// reshape is what makes encodeRowData ship JT400's byte-identical native
// 912/908 form on the cache-miss path.
func TestReconcileBinaryBindShapes(t *testing.T) {
	// Driver-chosen shapes: every []byte arrives as 449 VARCHAR FOR BIT
	// DATA; the string arrives as 449 with a real CCSID; the int as INTEGER.
	shapes := []PreparedParam{
		{SQLType: 449, FieldLength: 6, Precision: 4, CCSID: 65535},                  // 0: []byte -> VARBINARY column
		{SQLType: 449, FieldLength: 4, Precision: 2, CCSID: 65535},                  // 1: []byte -> BINARY column
		{SQLType: 449, FieldLength: 6, Precision: 4, CCSID: 37},                     // 2: string -> VARBINARY column (must NOT reshape)
		{SQLType: 449, FieldLength: 6, Precision: 4, CCSID: 65535},                  // 3: []byte -> non-binary (text) column
		{SQLType: 449, FieldLength: 6, Precision: 4, CCSID: 65535, ParamType: 0xF1}, // 4: OUT slot
		{SQLType: 497, FieldLength: 4},                                              // 5: int -> INTEGER column
	}
	values := []any{
		[]byte{0x11, 0x22, 0x33, 0x44},
		[]byte{0xAA, 0xBB},
		"hello",
		[]byte{0xDE, 0xAD},
		[]byte{}, // OUT placeholder
		int32(7),
	}
	pmf := []ParameterMarkerField{
		{SQLType: 908, FieldLength: 34, CCSID: 65535}, // 0: native VARBINARY(32)
		{SQLType: 912, FieldLength: 8, CCSID: 65535},  // 1: native BINARY(8)
		{SQLType: 908, FieldLength: 34, CCSID: 65535}, // 2: native VARBINARY (but value is a string)
		{SQLType: 449, FieldLength: 6, CCSID: 37},     // 3: real VARCHAR (text)
		{SQLType: 912, FieldLength: 8, CCSID: 65535},  // 4: native BINARY but OUT slot
		{SQLType: 497, FieldLength: 4},                // 5: INTEGER
	}

	reconcileBinaryBindShapes(shapes, values, pmf)

	// Slot 0: []byte into VARBINARY -> adopts 908 / FieldLength 34.
	if shapes[0].SQLType != 908 || shapes[0].FieldLength != 34 || shapes[0].CCSID != 65535 {
		t.Errorf("slot 0 (varbinary): got %+v, want SQLType 908 FieldLength 34 CCSID 65535", shapes[0])
	}
	// Slot 1: []byte into BINARY -> adopts 912 / FieldLength 8.
	if shapes[1].SQLType != 912 || shapes[1].FieldLength != 8 || shapes[1].CCSID != 65535 {
		t.Errorf("slot 1 (binary): got %+v, want SQLType 912 FieldLength 8 CCSID 65535", shapes[1])
	}
	// Slot 2: string bind is never reshaped, even into a binary column.
	if shapes[2].SQLType != 449 {
		t.Errorf("slot 2 (string): got SQLType %d, want 449 (strings not reshaped)", shapes[2].SQLType)
	}
	// Slot 3: non-binary PMF target -> untouched.
	if shapes[3].SQLType != 449 {
		t.Errorf("slot 3 (text target): got SQLType %d, want 449 (untouched)", shapes[3].SQLType)
	}
	// Slot 4: OUT slot -> untouched (and direction byte preserved).
	if shapes[4].SQLType != 449 || shapes[4].ParamType != 0xF1 {
		t.Errorf("slot 4 (OUT): got SQLType %d ParamType 0x%02X, want 449 / 0xF1 (untouched)", shapes[4].SQLType, shapes[4].ParamType)
	}
	// Slot 5: INTEGER bind -> untouched.
	if shapes[5].SQLType != 497 {
		t.Errorf("slot 5 (int): got SQLType %d, want 497 (untouched)", shapes[5].SQLType)
	}
}

// TestReconcileBinaryBindShapesEmptyPMF confirms the no-PMF and
// shapes-longer-than-PMF paths are safe no-ops (no panic, shapes intact).
func TestReconcileBinaryBindShapesEmptyPMF(t *testing.T) {
	shapes := []PreparedParam{{SQLType: 449, FieldLength: 6, CCSID: 65535}}
	values := []any{[]byte{0x01, 0x02, 0x03, 0x04}}

	reconcileBinaryBindShapes(shapes, values, nil)
	if shapes[0].SQLType != 449 {
		t.Errorf("nil pmf: shape changed to %d, want 449 untouched", shapes[0].SQLType)
	}

	// shapes longer than pmf: the extra slot is left untouched.
	shapes = append(shapes, PreparedParam{SQLType: 449, FieldLength: 4, CCSID: 65535})
	values = append(values, []byte{0xAA, 0xBB})
	reconcileBinaryBindShapes(shapes, values, []ParameterMarkerField{{SQLType: 908, FieldLength: 34, CCSID: 65535}})
	if shapes[0].SQLType != 908 {
		t.Errorf("slot 0: want reshaped to 908, got %d", shapes[0].SQLType)
	}
	if shapes[1].SQLType != 449 {
		t.Errorf("slot 1 (beyond pmf): want untouched 449, got %d", shapes[1].SQLType)
	}
}
