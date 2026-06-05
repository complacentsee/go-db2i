package hostserver

import "testing"

// TestPreparedParamsFromCachedDecimalFieldLength pins the cache-hit decimal
// FieldLength fix. The package SQLDA stores a packed/zoned/decfloat field's
// precision/scale word in its 2-byte "length" slot (DECIMAL(31,7) -> 0x1F07 =
// 7943), and normalizeSQLDALength passes that through unchanged for non-VAR
// types -- so the ParameterMarkerField that reaches preparedParamsFromCached
// carries FieldLength = the precision/scale word, not the wire byte width.
// Binding it verbatim made encodeRowData reject the slot ("packed bytes 16 !=
// FieldLength 7943") and broke every DECIMAL/NUMERIC bind on cache-hit.
// preparedParamsFromCached must recompute FieldLength from precision.
func TestPreparedParamsFromCachedDecimalFieldLength(t *testing.T) {
	in := []ParameterMarkerField{
		// DECIMAL(31,7): length slot = 0x1F07. Packed BCD width =
		// ceil((31+1)/2) = 16.
		{SQLType: 485, FieldLength: 0x1F07, Precision: 31, Scale: 7, ParamType: 0x00},
		// NUMERIC(9,2): length slot = 0x0902. Zoned width = 9.
		{SQLType: 489, FieldLength: 0x0902, Precision: 9, Scale: 2, ParamType: 0x00},
		// DECIMAL(5,2): length slot = 0x0502. Packed width = ceil(6/2) = 3.
		{SQLType: 484, FieldLength: 0x0502, Precision: 5, Scale: 2, ParamType: 0x00},
	}
	out, err := preparedParamsFromCached(in)
	if err != nil {
		t.Fatalf("preparedParamsFromCached: %v", err)
	}
	want := []struct {
		fieldLength      uint32
		precision, scale uint16
	}{
		{16, 31, 7},
		{9, 9, 2},
		{3, 5, 2},
	}
	for i, w := range want {
		if out[i].FieldLength != w.fieldLength || out[i].Precision != w.precision || out[i].Scale != w.scale {
			t.Errorf("shape[%d] = {FieldLength:%d Precision:%d Scale:%d}, want {FieldLength:%d Precision:%d Scale:%d}",
				i, out[i].FieldLength, out[i].Precision, out[i].Scale,
				w.fieldLength, w.precision, w.scale)
		}
	}
}

// TestCachedDecimalFieldLength unit-checks the width arithmetic across the
// packed (DECIMAL), zoned (NUMERIC), and DECFLOAT families. The packed cases
// must agree with encodePackedBCD's own byte count and the cache-miss
// fieldLenFor helper.
func TestCachedDecimalFieldLength(t *testing.T) {
	cases := []struct {
		name      string
		sqlType   uint16
		precision uint16
		want      uint32
	}{
		{"decimal_31", 485, 31, 16},
		{"decimal_5", 484, 5, 3},
		{"decimal_7", 485, 7, 4},
		{"decimal_15", 484, 15, 8},
		{"numeric_9", 489, 9, 9},
		{"numeric_7", 488, 7, 7},
		{"decfloat_16", 996, 16, 8},
		{"decfloat_34", 997, 34, 16},
		{"non_decimal", 497, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cachedDecimalFieldLength(tc.sqlType, tc.precision); got != tc.want {
				t.Errorf("cachedDecimalFieldLength(%d, %d) = %d, want %d",
					tc.sqlType, tc.precision, got, tc.want)
			}
			// Cross-check packed widths against the encoder's own output.
			if tc.sqlType == 484 || tc.sqlType == 485 {
				packed, err := encodePackedBCD("0", int(tc.precision), 0)
				if err != nil {
					t.Fatalf("encodePackedBCD: %v", err)
				}
				if uint32(len(packed)) != tc.want {
					t.Errorf("encodePackedBCD width = %d, want %d", len(packed), tc.want)
				}
			}
		})
	}
}
