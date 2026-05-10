package hostserver

import "testing"

// TestDecodeColumnBoolean pins the V7R5+ native BOOLEAN decode path
// (SQL types 2436 NN / 2437 nullable). JT400's SQLBoolean.convertFromRawBytes
// treats 0xF0 (EBCDIC '0') as false and *any* other byte as true; the
// encoder writes 0xF1 (EBCDIC '1') for true so the steady-state wire
// stays inside the {0xF0, 0xF1} pair.
//
// Live-validated against V7R6M0 (`CREATE TABLE t (flag BOOLEAN)` +
// INSERT TRUE/FALSE + SELECT) on 2026-05-10; this offline test pins
// the decoder shape against the JT400 contract so a wire-format
// regression doesn't slip past the unit suite.
func TestDecodeColumnBoolean(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sqlType uint16
		wire    byte
		want    bool
	}{
		{"NN false 0xF0", 2436, 0xF0, false},
		{"NN true 0xF1", 2436, 0xF1, true},
		{"nullable false 0xF0", 2437, 0xF0, false},
		{"nullable true 0xF1", 2437, 0xF1, true},
		// JT400's read side accepts anything other than 0xF0 as true.
		// Pin the loose-equality contract so a stricter implementation
		// can't silently break against an IBM i release that ships an
		// out-of-spec byte.
		{"nullable true odd byte", 2437, 0x01, true},
		{"nullable true 0xFF", 2437, 0xFF, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col := SelectColumn{Name: "FLAG", SQLType: tc.sqlType, Length: 1}
			v, n, err := decodeColumn([]byte{tc.wire}, col)
			if err != nil {
				t.Fatalf("decodeColumn: %v", err)
			}
			if n != 1 {
				t.Errorf("consumed %d bytes, want 1", n)
			}
			b, ok := v.(bool)
			if !ok {
				t.Fatalf("value %T (%v), want bool", v, v)
			}
			if b != tc.want {
				t.Errorf("decoded %v, want %v", b, tc.want)
			}
		})
	}

	t.Run("short input", func(t *testing.T) {
		col := SelectColumn{Name: "FLAG", SQLType: 2437, Length: 1}
		if _, _, err := decodeColumn(nil, col); err == nil {
			t.Fatal("expected error on empty input")
		}
	})
}
