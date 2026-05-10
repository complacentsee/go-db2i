package hostserver

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestDecodeColumnBINARY pins the native BINARY (912/913) decode
// shape. JT400's SQLBinary.convertFromRawBytes reads `maxLength`
// raw bytes off the wire with no length prefix; the column always
// carries CCSID 65535. Distinct SQL type from CHAR FOR BIT DATA
// (452/453 + CCSID 65535) but byte-identical wire shape -- this
// test pins the type dispatch alongside the existing CHAR tests.
func TestDecodeColumnBINARY(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sqlType uint16
	}{
		{"NN", 912},
		{"nullable", 913},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11}
			col := SelectColumn{Name: "B", SQLType: tc.sqlType, Length: 8, CCSID: 65535}
			got, n, err := decodeColumn(want, col)
			if err != nil {
				t.Fatalf("decodeColumn: %v", err)
			}
			if n != 8 {
				t.Errorf("consumed %d bytes, want 8", n)
			}
			b, ok := got.([]byte)
			if !ok {
				t.Fatalf("decoded type %T, want []byte", got)
			}
			if !bytes.Equal(b, want) {
				t.Errorf("decoded %x, want %x", b, want)
			}
		})
	}
}

// TestDecodeColumnVARBINARY pins the native VARBINARY (908/909)
// decode shape. JT400's SQLVarbinary.convertFromRawBytes reads a
// 2-byte BE unsigned length prefix followed by `length` bytes;
// the column always carries CCSID 65535. Wire-identical to
// VARCHAR FOR BIT DATA (449 + CCSID 65535) but distinct SQL type
// on V7R3+ servers that expose the dedicated VARBINARY type.
func TestDecodeColumnVARBINARY(t *testing.T) {
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}
	for _, tc := range []struct {
		name    string
		sqlType uint16
	}{
		{"NN", 908},
		{"nullable", 909},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 2-byte length prefix + payload + 8 bytes of trailing
			// padding (to simulate a slot-padded non-VLF row).
			wire := make([]byte, 2+16)
			binary.BigEndian.PutUint16(wire[:2], uint16(len(payload)))
			copy(wire[2:], payload)
			col := SelectColumn{Name: "V", SQLType: tc.sqlType, Length: 16, CCSID: 65535}
			got, n, err := decodeColumn(wire, col)
			if err != nil {
				t.Fatalf("decodeColumn: %v", err)
			}
			if n != 2+len(payload) {
				t.Errorf("consumed %d bytes, want %d", n, 2+len(payload))
			}
			b, ok := got.([]byte)
			if !ok {
				t.Fatalf("decoded type %T, want []byte", got)
			}
			if !bytes.Equal(b, payload) {
				t.Errorf("decoded %x, want %x", b, payload)
			}
		})
	}

	t.Run("declared length exceeds column max", func(t *testing.T) {
		wire := make([]byte, 2+8)
		binary.BigEndian.PutUint16(wire[:2], 17) // > max 16
		col := SelectColumn{Name: "V", SQLType: 909, Length: 16, CCSID: 65535}
		if _, _, err := decodeColumn(wire, col); err == nil {
			t.Fatal("expected error on declared length > max, got nil")
		}
	})
}
