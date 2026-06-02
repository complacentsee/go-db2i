package hostserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// Offline coverage tests for issue #39: the result-type decode path
// additions -- typed unsupported-type error, ROWID/DATALINK bytes,
// odd-graphic reject, and scaled-binary-integer decode. All synthetic
// bytes; no live DB. Mirrors the existing db_result_data_*_test style
// (table-driven where it helps, one decodeColumn call per case).

// TestDecodeColumnUnsupportedTypeIsTyped pins that an undecodable SQL
// type (XML 988/989, an ARRAY-ish unknown code) returns the typed
// UnsupportedResultTypeError. errors.As must reach the concrete struct
// and it must name the SQL type / length / CCSID; errors.Is must match
// the ErrUnsupportedResultType sentinel. This is the always-win item:
// without it a single un-decodable column turns the whole-row decode
// into an opaque failure.
func TestDecodeColumnUnsupportedTypeIsTyped(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sqlType uint16
		length  uint32
		ccsid   uint16
	}{
		{"XML_NN", 988, 1000, 1208},
		{"XML_nullable", 989, 1000, 1208},
		{"XML_locator", 2452, 28, 1208},
		{"unknown_code", 4242, 7, 37},
	} {
		t.Run(tc.name, func(t *testing.T) {
			col := SelectColumn{Name: "X", SQLType: tc.sqlType, Length: tc.length, CCSID: tc.ccsid}
			_, _, err := decodeColumn(make([]byte, 64), col)
			if err == nil {
				t.Fatal("expected error for unsupported type, got nil")
			}
			if !errors.Is(err, ErrUnsupportedResultType) {
				t.Errorf("errors.Is(err, ErrUnsupportedResultType) = false; err = %v", err)
			}
			var ute *UnsupportedResultTypeError
			if !errors.As(err, &ute) {
				t.Fatalf("errors.As(&UnsupportedResultTypeError) = false; err = %v (%T)", err, err)
			}
			if ute.SQLType != tc.sqlType || ute.Length != tc.length || ute.CCSID != tc.ccsid {
				t.Errorf("typed error = {%d, %d, %d}, want {%d, %d, %d}",
					ute.SQLType, ute.Length, ute.CCSID, tc.sqlType, tc.length, tc.ccsid)
			}
			// The message keeps the old informative shape so existing
			// text-matchers (test/conformance) still recognise it.
			if got := err.Error(); !bytes.Contains([]byte(got), []byte("unsupported SQL type")) {
				t.Errorf("message %q lost the legacy 'unsupported SQL type' prefix", got)
			}
		})
	}
}

// TestDecodeColumnROWID pins the ROWID (904/905) decode: a 2-byte BE
// unsigned-short length prefix followed by `length` payload bytes,
// returned as []byte verbatim. JT400's SQLRowID.convertFromRawBytes
// reads the same 2-byte SL; ROWID is up to 40 bytes. (Wire shape is
// JT400-documented, not fixture-captured -- the conformance test
// confirms it live.)
func TestDecodeColumnROWID(t *testing.T) {
	payload := []byte{0x00, 0x00, 0x00, 0x01, 0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34}
	const slot = 42 // fixed field width: 2-byte SL + up to 40 payload bytes
	for _, tc := range []struct {
		name    string
		sqlType uint16
	}{
		{"NN", 904},
		{"nullable", 905},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Fixed slot: 2-byte SL + payload + trailing pad, `slot` total.
			wire := make([]byte, slot)
			binary.BigEndian.PutUint16(wire[:2], uint16(len(payload)))
			copy(wire[2:], payload)
			col := SelectColumn{Name: "R", SQLType: tc.sqlType, Length: slot, CCSID: 65535}
			got, n, err := decodeColumn(wire, col)
			if err != nil {
				t.Fatalf("decodeColumn: %v", err)
			}
			// ROWID advances by the fixed slot width (col.Length), NOT 2+n,
			// so row stepping matches JT400's fixed getFieldLength advance.
			if n != slot {
				t.Errorf("consumed = %d, want %d (fixed slot)", n, slot)
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

	t.Run("value length exceeds slot", func(t *testing.T) {
		wire := make([]byte, slot)
		binary.BigEndian.PutUint16(wire[:2], 99) // 2+99 > slot 42
		col := SelectColumn{Name: "R", SQLType: 905, Length: slot, CCSID: 65535}
		if _, _, err := decodeColumn(wire, col); err == nil {
			t.Fatal("expected error on value length + SL > slot, got nil")
		}
	})

	// ROWID must NOT be classified var-length: JT400 steps it by the
	// fixed field length, so a NULL ROWID in a VLF row null-skips
	// col.Length, not 2 bytes.
	t.Run("classified fixed-length", func(t *testing.T) {
		for _, st := range []uint16{904, 905} {
			if isVarLengthSQLType(st) {
				t.Errorf("isVarLengthSQLType(%d) = true, want false (ROWID is fixed-step)", st)
			}
		}
	})
}

// TestDecodeRowVLF_ROWIDThenInteger is the column-shift regression for
// issue #39: in a VLF row, a ROWID column must advance by its FIXED slot
// width so the following column decodes from the right offset. If ROWID
// were (wrongly) treated as variable-length it would advance by 2+SL and
// slide the INTEGER, so this asserts the INTEGER reads back intact.
func TestDecodeRowVLF_ROWIDThenInteger(t *testing.T) {
	const slot = 42
	rid := []byte{0xAB, 0xCD}
	row := make([]byte, slot+4)
	binary.BigEndian.PutUint16(row[:2], uint16(len(rid))) // SL = 2
	copy(row[2:], rid)
	binary.BigEndian.PutUint32(row[slot:slot+4], 99) // INTEGER right after the slot

	cols := []SelectColumn{
		{Name: "RID", SQLType: 904, Length: slot, CCSID: 65535},
		{Name: "N", SQLType: 496, Length: 4},
	}
	indicators := make([]byte, 2*2) // two non-null columns, 2-byte indicators
	got, _, err := decodeRow(row, cols, indicators, 2, false /* VLF */)
	if err != nil {
		t.Fatalf("decodeRow: %v", err)
	}
	if b, ok := got[0].([]byte); !ok || !bytes.Equal(b, rid) {
		t.Errorf("ROWID = %v (%T), want %x", got[0], got[0], rid)
	}
	if n, ok := got[1].(int32); !ok || n != 99 {
		t.Errorf("INTEGER after ROWID = %v (%T), want int32(99) -- column shifted?", got[1], got[1])
	}
}

// TestDecodeColumnDATALINK pins the DATALINK (396/397) decode: a
// 2-byte BE SL prefix followed by char bytes decoded through the
// column CCSID to the link/URL string. Mirrors
// SQLDatalink.convertFromRawBytes (which then wraps the string in a
// java.net.URL).
func TestDecodeColumnDATALINK(t *testing.T) {
	// "http://x" in CCSID 1208 (UTF-8 passthrough), so the payload
	// bytes are the ASCII string verbatim.
	link := "http://x/y"
	for _, tc := range []struct {
		name    string
		sqlType uint16
	}{
		{"NN", 396},
		{"nullable", 397},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := make([]byte, 2+len(link)+8)
			binary.BigEndian.PutUint16(wire[:2], uint16(len(link)))
			copy(wire[2:], link)
			col := SelectColumn{Name: "D", SQLType: tc.sqlType, Length: uint32(2 + len(link) + 8), CCSID: 1208}
			got, n, err := decodeColumn(wire, col)
			if err != nil {
				t.Fatalf("decodeColumn: %v", err)
			}
			if n != 2+len(link) {
				t.Errorf("consumed = %d, want %d", n, 2+len(link))
			}
			s, ok := got.(string)
			if !ok {
				t.Fatalf("decoded type %T, want string", got)
			}
			if s != link {
				t.Errorf("decoded = %q, want %q", s, link)
			}
		})
	}

	t.Run("classified var-length", func(t *testing.T) {
		for _, st := range []uint16{396, 397} {
			if !isVarLengthSQLType(st) {
				t.Errorf("isVarLengthSQLType(%d) = false, want true", st)
			}
		}
	})
}

// TestDecodeColumnGraphicOddLengthErrors pins issue #39 item 6: a
// fixed GRAPHIC payload with an odd byte count is malformed (UTF-16 /
// UCS-2 is 2 bytes per code unit). The pre-fix lenient decoder
// silently dropped the trailing odd byte; now decodeColumn surfaces an
// error so a slot shift can't hide behind a dropped byte.
func TestDecodeColumnGraphicOddLengthErrors(t *testing.T) {
	// GRAPHIC with col.Length=5 (odd) -- 5 bytes can't be whole UTF-16
	// code units. CCSID 1200 so it takes the strict-decode path.
	wire := []byte{0x00, 0x41, 0x00, 0x42, 0x00}
	col := SelectColumn{Name: "G", SQLType: 468, Length: 5, CCSID: 1200}
	if _, _, err := decodeColumn(wire, col); err == nil {
		t.Fatal("expected error on odd-length graphic payload, got nil")
	}

	// An even-length GRAPHIC still decodes (regression guard for the
	// strict wrapper not breaking the happy path).
	wireOK := []byte{0x00, 0x41, 0x00, 0x42}
	colOK := SelectColumn{Name: "G", SQLType: 468, Length: 4, CCSID: 1200}
	got, n, err := decodeColumn(wireOK, colOK)
	if err != nil {
		t.Fatalf("even-length graphic: %v", err)
	}
	if n != 4 {
		t.Errorf("consumed = %d, want 4", n)
	}
	if s, _ := got.(string); s != "AB" {
		t.Errorf("decoded = %q, want %q", s, "AB")
	}
}

// TestDecodeColumnScaledInteger pins issue #39 item 7: a
// DECIMAL/NUMERIC stored as a binary SMALLINT/INTEGER/BIGINT carries
// the real type's Scale in the descriptor while the raw integer is the
// unscaled value. The decoder must render the scaled decimal string
// rather than the raw integer. A zero scale keeps the native int type.
func TestDecodeColumnScaledInteger(t *testing.T) {
	cases := []struct {
		name    string
		sqlType uint16
		raw     int64
		scale   uint16
		length  uint32
		want    any
	}{
		// INTEGER scaled: 12345 with scale 2 -> "123.45".
		{"int_scale2", 496, 12345, 2, 4, "123.45"},
		// Negative INTEGER scaled.
		{"int_neg_scale2", 497, -12345, 2, 4, "-123.45"},
		// SMALLINT scaled: 7 with scale 3 -> "0.007" (scale > digits).
		{"smallint_scale3", 500, 7, 3, 2, "0.007"},
		// BIGINT ignores scale (JT400 SQLBigint has no scale parameter
		// and never applies movePointLeft): renders as the raw int64.
		{"bigint_scale5_unscaled", 492, 100000, 5, 8, int64(100000)},
		// Scale 0 keeps the native integer type, not a string.
		{"int_scale0", 496, 42, 0, 4, int32(42)},
		{"smallint_scale0", 500, -9, 0, 2, int16(-9)},
		{"bigint_scale0", 492, 9223372036854775807, 0, 8, int64(9223372036854775807)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var wire []byte
			switch tc.length {
			case 2:
				wire = make([]byte, 2)
				binary.BigEndian.PutUint16(wire, uint16(int16(tc.raw)))
			case 4:
				wire = make([]byte, 4)
				binary.BigEndian.PutUint32(wire, uint32(int32(tc.raw)))
			case 8:
				wire = make([]byte, 8)
				binary.BigEndian.PutUint64(wire, uint64(tc.raw))
			}
			col := SelectColumn{Name: "N", SQLType: tc.sqlType, Length: tc.length, Scale: tc.scale}
			got, n, err := decodeColumn(wire, col)
			if err != nil {
				t.Fatalf("decodeColumn: %v", err)
			}
			if n != int(tc.length) {
				t.Errorf("consumed = %d, want %d", n, tc.length)
			}
			if got != tc.want {
				t.Errorf("decoded = %v (%T), want %v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

// TestScaledIntegerString unit-tests the scaled-decimal renderer in
// isolation, including the boundary cases the decodeColumn table
// doesn't reach (scale equal to the digit count, single digit).
func TestScaledIntegerString(t *testing.T) {
	cases := []struct {
		v     int64
		scale int
		want  string
	}{
		{12345, 2, "123.45"},
		{-12345, 2, "-123.45"},
		{5, 1, "0.5"},
		{5, 2, "0.05"},
		{99, 2, "0.99"},
		{100, 2, "1.00"},
		{0, 3, "0.000"},
		{-1, 4, "-0.0001"},
		{123, 0, "123"},
		{-123, 0, "-123"},
	}
	for _, tc := range cases {
		got := scaledIntegerString(tc.v, tc.scale)
		if got != tc.want {
			t.Errorf("scaledIntegerString(%d, %d) = %q, want %q", tc.v, tc.scale, got, tc.want)
		}
	}
}
