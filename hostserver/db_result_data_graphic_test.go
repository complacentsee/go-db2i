package hostserver

import (
	"encoding/binary"
	"testing"
)

// Graphic-column decode tests for issue #3: scalar GRAPHIC (468/469),
// VARGRAPHIC (464/465), and LONG VARGRAPHIC (472/473). The golden
// wire bytes here were captured live against PUB400 V7R5M0 via
// CAST(... AS [VAR]GRAPHIC(n) CCSID 1200) literals, so they match the
// real server layout rather than an assumed one:
//
//	VARGRAPHIC(10) 'ABC' -> 00 03 | 00 41 00 42 00 43        (SL=char count)
//	GRAPHIC(5)     'ABC' -> 00 41 00 42 00 43 00 20 00 20    (no SL, padded)

// TestDecodeColumnVarGraphic confirms a VARGRAPHIC column with a
// 2-byte SL = GRAPHIC CHARACTER count decodes its 2*count payload
// bytes from UTF-16 BE (CCSID 1200) and reports consumed = 2 + 2*SL.
func TestDecodeColumnVarGraphic(t *testing.T) {
	wire := []byte{0x00, 0x03, 0x00, 0x41, 0x00, 0x42, 0x00, 0x43} // SL=3, "ABC"
	col := SelectColumn{SQLType: 464, Length: 22, CCSID: 1200, Name: "V"}
	got, n, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if n != 8 { // 2-byte SL + 6 payload bytes
		t.Errorf("consumed = %d, want 8", n)
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("got type %T, want string", got)
	}
	if s != "ABC" {
		t.Errorf("decoded = %q, want %q", s, "ABC")
	}
}

// TestDecodeColumnLongVarGraphic confirms LONG VARGRAPHIC (473) shares
// the VARGRAPHIC wire shape (2-byte SL char count + UTF-16 BE
// payload). Mirrors QSYS2.SYSVIEWS.VIEW_DEFINITION, the issue's
// zero-setup reproducer.
func TestDecodeColumnLongVarGraphic(t *testing.T) {
	wire := []byte{0x00, 0x02, 0x00, 0x48, 0x00, 0x69} // SL=2, "Hi"
	col := SelectColumn{SQLType: 473, Length: 10002, CCSID: 1200, Name: "VIEW_DEFINITION"}
	got, n, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if n != 6 {
		t.Errorf("consumed = %d, want 6", n)
	}
	if s, _ := got.(string); s != "Hi" {
		t.Errorf("decoded = %q, want %q", s, "Hi")
	}
}

// TestDecodeColumnGraphicFixed confirms GRAPHIC (468) is FIXED-length:
// no SL prefix, payload = col.Length bytes, with U+0020 graphic-space
// padding preserved in the returned string (JDBC returns CHAR-like
// types space-padded). consumed = col.Length.
func TestDecodeColumnGraphicFixed(t *testing.T) {
	// GRAPHIC(5) value "ABC" padded with two U+0020 spaces.
	wire := []byte{0x00, 0x41, 0x00, 0x42, 0x00, 0x43, 0x00, 0x20, 0x00, 0x20}
	col := SelectColumn{SQLType: 468, Length: 10, CCSID: 1200, Name: "G"}
	got, n, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if n != 10 {
		t.Errorf("consumed = %d, want 10 (fixed col.Length)", n)
	}
	if s, _ := got.(string); s != "ABC  " {
		t.Errorf("decoded = %q, want %q", s, "ABC  ")
	}
}

// TestDecodeColumnGraphicUCS2_13488 confirms CCSID 13488 (strict
// UCS-2 BE) decodes through the same UTF-16 BE helper as CCSID 1200
// for BMP code points.
func TestDecodeColumnGraphicUCS2_13488(t *testing.T) {
	wire := []byte{0x00, 0x02, 0x00, 0x41, 0x00, 0x42} // SL=2, "AB"
	col := SelectColumn{SQLType: 465, Length: 22, CCSID: 13488, Name: "V"}
	got, _, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if s, _ := got.(string); s != "AB" {
		t.Errorf("decoded = %q, want %q", s, "AB")
	}
}

// TestDecodeColumnVarGraphicNonBMP confirms CCSID 1200 (true UTF-16,
// surrogate pairs allowed) reconstructs a non-BMP rune. U+1D11E
// (musical treble clef) is the surrogate pair D834 DD1E = two code
// units, so SL=2 with a 4-byte payload.
func TestDecodeColumnVarGraphicNonBMP(t *testing.T) {
	wire := []byte{0x00, 0x02, 0xD8, 0x34, 0xDD, 0x1E} // SL=2 code units, U+1D11E
	col := SelectColumn{SQLType: 464, Length: 22, CCSID: 1200, Name: "V"}
	got, n, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if n != 6 {
		t.Errorf("consumed = %d, want 6", n)
	}
	if s, _ := got.(string); s != "\U0001D11E" {
		t.Errorf("decoded = %q, want %q", s, "\U0001D11E")
	}
}

// TestDecodeColumnGraphicBitData confirms a graphic column tagged
// CCSID 65535 (FOR BIT DATA) returns raw bytes rather than running
// them through the UTF-16 decoder. Covers both the variable
// (VARGRAPHIC) and fixed (GRAPHIC) forms.
func TestDecodeColumnGraphicBitData(t *testing.T) {
	t.Run("vargraphic bit data", func(t *testing.T) {
		wire := []byte{0x00, 0x02, 0xDE, 0xAD, 0xBE, 0xEF} // SL=2 -> 4 payload bytes
		col := SelectColumn{SQLType: 464, Length: 22, CCSID: ccsidBinary, Name: "V"}
		got, n, err := decodeColumn(wire, col)
		if err != nil {
			t.Fatalf("decodeColumn: %v", err)
		}
		if n != 6 {
			t.Errorf("consumed = %d, want 6", n)
		}
		b, ok := got.([]byte)
		if !ok {
			t.Fatalf("got type %T, want []byte", got)
		}
		if want := []byte{0xDE, 0xAD, 0xBE, 0xEF}; !bytesEqual(b, want) {
			t.Errorf("bytes = % X, want % X", b, want)
		}
	})
	t.Run("graphic fixed bit data", func(t *testing.T) {
		wire := []byte{0xCA, 0xFE, 0xBA, 0xBE} // col.Length=4, no SL
		col := SelectColumn{SQLType: 468, Length: 4, CCSID: ccsidBinary, Name: "G"}
		got, n, err := decodeColumn(wire, col)
		if err != nil {
			t.Fatalf("decodeColumn: %v", err)
		}
		if n != 4 {
			t.Errorf("consumed = %d, want 4", n)
		}
		b, ok := got.([]byte)
		if !ok {
			t.Fatalf("got type %T, want []byte", got)
		}
		if want := []byte{0xCA, 0xFE, 0xBA, 0xBE}; !bytesEqual(b, want) {
			t.Errorf("bytes = % X, want % X", b, want)
		}
	})
}

// TestDecodeColumnVarGraphicBounds covers the length-validation
// guards on the variable graphic path.
func TestDecodeColumnVarGraphicBounds(t *testing.T) {
	col := SelectColumn{SQLType: 464, Length: 8, CCSID: 1200, Name: "V"} // max 3 chars

	t.Run("header too short", func(t *testing.T) {
		if _, _, err := decodeColumn([]byte{0x00}, col); err == nil {
			t.Error("expected error for 1-byte header, got nil")
		}
	})
	t.Run("declared length exceeds column max", func(t *testing.T) {
		// SL=10 chars -> 20 payload bytes > col.Length 8.
		if _, _, err := decodeColumn([]byte{0x00, 0x0A, 0x00, 0x41}, col); err == nil {
			t.Error("expected error for SL past column max, got nil")
		}
	})
	t.Run("payload truncated", func(t *testing.T) {
		// SL=3 -> wants 6 payload bytes, only 2 present.
		if _, _, err := decodeColumn([]byte{0x00, 0x03, 0x00, 0x41}, col); err == nil {
			t.Error("expected error for truncated payload, got nil")
		}
	})
}

// TestIsVarLengthSQLTypeGraphic locks in the classification the wire
// proved: VARGRAPHIC (464/465) and LONG VARGRAPHIC (472/473) carry a
// 2-byte SL prefix, but GRAPHIC (468/469) is FIXED-length and must
// NOT be treated as variable -- otherwise a NULL GRAPHIC column in a
// VLF row null-skips 2 bytes instead of col.Length and shifts every
// subsequent column.
func TestIsVarLengthSQLTypeGraphic(t *testing.T) {
	variable := []uint16{464, 465, 472, 473}
	for _, ty := range variable {
		if !isVarLengthSQLType(ty) {
			t.Errorf("isVarLengthSQLType(%d) = false, want true (var-graphic)", ty)
		}
	}
	fixed := []uint16{468, 469}
	for _, ty := range fixed {
		if isVarLengthSQLType(ty) {
			t.Errorf("isVarLengthSQLType(%d) = true, want false (GRAPHIC is fixed-length)", ty)
		}
	}
}

// TestDecodeRowGraphicAlignment is the row-walker regression guard:
// a graphic column followed by an INTEGER must leave the INTEGER
// correctly aligned in a VLF-compressed row. This exercises the
// consumed-byte return values (fixed col.Length for GRAPHIC, 2+2*SL
// for VARGRAPHIC) and the null-skip math through decodeRow.
func TestDecodeRowGraphicAlignment(t *testing.T) {
	be := binary.BigEndian
	intCol := SelectColumn{SQLType: SQLTypeInteger, Length: 4, CCSID: 0, Name: "N"}

	// indicators: 2 columns, both not-null unless noted.
	notNull := func() []byte { return []byte{0x00, 0x00, 0x00, 0x00} }
	col0Null := func() []byte { return []byte{0xFF, 0xFF, 0x00, 0x00} }

	intBytes := func(v int32) []byte {
		b := make([]byte, 4)
		be.PutUint32(b, uint32(v))
		return b
	}

	t.Run("vargraphic then int", func(t *testing.T) {
		graphic := SelectColumn{SQLType: 464, Length: 22, CCSID: 1200, Name: "V"}
		row := append([]byte{0x00, 0x03, 0x00, 0x41, 0x00, 0x42, 0x00, 0x43}, intBytes(12345)...)
		got, _, err := decodeRow(row, []SelectColumn{graphic, intCol}, notNull(), 2, false)
		if err != nil {
			t.Fatalf("decodeRow: %v", err)
		}
		if s, _ := got[0].(string); s != "ABC" {
			t.Errorf("col0 = %q, want %q", s, "ABC")
		}
		if got[1] != int32(12345) {
			t.Errorf("col1 = %v, want 12345 (alignment off)", got[1])
		}
	})

	t.Run("fixed graphic then int", func(t *testing.T) {
		graphic := SelectColumn{SQLType: 468, Length: 10, CCSID: 1200, Name: "G"}
		row := append([]byte{0x00, 0x41, 0x00, 0x42, 0x00, 0x43, 0x00, 0x20, 0x00, 0x20}, intBytes(67890)...)
		got, _, err := decodeRow(row, []SelectColumn{graphic, intCol}, notNull(), 2, false)
		if err != nil {
			t.Fatalf("decodeRow: %v", err)
		}
		if s, _ := got[0].(string); s != "ABC  " {
			t.Errorf("col0 = %q, want %q", s, "ABC  ")
		}
		if got[1] != int32(67890) {
			t.Errorf("col1 = %v, want 67890 (fixed-slot alignment off)", got[1])
		}
	})

	t.Run("null fixed graphic then int (isVarLengthSQLType regression)", func(t *testing.T) {
		// NULL GRAPHIC must null-skip col.Length (10) bytes, NOT 2.
		graphic := SelectColumn{SQLType: 468, Length: 10, CCSID: 1200, Name: "G"}
		row := append(make([]byte, 10), intBytes(777)...) // 10 skipped bytes + int
		got, _, err := decodeRow(row, []SelectColumn{graphic, intCol}, col0Null(), 2, false)
		if err != nil {
			t.Fatalf("decodeRow: %v", err)
		}
		if got[0] != nil {
			t.Errorf("col0 = %v, want nil", got[0])
		}
		if got[1] != int32(777) {
			t.Errorf("col1 = %v, want 777 (null GRAPHIC skipped wrong width)", got[1])
		}
	})

	t.Run("null vargraphic then int", func(t *testing.T) {
		// NULL VARGRAPHIC null-skips just its 2-byte SL prefix.
		graphic := SelectColumn{SQLType: 464, Length: 22, CCSID: 1200, Name: "V"}
		row := append([]byte{0x00, 0x00}, intBytes(888)...) // 2 skipped bytes + int
		got, _, err := decodeRow(row, []SelectColumn{graphic, intCol}, col0Null(), 2, false)
		if err != nil {
			t.Fatalf("decodeRow: %v", err)
		}
		if got[0] != nil {
			t.Errorf("col0 = %v, want nil", got[0])
		}
		if got[1] != int32(888) {
			t.Errorf("col1 = %v, want 888 (null VARGRAPHIC skipped wrong width)", got[1])
		}
	})
}
