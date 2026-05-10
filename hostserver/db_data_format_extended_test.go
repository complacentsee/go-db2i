package hostserver

import (
	"encoding/binary"
	"testing"
)

// TestEnrichWithExtendedColumnDescriptors pins the CP 0x3811 parser
// against a hand-crafted synthetic payload. Layout per JT400's
// DBExtendedColumnDescriptors + DBColumnDescriptorsDataFormat:
//
//	header  10 bytes (numCols=2 + 6 reserved)
//	fixed   2 * 16-byte records, one per column
//	         offset/length point to the variable-info section
//	variable LL/CP/value records: 0x3900 / 0x3901 / 0x3904
//
// The offset field in each fixed record measures from the LL start
// of the CP 0x3811 wrapper (i.e. 6 bytes before payload[0] in our
// post-LL/CP slice); the helper subtracts the 6 internally.
func TestEnrichWithExtendedColumnDescriptors(t *testing.T) {
	be := binary.BigEndian

	// Build the variable-info section for col 0: schema=GOTEST,
	// table=USERS, base column=NAME.
	col0 := buildExtRec(0x3900, []byte{0xD5, 0x81, 0x94, 0x85}) // "Name" in CCSID 37
	col0 = append(col0, buildExtRec(0x3901, []byte{0xE4, 0xE2, 0xC5, 0xD9, 0xE2})...)             // "USERS"
	col0 = append(col0, buildExtRec(0x3904, []byte{0xC7, 0xD6, 0xE3, 0xC5, 0xE2, 0xE3})...)       // "GOTEST"

	// Col 1: schema=PROD, table=ORDERS, base column=ID, label=Order ID (CCSID 37).
	col1 := buildExtRec(0x3900, []byte{0xC9, 0xC4})                                                  // "ID"
	col1 = append(col1, buildExtRec(0x3901, []byte{0xD6, 0xD9, 0xC4, 0xC5, 0xD9, 0xE2})...)          // "ORDERS"
	col1 = append(col1, buildExtRec(0x3904, []byte{0xD7, 0xD9, 0xD6, 0xC4})...)                      // "PROD"
	// Label with CCSID 37: "Order ID"
	col1Label := append([]byte{0x00, 0x25}, []byte{0xD6, 0x99, 0x84, 0x85, 0x99, 0x40, 0xC9, 0xC4}...)
	col1 = append(col1, buildExtRecRaw(0x3902, col1Label)...)

	// Compose: header (10) + 2*16 fixed records + variable data.
	fixedLen := 2 * 16
	headerLen := 10
	varStartFromLL := 6 + headerLen + fixedLen
	col0VarLen := len(col0)
	col1VarLen := len(col1)

	payload := make([]byte, headerLen+fixedLen)
	be.PutUint32(payload[0:4], 2) // numCols
	// 6 reserved bytes (zeros) at payload[4:10]

	// Col 0 fixed record at payload[10..26]
	payload[10] = 0x01 // updateable
	payload[11] = 0x01 // searchable
	be.PutUint16(payload[12:14], 0) // attributes
	be.PutUint32(payload[14:18], uint32(varStartFromLL))
	be.PutUint32(payload[18:22], uint32(col0VarLen))
	// 12..16 reserved

	// Col 1 fixed record at payload[26..42]
	payload[26] = 0x01
	payload[27] = 0x01
	be.PutUint16(payload[28:30], 0)
	be.PutUint32(payload[30:34], uint32(varStartFromLL+col0VarLen))
	be.PutUint32(payload[34:38], uint32(col1VarLen))

	payload = append(payload, col0...)
	payload = append(payload, col1...)

	cols := []SelectColumn{
		{Name: "NAME", SQLType: 448, CCSID: 37},
		{Name: "ID", SQLType: 496, CCSID: 0},
	}
	enrichWithExtendedColumnDescriptors(cols, payload)

	t.Run("col 0", func(t *testing.T) {
		if cols[0].Schema != "GOTEST" {
			t.Errorf("Schema = %q, want GOTEST", cols[0].Schema)
		}
		if cols[0].Table != "USERS" {
			t.Errorf("Table = %q, want USERS", cols[0].Table)
		}
		if cols[0].BaseColumnName != "Name" {
			t.Errorf("BaseColumnName = %q, want Name", cols[0].BaseColumnName)
		}
	})
	t.Run("col 1", func(t *testing.T) {
		if cols[1].Schema != "PROD" {
			t.Errorf("Schema = %q, want PROD", cols[1].Schema)
		}
		if cols[1].Table != "ORDERS" {
			t.Errorf("Table = %q, want ORDERS", cols[1].Table)
		}
		if cols[1].BaseColumnName != "ID" {
			t.Errorf("BaseColumnName = %q, want ID", cols[1].BaseColumnName)
		}
		if cols[1].Label != "Order ID" {
			t.Errorf("Label = %q, want %q", cols[1].Label, "Order ID")
		}
	})
}

// TestEnrichWithExtendedColumnDescriptorsEmpty confirms the helper
// no-ops cleanly on a payload that's too short to carry the
// 10-byte header or whose numCols field is zero. Mirrors the
// "extended metadata bit set but server didn't include it" case.
func TestEnrichWithExtendedColumnDescriptorsEmpty(t *testing.T) {
	cols := []SelectColumn{{Name: "X"}}
	enrichWithExtendedColumnDescriptors(cols, []byte{0, 0, 0, 0})  // too short
	if cols[0].Schema != "" || cols[0].Table != "" {
		t.Errorf("short payload leaked: %+v", cols[0])
	}
	enrichWithExtendedColumnDescriptors(cols, make([]byte, 10)) // numCols=0
	if cols[0].Schema != "" || cols[0].Table != "" {
		t.Errorf("numCols=0 leaked: %+v", cols[0])
	}
}

// buildExtRec wraps `value` in a LL/CP record (no CCSID prefix).
// Used for CPs 0x3900, 0x3901, 0x3904.
func buildExtRec(cp uint16, value []byte) []byte {
	out := make([]byte, 6+len(value))
	binary.BigEndian.PutUint32(out[0:4], uint32(6+len(value)))
	binary.BigEndian.PutUint16(out[4:6], cp)
	copy(out[6:], value)
	return out
}

// buildExtRecRaw wraps `valueWithPrefix` in a LL/CP record where the
// caller has already prefixed any CP-specific bytes (e.g. the CCSID
// prefix that 0x3902 carries).
func buildExtRecRaw(cp uint16, valueWithPrefix []byte) []byte {
	out := make([]byte, 6+len(valueWithPrefix))
	binary.BigEndian.PutUint32(out[0:4], uint32(6+len(valueWithPrefix)))
	binary.BigEndian.PutUint16(out[4:6], cp)
	copy(out[6:], valueWithPrefix)
	return out
}
