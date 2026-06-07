package hostserver

import (
	"encoding/binary"
	"os"
	"testing"
)

// TestArrayCrossesWireAsParameterNotResultColumn is the offline,
// byte-exact half of the issue #39 ARRAY finding: on DB2 for i an ARRAY
// value is delivered to a host-server client ONLY as a stored-procedure
// array parameter, never as a SELECT result-set column. (The live half
// is conformance/result_types_test.go's "ARRAY not a result column"
// subtest, which shows the server rejects an array projection with
// SQL-20441 / SQLSTATE 428H2 before any row data.)
//
// The fixture testdata/array_param_describe_3813.bin is the CP 0x3813
// (super-extended parameter-marker format) payload captured live from
// PUB400 V7R5M0 when describing
//
//	CREATE TYPE T AS INTEGER ARRAY[10]
//	CREATE PROCEDURE P (OUT A T) LANGUAGE SQL BEGIN SET A = ARRAY[..]; END
//	CALL P(?)
//
// It demonstrates the only wire shape an ARRAY actually takes:
//
//   - It rides in the PARAMETER-marker descriptor (CP 0x3813), not the
//     result-set data format (CP 0x3812).
//   - The parameter's SQL type is the ELEMENT type (497 = INTEGER
//     nullable), with the element length (4). There is no "array" SQL
//     type number on the wire.
//   - Array-ness is a single FLAG BIT: the 4-byte field-flags int at
//     per-field record offset +21 has bit 30 set (0x40000000), matching
//     JT400's DBSuperExtendedDataFormat.getArrayType ((flag>>30)&1).
//   - The parameter-direction byte at record offset +14 is 0xF1 (OUT),
//     confirming this is an output parameter -- the only context in
//     which JT400's JDServerRow array decode is reachable (it is keyed
//     on getVariableOutputIndex / isOutput and the CP 0x3901
//     DBVariableData "output parms" carrier).
//
// go-db2i intentionally does not decode arrays: they cannot reach the
// result-column decoder (decodeColumn) over a SELECT, and the
// procedure-array-parameter path would require describing/binding array
// UDT parameters plus parsing reply CP 0x3901 -- a separate feature, not
// a result-type decode gap. This test pins the evidence so the finding
// stays anchored to a real capture if that feature is ever revisited.
func TestArrayCrossesWireAsParameterNotResultColumn(t *testing.T) {
	payload, err := os.ReadFile("testdata/array_param_describe_3813.bin")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	// It parses cleanly as a super-extended parameter-marker format.
	fields, err := parseSuperExtendedParameterMarkerFormat(payload)
	if err != nil {
		t.Fatalf("parseSuperExtendedParameterMarkerFormat: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("got %d parameter fields, want 1", len(fields))
	}
	f := fields[0]

	// The descriptor carries the ELEMENT type, not an array type code.
	if f.SQLType != 497 {
		t.Errorf("param SQLType = %d, want 497 (INTEGER nullable, the array element type)", f.SQLType)
	}
	if f.FieldLength != 4 {
		t.Errorf("param FieldLength = %d, want 4 (INTEGER element width)", f.FieldLength)
	}
	// 0xF1 = OUT direction. The array decode path in JT400 is reachable
	// only for OUT/INOUT (0xF1/0xF2) parameters.
	if f.ParamType != 0xF1 {
		t.Errorf("param direction byte = 0x%02X, want 0xF1 (OUT)", f.ParamType)
	}

	// Array-ness is the flag bit, independent of the SQL type. Read the
	// 4-byte field-flags int at the per-field record offset +21
	// (record 0 starts at the 16-byte header; flag int at 16+21).
	const headerLen = 16
	const flagRelOff = 21
	flag := binary.BigEndian.Uint32(payload[headerLen+flagRelOff : headerLen+flagRelOff+4])
	if got := (flag >> 30) & 1; got != 1 {
		t.Errorf("array flag bit (flag=0x%08X >>30 &1) = %d, want 1 (this column IS an array parameter)", flag, got)
	}

	// Issue #68 Phase 1: the production parser now exposes the array
	// flag + declared cardinality directly, so callers no longer repeat
	// the offset math above.
	if !f.IsArray {
		t.Errorf("parsed field IsArray = false, want true")
	}
	if f.Flags != flag {
		t.Errorf("parsed field Flags = 0x%08X, want 0x%08X (raw +21 field-flags int)", f.Flags, flag)
	}
	if f.ArrayMaxCardinality != 10 {
		t.Errorf("parsed field ArrayMaxCardinality = %d, want 10 (INTEGER ARRAY[10])", f.ArrayMaxCardinality)
	}
}
