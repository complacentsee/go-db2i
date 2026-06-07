package hostserver

import (
	"encoding/binary"
	"fmt"
)

// db_array.go implements the stored-procedure ARRAY parameter wire codecs
// for issue #68. On DB2 for i an ARRAY crosses the host-server wire only
// as a procedure parameter (IN/OUT/INOUT), never a result column
// (#39 / SQL-20441). When any parameter is an array, the EXECUTE request's
// value block is CP 0x382F (DBVariableData) instead of the scalar
// CP 0x381F, and the OUT/INOUT values come back in CP 0x3901
// (DBVariableData) instead of the scalar CP 0x380E.
//
// The byte layout (HEADER -> INDICATORS -> DATA, fixed element stride)
// was captured live from JT400 21.0.4 on PUB400 V7R5M0 and confirmed in
// Phase 0; see testdata/jtopen-fixtures/array_param_phase0.md and the
// offline fixtures array_param_execute_382f.bin / array_param_reply_3901.bin.

const (
	// cpDBVariableData is the IN/INOUT request value block when any
	// parameter is an array (replaces the scalar cpDBExtendedData 0x381F).
	cpDBVariableData uint16 = 0x382F
	// cpDBVariableReplyData is the OUT/INOUT reply value block for array
	// CALLs (replaces the scalar 0x380E synthetic OUT row).
	cpDBVariableReplyData uint16 = 0x3901
)

// ArrayValue carries a stored-procedure ARRAY parameter's elements across
// the hostserver boundary, in both directions:
//
//   - IN/INOUT: set as a PreparedParam.Value; EncodeDBVariableData
//     serializes it into CP 0x382F.
//   - OUT/INOUT: produced by parseVariableResultData from a CP 0x3901
//     reply for the driver to scan into a db2i.Array[T].
//
// Elements holds one decoded element per slot; a nil entry is a NULL
// element. Null marks a whole-array SQL NULL, distinct from an empty
// Elements (an array with zero elements).
type ArrayValue struct {
	Elements []any
	Null     bool
}

// anyArrayParam reports whether any parameter is an array, which switches
// the EXECUTE request's value block to CP 0x382F. Mirrors JT400's
// row-level containsArray_ flag (set when any field is NATIVE_ARRAY).
func anyArrayParam(params []PreparedParam) bool {
	for i := range params {
		if params[i].IsArray {
			return true
		}
	}
	return false
}

// arrayValueOf extracts the ArrayValue from a bound array parameter's
// value, returning (value, isWholeNull, cardinality, error).
func arrayValueOf(v any) (ArrayValue, bool, int, error) {
	av, ok := v.(ArrayValue)
	if !ok {
		return ArrayValue{}, false, 0, fmt.Errorf("hostserver: array parameter value is %T, want hostserver.ArrayValue", v)
	}
	if av.Null {
		return av, true, 0, nil
	}
	return av, false, len(av.Elements), nil
}

// elementTypeForWire returns the NOT-NULL (even) form of an element/
// scalar SQL type code for the CP 0x382F descriptor. Per-element (and
// per-scalar) nullability is carried by the 2-byte indicator, not the
// type code, so JT400 always writes the not-null code in the descriptor
// even when the describe reported the nullable code (e.g. INTEGER 497 ->
// 496, VARCHAR 449 -> 448). Verified against array_param_execute_382f.bin.
func elementTypeForWire(sqlType uint16) uint16 { return sqlType &^ 1 }

// EncodeDBVariableData builds the CP 0x382F payload for the EXECUTE
// request when any parameter is an array. It carries only INPUT
// parameters (IN and INOUT 0xF2; OUT-only 0xF1 is excluded, matching
// JT400's parameterInputCount_ sizing -- a pure-OUT array proc therefore
// gets a column count of 0). Layout (big-endian):
//
//	[4B consistency token = 1]
//	[2B column count = number of input params]
//	descriptors, in input order:
//	  non-null array : 0x9911 | elemType(2) | elemDataLen(4) | cardinality(4)  (12B)
//	  whole-null array: 0x9911 0xFFFF                                          (4B)
//	  scalar          : 0x9912 | sqlType(2)  | dataLen(4)                      (8B)
//	indicators (contiguous, per column then per element), 2B each
//	  (0x0000 not null, 0xFFFF null); arrays contribute `cardinality`,
//	  scalars 1, a whole-null array 0.
//	data (contiguous), each element/value at its fixed FieldLength stride
//	  via encodeScalarValue; a null element/scalar leaves a zeroed slot;
//	  a whole-null array contributes nothing.
func EncodeDBVariableData(params []PreparedParam, values []any) ([]byte, error) {
	if len(params) != len(values) {
		return nil, fmt.Errorf("hostserver: EncodeDBVariableData shape/value count mismatch (%d shapes, %d values)", len(params), len(values))
	}
	be := binary.BigEndian

	type inCol struct {
		p PreparedParam
		v any
	}
	var inputs []inCol
	for i := range params {
		if params[i].ParamType == 0xF1 { // OUT-only: no request-side data
			continue
		}
		inputs = append(inputs, inCol{params[i], values[i]})
	}

	// Size the three regions in one pass so the buffer is allocated once.
	descLen, indLen, dataLen := 0, 0, 0
	for _, c := range inputs {
		if c.p.IsArray {
			_, isNull, n, err := arrayValueOf(c.v)
			if err != nil {
				return nil, err
			}
			if isNull {
				descLen += 4
				continue
			}
			descLen += 12
			indLen += n * 2
			dataLen += n * int(c.p.FieldLength)
		} else {
			descLen += 8
			indLen += 2
			dataLen += int(c.p.FieldLength)
		}
	}

	const headerLen = 6 // 4B token + 2B column count
	buf := make([]byte, headerLen+descLen+indLen+dataLen)
	be.PutUint32(buf[0:4], 1)
	be.PutUint16(buf[4:6], uint16(len(inputs)))

	// Descriptors.
	off := headerLen
	for _, c := range inputs {
		if c.p.IsArray {
			_, isNull, n, _ := arrayValueOf(c.v)
			be.PutUint16(buf[off:off+2], 0x9911)
			if isNull {
				be.PutUint16(buf[off+2:off+4], 0xFFFF)
				off += 4
				continue
			}
			be.PutUint16(buf[off+2:off+4], elementTypeForWire(c.p.SQLType))
			be.PutUint32(buf[off+4:off+8], c.p.FieldLength)
			be.PutUint32(buf[off+8:off+12], uint32(n))
			off += 12
		} else {
			be.PutUint16(buf[off:off+2], 0x9912)
			be.PutUint16(buf[off+2:off+4], elementTypeForWire(c.p.SQLType))
			be.PutUint32(buf[off+4:off+8], c.p.FieldLength)
			off += 8
		}
	}

	// Indicators (all columns, contiguous).
	for _, c := range inputs {
		if c.p.IsArray {
			av, isNull, n, _ := arrayValueOf(c.v)
			if isNull {
				continue
			}
			for e := 0; e < n; e++ {
				if av.Elements[e] == nil {
					be.PutUint16(buf[off:off+2], 0xFFFF)
				} else {
					be.PutUint16(buf[off:off+2], 0)
				}
				off += 2
			}
		} else {
			if c.v == nil {
				be.PutUint16(buf[off:off+2], 0xFFFF)
			} else {
				be.PutUint16(buf[off:off+2], 0)
			}
			off += 2
		}
	}

	// Data (all columns, fixed element stride).
	for ci, c := range inputs {
		if c.p.IsArray {
			av, isNull, n, _ := arrayValueOf(c.v)
			if isNull {
				continue
			}
			for e := 0; e < n; e++ {
				if av.Elements[e] == nil {
					off += int(c.p.FieldLength) // null element: zeroed slot retained
					continue
				}
				if err := encodeScalarValue(buf, off, c.p, av.Elements[e], "", ci); err != nil {
					return nil, fmt.Errorf("hostserver: array param %d element %d: %w", ci, e, err)
				}
				off += int(c.p.FieldLength)
			}
		} else {
			if c.v == nil {
				off += int(c.p.FieldLength)
				continue
			}
			if err := encodeScalarValue(buf, off, c.p, c.v, "", ci); err != nil {
				return nil, err
			}
			off += int(c.p.FieldLength)
		}
	}
	return buf, nil
}

// findVariableResultData returns the CP 0x3901 DBVariableData reply
// payload (OUT/INOUT array + scalar values), or nil if the reply doesn't
// carry it. Mirrors findExtendedResultData's role for the scalar 0x380E.
func (r *DBReply) findVariableResultData() []byte {
	for i := range r.Params {
		if r.Params[i].CodePoint == cpDBVariableReplyData {
			return r.Params[i].Data
		}
	}
	return nil
}

// parseVariableResultData decodes a CP 0x3901 reply into one value per
// OUT/INOUT parameter slot, aligned to shapes (IN-only slots stay nil).
// The reply carries ONLY OUT/INOUT columns, in OUT order (NOTE 1), so we
// pair reply column c with the c-th OUT/INOUT shape slot and decode each
// element with that slot's describe-side CCSID/scale/precision. Array
// columns decode into an ArrayValue; scalar columns into a single value
// (a nil for a SQL-NULL scalar). Trailing bytes beyond the structure
// (JT400 emits 4 on the reply) are ignored.
func parseVariableResultData(data []byte, shapes []PreparedParam) ([]any, []uint16, error) {
	be := binary.BigEndian
	if len(data) < 6 {
		return nil, nil, fmt.Errorf("hostserver: 0x3901 reply too short: %d bytes", len(data))
	}

	// OUT/INOUT shape slots, in declaration order -> the reply columns.
	var outSlots []int
	for i := range shapes {
		if shapes[i].ParamType == 0xF1 || shapes[i].ParamType == 0xF2 {
			outSlots = append(outSlots, i)
		}
	}
	colCount := int(be.Uint16(data[4:6]))
	if colCount != len(outSlots) {
		return nil, nil, fmt.Errorf("hostserver: 0x3901 column count %d != OUT/INOUT param count %d", colCount, len(outSlots))
	}

	type repCol struct {
		isArray, nullArr bool
		elemType         uint16
		elemLen          uint32
		card             int
	}
	rcs := make([]repCol, colCount)
	off := 6
	for c := 0; c < colCount; c++ {
		if off+2 > len(data) {
			return nil, nil, fmt.Errorf("hostserver: 0x3901 descriptor %d truncated", c)
		}
		switch tag := be.Uint16(data[off : off+2]); tag {
		case 0x9911:
			if off+4 > len(data) {
				return nil, nil, fmt.Errorf("hostserver: 0x3901 array descriptor %d truncated", c)
			}
			if be.Uint16(data[off+2:off+4]) == 0xFFFF {
				rcs[c] = repCol{isArray: true, nullArr: true}
				off += 4
			} else {
				if off+12 > len(data) {
					return nil, nil, fmt.Errorf("hostserver: 0x3901 array descriptor %d truncated", c)
				}
				rcs[c] = repCol{
					isArray:  true,
					elemType: be.Uint16(data[off+2 : off+4]),
					elemLen:  be.Uint32(data[off+4 : off+8]),
					card:     int(be.Uint32(data[off+8 : off+12])),
				}
				off += 12
			}
		case 0x9912:
			if off+8 > len(data) {
				return nil, nil, fmt.Errorf("hostserver: 0x3901 scalar descriptor %d truncated", c)
			}
			rcs[c] = repCol{
				elemType: be.Uint16(data[off+2 : off+4]),
				elemLen:  be.Uint32(data[off+4 : off+8]),
				card:     1,
			}
			off += 8
		default:
			return nil, nil, fmt.Errorf("hostserver: 0x3901 descriptor %d unknown tag 0x%04X", c, tag)
		}
	}

	// Indicators precede all data; compute the data-region start.
	indStart := off
	totalInd := 0
	for _, rc := range rcs {
		if rc.nullArr {
			continue
		}
		totalInd += rc.card
	}
	dataStart := indStart + totalInd*2

	out := make([]any, len(shapes))
	types := make([]uint16, len(shapes))
	indPos, dataPos := indStart, dataStart
	for c, rc := range rcs {
		slot := outSlots[c]
		shape := shapes[slot]
		elemCol := SelectColumn{
			SQLType:   rc.elemType,
			Length:    rc.elemLen,
			Scale:     shape.Scale,
			Precision: shape.Precision,
			CCSID:     shape.CCSID,
		}
		types[slot] = rc.elemType

		decodeOne := func() (any, error) {
			if indPos+2 > len(data) || dataPos+int(rc.elemLen) > len(data) {
				return nil, fmt.Errorf("hostserver: 0x3901 col %d data truncated", c)
			}
			ind := be.Uint16(data[indPos : indPos+2])
			indPos += 2
			b := data[dataPos : dataPos+int(rc.elemLen)]
			dataPos += int(rc.elemLen)
			if ind == 0xFFFF {
				return nil, nil
			}
			v, _, err := decodeColumn(b, elemCol)
			return v, err
		}

		if rc.isArray {
			if rc.nullArr {
				out[slot] = ArrayValue{Null: true}
				continue
			}
			elems := make([]any, rc.card)
			for e := 0; e < rc.card; e++ {
				v, err := decodeOne()
				if err != nil {
					return nil, nil, fmt.Errorf("hostserver: 0x3901 col %d element %d: %w", c, e, err)
				}
				elems[e] = v
			}
			out[slot] = ArrayValue{Elements: elems}
		} else {
			v, err := decodeOne()
			if err != nil {
				return nil, nil, fmt.Errorf("hostserver: 0x3901 col %d: %w", c, err)
			}
			out[slot] = v
		}
	}
	return out, types, nil
}
