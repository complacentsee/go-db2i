package hostserver

import (
	"encoding/binary"
	"fmt"
)

// DecodeDBRequest is the inverse of BuildDBRequest. It splits a DB-
// service request payload (everything after the 20-byte DSS header)
// into the 20-byte template plus an ordered list of variable-length
// parameters. Parameter order is preserved -- the caller can rely on
// it for byte-equivalence comparisons against JT400 trace fixtures
// where param order is part of the wire shape.
//
// Returns an error if the payload is truncated or a parameter's LL
// runs past the end of the buffer. Trailing bytes that aren't a
// complete LL/CP/Data header are an error rather than silently
// dropped; the wire spec requires the parameter table to end on a
// param boundary.
func DecodeDBRequest(payload []byte) (DBRequestTemplate, []DBParam, error) {
	if len(payload) < 20 {
		return DBRequestTemplate{}, nil, fmt.Errorf("hostserver: DB request payload %d bytes < 20-byte template", len(payload))
	}
	be := binary.BigEndian
	tpl := DBRequestTemplate{
		ORSBitmap:                 be.Uint32(payload[0:4]),
		ReturnORSHandle:           be.Uint16(payload[8:10]),
		FillORSHandle:             be.Uint16(payload[10:12]),
		BasedOnORSHandle:          be.Uint16(payload[12:14]),
		RPBHandle:                 be.Uint16(payload[14:16]),
		ParameterMarkerDescriptor: be.Uint16(payload[16:18]),
	}
	declared := be.Uint16(payload[18:20])

	rest := payload[20:]
	params := make([]DBParam, 0, declared)
	for len(rest) > 0 {
		if len(rest) < 6 {
			return tpl, params, fmt.Errorf("hostserver: trailing %d bytes < 6-byte param header", len(rest))
		}
		ll := be.Uint32(rest[0:4])
		if ll < 6 {
			return tpl, params, fmt.Errorf("hostserver: param LL %d < 6", ll)
		}
		if int(ll) > len(rest) {
			return tpl, params, fmt.Errorf("hostserver: param LL %d overruns %d remaining bytes", ll, len(rest))
		}
		cp := be.Uint16(rest[4:6])
		data := make([]byte, int(ll)-6)
		copy(data, rest[6:ll])
		params = append(params, DBParam{CodePoint: cp, Data: data})
		rest = rest[ll:]
	}
	if int(declared) != len(params) {
		return tpl, params, fmt.Errorf("hostserver: param count mismatch: template says %d, decoded %d", declared, len(params))
	}
	return tpl, params, nil
}

// DecodeDBRequestFrame parses a single complete DSS request frame
// (header + payload) and returns the header, template, and ordered
// parameter list. Convenience wrapper for callers walking a trace
// stream where each frame arrives as a single byte slice.
//
// The DSS header is parsed via Header.UnmarshalBinary, which
// validates the 0xE0 sanity byte; a malformed header returns the
// header-level error without attempting to decode the body.
func DecodeDBRequestFrame(frame []byte) (Header, DBRequestTemplate, []DBParam, error) {
	if len(frame) < HeaderLength {
		return Header{}, DBRequestTemplate{}, nil, fmt.Errorf("hostserver: frame %d bytes < %d-byte DSS header", len(frame), HeaderLength)
	}
	var hdr Header
	if err := hdr.UnmarshalBinary(frame[:HeaderLength]); err != nil {
		return Header{}, DBRequestTemplate{}, nil, err
	}
	if hdr.ServerID != ServerDatabase {
		return hdr, DBRequestTemplate{}, nil, fmt.Errorf("hostserver: frame is not a DB-service request (server-id 0x%04X)", uint16(hdr.ServerID))
	}
	tpl, params, err := DecodeDBRequest(frame[HeaderLength:])
	return hdr, tpl, params, err
}
