package hostserver

import (
	"encoding/binary"
	"fmt"
)

// Database-server (as-database, server ID 0xE004) request and reply
// IDs. Unlike the as-signon and start-server flows -- which speak a
// pair of dedicated request IDs (0x7001/0xF001 etc.) -- as-database
// runs over a generic "DB request" datastream whose ReqRepID
// distinguishes the function (server attributes, prepare, execute,
// etc.) and whose reply is always the generic 0x2800 reply.
//
// Function IDs we care about right now (more land in M2..M5):
const (
	// SQL service function IDs.
	ReqDBSetSQLAttributes      uint16 = 0x1F80 // request server attribs change + reply
	ReqDBRetrieveSQLAttributes uint16 = 0x1F81

	// Generic DB reply.
	RepDBReply uint16 = 0x2800

	// "End of conversation" request -- closes the connection cleanly.
	ReqDBEndConversation uint16 = 0x1FFF
)

// Operation Result Set (ORS) bitmap flags. The DB request template
// carries a 4-byte bitmap that selects which results the server
// should send back. Bit 1 (0x80000000) is "send reply at all"; the
// other bits select what's interesting in that reply.
//
// JTOpen wires up many of these by default; for our M2 work we
// only need a subset. Flags listed here are the ones we actively
// set or compare against; more land as M2..M5 fill in.
const (
	ORSReturnData           uint32 = 0x80000000
	ORSMessageID            uint32 = 0x40000000
	ORSFirstLevelText       uint32 = 0x20000000
	ORSSecondLevelText      uint32 = 0x10000000
	ORSDataFormat           uint32 = 0x08000000
	ORSResultData           uint32 = 0x04000000
	ORSSQLCA                uint32 = 0x02000000
	ORSServerAttributes     uint32 = 0x01000000
	ORSParameterMarkerFmt   uint32 = 0x00800000
	ORSPackageInfo          uint32 = 0x00100000
	ORSExtendedColumnDescrs uint32 = 0x00020000
)

// DBRequestTemplate is the 20-byte fixed template that follows the
// DSS header on every database-service request. Layout (relative to
// start of template, i.e. payload offset 0):
//
//	0..3   ORSBitmap                   (uint32 BE)
//	4..7   reserved (zero on the wire)
//	8..9   ReturnORSHandle             (uint16 BE)
//	10..11 FillORSHandle               (uint16 BE)
//	12..13 BasedOnORSHandle            (uint16 BE)
//	14..15 RPBHandle                   (uint16 BE)
//	16..17 ParameterMarkerDescriptor   (uint16 BE)
//	18..19 ParameterCount              (uint16 BE; filled in by encoder)
//
// Both ORSBitmap and the handles are typically left at the JTOpen
// defaults: bitmap selects which results to send, handles are 0/1
// scratch slots inside the server's SQL state.
type DBRequestTemplate struct {
	ORSBitmap                 uint32
	ReturnORSHandle           uint16
	FillORSHandle             uint16
	BasedOnORSHandle          uint16
	RPBHandle                 uint16
	ParameterMarkerDescriptor uint16
}

// DBParam is one variable-length parameter on a DB request. Each one
// is wire-encoded as LL(4) + CP(2) + Data, where LL includes its
// own 4 bytes (i.e. LL = 6 + len(Data)). Helpers below build the
// flavours JTOpen uses (CCSID-prefixed strings, UCS-2 strings,
// raw bytes, etc.).
type DBParam struct {
	CodePoint uint16
	Data      []byte
}

// BuildDBRequest builds a DB-service request payload (everything
// after the 20-byte DSS header). It returns the populated Header
// (Length, ServerID = SQL, TemplateLength = 20, ReqRepID set) and
// the payload bytes. CorrelationID is the caller's responsibility
// to set on hdr before writing -- JTOpen uses an incrementing
// per-connection sequence.
func BuildDBRequest(reqRepID uint16, tpl DBRequestTemplate, params []DBParam) (Header, []byte, error) {
	// Template (20) + sum(LL each param)
	size := 20
	for i, p := range params {
		ll := 6 + len(p.Data)
		if ll > 0xFFFFFFFF {
			return Header{}, nil, fmt.Errorf("hostserver: db request param %d (CP 0x%04X) too large: %d bytes", i, p.CodePoint, len(p.Data))
		}
		size += ll
	}
	if len(params) > 0xFFFF {
		return Header{}, nil, fmt.Errorf("hostserver: db request too many params: %d (max 65535)", len(params))
	}

	payload := make([]byte, size)
	be := binary.BigEndian

	// Template.
	be.PutUint32(payload[0:4], tpl.ORSBitmap)
	// Bytes 4..7 reserved (zero).
	be.PutUint16(payload[8:10], tpl.ReturnORSHandle)
	be.PutUint16(payload[10:12], tpl.FillORSHandle)
	be.PutUint16(payload[12:14], tpl.BasedOnORSHandle)
	be.PutUint16(payload[14:16], tpl.RPBHandle)
	be.PutUint16(payload[16:18], tpl.ParameterMarkerDescriptor)
	be.PutUint16(payload[18:20], uint16(len(params)))

	// Parameters.
	off := 20
	for _, p := range params {
		ll := uint32(6 + len(p.Data))
		be.PutUint32(payload[off:off+4], ll)
		be.PutUint16(payload[off+4:off+6], p.CodePoint)
		copy(payload[off+6:off+int(ll)], p.Data)
		off += int(ll)
	}

	hdr := Header{
		Length:         uint32(HeaderLength + size),
		ServerID:       ServerDatabase,
		TemplateLength: 20,
		ReqRepID:       reqRepID,
	}
	return hdr, payload, nil
}

// --- Param-data helpers (encoder side) ---

// DBParamShort packs a 2-byte big-endian short -- the encoding for
// CPs that JTOpen describes as "short", e.g. ambiguous-select-option
// (0x3811), naming-convention (0x3806), date-format (0x3807).
func DBParamShort(cp uint16, v int16) DBParam {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(v))
	return DBParam{CodePoint: cp, Data: b}
}

// DBParamByte packs a 1-byte param -- e.g. autocommit (0x3824),
// input-locator-type (0x3829).
func DBParamByte(cp uint16, v byte) DBParam {
	return DBParam{CodePoint: cp, Data: []byte{v}}
}

// DBParamFixedString packs a fixed-length CCSID-tagged string
// (JTOpen's `addParameter(int, ConvTable, String, true)` overload):
// CCSID(2) + raw bytes. Used for identifiers like client functional
// level, NLSS sort-table name, application name. The caller is
// responsible for converting `value` to the right CCSID; this
// helper just frames the bytes.
func DBParamFixedString(cp uint16, ccsid uint16, valueBytes []byte) DBParam {
	b := make([]byte, 2+len(valueBytes))
	binary.BigEndian.PutUint16(b[0:2], ccsid)
	copy(b[2:], valueBytes)
	return DBParam{CodePoint: cp, Data: b}
}

// DBParamVarString packs a variable-length CCSID-tagged string
// (JTOpen's `addParameter(int, ConvTable, String)` default
// overload): CCSID(2) + StreamLength(2) + raw bytes. Used for
// statement text, package names, etc. that may legitimately be
// short or long.
func DBParamVarString(cp uint16, ccsid uint16, valueBytes []byte) DBParam {
	if len(valueBytes) > 0xFFFF {
		// Defensive: caller bug to pass >64KB here. The 4-byte
		// length variant is a separate helper if we ever need it.
		valueBytes = valueBytes[:0xFFFF]
	}
	b := make([]byte, 4+len(valueBytes))
	binary.BigEndian.PutUint16(b[0:2], ccsid)
	binary.BigEndian.PutUint16(b[2:4], uint16(len(valueBytes)))
	copy(b[4:], valueBytes)
	return DBParam{CodePoint: cp, Data: b}
}
