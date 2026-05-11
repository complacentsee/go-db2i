package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ReadDBReplyMatching reads frames from r and returns the first one
// whose CorrelationID matches wantCorrelation. Any frames with a
// different correlation are drained silently (they're typically
// server-side trailers like an empty 40-byte template-only reply
// PUB400 sends after a successful SET_SQL_ATTRIBUTES with
// 0x3825/0x3821 client-support flags).
//
// Returns the matching frame's Header + payload, or the underlying
// I/O error if r drops before a matching frame arrives. maxDrain
// caps how many stale frames we'll skip; if exceeded, the function
// returns an error so a runaway server doesn't lock the caller in
// a read loop.
func ReadDBReplyMatching(r io.Reader, wantCorrelation uint32, maxDrain int) (Header, []byte, error) {
	if maxDrain <= 0 {
		maxDrain = 8
	}
	for skipped := 0; skipped <= maxDrain; skipped++ {
		hdr, payload, err := ReadFrame(r)
		if err != nil {
			return Header{}, nil, err
		}
		if hdr.CorrelationID == wantCorrelation {
			return hdr, payload, nil
		}
		// Stale (out-of-order or trailer) reply -- drop it.
	}
	return Header{}, nil, fmt.Errorf("hostserver: no reply with correlation %d after %d frames",
		wantCorrelation, maxDrain)
}

// DB-server reply CPs that we surface in M2..M5. The full set is in
// JTOpen's DBBaseReplyDS#parseInfo (~150 cases); we add entries here
// as they show up in fixtures we exercise.
const (
	cpDBMessageID          uint16 = 0x3801 // CCSID(2) + bytes  (CCSID-tagged string)
	cpDBFirstLevelText     uint16 = 0x3802 // CCSID(2) + SL(2) + bytes
	cpDBSecondLevelText    uint16 = 0x3803 // ditto
	cpDBServerAttributes   uint16 = 0x3804 // 2 reserved bytes + ServerAttributes payload
	cpDBDataFormat         uint16 = 0x3805
	cpDBResultData         uint16 = 0x3806
	cpDBSQLCA              uint16 = 0x3807
	cpDBParameterMarkerFmt uint16 = 0x3808
	// cpDBDataCompressionRLE wraps the entire post-template parameter
	// list of a 0x2800 reply when the server applied whole-datastream
	// RLE-1 compression. The wrapper carries a 4-byte
	// decompressed-length header followed by the RLE-1 compressed
	// bytes that, once expanded, form the original LL/CP/data
	// parameter stream the reply would have shipped uncompressed.
	// Mirrors JT400's AS400JDBCConnectionImpl.DATA_COMPRESSION_RLE_.
	cpDBDataCompressionRLE uint16 = 0x3832
)

// dataCompressedMask is the high bit of the 32-bit word at template
// offset 4 (full-frame offset 24 in JT400's DBBaseReplyDS.parse).
// When set, the post-template payload is wrapped in CP
// cpDBDataCompressionRLE and ParseDBReply must unwrap before walking
// the parameter list.
const dataCompressedMask uint32 = 0x80000000

// DBReply is the parsed shell of a 0x2800 reply. ErrorClass /
// ReturnCode are the SQLCA-style status word from the template;
// anything else is folded into a CP map that specific reply-type
// parsers (e.g. ParseDBReplyServerAttributes) interpret.
//
// JTOpen names errorClass / returnCode out of DBBaseReplyDS at
// template offsets 14 / 16 -- which in our payload-only world
// (header stripped before we get here) are offsets 14 / 16 within
// the 20-byte template, i.e. payload offsets 14 / 16.
//
// ORSBitmap is the response bitmap echoed in payload[0:4]. Per
// JT400 (DBBaseReplyDS#parse) bit 16 / 0x00010000 means "variable-
// length-field compression actually used in the result data" --
// callers parsing CP 0x380E need this to pick VLF vs the fixed
// row-padding layout. Other bits mostly mirror what the request
// asked for.
type DBReply struct {
	ORSBitmap  uint32
	ErrorClass uint16
	ReturnCode uint32
	Params     []DBParam
}

// RowsAffected reads the rows-affected count from the reply's
// CP 0x3807 SQLCA. SQLERRD is six int32 BE starting at offset 96
// of the SQLCA data; rows-affected sits at SQLERRD(3) in SQL-
// standard 1-indexed terms (sqlerrd_[2] in JT400's 0-indexed
// Java array), which is byte offset 104.
//
// Returns 0 when no SQLCA is present, the SQLCA is too short, or
// the value is sentinel-negative (IBM i uses negative SQLERRD
// fields for "info not available"; JDBC's getUpdateCount mirrors
// the same fallback).
func (r *DBReply) RowsAffected() int64 {
	for i := range r.Params {
		if r.Params[i].CodePoint != cpDBSQLCA {
			continue
		}
		data := r.Params[i].Data
		if len(data) < 108 {
			return 0
		}
		v := int32(binary.BigEndian.Uint32(data[104:108]))
		if v < 0 {
			return 0
		}
		return int64(v)
	}
	return 0
}

// VLFCompressed reports whether the server set the variable-length-
// field compression bit (0x00010000) in the response ORS bitmap.
// True means CP 0x380E result data must be decoded with the row-
// info-header + row-info-array layout; false means each row occupies
// exactly rowSize bytes with VARCHAR slots padded to their declared
// max width.
func (r *DBReply) VLFCompressed() bool {
	return r.ORSBitmap&ORSVarFieldComp != 0
}

// ParseDBReply walks the 0x2800 reply payload: the 20-byte template
// followed by zero or more LL/CP/data parameters. It surfaces
// ErrorClass and ReturnCode out of the template, then collects
// every CP/data pair so that callers can pick out the ones
// relevant to the function they invoked. Unknown CPs are kept
// (rather than skipped) so debugging tools can dump the full
// reply if needed.
//
// Whole-datastream RLE-1 compression (template's high bit of the
// 32-bit word at offset 4) is unwrapped transparently: when the
// post-template payload arrives as a single CP 0x3832 wrapper, the
// helper decompresses its inner bytes and walks the resulting
// LL/CP/data stream as if it had arrived uncompressed. Mirrors
// JT400's DBBaseReplyDS.parse rleCompressed_ path.
func ParseDBReply(payload []byte) (*DBReply, error) {
	const tplLen = 20
	if len(payload) < tplLen {
		return nil, fmt.Errorf("hostserver: db reply too short: %d bytes (want >= %d)", len(payload), tplLen)
	}
	be := binary.BigEndian
	if be.Uint32(payload[4:8])&dataCompressedMask != 0 {
		var err error
		payload, err = unwrapCompressedReply(payload)
		if err != nil {
			return nil, err
		}
	}
	rep := &DBReply{
		// Template offsets within the 20-byte template:
		//   0..3   ORS bitmap echoed
		//   4..13  reserved / handle echoes
		//   14..15 ErrorClass
		//   16..19 ReturnCode
		ORSBitmap:  be.Uint32(payload[0:4]),
		ErrorClass: be.Uint16(payload[14:16]),
		ReturnCode: be.Uint32(payload[16:20]),
	}

	for pos := tplLen; pos+6 <= len(payload); {
		ll := be.Uint32(payload[pos : pos+4])
		if ll < 6 || pos+int(ll) > len(payload) {
			return nil, fmt.Errorf("hostserver: bad LL %d at db reply payload offset %d (frame len %d)", ll, pos, len(payload))
		}
		cp := be.Uint16(payload[pos+4 : pos+6])
		data := payload[pos+6 : pos+int(ll)]
		// Always allocate a fresh, non-nil backing array so callers
		// can rely on Data being safe to retain after the underlying
		// reply buffer is recycled, and on `Data == nil` not having
		// to be distinguished from "empty payload" (LL=6 is a
		// legitimate header-only param).
		buf := make([]byte, len(data))
		copy(buf, data)
		rep.Params = append(rep.Params, DBParam{
			CodePoint: cp,
			Data:      buf,
		})
		pos += int(ll)
	}
	return rep, nil
}

// unwrapCompressedReply inflates a payload that arrived with the
// whole-datastream RLE-1 wrapper. Returns a new payload whose
// post-template bytes are the decompressed parameter stream the
// uncompressed reply would have shipped. The 20-byte template is
// copied through unchanged; only the high compression bit is left
// set in template[4:8] (callers don't read those bytes -- the bit
// is a wire-level marker, not user-visible state).
//
// Wire format of the wrapped payload (offsets from payload start):
//
//	0..19  Reply template (high bit of payload[4:8] set)
//	20..23 ll  (compressed payload length + 10) -- informational only
//	24..25 CP  (cpDBDataCompressionRLE / 0x3832)
//	26..29 Decompressed length of the inner parameter stream
//	30..   RLE-1 compressed parameter stream, runs to end of payload
//
// The inner LL field is informational; JT400's DBBaseReplyDS.parse
// ignores it and feeds the decompressor everything from full-frame
// offset 50 to the end of the frame -- which in our payload world
// is payload[30:]. We mirror that here. Returns an error if the high
// bit is set but the first parameter isn't CP 0x3832, matching
// JT400's IOException when the compression scheme CP is anything
// other than DATA_COMPRESSION_RLE_.
func unwrapCompressedReply(payload []byte) ([]byte, error) {
	const (
		tplLen         = 20
		wrapHdrLen     = 10 // ll(4) + CP(2) + decompressed_len(4)
		minWrapPayload = tplLen + wrapHdrLen
		// maxDecompressedReplyLen guards the preallocation
		// inside decompressDataStreamRLE against a malformed (or
		// hostile) wire claiming a multi-GiB output size. 64 MiB
		// is two orders of magnitude above LOB_BLOCK_SIZE (1 MiB)
		// and well above any real-world host-server reply
		// go-db2i has captured. JT400 lets the JVM raise
		// OutOfMemoryError; Go's make() would panic the process.
		maxDecompressedReplyLen = 64 * 1024 * 1024
	)
	if len(payload) < minWrapPayload {
		return nil, fmt.Errorf("hostserver: compressed reply too short: %d bytes (want >= %d)", len(payload), minWrapPayload)
	}
	be := binary.BigEndian
	cp := be.Uint16(payload[tplLen+4 : tplLen+6])
	if cp != cpDBDataCompressionRLE {
		return nil, fmt.Errorf("hostserver: compressed reply CP 0x%04X (want 0x%04X)", cp, cpDBDataCompressionRLE)
	}
	decompressedLenU := be.Uint32(payload[tplLen+6 : tplLen+10])
	if decompressedLenU > maxDecompressedReplyLen {
		return nil, fmt.Errorf("hostserver: compressed reply declared decompressed length %d exceeds cap %d", decompressedLenU, maxDecompressedReplyLen)
	}
	decompressedLen := int(decompressedLenU)
	compressed := payload[tplLen+wrapHdrLen:]
	decompressed, err := decompressDataStreamRLE(compressed, decompressedLen)
	if err != nil {
		return nil, fmt.Errorf("hostserver: decompress reply payload (wire=%d expected=%d): %w", len(compressed), decompressedLen, err)
	}
	if len(decompressed) != decompressedLen {
		return nil, fmt.Errorf("hostserver: decompress reply short (got %d, want %d)", len(decompressed), decompressedLen)
	}
	out := make([]byte, tplLen+len(decompressed))
	copy(out, payload[:tplLen])
	copy(out[tplLen:], decompressed)
	return out, nil
}

// ServerAttributes is the parsed form of a 0x3804 CP payload --
// the result of a 0x1F80 SET_SQL_ATTRIBUTES request. Field offsets
// mirror JTOpen's DBReplyServerAttributes (which indexes off the
// CP data start, i.e. after the 2 reserved bytes that follow
// LL/CP). We only surface fields that go-db2i actually consumes;
// the rest land if/when needed.
//
// EBCDIC strings come back as raw bytes -- caller decodes through
// the appropriate CCSID converter. For names that JTOpen treats as
// CCSID 37 (US English -- the "system" CCSID), CCSID37.Decode is
// the right call.
type ServerAttributes struct {
	// Numeric POs.
	DateFormatPO            uint16
	DateSeparatorPO         uint16
	TimeFormatPO            uint16
	TimeSeparatorPO         uint16
	DecimalSeparatorPO      uint16
	NamingConventionPO      uint16
	IgnoreDecimalDataError  uint16
	CommitmentControlLevel  uint16
	DRDAPackageSize         uint16
	TranslationIndicator    uint8
	ServerCCSID             uint16
	ServerNLSSValue         uint16

	// EBCDIC bytes (decode through CCSID 37 for these).
	ServerLanguageId           []byte // 3 bytes,  e.g. "ENU"
	ServerLanguageTable        []byte // 10 bytes, e.g. "*HEX      "
	ServerLanguageTableLibrary []byte // 10 bytes
	LanguageFeatureCode        []byte // 4 bytes,  e.g. "2924"
	ServerFunctionalLevel      []byte // 10 bytes, e.g. "V7R5M00016"
	RelationalDBName           []byte // 18 bytes, e.g. "PUB400            "
	DefaultSQLLibraryName      []byte // 10 bytes, e.g. "AFTRAEGE11"
	ServerJobIdentifier        []byte // 26 bytes: 10 job + 10 user + 6 number
	DefaultSQLSchemaName       []byte // variable, length given inline
}

// VRM extracts the V/R/M (version/release/modification) numbers
// embedded in the ServerFunctionalLevel string. JTOpen reads the
// low nibble of bytes at offsets 51, 53, 55 of the attribute
// payload (which sit inside ServerFunctionalLevel at relative
// offsets 1, 3, 5 -- e.g. "V7R5M00016" -> 7, 5, 0).
//
// Returns 0 if the level string is too short or doesn't follow the
// expected V?R?M? layout.
func (a *ServerAttributes) VRM() uint32 {
	if len(a.ServerFunctionalLevel) < 6 {
		return 0
	}
	v := uint32(a.ServerFunctionalLevel[1] & 0x0F)
	r := uint32(a.ServerFunctionalLevel[3] & 0x0F)
	m := uint32(a.ServerFunctionalLevel[5] & 0x0F)
	return (v << 16) | (r << 8) | m
}

// ParseServerAttributes decodes the bytes inside CP 0x3804 (the
// ones AFTER the 2 reserved-byte prefix that DBReplyServerAttributes
// skips with offset+8 vs offset+6). The required minimum length is
// 114 bytes (everything up through ServerJobIdentifier); the
// schema-length+name tail at offset 114+ is optional -- PUB400 has
// been observed to omit it on a freshly-opened connection while
// the same fixture captured under a different session includes it.
//
// The 2 prefix bytes are NOT included in `data`; pass the slice
// that begins at the DateFormatPO field (offset 0 in JTOpen's
// DBReplyServerAttributes).
func ParseServerAttributes(data []byte) (*ServerAttributes, error) {
	const fixedLen = 114 // through end of ServerJobIdentifier
	if len(data) < fixedLen {
		return nil, fmt.Errorf("hostserver: server-attributes payload too short: %d bytes (want >= %d)", len(data), fixedLen)
	}
	be := binary.BigEndian
	a := &ServerAttributes{
		DateFormatPO:           be.Uint16(data[0:2]),
		DateSeparatorPO:        be.Uint16(data[2:4]),
		TimeFormatPO:           be.Uint16(data[4:6]),
		TimeSeparatorPO:        be.Uint16(data[6:8]),
		DecimalSeparatorPO:     be.Uint16(data[8:10]),
		NamingConventionPO:     be.Uint16(data[10:12]),
		IgnoreDecimalDataError: be.Uint16(data[12:14]),
		CommitmentControlLevel: be.Uint16(data[14:16]),
		DRDAPackageSize:        be.Uint16(data[16:18]),
		TranslationIndicator:   data[18],
		ServerCCSID:            be.Uint16(data[19:21]),
		ServerNLSSValue:        be.Uint16(data[21:23]),

		ServerLanguageId:           append([]byte(nil), data[23:26]...),
		ServerLanguageTable:        append([]byte(nil), data[26:36]...),
		ServerLanguageTableLibrary: append([]byte(nil), data[36:46]...),
		LanguageFeatureCode:        append([]byte(nil), data[46:50]...),
		ServerFunctionalLevel:      append([]byte(nil), data[50:60]...),
		RelationalDBName:           append([]byte(nil), data[60:78]...),
		DefaultSQLLibraryName:      append([]byte(nil), data[78:88]...),
		ServerJobIdentifier:        append([]byte(nil), data[88:114]...),
	}
	// Optional schema-length + name tail.
	if len(data) >= 116 {
		schemaLen := int(be.Uint16(data[114:116]))
		if schemaLen > 0 {
			end := 116 + schemaLen
			if end > len(data) {
				return nil, fmt.Errorf("hostserver: server-attributes schema length %d overruns payload (%d bytes available after offset 116)", schemaLen, len(data)-116)
			}
			a.DefaultSQLSchemaName = append([]byte(nil), data[116:end]...)
		}
	}
	return a, nil
}

// FindServerAttributes scans rep.Params for CP 0x3804 and returns
// the parsed payload. The CP carries 2 reserved bytes at the
// start (0x0000 in observed traces) which we skip before handing
// the bytes to ParseServerAttributes.
//
// Returns nil, nil if the reply doesn't include the CP.
func (r *DBReply) FindServerAttributes() (*ServerAttributes, error) {
	for _, p := range r.Params {
		if p.CodePoint == cpDBServerAttributes {
			if len(p.Data) < 2 {
				return nil, fmt.Errorf("hostserver: server-attributes CP data too short: %d bytes", len(p.Data))
			}
			return ParseServerAttributes(p.Data[2:])
		}
	}
	return nil, nil
}
