package hostserver

import (
	"encoding/binary"
	"fmt"
)

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
)

// DBReply is the parsed shell of a 0x2800 reply. ErrorClass /
// ReturnCode are the SQLCA-style status word from the template;
// anything else is folded into a CP map that specific reply-type
// parsers (e.g. ParseDBReplyServerAttributes) interpret.
//
// JTOpen names errorClass / returnCode out of DBBaseReplyDS at
// template offsets 14 / 16 -- which in our payload-only world
// (header stripped before we get here) are offsets 14 / 16 within
// the 20-byte template, i.e. payload offsets 14 / 16.
type DBReply struct {
	ErrorClass uint16
	ReturnCode uint32
	Params     []DBParam
}

// ParseDBReply walks the 0x2800 reply payload: the 20-byte template
// followed by zero or more LL/CP/data parameters. It surfaces
// ErrorClass and ReturnCode out of the template, then collects
// every CP/data pair so that callers can pick out the ones
// relevant to the function they invoked. Unknown CPs are kept
// (rather than skipped) so debugging tools can dump the full
// reply if needed.
func ParseDBReply(payload []byte) (*DBReply, error) {
	const tplLen = 20
	if len(payload) < tplLen {
		return nil, fmt.Errorf("hostserver: db reply too short: %d bytes (want >= %d)", len(payload), tplLen)
	}
	be := binary.BigEndian
	rep := &DBReply{
		// Template offsets within the 20-byte template:
		//   0..3   ORS bitmap echoed
		//   4..13  reserved / handle echoes
		//   14..15 ErrorClass
		//   16..19 ReturnCode
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
		rep.Params = append(rep.Params, DBParam{
			CodePoint: cp,
			Data:      append([]byte(nil), data...),
		})
		pos += int(ll)
	}
	return rep, nil
}

// ServerAttributes is the parsed form of a 0x3804 CP payload --
// the result of a 0x1F80 SET_SQL_ATTRIBUTES request. Field offsets
// mirror JTOpen's DBReplyServerAttributes (which indexes off the
// CP data start, i.e. after the 2 reserved bytes that follow
// LL/CP). We only surface fields that goJTOpen actually consumes;
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
