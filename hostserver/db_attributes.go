package hostserver

import (
	"fmt"
	"io"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// DBAttributesOptions is the subset of DBSQLAttributesDS knobs that
// goJTOpen sets on every database connection. The defaults match
// what JTOpen sends when the JDBC driver is opened with no extra
// URL properties:
//
//	ClientCCSID = 13488 (UCS-2 BE; the negotiated client charset)
//	NLSSIdentifier = "9024" (translation table identifier; CCSID 37)
//	ClientFunctionalLevel = "V7R2M01   " (10-char ID, EBCDIC CCSID 37,
//	                       padded with spaces; gates which protocol
//	                       features the server enables)
//
// Callers tweak these via NewDBAttributesOptions().With...(); the
// raw struct is exposed so power-users can override anything.
type DBAttributesOptions struct {
	// ClientCCSID is the CCSID we want the server to use when
	// returning string data to us. 13488 (UCS-2 BE) is JTOpen's
	// default and is what the captured fixture uses.
	ClientCCSID uint16
	// NLSSIdentifier is the 4-char translation-table identifier;
	// JTOpen sends "9024" (CCSID 37 -> ours). Bytes are EBCDIC.
	NLSSIdentifier string
	// ClientFunctionalLevel is the 10-char level identifier; JTOpen
	// V7R5+ sends "V7R2M01   ". Padding to 10 bytes with EBCDIC
	// spaces is the encoder's job.
	ClientFunctionalLevel string
}

// DefaultDBAttributesOptions returns the minimum-acceptable defaults
// for an as-database session. These match the JTOpen JDBC driver
// V7R5+ when opened without overrides; they're enough for PUB400 to
// reply with a fully populated ServerAttributes block.
func DefaultDBAttributesOptions() DBAttributesOptions {
	return DBAttributesOptions{
		ClientCCSID:           13488, // UCS-2 BE
		NLSSIdentifier:        "9024",
		ClientFunctionalLevel: "V7R2M01   ",
	}
}

// SetSQLAttributesRequest builds a 0x1F80 SET_SQL_ATTRIBUTES request
// payload (without the DSS header). The request asks the server to
// (a) accept the attributes the client wants to use and (b) reply
// with its own attribute set so the client can adjust to it.
//
// The ORS bitmap is set to 0x81040000:
//
//	0x80000000 (Bit 1)  reply should be sent immediately
//	0x01000000 (Bit 8)  Server Attributes
//	0x00040000 (Bit 14) RLE compression reply desired -- harmless
//	                    on uncompressed replies (server ignores)
//
// This matches what JTOpen sends in our captured fixture.
func SetSQLAttributesRequest(opts DBAttributesOptions) (Header, []byte, error) {
	nlssBytes, err := ebcdic.CCSID37.Encode(opts.NLSSIdentifier)
	if err != nil {
		return Header{}, nil, fmt.Errorf("hostserver: encode NLSS identifier: %w", err)
	}
	if len(nlssBytes) != 4 {
		return Header{}, nil, fmt.Errorf("hostserver: NLSS identifier must encode to 4 bytes, got %d (%q)", len(nlssBytes), opts.NLSSIdentifier)
	}
	cflBytes, err := ebcdic.CCSID37.Encode(opts.ClientFunctionalLevel)
	if err != nil {
		return Header{}, nil, fmt.Errorf("hostserver: encode client functional level: %w", err)
	}
	if len(cflBytes) != 10 {
		return Header{}, nil, fmt.Errorf("hostserver: client functional level must encode to 10 bytes, got %d (%q)", len(cflBytes), opts.ClientFunctionalLevel)
	}

	tpl := DBRequestTemplate{
		ORSBitmap: ORSReturnData | ORSServerAttributes | 0x00040000, // 0x81040000
	}
	params := []DBParam{
		// 0x3801 setClientCCSID -- 2-byte short.
		DBParamShort(0x3801, int16(opts.ClientCCSID)),
		// 0x3802 setNLSSIdentifier -- fixed CCSID-tagged string.
		DBParamFixedString(0x3802, 37, nlssBytes),
		// 0x3803 setClientFunctionalLevel -- fixed CCSID-tagged string.
		DBParamFixedString(0x3803, 37, cflBytes),
	}
	hdr, payload, err := BuildDBRequest(ReqDBSetSQLAttributes, tpl, params)
	if err != nil {
		return Header{}, nil, fmt.Errorf("hostserver: build set-sql-attributes req: %w", err)
	}
	return hdr, payload, nil
}

// SetSQLAttributes runs a SET_SQL_ATTRIBUTES round trip on conn and
// returns the parsed ServerAttributes from the server's reply. The
// caller must have already completed StartDatabaseService() on this
// connection -- this is the next step.
//
// On success, the client knows the server's CCSID, the default SQL
// library, the prestart-job identifier in the SQL service, etc. M2
// SELECT round trips will use these (especially CCSID and the
// schema name) to format their statement text correctly.
func SetSQLAttributes(conn io.ReadWriter, opts DBAttributesOptions) (*ServerAttributes, error) {
	hdr, payload, err := SetSQLAttributesRequest(opts)
	if err != nil {
		return nil, err
	}
	hdr.CorrelationID = 1 // first DB request after StartDatabaseService
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send set-sql-attributes req: %w", err)
	}
	repHdr, repPayload, err := ReadFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read set-sql-attributes reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: unexpected reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse db reply: %w", err)
	}
	if rep.ReturnCode != 0 {
		return nil, fmt.Errorf("hostserver: set-sql-attributes RC=%d errorClass=0x%04X", rep.ReturnCode, rep.ErrorClass)
	}
	attrs, err := rep.FindServerAttributes()
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse server attributes: %w", err)
	}
	if attrs == nil {
		return nil, fmt.Errorf("hostserver: set-sql-attributes reply missing CP 0x3804 (server attributes)")
	}
	return attrs, nil
}
