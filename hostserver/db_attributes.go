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
//	LanguageFeatureCode = "2924" (4-digit numeric ID; "2924" is
//	                       English/Germany blend, what JTOpen
//	                       captured against PUB400 sends)
//	ClientFunctionalLevel = "V7R2M01   " (10-char ID, EBCDIC CCSID 37,
//	                       padded with spaces; gates which protocol
//	                       features the server enables)
//	DefaultSQLLibrary = "AFTRAEGE11" (per-user library; PUB400's
//	                       reply may be truncated without this set)
//
// Callers tweak these via NewDBAttributesOptions().With...(); the
// raw struct is exposed so power-users can override anything.
type DBAttributesOptions struct {
	// ClientCCSID is the CCSID we want the server to use when
	// returning string data to us. 13488 (UCS-2 BE) is JTOpen's
	// default and is what the captured fixture uses.
	ClientCCSID uint16
	// LanguageFeatureCode is the 4-digit numeric language code
	// (CP 0x3802). JTOpen V7R5+ on PUB400 sends "2924"; encoder
	// uses CCSID 37 + EBCDIC numeric mapping.
	LanguageFeatureCode string
	// ClientFunctionalLevel is the 10-char level identifier; JTOpen
	// V7R5+ sends "V7R2M01   ". Padding to 10 bytes with EBCDIC
	// spaces is the encoder's job.
	ClientFunctionalLevel string
	// DefaultSQLLibrary is the per-user default schema (CP 0x380F).
	// Empty = don't send the CP. JTOpen sends the user's home
	// library here ("AFTRAEGE11" on PUB400 for AFTRAEGE1).
	DefaultSQLLibrary string
	// DateFormat overrides the date format CP 0x3805 we ask the
	// server to use for date columns. The default 0xF0 (EBCDIC '0',
	// JOB) lets the server pick its job-default format -- on PUB400
	// that's YMD ("YY-MM-DD"). Set to DateFormatISO to receive
	// dates in 10-char "YYYY-MM-DD" with no centruy-boundary
	// guesswork in the decoder; or to DateFormatJOB to keep the
	// fixture-compatible default.
	DateFormat byte
	// IsolationLevel controls CP 0x380E (commitment control level)
	// in SET_SQL_ATTRIBUTES. Default IsolationDefault leaves the CP
	// unchanged (matches the long-standing fixture-compat default
	// of *NONE). Set to IsolationReadCommitted for transactions
	// that need rollback support; IsolationCommitNone disables
	// transactions entirely (suitable for non-journaled tables).
	IsolationLevel int16
}

// DateFormat constants for DBAttributesOptions.DateFormat. EBCDIC
// digits in CP 0x3805. PUB400 honours the request iff the CP is
// present (default 0 = JOB).
const (
	DateFormatJOB byte = 0xF0 // EBCDIC '0' -- server-default
	DateFormatUSA byte = 0xF1 // MM/DD/YYYY (10 chars)
	DateFormatISO byte = 0xF2 // YYYY-MM-DD (10 chars)
	DateFormatEUR byte = 0xF3 // DD.MM.YYYY (10 chars)
	DateFormatJIS byte = 0xF4 // YYYY-MM-DD (10 chars)
	DateFormatMDY byte = 0xF5 // MM/DD/YY (8 chars)
	DateFormatDMY byte = 0xF6 // DD/MM/YY (8 chars)
	DateFormatYMD byte = 0xF7 // YY-MM-DD (8 chars)
)

// isolationLevelWireValue maps an IsolationLevel option (which uses
// -1 to mean "leave at default") to the int16 we send on the wire.
// The wire CP is always present; -1 just means "use 0 (default)".
func isolationLevelWireValue(level int16) int16 {
	if level == IsolationDefault {
		return 0
	}
	return level
}

// CommitmentControlLevel values for DBAttributesOptions.IsolationLevel.
// CP 0x380E (short). Maps the standard JDBC isolation constants to
// IBM i's commitment control names; values per JT400's
// DBSQLAttributesDS.setCommitmentControlLevelParserOption.
const (
	IsolationDefault       int16 = -1 // don't send the CP -- server picks
	IsolationCommitNone    int16 = 0  // *NONE -- no isolation, no journaling
	IsolationReadCommitted int16 = 1  // *CS   -- cursor stability (JDBC TRANSACTION_READ_COMMITTED)
	IsolationAllCS         int16 = 2  // *ALL  -- read uncommitted (TRANSACTION_READ_UNCOMMITTED, ish)
	IsolationRepeatableRd  int16 = 3  // *RR   -- repeatable read
	IsolationSerializable  int16 = 4  // *RS   -- serializable
)

// DefaultDBAttributesOptions returns the minimum-acceptable defaults
// for an as-database session. These match the JTOpen JDBC driver
// V7R5+ when opened without overrides; they're enough for PUB400 to
// accept a PREPARE_DESCRIBE without falling back to a legacy mode
// that returns SQL -401.
func DefaultDBAttributesOptions() DBAttributesOptions {
	return DBAttributesOptions{
		ClientCCSID:           13488, // UCS-2 BE
		LanguageFeatureCode:   "2924",
		ClientFunctionalLevel: "V7R2M01   ",
		// DefaultSQLLibrary -- PUB400 V7R5 returns SQL -401 on
		// PREPARE_DESCRIBE if this CP is missing from the
		// SET_SQL_ATTRIBUTES init. We default to the user's
		// home library on PUB400 (AFTRAEGE11). For other servers
		// callers should override before calling SetSQLAttributes.
		DefaultSQLLibrary: "AFTRAEGE11",
		// DateFormat: JOB (server picks). Tests pin this for
		// fixture byte-equality; production callers can flip to
		// DateFormatISO so dates come back already-ISO and the
		// decoder skips the YMD-to-ISO conversion.
		DateFormat: DateFormatJOB,
		// IsolationLevel: leave unset, server picks. Tests rely
		// on this for fixture parity; transaction-using callers
		// flip to IsolationReadCommitted via WithIsolation.
		IsolationLevel: IsolationDefault,
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
	if len(opts.LanguageFeatureCode) != 4 {
		return Header{}, nil, fmt.Errorf("hostserver: language feature code must be 4 chars, got %d (%q)",
			len(opts.LanguageFeatureCode), opts.LanguageFeatureCode)
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
	// Minimum attribute set for V7R5+ -- mirrors what JTOpen
	// AS400JDBCConnectionImpl sends on connection open. We
	// originally tried 3 attributes; PUB400 quietly downgraded
	// the session to a legacy mode that rejected
	// PREPARE_DESCRIBE with SQL -401. The full V7R5+ set below
	// is what JTOpen sends in our captured fixture (sent #6 of
	// connect_only.trace).
	params := []DBParam{
		DBParamShort(0x3801, int16(opts.ClientCCSID)),
		// 0x3802 LanguageFeatureCode -- numeric-only encoding,
		// no SL field. JTOpen sends "2924" for German PUB400.
		DBParamNumericString(0x3802, opts.LanguageFeatureCode),
		// 0x3803 ClientFunctionalLevel -- fixed CCSID-tagged
		// 10-byte string with 2 trailing pad bytes (LL=20).
		DBParamFixedString(0x3803, 37, cflBytes),
		DBParamByte(0x3805, opts.DateFormat),
		DBParamShort(0x3806, 0x0001),
		DBParamByte(0x3824, 0xE8),
		// CP 0x380E: commitment control level. 0 = server default
		// (matches fixtures); explicit values come from
		// IsolationLevel. We always send the CP -- the value
		// changes based on the option, but JT400 always emits the
		// CP so byte-equality tests rely on it being present.
		DBParamShort(0x380E, isolationLevelWireValue(opts.IsolationLevel)),
		DBParamShort(0x380C, 0x0000),
		DBParamShort(0x3823, 0x0000),
	}
	// 0x380F default SQL library -- variable-length CCSID-tagged.
	// JTOpen sends this when the JDBC URL specifies libraries=;
	// our default omits it.
	if opts.DefaultSQLLibrary != "" {
		dlBytes, err := ebcdic.CCSID37.Encode(opts.DefaultSQLLibrary)
		if err != nil {
			return Header{}, nil, fmt.Errorf("hostserver: encode default SQL library: %w", err)
		}
		params = append(params, DBParamVarString(0x380F, 273, dlBytes))
	}
	params = append(params,
		DBParamShort(0x3812, 0x0001),                                    // PackageAddStmtAllowed
		DBParamByte(0x3821, 0xF2),                                       // UseExtendedFormatsIndicator
		DBParam{CodePoint: 0x3822, Data: []byte{0x00, 0x00, 0x80, 0x00}}, // LOBFieldThreshold
		DBParamShort(0x3811, 0x0001),                                    // AmbiguousSelectOption
		DBParam{CodePoint: 0x3825, Data: []byte{0xF6, 0x00, 0x00, 0x00}}, // ClientSupportInfo (V7R5+)
		DBParam{CodePoint: 0x3827, Data: []byte{0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00}}, // DecimalPrecisionIndicators
		DBParamByte(0x3828, 0x00),                                       // HexConstantParserOption
		DBParamShort(0x3830, 0x0001),                                    // LocatorPersistence
	)
	// Application info CPs. We match JTOpen's exact "JDBC" /
	// "IBM Toolbox for Java" / "07060001" identifiers because
	// PUB400 V7R5 returns SQL -401 on PREPARE_DESCRIBE if the
	// session was initialised with a different application
	// signature. Long-term we want our own identifiers, but
	// that's M3+ once we understand exactly what PUB400 keys on.
	intfType, _ := ebcdic.CCSID37.Encode("JDBC")
	intfName, _ := ebcdic.CCSID37.Encode("IBM Toolbox for Java")
	intfLevel, _ := ebcdic.CCSID37.Encode("07060001")
	params = append(params,
		DBParamVarString(0x383C, 37, intfType),
		DBParamVarString(0x383D, 37, intfName),
		DBParamVarString(0x383E, 37, intfLevel),
		DBParamByte(0x383F, 0xE8),
	)
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
