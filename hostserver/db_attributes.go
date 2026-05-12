package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// DBAttributesOptions is the subset of DBSQLAttributesDS knobs that
// go-db2i sets on every database connection. The defaults match
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
	// DateFormat overrides the session date format CP 0x3807
	// (DateFormatParserOption). It controls how the server formats
	// DATE columns in result sets AND how it parses DATE bytes in
	// prepared-statement binds. The default DateFormatJOB lets the
	// server pick its job-default format -- on PUB400 that's YMD
	// ("YY-MM-DD"). Set to DateFormatISO to receive dates in
	// 10-char "YYYY-MM-DD" with no century-boundary guesswork in
	// the decoder; or to DateFormatJOB to keep the fixture-
	// compatible default.
	//
	// NOTE: pre-2026-05-08 builds of go-db2i pumped this byte into
	// CP 0x3805, which is actually the TranslateIndicator (per
	// JTOpen's DBSQLAttributesDS.setTranslateIndicator). DateFormatJOB
	// happened to coincide with the only valid TranslateIndicator
	// value (0xF0); any other DateFormat silently broke the
	// connection's translate behaviour AND left the date format at
	// the server default. The CP is now correctly routed to 0x3807
	// (short int, 0..7 per JTOpen's parser-option mapping).
	DateFormat byte
	// IsolationLevel controls CP 0x380E (commitment control level)
	// in SET_SQL_ATTRIBUTES. Default IsolationDefault leaves the CP
	// unchanged (matches the long-standing fixture-compat default
	// of *NONE). Set to IsolationReadCommitted for transactions
	// that need rollback support; IsolationCommitNone disables
	// transactions entirely (suitable for non-journaled tables).
	IsolationLevel int16
	// LOBThreshold is the byte count at and below which the server
	// inlines LOB columns in result data (and accepts inline LOB
	// shapes on bind) instead of allocating a server-side locator.
	// Sent as CP 0x3822 in SET_SQL_ATTRIBUTES; mirrors JT400's
	// JDProperties.LOB_THRESHOLD ("lob threshold" JDBC URL knob).
	//
	// 0 = use the wire-default 32768 (the historical hard-coded
	// value go-db2i has always sent). Set to a smaller value to
	// keep large CLOBs out of the row-data buffer; set to a larger
	// value (up to 15728640, the server's documented cap) to inline
	// bigger LOBs and skip the RETRIEVE_LOB_DATA round trip for
	// repetitive small-LOB workloads.
	LOBThreshold uint32

	// ExtendedDynamic tells SET_SQL_ATTRIBUTES to emit the full
	// JT400 wire shape that JT400's AS400JDBCConnectionImpl uses
	// when the JDBC URL set "extended dynamic=true". JT400 emits 5
	// additional date/time/separator CPs (0x3807-0x380B) with the
	// session's defaults so the server has a definite value to feed
	// into the package-suffix derivation, instead of relying on the
	// job-default fallback. v0.7.2 live testing against IBM Cloud
	// V7R6M0 confirmed: without these 5 extras the server doesn't
	// file PREPAREd statements into the *PGM even with otherwise
	// byte-identical wire shape.
	ExtendedDynamic bool

	// Naming selects the SQL naming convention the server applies
	// when parsing unqualified identifiers. 0 = "sql" (period-
	// qualified, MYLIB.TABLE; the JT400 NAMING_SQL value and the
	// go-db2i historical default); 1 = "system" (slash-qualified,
	// MYLIB/TABLE; the JT400 NAMING_SYSTEM value and JT400's
	// default for migrating RPG/CL shops). Sent as CP 0x380C
	// (NamingConventionParserOption) in SET_SQL_ATTRIBUTES; mirrors
	// JT400's setNamingConventionParserOption per JDProperties.NAMING.
	Naming int16

	// TimeFormat overrides CP 0x3809 (TimeFormatParserOption).
	// Values mirror JT400's TIME_FORMAT choice index:
	//   0=hms 1=usa 2=iso 3=eur 4=jis
	// -1 (default) = omit the CP so the server falls back to the
	// job-default time format. JT400 NOTSET behaves identically.
	TimeFormat int8

	// DateSeparator overrides CP 0x3808 (DateSeparatorParserOption).
	// Values mirror JT400's DATE_SEPARATOR choice index:
	//   0='/' 1='-' 2='.' 3=',' 4=' '
	// -1 (default) = let the date-format-inferred separator (or the
	// job default) win. Explicit setting takes precedence over the
	// DateFormat-derived value emitted by the dateSeparatorParserIndex
	// helper.
	DateSeparator int8

	// TimeSeparator overrides CP 0x380A (TimeSeparatorParserOption).
	// Values mirror JT400's TIME_SEPARATOR choice index:
	//   0=':' 1='.' 2=',' 3=' '
	// -1 (default) = omit the CP (job default).
	TimeSeparator int8

	// DecimalSeparator overrides CP 0x380B (DecimalSeparatorParserOption).
	// Values mirror JT400's DECIMAL_SEPARATOR choice index:
	//   0='.' 1=','
	// -1 (default) = omit the CP (job default).
	DecimalSeparator int8

	// QueryOptimizeGoal sets the server-side optimizer goal byte on
	// CP 0x3833 in SET_SQL_ATTRIBUTES. Mirrors JT400's
	// "query optimize goal" JDBC URL property:
	//
	//   QueryOptimizeFirstIO (0xC6, 'F') -- optimize for time-to-
	//                                       first-row (streaming, OLTP)
	//   QueryOptimizeAllIO   (0xC1, 'A') -- optimize for total
	//                                       throughput (reports / analytics)
	//   QueryOptimizeUnset   (0)         -- omit the CP entirely;
	//                                       server uses its job default
	//                                       (typically *ALLIO on V7R5+)
	//
	// Default 0 preserves byte-equality with pre-v0.7.17 fixtures.
	QueryOptimizeGoal byte
}

// DateFormat constants for DBAttributesOptions.DateFormat. The
// byte values are EBCDIC digits ('0'..'7') for historical reasons
// and form the API surface; on the wire we send the integer index
// (0..7) at CP 0x3807 (DateFormatParserOption) per JTOpen's mapping.
// JOB means "let the server pick"; the CP is omitted in that case
// so the server falls through to its job default.
const (
	DateFormatJOB byte = 0xF0 // server default -- CP 0x3807 not sent
	DateFormatUSA byte = 0xF1 // MM/DD/YYYY (10 chars), index 4
	DateFormatISO byte = 0xF2 // YYYY-MM-DD (10 chars), index 5
	DateFormatEUR byte = 0xF3 // DD.MM.YYYY (10 chars), index 6
	DateFormatJIS byte = 0xF4 // YYYY-MM-DD (10 chars), index 7
	DateFormatMDY byte = 0xF5 // MM/DD/YY (8 chars),    index 1
	DateFormatDMY byte = 0xF6 // DD/MM/YY (8 chars),    index 2
	DateFormatYMD byte = 0xF7 // YY-MM-DD (8 chars),    index 3
)

// dateFormatParserIndex maps a DateFormat* byte constant to the
// integer index JTOpen sends at CP 0x3807 (DateFormatParserOption).
// Mirrors AS400JDBCConnectionImpl.java's getDateFormatPO mapping:
// 0=JULIAN, 1=MDY, 2=DMY, 3=YMD, 4=USA, 5=ISO, 6=EUR, 7=JIS.
//
// Returns ok=false for DateFormatJOB (caller must omit the CP) and
// for unrecognised bytes (caller treats as JOB). Julian is not
// exposed via DateFormat* constants because no captured workload
// uses it; if a caller needs it they can fall through to JOB and
// configure on the server side.
func dateFormatParserIndex(format byte) (int16, bool) {
	switch format {
	case DateFormatMDY:
		return 1, true
	case DateFormatDMY:
		return 2, true
	case DateFormatYMD:
		return 3, true
	case DateFormatUSA:
		return 4, true
	case DateFormatISO:
		return 5, true
	case DateFormatEUR:
		return 6, true
	case DateFormatJIS:
		return 7, true
	default:
		// JOB or unknown -- omit CP, server picks.
		return 0, false
	}
}

// dateSeparatorParserIndex returns the CP 0x3808
// (DateSeparatorParserOption) index implied by a date format. JTOpen
// pairs each format with a canonical separator (mirrors the wire
// shapes documented in encodeDateStringForFormat):
//
//	USA / MDY / DMY  -> '/'  (index 0)
//	ISO / JIS / YMD  -> '-'  (index 1)
//	EUR              -> '.'  (index 2)
//
// Returns ok=false for JOB so the caller can omit the CP and let
// the server pair the separator with its job-default format.
func dateSeparatorParserIndex(format byte) (int16, bool) {
	switch format {
	case DateFormatUSA, DateFormatMDY, DateFormatDMY:
		return 0, true // SLASH
	case DateFormatISO, DateFormatJIS, DateFormatYMD:
		return 1, true // DASH
	case DateFormatEUR:
		return 2, true // PERIOD
	default:
		return 0, false
	}
}

// isolationLevelWireValue maps an IsolationLevel option (which uses
// -1 to mean "leave at default") to the int16 we send on the wire.
// The wire CP is always present; -1 just means "use 0 (default)".
func isolationLevelWireValue(level int16) int16 {
	if level == IsolationDefault {
		return 0
	}
	return level
}

// defaultLOBThreshold is the historical hard-coded LOB-field
// threshold sent by go-db2i before LOBThreshold became
// configurable. Matches JT400's "lob threshold" default behaviour
// for V7R5+ where the property is left unset by the application.
const defaultLOBThreshold uint32 = 32768

// dbParamLOBThreshold builds the CP 0x3822 (LOBFieldThreshold)
// parameter for SET_SQL_ATTRIBUTES. Zero falls back to
// defaultLOBThreshold so existing callers keep their wire shape;
// callers can pass an explicit value (including very small or very
// large) to drive the server's inline-LOB threshold.
func dbParamLOBThreshold(threshold uint32) DBParam {
	if threshold == 0 {
		threshold = defaultLOBThreshold
	}
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, threshold)
	return DBParam{CodePoint: 0x3822, Data: b}
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

// QueryOptimizeGoal values for DBAttributesOptions.QueryOptimizeGoal.
// Sent on CP 0x3833 in SET_SQL_ATTRIBUTES as a single EBCDIC byte
// matching JT400's `query optimize goal` JDBC URL property:
//
//	'F' (0xC6) → *FIRSTIO -- optimize for time-to-first-row (good
//	             for streaming reads, OLTP, ResultSet.next() loops)
//	'A' (0xC1) → *ALLIO   -- optimize for total throughput (good
//	             for reports / analytics that drain whole result sets)
//
// Note CP 0x3833 is context-sensitive in the host-server protocol:
// in SET_SQL_ATTRIBUTES it's QueryOptimizeGoal (this constant);
// in OPEN_DESCRIBE_FETCH it's VariableFieldCompr (cpDBVariableFieldCompr
// in db_select.go). The constants live in separate files to make
// the dual use unambiguous.
//
// QueryOptimizeUnset (0) is the wire-equivalence sentinel: when the
// field stays at its zero value, the CP is omitted entirely from
// SET_SQL_ATTRIBUTES, matching the existing select_dummy fixture
// byte-for-byte.
const (
	QueryOptimizeUnset   byte = 0x00
	QueryOptimizeFirstIO byte = 0xC6 // EBCDIC 'F'
	QueryOptimizeAllIO   byte = 0xC1 // EBCDIC 'A'
)

// cpDBQueryOptimizeGoal is the SET_SQL_ATTRIBUTES code point for
// the query-optimize-goal byte. See QueryOptimize* for the encoded
// values. (Numerically identical to cpDBVariableFieldCompr in
// db_select.go, which is the same CP in the OPEN_DESCRIBE_FETCH
// context -- different meaning per frame type.)
const cpDBQueryOptimizeGoal uint16 = 0x3833

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
		// TimeFormat / DateSeparator / TimeSeparator /
		// DecimalSeparator: -1 = omit the CP entirely so the server
		// falls back to the job default (JT400 NOTSET behaviour).
		TimeFormat:       -1,
		DateSeparator:    -1,
		TimeSeparator:    -1,
		DecimalSeparator: -1,
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
		ORSBitmap: ORSReturnData | ORSServerAttributes | ORSDataCompression, // 0x81040000
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
		// CP 0x3805 is JTOpen's TranslateIndicator (per
		// DBSQLAttributesDS.setTranslateIndicator). 0xF0 = "translate
		// host server data to the client's CCSID" -- the only value
		// our wire path is set up to consume. DateFormat lives on a
		// separate CP (0x3807) appended below.
		DBParamByte(0x3805, 0xF0),
		DBParamShort(0x3806, 0x0001),
		DBParamByte(0x3824, 0xE8),
		// CP 0x380E: commitment control level. 0 = server default
		// (matches fixtures); explicit values come from
		// IsolationLevel. We always send the CP -- the value
		// changes based on the option, but JT400 always emits the
		// CP so byte-equality tests rely on it being present.
		DBParamShort(0x380E, isolationLevelWireValue(opts.IsolationLevel)),
	}
	// CP 0x3807 (DateFormatParserOption) + CP 0x3808
	// (DateSeparatorParserOption). Sent as 2-byte shorts only when
	// the user picks an explicit format -- omitted for JOB so the
	// server falls back to its job default. Mirrors JTOpen's
	// AS400JDBCConnectionImpl.java which calls
	// setDateFormatParserOption only when the JDBC URL set
	// "date format". Same shape for the separator.
	//
	// Exception: ExtendedDynamic=true forces emission of all 5
	// date/time/separator CPs (0x3807-0x380B) with JT400's
	// JDBC-default values so the server's package-suffix derivation
	// has a definite value to key on. JT400's
	// AS400JDBCConnectionImpl follows the same pattern when
	// "extended dynamic=true" -- v0.7.2 live testing against IBM
	// Cloud V7R6M0 showed without these the server doesn't file
	// PREPAREd statements into the *PGM even with otherwise
	// byte-identical wire shape on the PREPARE_DESCRIBE itself.
	// Wire order must match JT400's: 0x3807-0x380B come immediately
	// after 0x380E and before 0x380C/0x3823/0x380F.
	// CP 0x3807 (date format) + CP 0x3808 (date separator) +
	// CP 0x3809 (time format) + CP 0x380A (time separator) +
	// CP 0x380B (decimal separator). Each is emitted independently:
	//
	//   - ExtendedDynamic forces all five with JT400's documented
	//     defaults so the package-suffix derivation has a definite
	//     value to key on (CP 0x3807/0x3808 land via the existing
	//     DateFormat path; 0x3809/0x380A/0x380B default to 0).
	//   - DateFormat (non-JOB) emits 0x3807 + auto-derived 0x3808.
	//   - An explicit opts.DateSeparator (>= 0) overrides the auto-
	//     derived 0x3808.
	//   - opts.TimeFormat / TimeSeparator / DecimalSeparator emit
	//     0x3809 / 0x380A / 0x380B when non-negative; otherwise the
	//     CP is omitted so the server uses its job default.
	if opts.ExtendedDynamic {
		params = append(params,
			DBParamShort(0x3807, 0x0001), // DateFormatParserOption: mdy
		)
		dateSep := int16(0) // slash (JT400's ExtendedDynamic default)
		if opts.DateSeparator >= 0 {
			dateSep = int16(opts.DateSeparator)
		}
		params = append(params, DBParamShort(0x3808, dateSep))
		timeFmt := int16(0) // hms
		if opts.TimeFormat >= 0 {
			timeFmt = int16(opts.TimeFormat)
		}
		params = append(params, DBParamShort(0x3809, timeFmt))
		timeSep := int16(0) // colon
		if opts.TimeSeparator >= 0 {
			timeSep = int16(opts.TimeSeparator)
		}
		params = append(params, DBParamShort(0x380A, timeSep))
		decSep := int16(0) // period
		if opts.DecimalSeparator >= 0 {
			decSep = int16(opts.DecimalSeparator)
		}
		params = append(params, DBParamShort(0x380B, decSep))
	} else {
		if idx, ok := dateFormatParserIndex(opts.DateFormat); ok {
			params = append(params, DBParamShort(0x3807, idx))
			// Caller may override the auto-derived separator.
			if opts.DateSeparator >= 0 {
				params = append(params, DBParamShort(0x3808, int16(opts.DateSeparator)))
			} else if sep, ok := dateSeparatorParserIndex(opts.DateFormat); ok {
				params = append(params, DBParamShort(0x3808, sep))
			}
		} else if opts.DateSeparator >= 0 {
			// Explicit DateSeparator without a DateFormat: emit
			// 0x3808 alone (JT400 allows separators without format).
			params = append(params, DBParamShort(0x3808, int16(opts.DateSeparator)))
		}
		if opts.TimeFormat >= 0 {
			params = append(params, DBParamShort(0x3809, int16(opts.TimeFormat)))
		}
		if opts.TimeSeparator >= 0 {
			params = append(params, DBParamShort(0x380A, int16(opts.TimeSeparator)))
		}
		if opts.DecimalSeparator >= 0 {
			params = append(params, DBParamShort(0x380B, int16(opts.DecimalSeparator)))
		}
	}
	params = append(params,
		DBParamShort(0x380C, opts.Naming), // NamingConventionParserOption: 0=sql, 1=system
		DBParamShort(0x3823, 0x0000),
	)
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
		dbParamLOBThreshold(opts.LOBThreshold),                          // LOBFieldThreshold (CP 0x3822)
		DBParamShort(0x3811, 0x0001),                                    // AmbiguousSelectOption
		DBParam{CodePoint: 0x3825, Data: []byte{0xF6, 0x00, 0x00, 0x00}}, // ClientSupportInfo (V7R5+)
		DBParam{CodePoint: 0x3827, Data: []byte{0x00, 0x1F, 0x00, 0x1F, 0x00, 0x00}}, // DecimalPrecisionIndicators
		DBParamByte(0x3828, 0x00),                                       // HexConstantParserOption
		DBParamShort(0x3830, 0x0001),                                    // LocatorPersistence
	)
	// CP 0x3833 QueryOptimizeGoal -- omitted entirely unless the
	// caller set it via ?query-optimize-goal=. Position in the wire
	// order (between LocatorPersistence and the application-info CPs)
	// matches the captured JT400 fixture select_dummy_qog_*.trace.
	if opts.QueryOptimizeGoal != QueryOptimizeUnset {
		params = append(params, DBParamByte(cpDBQueryOptimizeGoal, opts.QueryOptimizeGoal))
	}
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
	// Only fail on errorClass != 0 (real SQL error). errorClass=0 with
	// non-zero RC is a warning/informational (e.g., +8001 when the
	// requested date format is accepted but flagged for some IBM i
	// session-attribute interaction). JT400 is similarly tolerant.
	if dbErr := makeDb2Error(rep, "SET_SQL_ATTRIBUTES"); dbErr != nil {
		return nil, dbErr
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
