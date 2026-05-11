package hostserver

import (
	"fmt"
	"io"
	"strings"
)

// Extended-dynamic package operations on the database host server.
//
// JT400 calls these "SQL Package" function codes. A package is a
// server-side *PGM object that caches PREPAREd statements across
// connections so a re-connecting client (or a co-tenant Go client)
// skips the PREPARE round-trip. See `plans/m10-package-caching.md`
// for the wire-level walkthrough.
const (
	// ReqDBSQLCreatePackage builds a brand-new package on the server.
	// JTOpen sends this on connect when `extended dynamic=true` and
	// the named package doesn't yet exist.
	ReqDBSQLCreatePackage uint16 = 0x180F

	// ReqDBSQLReturnPackage asks the server to ship the current
	// contents of the named package back to the client. JTOpen sends
	// this on connect when `package cache=true` -- the reply
	// carries a CP 0x380B (cpPackageReplyInfo) payload with N
	// statement entries the client cache will be primed with.
	ReqDBSQLReturnPackage uint16 = 0x1815

	// ReqDBSQLDeletePackage drops the named package and every
	// statement entry in it. Not wired into the driver yet -- here
	// for symmetry with the JT400 surface and so future tooling
	// (e.g. a "warm fresh" CLI) has a constant to refer to.
	ReqDBSQLDeletePackage uint16 = 0x1811

	// ReqDBSQLClearPackage empties the named package's statement
	// entries without destroying the *PGM. Same status as
	// ReqDBSQLDeletePackage -- constant defined for completeness.
	ReqDBSQLClearPackage uint16 = 0x1810
)

// Package-context code points used on CREATE_PACKAGE and
// RETURN_PACKAGE requests. The numeric values overlap with several
// existing constants in this package (cpDBServerAttributes,
// cpDBMessageID, cpDBCursorName) -- IBM i reuses the same numeric CP
// across different function-code contexts. Distinct Go names
// disambiguate by intent.
const (
	// cpPackageName is the 10-char package name (CCSID-tagged
	// var-string). Matches JT400 wire output from
	// JDPackageManager.java:482-501. Also appears (empty) on
	// PREPARE_DESCRIBE under extended-dynamic mode as a marker
	// telling the server "use the package context already
	// established by CREATE_PACKAGE on this connection".
	cpPackageName uint16 = 0x3804

	// cpPackageLibrary is the package library (CCSID-tagged
	// var-string). JT400 wire reality: this is 0x3801, NOT 0x3805
	// as the plan originally guessed. (0x3805 is cpDBDataFormat in
	// reply context.)
	cpPackageLibrary uint16 = 0x3801

	// cpPackageReturnOption is the trailing options field on
	// RETURN_PACKAGE; JT400 sends it as a 4-byte zero, meaning
	// "return ALL statements".
	cpPackageReturnOption uint16 = 0x3815

	// cpPackageReplyInfo wraps the per-statement entries on a
	// RETURN_PACKAGE reply. Same numeric value as cpDBCursorName,
	// but distinct context.
	cpPackageReplyInfo uint16 = 0x380B
)

// PackageOptions is the subset of session options the package-name
// suffix derivation reads. Each field maps to a JT400 JDProperties
// entry; the integer values MUST match JT400's enum positions
// (JDProperties.java) for the byte-for-byte interop rule to hold.
// See memory file `project_db2i_m10_jt400_interop.md`.
type PackageOptions struct {
	// TranslateHex: 0 = binary (default), 1 = character.
	TranslateHex int
	// CommitMode: 0 = NONE, 1 = CHG (UR), 2 = CS, 3 = ALL (RS), 4 = RR.
	// JT400 special-cases the 4=RR value because it overflows the
	// 3-bit field; see overflow handling in SuffixFromOptions.
	CommitMode int
	// DateFormat: 0 = julian, 1 = mdy, 2 = dmy, 3 = ymd,
	// 4 = usa, 5 = iso, 6 = eur, 7 = jis.
	DateFormat int
	// DateSeparator: 0 = slash, 1 = dash, 2 = period, 3 = comma,
	// 4 = blank.
	DateSeparator int
	// DecimalSeparator: 0 = period, 1 = comma.
	DecimalSeparator int
	// Naming: 0 = sql, 1 = system.
	Naming int
	// TimeFormat: 0 = hms, 1 = usa, 2 = iso, 3 = eur, 4 = jis.
	TimeFormat int
	// TimeSeparator: 0 = colon, 1 = period, 2 = comma, 3 = blank.
	TimeSeparator int
}

// suffixInvariant is JDPackageManager.java's SUFFIX_INVARIANT_ -- 36
// characters indexed 0..35 ('9' is index 0, 'A' is index 35).
const suffixInvariant = "9876543210ZYXWVUTSRQPONMLKJIHGFEDCBA"

// SuffixFromOptions returns the 4-char package-name suffix JT400
// computes for opts. Mirrors JDPackageManager.java:442-522 byte-for-
// byte; any divergence breaks the cross-driver shared-cache rule.
//
// The formula:
//
//	char1 = suffixInvariant[translateHex]
//	char2 = suffixInvariant[(commitMode<<3) | dateFormat]      // with RR overflow handling
//	char3 = suffixInvariant[(decimalSep<<4) | (naming<<3) | dateSep]
//	char4 = suffixInvariant[(timeFormat<<2) | timeSep]
//
// The RR overflow re-uses dateSep bits to encode commitMode==4 (RR),
// because the 2-bit commit field can only express 0..3.
func SuffixFromOptions(opts PackageOptions) string {
	commitMode := opts.CommitMode
	dateSep := opts.DateSeparator

	// RR overflow: commit=4 doesn't fit in the 3-bit slot
	// (commit<<3 |= 32 already exceeds the SUFFIX_INVARIANT_ index
	// range). JT400 borrows dateSep bits to encode RR.
	if commitMode == 4 {
		switch dateSep {
		case 0, 1, 2:
			commitMode = dateSep
			dateSep = 6
		case 3, 4:
			commitMode = dateSep - 2
			dateSep = 7
		}
	}

	idx1 := opts.TranslateHex
	idx2 := (commitMode << 3) | opts.DateFormat
	idx3 := (opts.DecimalSeparator << 4) | (opts.Naming << 3) | dateSep
	idx4 := (opts.TimeFormat << 2) | opts.TimeSeparator

	// Defensive bounds-clip. JT400 doesn't clamp -- it relies on
	// JDProperties producing valid indexes -- but a divergent value
	// here from a future malformed Config would panic on a slice
	// access. Clamp instead and let the test harness catch the
	// divergence (the resulting suffix won't match JT400, so a
	// shared-cache test will fail loudly).
	idx1 = clipSuffixIndex(idx1)
	idx2 = clipSuffixIndex(idx2)
	idx3 = clipSuffixIndex(idx3)
	idx4 = clipSuffixIndex(idx4)

	var sb strings.Builder
	sb.Grow(4)
	sb.WriteByte(suffixInvariant[idx1])
	sb.WriteByte(suffixInvariant[idx2])
	sb.WriteByte(suffixInvariant[idx3])
	sb.WriteByte(suffixInvariant[idx4])
	return sb.String()
}

func clipSuffixIndex(i int) int {
	if i < 0 {
		return 0
	}
	if i >= len(suffixInvariant) {
		return len(suffixInvariant) - 1
	}
	return i
}

// BuildPackageName takes the user-provided 1..6-char base (DSN
// `package=...` property) and the active session options and returns
// the 10-char on-wire package name. The base is upper-cased,
// space→underscore, and padded with spaces to 6 chars (matching JT400
// `JDPackageManager.java:466`). The 4-char suffix follows.
//
// The 6+4 split is invariant -- JT400 always sends exactly 10 chars
// (with trailing spaces from short bases). Anything > 6 chars in the
// base is truncated; callers should validate at DSN parse time.
func BuildPackageName(base string, opts PackageOptions) string {
	// 6-char base: upper, space→underscore, pad with EBCDIC blanks.
	// We pad with ASCII spaces here; the wire encoder lifts them to
	// EBCDIC 0x40 via the CCSID conversion downstream.
	b := strings.ToUpper(base)
	b = strings.ReplaceAll(b, " ", "_")
	if len(b) > 6 {
		b = b[:6]
	}
	for len(b) < 6 {
		b += " "
	}
	return b + SuffixFromOptions(opts)
}

// PackageStatement is one cached PREPARE entry inside a package. We
// store enough state to replay an EXECUTE (or OPEN_DESCRIBE_FETCH)
// without re-issuing PREPARE_DESCRIBE: the server-assigned 18-byte
// statement name, the original SQL text JT400 used as the cache key,
// and the data + parameter-marker descriptors the server already
// produced.
type PackageStatement struct {
	// Name is the 18-char server-assigned statement name inside the
	// package (e.g. "QZAF4818 5E802E001"). Used in place of the
	// driver's per-connection "STMT0001" rotation on cache-hit
	// EXECUTEs.
	Name string

	// SQLText is the SQL string JT400 prepared. Used as the lookup
	// key for `PackageManager.Lookup`. Stored in the same form JT400
	// sends on the wire -- UCS-2 BE bytes -- so comparison is
	// byte-equal to the cached payload without re-encoding.
	SQLText string

	// DataFormat is the per-column result-set descriptor the server
	// returned at original PREPARE time. Replayed verbatim on the
	// cache-hit EXECUTE so the result-row parser knows the column
	// types without another DESCRIBE.
	DataFormat []SelectColumn

	// ParameterMarkerFormat is the per-marker input descriptor the
	// server returned at original PREPARE time. Replayed verbatim
	// when binding cache-hit EXECUTE inputs.
	ParameterMarkerFormat []ParameterMarkerField
}

// PackageManager holds the per-connection package state. M10-1
// ships only the scaffolding; M10-3 wires it into the connect-time
// RETURN_PACKAGE round trip and the per-statement cache lookup.
type PackageManager struct {
	// Name is the resolved 10-char package name on the wire
	// (BuildPackageName output).
	Name string
	// Library is the package library (DSN `package-library`,
	// default "QGPL").
	Library string
	// CCSID is the package-CCSID (DSN `package-ccsid`); JT400
	// defaults to 13488 (UCS-2 BE).
	CCSID uint16
	// Cached statements downloaded via RETURN_PACKAGE. Populated by
	// M10-3 on connect.
	Cached []PackageStatement
}

// BuildCreatePackageParams builds the parameter list for a
// CREATE_PACKAGE (0x180F) request. The caller assembles the
// surrounding DBRequestTemplate and adds the result via
// BuildDBRequest.
//
// Wire shape (from fixture prepared_package_first_use.trace frame
// #5): 2 params -- CP 0x3804 (package name, CCSID-tagged var-string)
// + CP 0x3801 (library, CCSID-tagged var-string).
func BuildCreatePackageParams(name, library string, ccsid uint16) ([]DBParam, error) {
	nameBytes, err := ebcdicVarStringBytes(name, ccsid)
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode package name: %w", err)
	}
	libBytes, err := ebcdicVarStringBytes(library, ccsid)
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode package library: %w", err)
	}
	return []DBParam{
		DBParamVarString(cpPackageName, ccsid, nameBytes),
		DBParamVarString(cpPackageLibrary, ccsid, libBytes),
	}, nil
}

// BuildReturnPackageParams builds the parameter list for a
// RETURN_PACKAGE (0x1815) request -- the connect-time download path
// when `package cache=true`. Wire shape from fixture
// prepared_package_cache_download.trace frame #6: 3 params, name +
// library + an option DWORD (0 = "return ALL statements").
func BuildReturnPackageParams(name, library string, ccsid uint16) ([]DBParam, error) {
	nameBytes, err := ebcdicVarStringBytes(name, ccsid)
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode package name: %w", err)
	}
	libBytes, err := ebcdicVarStringBytes(library, ccsid)
	if err != nil {
		return nil, fmt.Errorf("hostserver: encode package library: %w", err)
	}
	return []DBParam{
		DBParamVarString(cpPackageName, ccsid, nameBytes),
		DBParamVarString(cpPackageLibrary, ccsid, libBytes),
		// 4 bytes of zero: "return all statements". JT400 mirrors
		// this on every RETURN_PACKAGE call (no per-statement
		// filtering on the wire; clients filter client-side after
		// receiving the reply).
		{CodePoint: cpPackageReturnOption, Data: []byte{0x00, 0x00, 0x00, 0x00}},
	}, nil
}

// SendCreatePackage issues a CREATE_PACKAGE (0x180F) request on
// conn and reads the matching reply. JT400 sends this on connect
// when extended-dynamic is on and the named *PGM doesn't yet exist
// on the server. The server is idempotent: if the *PGM already
// exists, it returns success without changes.
//
// nextCorr is the connection's correlation-ID counter. The reply
// is consumed (and any error decoded) inside this call.
func SendCreatePackage(conn io.ReadWriter, name, library string, ccsid uint16, nextCorr func() uint32) error {
	params, err := BuildCreatePackageParams(name, library, ccsid)
	if err != nil {
		return fmt.Errorf("hostserver: build CREATE_PACKAGE: %w", err)
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLCreatePackage, DBRequestTemplate{
		// JT400 sets ORS bitmap to 0x80040000 -- ReturnSQLCA only
		// (we need SQLCA to detect "package already exists" and
		// other lifecycle outcomes). Matches fixture frame #5.
		ORSBitmap: 0x80040000,
	}, params)
	if err != nil {
		return fmt.Errorf("hostserver: build CREATE_PACKAGE: %w", err)
	}
	hdr.CorrelationID = nextCorr()
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send CREATE_PACKAGE: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, hdr.CorrelationID, 4)
	if err != nil {
		return fmt.Errorf("hostserver: read CREATE_PACKAGE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return fmt.Errorf("hostserver: CREATE_PACKAGE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return fmt.Errorf("hostserver: parse CREATE_PACKAGE reply: %w", err)
	}
	if dbErr := makeDb2Error(rep, "CREATE_PACKAGE"); dbErr != nil {
		// Common-case "package already exists" comes back as
		// SQL +20000 / SQLSTATE 01ZZZ in the SQLCA -- a warning,
		// not an error, and the server has already prepared the
		// existing *PGM for the rest of this connection. We let
		// the caller decide what to do with the surface error;
		// the driver's package-error config handler folds warning
		// cases into an slog.Warn.
		return dbErr
	}
	return nil
}

// SendReturnPackage issues a RETURN_PACKAGE (0x1815) request on
// conn and reads the matching reply. JT400 sends this on connect
// when package-cache=true; the reply carries CP 0x380B with the
// server-cached statement entries. The body of the reply is
// returned verbatim for the caller (or a follow-up parser) to
// decode -- the M10-3 deliverable ships the wire round-trip; the
// per-statement parse is wired in a follow-up.
func SendReturnPackage(conn io.ReadWriter, name, library string, ccsid uint16, nextCorr func() uint32) ([]byte, error) {
	params, err := BuildReturnPackageParams(name, library, ccsid)
	if err != nil {
		return nil, fmt.Errorf("hostserver: build RETURN_PACKAGE: %w", err)
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLReturnPackage, DBRequestTemplate{
		ORSBitmap: 0x80140000, // ORSReturnSQLCA | ORSPackageInfo
	}, params)
	if err != nil {
		return nil, fmt.Errorf("hostserver: build RETURN_PACKAGE: %w", err)
	}
	hdr.CorrelationID = nextCorr()
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send RETURN_PACKAGE: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, hdr.CorrelationID, 4)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read RETURN_PACKAGE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: RETURN_PACKAGE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse RETURN_PACKAGE reply: %w", err)
	}
	if dbErr := makeDb2Error(rep, "RETURN_PACKAGE"); dbErr != nil {
		return nil, dbErr
	}
	// Pull the cpPackageReplyInfo (0x380B) payload out of the parsed
	// reply, if present. JT400 ships it as one CP with the entire
	// package-info structure inside. The detailed parse is deferred
	// to M10-followup; M10-3 demonstrates the wire round-trip is
	// happy.
	for _, p := range rep.Params {
		if p.CodePoint == cpPackageReplyInfo {
			out := make([]byte, len(p.Data))
			copy(out, p.Data)
			return out, nil
		}
	}
	return nil, nil
}

// ebcdicVarStringBytes converts s to EBCDIC bytes for the given
// CCSID, ready to be wrapped by DBParamVarString. The package-name
// and library fields are always CCSID-37 (US EBCDIC) in the JT400
// wire we observed; supporting arbitrary CCSIDs here is forward
// compatibility for the JT400 `package-ccsid` property.
func ebcdicVarStringBytes(s string, ccsid uint16) ([]byte, error) {
	// The package name + library are restricted to A-Z, 0-9, '_',
	// '#', '@', '$' per IBM i object-name rules. Every such code
	// point round-trips identically across CCSID 37 and the other
	// SBCS EBCDIC pages, so a single asciiToEBCDIC37 helper covers
	// every CCSID we accept. If we ever expand the accepted name
	// charset we'll need a proper conversion table.
	if ccsid != 37 && ccsid != 13488 {
		// 13488 is JT400's `package-ccsid` default but on the
		// CREATE_PACKAGE wire the name is sent in JOB CCSID, not
		// 13488. Allow the call to proceed; caller is responsible
		// for picking the right ccsid value.
	}
	out := make([]byte, len(s))
	for i, r := range []byte(s) {
		b, err := asciiToEBCDIC37(r)
		if err != nil {
			return nil, fmt.Errorf("position %d (%q): %w", i, r, err)
		}
		out[i] = b
	}
	return out, nil
}

// asciiToEBCDIC37 maps the IBM-i-object-name charset to EBCDIC
// CP 37. Returns an error for any byte outside the accepted set so
// the caller can surface a clear DSN-validation message.
func asciiToEBCDIC37(b byte) (byte, error) {
	switch {
	case b >= 'A' && b <= 'I':
		return 0xC1 + (b - 'A'), nil
	case b >= 'J' && b <= 'R':
		return 0xD1 + (b - 'J'), nil
	case b >= 'S' && b <= 'Z':
		return 0xE2 + (b - 'S'), nil
	case b >= '0' && b <= '9':
		return 0xF0 + (b - '0'), nil
	case b == ' ':
		return 0x40, nil
	case b == '_':
		return 0x6D, nil
	case b == '#':
		return 0x7B, nil
	case b == '@':
		return 0x7C, nil
	case b == '$':
		return 0x5B, nil
	default:
		return 0, fmt.Errorf("byte 0x%02X not allowed in package name", b)
	}
}
