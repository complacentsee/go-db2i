package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode/utf16"

	"github.com/complacentsee/go-db2i/ebcdic"
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
	// Name is the 18-char server-assigned statement name decoded
	// from EBCDIC (e.g. "QZAF481815E802E001"). Useful for logs and
	// QSYS2.SYSPACKAGE.STATEMENT_NAME cross-checks. The on-wire
	// EXECUTE uses NameBytes (verbatim EBCDIC).
	Name string

	// NameBytes is the raw 18-byte EBCDIC statement name as it sat
	// inside the CP 0x380B payload. Sent verbatim in CP 0x3806 on
	// cache-hit EXECUTEs so the server-side byte equality holds
	// even if the name contains characters that would normalise
	// across CCSIDs.
	NameBytes []byte

	// StatementType is JT400's statement-type code stored inside
	// the package (2 = SELECT, 3 = INSERT, 4 = UPDATE, 5 = DELETE
	// in our wire usage). Useful for an early "is this cacheable
	// from this caller's verb" cross-check.
	StatementType uint16

	// NeedsDefaultCollection is the byte at entry+0; nonzero means
	// the server will look up unqualified names against the package
	// header's default-collection field. Mirrors JT400's
	// DBReplyPackageInfo.getStatementNeedsDefaultCollection.
	NeedsDefaultCollection byte

	// SQLText is the SQL string JT400 prepared, decoded from the
	// package header's CCSID (UCS-2 BE in our V7R6 fixtures).
	// Used as the lookup key for `PackageManager.Lookup`.
	SQLText string

	// DataFormat is the per-column result-set descriptor the server
	// returned at original PREPARE time, decoded from the SQLDA
	// region inside the entry. Empty for non-SELECT statements.
	DataFormat []SelectColumn

	// ParameterMarkerFormat is the per-marker input descriptor the
	// server returned at original PREPARE time, decoded from the
	// pm-format SQLDA region. Empty for marker-less statements.
	ParameterMarkerFormat []ParameterMarkerField

	// RawDataFormat is the raw bytes of the data-format SQLDA
	// region (or nil if length was 0/6, the "no format" sentinels).
	// Retained so a future EXECUTE/OPEN-by-name path can re-send
	// them on the wire byte-equal to what the server stored.
	RawDataFormat []byte

	// RawParameterMarkerFormat is the raw bytes of the
	// parameter-marker-format SQLDA region (or nil if absent).
	RawParameterMarkerFormat []byte
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
// server-cached statement entries, which we parse into
// PackageStatement values so the driver can drive the cache-hit
// fast path. Returns nil/nil when the package exists but holds
// zero statements (fresh *PGM).
func SendReturnPackage(conn io.ReadWriter, name, library string, ccsid uint16, nextCorr func() uint32) ([]PackageStatement, error) {
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
	// reply, if present, and decode it into PackageStatement entries.
	for _, p := range rep.Params {
		if p.CodePoint == cpPackageReplyInfo {
			return ParsePackageInfo(p.Data)
		}
	}
	return nil, nil
}

// Per-statement entry layout inside a CP 0x380B (package info)
// reply. Mirrors JT400's DBReplyPackageInfo.java byte-for-byte. The
// values below are byte offsets within ONE 64-byte entry record.
const (
	packageEntryHeaderLen = 42 // bytes before the first entry record
	packageEntrySize      = 64 // fixed stride per entry

	pkgEntryNeedsCollection = 0  // 1 byte
	pkgEntryStatementType   = 1  // 2 bytes
	pkgEntryStatementName   = 3  // 18 bytes (EBCDIC, padded with 0x40)
	pkgEntryReserved        = 21 // 19 bytes
	pkgEntryFormatOffset    = 40 // 4 bytes
	pkgEntryFormatLen       = 44 // 4 bytes
	pkgEntryTextOffset      = 48 // 4 bytes
	pkgEntryTextLen         = 52 // 4 bytes
	pkgEntryPMFormatOffset  = 56 // 4 bytes
	pkgEntryPMFormatLen     = 60 // 4 bytes

	// Inside the CP 0x380B body the offset fields are written "as
	// if" there were a 6-byte LL/CP wrapper preceding the body --
	// matching JT400's `(offset_ - 6) + offset` indexing trick.
	// We unfold by subtracting 6 from each stored offset before
	// indexing into the body slice. Length fields are NOT adjusted
	// (they're the raw region byte counts).
	packageOffsetBias = 6

	// JT400 treats length=6 (the LL/CP wrapper bytes alone) and
	// length=0 as "no data here" for the format / pm-format regions.
	// See DBReplyPackageInfo.java:getDataFormat.
	packageEmptyFormatLen = 6
)

// CCSID written into the CP 0x380B header at offset+4. 0x34b0 =
// 13488 (UCS-2 BE), which is the only value our V7R6M0 fixtures
// have observed; JT400's DBReplyPackageInfo treats it as
// authoritative for both the SQL text and the statement-name
// decoding. Defined as a named constant so future expansion to
// CCSID 1200 (UTF-16 BE) or 37 (US EBCDIC) lands as a switch.
const packageHeaderCCSIDOffset = 4

// SQLDA layout inside the format / pm-format region of a package
// entry. JT400's DBSQLDADataFormat.java reads:
//
//	+0..+13 8 bytes EBCDIC "SQLDA   " + 6 bytes header
//	+14..+15 numberOfFields (uint16 BE)
//	+16+     per-field fixed records of 80 bytes
//
// Each per-field record:
//
//	+0..+1   SQL type (uint16 BE)
//	+2..+3   length (uint16 BE; high byte = precision)
//	+3        scale (byte; same byte as length low)
//	+18..+19 CCSID (uint16 BE) -- relative to SQLDA start +34 minus 16
//	+32      parameter direction byte (EBCDIC 'I'=C9 / 'O'=D6 / 'B'=C2)
//	+48..+49 name length (uint16 BE)
//	+50..    name bytes (job CCSID)
const (
	sqldaHeaderLen          = 16
	sqldaFieldRecordLen     = 80 // REPEATED_LENGTH_ in JT400
	sqldaNumberOfFieldsHigh = 14 // numberOfFields lives at SQLDA+14..+15
	// per-field offsets, relative to the start of the 80-byte record
	sqldaFieldSQLType    = 0
	sqldaFieldLength     = 2
	sqldaFieldScale      = 3
	sqldaFieldPrecision  = 2
	sqldaFieldCCSID      = 18
	sqldaFieldParamType  = 32
	sqldaFieldNameLength = 48
	sqldaFieldNameStart  = 50
)

// SQLDA "magic" prefix in EBCDIC. JT400's DBSQLDADataFormat doesn't
// itself check this, but our parser validates the marker so a
// malformed payload surfaces with a useful error instead of garbage
// SQL types.
var sqldaMagicPrefix = []byte{0xe2, 0xd8, 0xd3, 0xc4, 0xc1} // "SQLDA"

// ParsePackageInfo decodes the CP 0x380B (package info) reply body
// into a slice of PackageStatement values. The body is what
// SendReturnPackage receives as the cpPackageReplyInfo CP's payload.
//
// Layout (from JT400 DBReplyPackageInfo.java):
//
//	+0..3   total length      (uint32 BE; matches len(body))
//	+4..5   CCSID             (uint16 BE; 13488 in our fixtures)
//	+6..23  default collection (18 bytes, EBCDIC blank-padded)
//	+24..25 statement count   (uint16 BE)
//	+26..41 reserved          (16 bytes, all zero in fixtures)
//	+42..   per-statement entries, fixed 64-byte stride
//
// Each entry's variable-length SQL text + SQLDA format regions live
// past the last entry record; offsets are written assuming a 6-byte
// LL/CP wrapper precedes the body.
//
// Returns an error if the body is shorter than the declared total
// length or the statement count cannot be reconciled with the entry
// records present.
func ParsePackageInfo(body []byte) ([]PackageStatement, error) {
	if len(body) < packageEntryHeaderLen {
		return nil, fmt.Errorf("hostserver: CP 0x380B body too short: %d bytes", len(body))
	}
	be := binary.BigEndian
	totalLen := be.Uint32(body[0:4])
	if int(totalLen) > len(body) {
		return nil, fmt.Errorf("hostserver: CP 0x380B total_length=%d exceeds body=%d", totalLen, len(body))
	}
	headerCCSID := be.Uint16(body[packageHeaderCCSIDOffset : packageHeaderCCSIDOffset+2])
	count := be.Uint16(body[24:26])
	if count == 0 {
		return nil, nil
	}
	// Sanity: the entry records alone must fit in totalLen.
	if int(totalLen) < packageEntryHeaderLen+int(count)*packageEntrySize {
		return nil, fmt.Errorf("hostserver: CP 0x380B truncated: total=%d count=%d (need >= %d)",
			totalLen, count, packageEntryHeaderLen+int(count)*packageEntrySize)
	}

	out := make([]PackageStatement, 0, count)
	for i := 0; i < int(count); i++ {
		entryOff := packageEntryHeaderLen + i*packageEntrySize
		eb := body[entryOff : entryOff+packageEntrySize]

		nameBytes := make([]byte, 18)
		copy(nameBytes, eb[pkgEntryStatementName:pkgEntryStatementName+18])
		nameStr, err := ebcdic.CCSID37.Decode(nameBytes)
		if err != nil {
			// CCSID37 covers IBM i statement-name characters
			// (uppercase + digits + a few punct); a decode failure
			// here means corrupt bytes, surface it.
			return nil, fmt.Errorf("hostserver: CP 0x380B entry %d name decode: %w", i, err)
		}
		ps := PackageStatement{
			Name:                   strings.TrimRight(nameStr, " "),
			NameBytes:              nameBytes,
			StatementType:          be.Uint16(eb[pkgEntryStatementType : pkgEntryStatementType+2]),
			NeedsDefaultCollection: eb[pkgEntryNeedsCollection],
		}

		textOff := be.Uint32(eb[pkgEntryTextOffset : pkgEntryTextOffset+4])
		textLen := be.Uint32(eb[pkgEntryTextLen : pkgEntryTextLen+4])
		if textLen > 0 {
			start, err := unbiasPackageOffset(textOff, textLen, uint32(len(body)))
			if err != nil {
				return nil, fmt.Errorf("hostserver: CP 0x380B entry %d text: %w", i, err)
			}
			ps.SQLText, err = decodePackageText(body[start:start+int(textLen)], headerCCSID)
			if err != nil {
				return nil, fmt.Errorf("hostserver: CP 0x380B entry %d text decode: %w", i, err)
			}
		}

		fmtOff := be.Uint32(eb[pkgEntryFormatOffset : pkgEntryFormatOffset+4])
		fmtLen := be.Uint32(eb[pkgEntryFormatLen : pkgEntryFormatLen+4])
		if fmtLen > packageEmptyFormatLen {
			start, err := unbiasPackageOffset(fmtOff, fmtLen, uint32(len(body)))
			if err != nil {
				return nil, fmt.Errorf("hostserver: CP 0x380B entry %d data-format: %w", i, err)
			}
			raw := body[start : start+int(fmtLen)]
			cols, err := parsePackageSQLDADataFormat(raw)
			if err != nil {
				return nil, fmt.Errorf("hostserver: CP 0x380B entry %d data-format: %w", i, err)
			}
			ps.DataFormat = cols
			ps.RawDataFormat = append([]byte(nil), raw...)
		}

		pmOff := be.Uint32(eb[pkgEntryPMFormatOffset : pkgEntryPMFormatOffset+4])
		pmLen := be.Uint32(eb[pkgEntryPMFormatLen : pkgEntryPMFormatLen+4])
		if pmLen > packageEmptyFormatLen {
			start, err := unbiasPackageOffset(pmOff, pmLen, uint32(len(body)))
			if err != nil {
				return nil, fmt.Errorf("hostserver: CP 0x380B entry %d pm-format: %w", i, err)
			}
			raw := body[start : start+int(pmLen)]
			markers, err := parsePackageSQLDAParameterMarkerFormat(raw)
			if err != nil {
				return nil, fmt.Errorf("hostserver: CP 0x380B entry %d pm-format: %w", i, err)
			}
			ps.ParameterMarkerFormat = markers
			ps.RawParameterMarkerFormat = append([]byte(nil), raw...)
		}

		out = append(out, ps)
	}
	return out, nil
}

// unbiasPackageOffset converts a stored offset (which assumes a
// 6-byte LL/CP wrapper precedes the body) into a valid index into
// the body slice, validating it against the region length.
func unbiasPackageOffset(off, regionLen, bodyLen uint32) (int, error) {
	if off < packageOffsetBias {
		return 0, fmt.Errorf("offset %d below LL/CP bias of %d", off, packageOffsetBias)
	}
	start := off - packageOffsetBias
	if start+regionLen > bodyLen {
		return 0, fmt.Errorf("region [%d,%d) past body len %d", start, start+regionLen, bodyLen)
	}
	return int(start), nil
}

// decodePackageText decodes a SQL-text region inside the package
// payload using the package header's CCSID. JT400 supports CCSIDs 37
// (US EBCDIC) and 13488 (UCS-2 BE) here; our V7R6M0 fixtures only
// ship 13488, so we implement that path and reject anything else
// with a typed error.
func decodePackageText(raw []byte, ccsid uint16) (string, error) {
	switch ccsid {
	case 13488, 1200:
		// UCS-2 BE / UTF-16 BE -- text is N UCS-2 code units. The
		// region may have trailing zero padding past the last real
		// character; trim that before decoding so the resulting Go
		// string doesn't carry NUL runs.
		if len(raw)%2 != 0 {
			return "", fmt.Errorf("UCS-2 text length %d is not even", len(raw))
		}
		codes := make([]uint16, 0, len(raw)/2)
		for i := 0; i+1 < len(raw); i += 2 {
			c := binary.BigEndian.Uint16(raw[i : i+2])
			if c == 0 {
				break
			}
			codes = append(codes, c)
		}
		return string(utf16.Decode(codes)), nil
	case 37:
		// Trim trailing zero bytes; then EBCDIC decode.
		end := len(raw)
		for end > 0 && raw[end-1] == 0 {
			end--
		}
		return ebcdic.CCSID37.Decode(raw[:end])
	default:
		return "", fmt.Errorf("unsupported package text CCSID %d", ccsid)
	}
}

// parsePackageSQLDADataFormat decodes the SQLDA-format bytes from a
// package data-format region into SelectColumn descriptors. Mirrors
// JT400's DBSQLDADataFormat field-by-field. Returns nil/nil when
// the SQLDA holds zero fields.
func parsePackageSQLDADataFormat(raw []byte) ([]SelectColumn, error) {
	numFields, err := validateSQLDAHeader(raw)
	if err != nil {
		return nil, err
	}
	if numFields == 0 {
		return nil, nil
	}
	be := binary.BigEndian
	cols := make([]SelectColumn, 0, numFields)
	for i := 0; i < numFields; i++ {
		base := sqldaHeaderLen + i*sqldaFieldRecordLen
		sqlType := be.Uint16(raw[base+sqldaFieldSQLType : base+sqldaFieldSQLType+2])
		length := uint32(be.Uint16(raw[base+sqldaFieldLength : base+sqldaFieldLength+2]))
		precision := uint16(raw[base+sqldaFieldPrecision])
		scale := uint16(raw[base+sqldaFieldScale])
		ccsid := be.Uint16(raw[base+sqldaFieldCCSID : base+sqldaFieldCCSID+2])
		col := SelectColumn{
			SQLType:   sqlType,
			Length:    length,
			Scale:     scale,
			Precision: precision,
			CCSID:     ccsid,
		}
		name, err := readSQLDAFieldName(raw, base)
		if err == nil {
			col.Name = name
		}
		col.TypeName, col.DisplaySize, col.Signed = sqlTypeMetadata(col.SQLType, col.Length, col.Precision, col.Scale)
		col.Nullable = col.SQLType%2 == 1
		cols = append(cols, col)
	}
	return cols, nil
}

// parsePackageSQLDAParameterMarkerFormat decodes the SQLDA bytes
// from a package pm-format region into ParameterMarkerField shapes.
// JT400's DBSQLDADataFormat returns -1 for LOBLocator/LOBMaxSize on
// SQLDA, matching the package-storage rule that LOBs trigger a
// re-prepare; we leave both zero.
func parsePackageSQLDAParameterMarkerFormat(raw []byte) ([]ParameterMarkerField, error) {
	numFields, err := validateSQLDAHeader(raw)
	if err != nil {
		return nil, err
	}
	if numFields == 0 {
		return nil, nil
	}
	be := binary.BigEndian
	out := make([]ParameterMarkerField, 0, numFields)
	for i := 0; i < numFields; i++ {
		base := sqldaHeaderLen + i*sqldaFieldRecordLen
		f := ParameterMarkerField{
			SQLType:     be.Uint16(raw[base+sqldaFieldSQLType : base+sqldaFieldSQLType+2]),
			FieldLength: uint32(be.Uint16(raw[base+sqldaFieldLength : base+sqldaFieldLength+2])),
			Precision:   uint16(raw[base+sqldaFieldPrecision]),
			Scale:       uint16(raw[base+sqldaFieldScale]),
			CCSID:       be.Uint16(raw[base+sqldaFieldCCSID : base+sqldaFieldCCSID+2]),
			ParamType:   sqldaParamDirection(raw[base+sqldaFieldParamType]),
		}
		if name, err := readSQLDAFieldName(raw, base); err == nil {
			f.Name = name
		}
		out = append(out, f)
	}
	return out, nil
}

// validateSQLDAHeader checks the 16-byte SQLDA header and returns
// the number-of-fields field. The "SQLDA" EBCDIC magic is
// mandatory: a body that doesn't start with it is from a
// non-SQLDA descriptor variant we don't support yet.
func validateSQLDAHeader(raw []byte) (int, error) {
	if len(raw) < sqldaHeaderLen {
		return 0, fmt.Errorf("SQLDA region too short: %d bytes", len(raw))
	}
	if !bytesHasPrefix(raw, sqldaMagicPrefix) {
		return 0, fmt.Errorf("SQLDA region missing %q magic prefix (first bytes: %x)",
			"SQLDA", raw[:5])
	}
	numFields := int(binary.BigEndian.Uint16(raw[sqldaNumberOfFieldsHigh : sqldaNumberOfFieldsHigh+2]))
	if numFields < 0 || numFields > 1<<16 {
		return 0, fmt.Errorf("SQLDA implausible field count %d", numFields)
	}
	want := sqldaHeaderLen + numFields*sqldaFieldRecordLen
	if len(raw) < want {
		return 0, fmt.Errorf("SQLDA truncated: have %d bytes, need >= %d for %d fields",
			len(raw), want, numFields)
	}
	return numFields, nil
}

// bytesHasPrefix avoids importing the "bytes" package just for one
// HasPrefix call (db_package.go already pulls "binary" / "strings"
// and that's plenty).
func bytesHasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}

// sqldaParamDirection maps the EBCDIC direction byte JT400 stores
// at SQLDA field offset +32 to the 0xF0/0xF1/0xF2 surface our
// PreparedParam.ParamType uses elsewhere in the package.
//
//	'I' (0xC9) -> 0xF0 (input)
//	'O' (0xD6) -> 0xF1 (output)
//	'B' (0xC2) -> 0xF2 (inout)
//
// Anything else falls back to "input" (matches DBSQLDADataFormat's
// default switch arm).
func sqldaParamDirection(b byte) byte {
	switch b {
	case 0xD6:
		return 0xF1
	case 0xC2:
		return 0xF2
	default:
		return 0xF0
	}
}

// readSQLDAFieldName returns the name stored in a SQLDA per-field
// record. The name length lives at sqldaFieldNameLength (uint16 BE)
// and the bytes follow at sqldaFieldNameStart. JT400 decodes with
// the connection's job CCSID; we default to CCSID 37 (US EBCDIC),
// matching what our IBM Cloud V7R6M0 LPAR has been seen to use for
// these auto-generated marker names ("00001", "00002", ...).
func readSQLDAFieldName(raw []byte, fieldBase int) (string, error) {
	be := binary.BigEndian
	if fieldBase+sqldaFieldNameStart > len(raw) {
		return "", fmt.Errorf("field record %d truncated", fieldBase)
	}
	nameLen := int(be.Uint16(raw[fieldBase+sqldaFieldNameLength : fieldBase+sqldaFieldNameLength+2]))
	if nameLen == 0 {
		return "", nil
	}
	start := fieldBase + sqldaFieldNameStart
	if start+nameLen > len(raw) {
		return "", fmt.Errorf("field name overruns record: start=%d len=%d raw=%d", start, nameLen, len(raw))
	}
	return ebcdic.CCSID37.Decode(raw[start : start+nameLen])
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
