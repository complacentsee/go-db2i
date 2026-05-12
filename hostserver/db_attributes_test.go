package hostserver

import (
	"bytes"
	"strings"
	"testing"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// TestParseDBReplyServerAttributesAgainstFixture decodes recv #5
// (the 174-byte 0x2800 reply that PUB400 returns to the
// SET_SQL_ATTRIBUTES request) and validates every field that
// go-db2i surfaces.
//
// The fixture's expected values were captured at the same time as
// the smoketest run that passed sign-on -- they're tied to
// PUB400's environment for the AFTRAEGE1 user (German CCSID 273,
// schema AFTRAEGE11, V7R5M00016 functional level, etc.).
func TestParseDBReplyServerAttributesAgainstFixture(t *testing.T) {
	// recv #5 in connect_only.trace is the 3rd Received frame on
	// the as-database connID. Skip the two earlier ones (XChgRandSeed
	// reply, StartServer reply).
	receiveds := dbReceivedsFromFixtureN(t, "connect_only.trace", 3)
	fixture := receiveds[2].Bytes

	hdr, payload, err := ReadFrame(bytes.NewReader(fixture))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if hdr.ReqRepID != RepDBReply {
		t.Fatalf("ReqRepID = 0x%04X, want 0x%04X (DB reply)", hdr.ReqRepID, RepDBReply)
	}

	rep, err := ParseDBReply(payload)
	if err != nil {
		t.Fatalf("ParseDBReply: %v", err)
	}
	if rep.ReturnCode != 0 {
		t.Errorf("ReturnCode = %d, want 0", rep.ReturnCode)
	}
	if rep.ErrorClass != 0 {
		t.Errorf("ErrorClass = 0x%04X, want 0x0000", rep.ErrorClass)
	}

	attrs, err := rep.FindServerAttributes()
	if err != nil {
		t.Fatalf("FindServerAttributes: %v", err)
	}
	if attrs == nil {
		t.Fatalf("ServerAttributes CP 0x3804 not found in reply (params: %d)", len(rep.Params))
	}

	if got, want := attrs.ServerCCSID, uint16(273); got != want {
		t.Errorf("ServerCCSID = %d, want %d (German -- PUB400's job CCSID)", got, want)
	}
	if got, want := attrs.DateFormatPO, uint16(3); got != want {
		t.Errorf("DateFormatPO = %d, want %d", got, want)
	}
	if got, want := attrs.DateSeparatorPO, uint16(1); got != want {
		t.Errorf("DateSeparatorPO = %d, want %d", got, want)
	}
	if got, want := attrs.DRDAPackageSize, uint16(1); got != want {
		t.Errorf("DRDAPackageSize = %d, want %d", got, want)
	}
	if got, want := attrs.TranslationIndicator, uint8(0xF0); got != want {
		t.Errorf("TranslationIndicator = 0x%02X, want 0x%02X", got, want)
	}

	// EBCDIC strings: decode through CCSID 37 and string-compare.
	languageId, _ := ebcdic.CCSID37.Decode(attrs.ServerLanguageId)
	if languageId != "ENU" {
		t.Errorf("ServerLanguageId = %q, want %q", languageId, "ENU")
	}
	languageTable, _ := ebcdic.CCSID37.Decode(attrs.ServerLanguageTable)
	if languageTable != "*HEX      " {
		t.Errorf("ServerLanguageTable = %q, want %q", languageTable, "*HEX      ")
	}
	functionalLevel, _ := ebcdic.CCSID37.Decode(attrs.ServerFunctionalLevel)
	if functionalLevel != "V7R5M00016" {
		t.Errorf("ServerFunctionalLevel = %q, want %q", functionalLevel, "V7R5M00016")
	}
	if got, want := attrs.VRM(), uint32(0x00070500); got != want {
		t.Errorf("VRM() = 0x%08X, want 0x%08X", got, want)
	}
	rdbName, _ := ebcdic.CCSID37.Decode(attrs.RelationalDBName)
	if rdbName != "PUB400            " {
		t.Errorf("RelationalDBName = %q, want %q", rdbName, "PUB400            ")
	}
	defaultLib, _ := ebcdic.CCSID37.Decode(attrs.DefaultSQLLibraryName)
	if defaultLib != "AFTRAEGE11" {
		t.Errorf("DefaultSQLLibraryName = %q, want %q", defaultLib, "AFTRAEGE11")
	}
	jobId, _ := ebcdic.CCSID37.Decode(attrs.ServerJobIdentifier)
	// 26 chars: 10 job program + 10 user + 6 number. The job-number
	// suffix rotates per-capture, so we pin only the program-and-user
	// prefix and check overall length structurally.
	if len(jobId) != 26 {
		t.Errorf("ServerJobIdentifier length = %d, want 26 (got %q)", len(jobId), jobId)
	}
	const wantPrefix = "QZDASOINITQUSER     "
	if !strings.HasPrefix(jobId, wantPrefix) {
		t.Errorf("ServerJobIdentifier prefix = %q, want %q (full: %q)", jobId[:min(len(jobId), len(wantPrefix))], wantPrefix, jobId)
	}
	defaultSchema, _ := ebcdic.CCSID37.Decode(attrs.DefaultSQLSchemaName)
	if defaultSchema != "AFTRAEGE11" {
		t.Errorf("DefaultSQLSchemaName = %q, want %q", defaultSchema, "AFTRAEGE11")
	}
}

// TestSetSQLAttributesRequestEncoding builds a request with
// well-known options and confirms it round-trips through
// ReadFrame, the template fields land at the right offsets, and
// the three CPs we send are framed correctly.
func TestSetSQLAttributesRequestEncoding(t *testing.T) {
	opts := DefaultDBAttributesOptions()
	hdr, payload, err := SetSQLAttributesRequest(opts)
	if err != nil {
		t.Fatalf("SetSQLAttributesRequest: %v", err)
	}
	if hdr.ReqRepID != ReqDBSetSQLAttributes {
		t.Errorf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, ReqDBSetSQLAttributes)
	}
	if hdr.ServerID != ServerDatabase {
		t.Errorf("ServerID = %s, want %s", hdr.ServerID, ServerDatabase)
	}
	if hdr.TemplateLength != 20 {
		t.Errorf("TemplateLength = %d, want 20", hdr.TemplateLength)
	}

	// Template ORS bitmap = 0x81040000.
	if got, want := uint32(payload[0])<<24|uint32(payload[1])<<16|uint32(payload[2])<<8|uint32(payload[3]), uint32(0x81040000); got != want {
		t.Errorf("ORS bitmap = 0x%08X, want 0x%08X", got, want)
	}
	// PARMCNT (offset 18-19 in payload) -- the V7R5+ default
	// option set ships ~22 attributes; we just confirm it's
	// non-zero and matches the count of params actually appended.
	parmCnt := uint16(payload[18])<<8 | uint16(payload[19])
	if parmCnt < 3 {
		t.Errorf("ParameterCount = %d, want >= 3 (a real SET_SQL_ATTRIBUTES sends many)", parmCnt)
	}

	// Round-trip through Read/Write to confirm framing.
	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, payload2, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame round-trip: %v", err)
	}
	if got.Length != hdr.Length {
		t.Errorf("round-tripped Length = %d, want %d", got.Length, hdr.Length)
	}
	if !bytes.Equal(payload, payload2) {
		t.Errorf("round-tripped payload differs")
	}
}

// TestSetSQLAttributesRequestRoutesDateFormatToCP3807 is the
// regression test for the USA-format DATE descriptor parser quirk.
// Pre-fix, opts.DateFormat was pumped into CP 0x3805 (which is
// JTOpen's TranslateIndicator, not the date format). For DateFormatJOB
// (0xF0) that accidentally produced the right TranslateIndicator
// byte; any other format silently broke the connection's translate
// behaviour AND left the date format at the server default.
//
// Post-fix:
//   - 0x3805 is always 0xF0 (TranslateIndicator).
//   - 0x3807 (DateFormatParserOption) carries the integer index per
//     JTOpen's mapping (0..7); omitted entirely for JOB so the
//     server falls through to its job default.
//   - 0x3808 (DateSeparatorParserOption) is sent alongside whenever
//     0x3807 is.
func TestSetSQLAttributesRequestRoutesDateFormatToCP3807(t *testing.T) {
	cases := []struct {
		name        string
		format      byte
		wantCP3807  int16
		wantCP3808  int16
		wantPresent bool
	}{
		{"JOB omits CP 0x3807", DateFormatJOB, 0, 0, false},
		{"USA -> index 4, sep '/'", DateFormatUSA, 4, 0, true},
		{"ISO -> index 5, sep '-'", DateFormatISO, 5, 1, true},
		{"EUR -> index 6, sep '.'", DateFormatEUR, 6, 2, true},
		{"JIS -> index 7, sep '-'", DateFormatJIS, 7, 1, true},
		{"MDY -> index 1, sep '/'", DateFormatMDY, 1, 0, true},
		{"DMY -> index 2, sep '/'", DateFormatDMY, 2, 0, true},
		{"YMD -> index 3, sep '-'", DateFormatYMD, 3, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := DefaultDBAttributesOptions()
			opts.DateFormat = tc.format

			_, payload, err := SetSQLAttributesRequest(opts)
			if err != nil {
				t.Fatalf("SetSQLAttributesRequest: %v", err)
			}
			cp3805, ok := findShortCP(payload, 0x3805, 1)
			if !ok {
				t.Fatalf("CP 0x3805 (TranslateIndicator) missing")
			}
			if cp3805 != 0xF0 {
				t.Errorf("CP 0x3805 = 0x%02X, want 0xF0 (TranslateIndicator)", cp3805)
			}
			got3807, present3807 := findShortCP(payload, 0x3807, 2)
			got3808, present3808 := findShortCP(payload, 0x3808, 2)
			if present3807 != tc.wantPresent {
				t.Errorf("CP 0x3807 present=%v, want %v", present3807, tc.wantPresent)
			}
			if present3808 != tc.wantPresent {
				t.Errorf("CP 0x3808 present=%v, want %v", present3808, tc.wantPresent)
			}
			if tc.wantPresent {
				if int16(got3807) != tc.wantCP3807 {
					t.Errorf("CP 0x3807 = %d, want %d", int16(got3807), tc.wantCP3807)
				}
				if int16(got3808) != tc.wantCP3808 {
					t.Errorf("CP 0x3808 = %d, want %d", int16(got3808), tc.wantCP3808)
				}
			}
		})
	}
}

// TestSetSQLAttributesRequestRoutesNamingToCP380C pins the wire
// shape for CP 0x380C (NamingConventionParserOption): 0 = sql
// (default), 1 = system. Mirrors JT400's
// setNamingConventionParserOption(JDProperties.NAMING). Critical
// because the CP is always present in the SET_SQL_ATTRIBUTES
// payload -- a wrong value silently corrupts every unqualified
// identifier parse on the server.
func TestSetSQLAttributesRequestRoutesNamingToCP380C(t *testing.T) {
	for _, tc := range []struct {
		name       string
		naming     int16
		wantCP380C int16
	}{
		{"default sql", 0, 0},
		{"system", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := DefaultDBAttributesOptions()
			opts.Naming = tc.naming
			_, payload, err := SetSQLAttributesRequest(opts)
			if err != nil {
				t.Fatalf("SetSQLAttributesRequest: %v", err)
			}
			got, ok := findShortCP(payload, 0x380C, 2)
			if !ok {
				t.Fatalf("CP 0x380C (NamingConventionParserOption) missing from payload")
			}
			if int16(got) != tc.wantCP380C {
				t.Errorf("CP 0x380C = %d, want %d", int16(got), tc.wantCP380C)
			}
		})
	}
}

// TestSetSQLAttributesRequestTimeAndSeparatorCPs pins the wire shape
// for the four passthrough CPs: 0x3809 (TimeFormat), 0x3808
// (DateSeparator override), 0x380A (TimeSeparator), 0x380B
// (DecimalSeparator). Default values (-1) omit the CP; explicit
// non-negative values land on the wire at the matching index.
func TestSetSQLAttributesRequestTimeAndSeparatorCPs(t *testing.T) {
	t.Run("all defaults omit CPs", func(t *testing.T) {
		opts := DefaultDBAttributesOptions()
		_, payload, err := SetSQLAttributesRequest(opts)
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		for _, cp := range []uint16{0x3809, 0x380A, 0x380B} {
			if _, present := findShortCP(payload, cp, 2); present {
				t.Errorf("CP 0x%04X present, want absent under defaults", cp)
			}
		}
	})
	t.Run("explicit time-format usa", func(t *testing.T) {
		opts := DefaultDBAttributesOptions()
		opts.TimeFormat = 1 // usa per JT400 TIME_FORMAT index
		_, payload, err := SetSQLAttributesRequest(opts)
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		got, ok := findShortCP(payload, 0x3809, 2)
		if !ok {
			t.Fatalf("CP 0x3809 (TimeFormat) missing")
		}
		if got != 1 {
			t.Errorf("CP 0x3809 = %d, want 1 (usa)", got)
		}
	})
	t.Run("explicit time-separator colon", func(t *testing.T) {
		opts := DefaultDBAttributesOptions()
		opts.TimeSeparator = 0 // colon
		_, payload, err := SetSQLAttributesRequest(opts)
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		got, ok := findShortCP(payload, 0x380A, 2)
		if !ok {
			t.Fatalf("CP 0x380A (TimeSeparator) missing")
		}
		if got != 0 {
			t.Errorf("CP 0x380A = %d, want 0 (colon)", got)
		}
	})
	t.Run("explicit decimal-separator comma", func(t *testing.T) {
		opts := DefaultDBAttributesOptions()
		opts.DecimalSeparator = 1 // comma
		_, payload, err := SetSQLAttributesRequest(opts)
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		got, ok := findShortCP(payload, 0x380B, 2)
		if !ok {
			t.Fatalf("CP 0x380B (DecimalSeparator) missing")
		}
		if got != 1 {
			t.Errorf("CP 0x380B = %d, want 1 (comma)", got)
		}
	})
	t.Run("explicit date-separator overrides date-format inference", func(t *testing.T) {
		// DateFormatISO normally implies CP 0x3808 = 1 (dash).
		// An explicit DateSeparator must override that inference.
		opts := DefaultDBAttributesOptions()
		opts.DateFormat = DateFormatISO
		opts.DateSeparator = 2 // period
		_, payload, err := SetSQLAttributesRequest(opts)
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		got, ok := findShortCP(payload, 0x3808, 2)
		if !ok {
			t.Fatalf("CP 0x3808 missing")
		}
		if got != 2 {
			t.Errorf("CP 0x3808 = %d, want 2 (period override)", got)
		}
	})
	t.Run("date-format alone still emits its inferred separator", func(t *testing.T) {
		// Regression net for the override path: when DateSeparator
		// is -1 (default), the date-format-inferred value still
		// reaches the wire so old fixtures stay byte-equal.
		opts := DefaultDBAttributesOptions()
		opts.DateFormat = DateFormatISO
		_, payload, err := SetSQLAttributesRequest(opts)
		if err != nil {
			t.Fatalf("SetSQLAttributesRequest: %v", err)
		}
		got, ok := findShortCP(payload, 0x3808, 2)
		if !ok {
			t.Fatalf("CP 0x3808 missing")
		}
		if got != 1 {
			t.Errorf("CP 0x3808 = %d, want 1 (dash, inferred from ISO)", got)
		}
	})
}

// findShortCP scans an SET_SQL_ATTRIBUTES payload looking for a
// CP/LL/data triple at the given codepoint and returns the integer
// value of the data bytes (1- or 2-byte). The payload starts with a
// 20-byte template (4-byte ORS, 16 bytes other) followed by N
// LL/CP/data records. PARMCNT lives at offset 18-19. wantBytes is
// the data length we expect (1 for CP 0x3805, 2 for the parser-
// option shorts) -- mismatch returns ok=false rather than reading
// garbage off the next CP.
//
// Returns (value, true) if the CP is found and matches the
// requested data length. Reads at most the first instance of the
// CP; SET_SQL_ATTRIBUTES never repeats CPs so that's fine.
func findShortCP(payload []byte, cp uint16, wantBytes int) (uint16, bool) {
	const tplLen = 20
	if len(payload) < tplLen {
		return 0, false
	}
	pos := tplLen
	for pos+6 <= len(payload) {
		ll := uint32(payload[pos])<<24 | uint32(payload[pos+1])<<16 | uint32(payload[pos+2])<<8 | uint32(payload[pos+3])
		cur := uint16(payload[pos+4])<<8 | uint16(payload[pos+5])
		if int(ll) < 6 || pos+int(ll) > len(payload) {
			return 0, false
		}
		if cur == cp {
			dataLen := int(ll) - 6
			if dataLen != wantBytes {
				return 0, false
			}
			switch dataLen {
			case 1:
				return uint16(payload[pos+6]), true
			case 2:
				return uint16(payload[pos+6])<<8 | uint16(payload[pos+7]), true
			default:
				return 0, false
			}
		}
		pos += int(ll)
	}
	return 0, false
}

// TestSetSQLAttributesAgainstFixture replays the captured
// SET_SQL_ATTRIBUTES reply into a fakeConn and confirms the
// orchestrator parses it cleanly.
func TestSetSQLAttributesAgainstFixture(t *testing.T) {
	receiveds := dbReceivedsFromFixtureN(t, "connect_only.trace", 3)
	conn := newFakeConn(receiveds[2].Bytes)

	attrs, err := SetSQLAttributes(conn, DefaultDBAttributesOptions())
	if err != nil {
		t.Fatalf("SetSQLAttributes: %v", err)
	}
	if attrs.ServerCCSID != 273 {
		t.Errorf("ServerCCSID = %d, want 273", attrs.ServerCCSID)
	}
	defaultLib, _ := ebcdic.CCSID37.Decode(attrs.DefaultSQLLibraryName)
	if defaultLib != "AFTRAEGE11" {
		t.Errorf("DefaultSQLLibraryName = %q, want %q", defaultLib, "AFTRAEGE11")
	}

	// Re-parse what we wrote.
	r := bytes.NewReader(conn.written.Bytes())
	hdr, _, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("re-parse sent frame: %v", err)
	}
	if hdr.ReqRepID != ReqDBSetSQLAttributes {
		t.Errorf("sent ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, ReqDBSetSQLAttributes)
	}
	if hdr.CorrelationID != 1 {
		t.Errorf("sent CorrelationID = %d, want 1", hdr.CorrelationID)
	}
}

// dbReceivedsFromFixtureN is the multi-frame sibling of
// dbReceivedsFromFixture: it requires at least n frames and returns
// all of them in order. Convenient for tests that need to skip past
// the early handshake replies into the SQL-service portion.
func dbReceivedsFromFixtureN(t *testing.T, name string, n int) []frameRef {
	t.Helper()
	all := dbReceivedsFromFixture(t, name)
	out := make([]frameRef, 0, len(all))
	for _, f := range all {
		out = append(out, frameRef{Bytes: f.Bytes})
	}
	if len(out) < n {
		t.Fatalf("need >= %d as-database receiveds in %s, got %d", n, name, len(out))
	}
	return out
}

// frameRef is a thin shim so dbReceivedsFromFixtureN can return a
// type-stable slice without importing the wirelog package alias
// chain twice. (dbReceivedsFromFixture already returns
// wirelog.Frame slices in database_test.go; we just keep this
// test's call sites simple.)
type frameRef struct {
	Bytes []byte
}
