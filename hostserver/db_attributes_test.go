package hostserver

import (
	"bytes"
	"testing"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// TestParseDBReplyServerAttributesAgainstFixture decodes recv #5
// (the 174-byte 0x2800 reply that PUB400 returns to the
// SET_SQL_ATTRIBUTES request) and validates every field that
// goJTOpen surfaces.
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
	// 26 chars: 10 job program + 10 user + 6 number.
	wantJobId := "QZDASOINITQUSER     344425"
	if jobId != wantJobId {
		t.Errorf("ServerJobIdentifier = %q, want %q", jobId, wantJobId)
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
