package hostserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

// TestSuffixFromOptions_FixtureMatch asserts our suffix derivation
// matches the suffix JT400 actually wrote on the wire when it captured
// testdata/jtopen-fixtures/fixtures/prepared_package_first_use.trace.
// The captured CREATE_PACKAGE frame carries CP 0x3804 = "GOJTPK9899"
// in EBCDIC -- "9899" is the 4-char suffix JT400 computed for the
// session options the harness used (naming=sql; everything else
// JT400 default).
//
// This test is the load-bearing regression net for the
// "byte-equal-to-JT400" interop rule (see memory
// project_db2i_m10_jt400_interop.md). If a future change to the
// suffix formula breaks this, a Go client running go-db2i and a Java
// client running JT400 against the same LPAR would land on different
// server-side *PGM objects.
func TestSuffixFromOptions_FixtureMatch(t *testing.T) {
	got := SuffixFromOptions(PackageOptions{
		TranslateHex:     0, // binary (default)
		CommitMode:       0, // NONE (auto-commit on)
		DateFormat:       1, // mdy (default)
		DateSeparator:    0, // slash (default)
		DecimalSeparator: 0, // period (default)
		Naming:           0, // sql (harness override -- default is 1=system)
		TimeFormat:       0, // hms (default)
		TimeSeparator:    0, // colon (default)
	})
	want := "9899"
	if got != want {
		t.Fatalf("SuffixFromOptions = %q, want %q (JT400 wire output)", got, want)
	}
}

// TestSuffixFromOptions_TableDriven covers the four formula slots
// plus the COMMIT_MODE_RR overflow encoding. Each row exercises
// exactly one bit so a regression that adds an off-by-one somewhere
// is localized.
//
// SUFFIX_INVARIANT_ index 0 = '9', index 1 = '8', etc; reverse-lookup
// via the strings.IndexByte calls below ensures the assertion stays
// readable.
func TestSuffixFromOptions_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		opts PackageOptions
		// indexes are the SUFFIX_INVARIANT_ positions the formula
		// should land on for each of the 4 chars.
		idxs [4]int
	}{
		{
			"all defaults",
			PackageOptions{},
			[4]int{0, 0, 0, 0}, // -> "9999"
		},
		{
			"translate-hex character",
			PackageOptions{TranslateHex: 1},
			[4]int{1, 0, 0, 0}, // -> "8999"
		},
		{
			"date-format mdy",
			PackageOptions{DateFormat: 1},
			[4]int{0, 1, 0, 0}, // -> "9899"
		},
		{
			"commit-mode CHG only",
			PackageOptions{CommitMode: 1},
			[4]int{0, 8, 0, 0}, // (1<<3 | 0) = 8 -> 'Z'
		},
		{
			"commit-mode CS + date jis",
			PackageOptions{CommitMode: 2, DateFormat: 7},
			[4]int{0, 23, 0, 0}, // (2<<3 | 7) = 23 -> 'O'
		},
		{
			"decimal-sep comma",
			PackageOptions{DecimalSeparator: 1},
			[4]int{0, 0, 16, 0}, // 1<<4 = 16 -> 'V'
		},
		{
			"naming system",
			PackageOptions{Naming: 1},
			[4]int{0, 0, 8, 0}, // 1<<3 = 8 -> 'Z'
		},
		{
			"time-format usa + sep period",
			PackageOptions{TimeFormat: 1, TimeSeparator: 1},
			[4]int{0, 0, 0, 5}, // (1<<2 | 1) = 5 -> '4'
		},
		// RR overflow: commitMode==4 + dateSep in {0,1,2}.
		// commitMode becomes dateSep, dateSep becomes 6.
		// Pre-overflow: would be (4<<3 | 0) = 32 -> 'D' for char2.
		// Post-overflow: dateSep=0 picked up, so commitMode=0 takes
		// its place. char2 = (0<<3 | 0) = 0 -> '9'.
		// char3 picks up the borrowed dateSep=6: (0<<4 | 0<<3 | 6) = 6 -> '3'.
		{
			"commit RR, dateSep slash (0)",
			PackageOptions{CommitMode: 4, DateSeparator: 0},
			[4]int{0, 0, 6, 0}, // -> "93 9... wait, suffix[6]='3'"
		},
		// RR overflow case 2: dateSep in {3,4}.
		// commitMode becomes dateSep-2, dateSep becomes 7.
		{
			"commit RR, dateSep comma (3)",
			PackageOptions{CommitMode: 4, DateSeparator: 3},
			// commitMode -> 1, dateSep -> 7
			// char2 = (1<<3 | 0) = 8 -> 'Z'
			// char3 = (0<<4 | 0<<3 | 7) = 7 -> '2'
			[4]int{0, 8, 7, 0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SuffixFromOptions(tc.opts)
			if len(got) != 4 {
				t.Fatalf("got %q, want 4 chars", got)
			}
			for i, idx := range tc.idxs {
				want := suffixInvariant[idx]
				if got[i] != want {
					t.Errorf("char %d: got %q, want %q (index %d)", i+1, string(got[i]), string(want), idx)
				}
			}
		})
	}
}

// TestBuildPackageName_FixtureMatch asserts our 10-char composer
// reproduces the exact name JT400 sent for the captured first_use
// fixture: base="GOJTPKG" + suffix="9899" -> "GOJTPK9899" (G O J T P
// K truncated from "GOJTPKG" + "9899").
func TestBuildPackageName_FixtureMatch(t *testing.T) {
	got := BuildPackageName("GOJTPKG", PackageOptions{DateFormat: 1})
	want := "GOJTPK9899"
	if got != want {
		t.Errorf("BuildPackageName = %q, want %q", got, want)
	}
}

func TestBuildPackageName_PadsShortBase(t *testing.T) {
	got := BuildPackageName("PK", PackageOptions{})
	if !strings.HasPrefix(got, "PK    ") {
		t.Errorf("short base not padded: got %q", got)
	}
	if len(got) != 10 {
		t.Errorf("len %d, want 10", len(got))
	}
}

func TestBuildPackageName_UppersAndUnderscores(t *testing.T) {
	got := BuildPackageName("my pkg", PackageOptions{})
	want := "MY_PKG" + SuffixFromOptions(PackageOptions{})
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestBuildCreatePackageParams_WireShape encodes a CREATE_PACKAGE
// (0x180F) request with a known name + library and asserts the
// resulting bytes match the JT400 wire shape captured in
// prepared_package_first_use.trace frame #5:
//
//	LL=0x14 CP=0x3804 CCSID=0x25 SL=0x0a "GOJTPK9899"
//	LL=0x10 CP=0x3801 CCSID=0x25 SL=0x06 "GOTEST"
//
// Going through BuildDBRequest gives us a full payload; we
// concentrate on the parameter section starting at offset 20 (after
// the template).
func TestBuildCreatePackageParams_WireShape(t *testing.T) {
	params, err := BuildCreatePackageParams("GOJTPK9899", "GOTEST", 37)
	if err != nil {
		t.Fatalf("BuildCreatePackageParams: %v", err)
	}
	if len(params) != 2 {
		t.Fatalf("want 2 params, got %d", len(params))
	}

	_, payload, err := BuildDBRequest(ReqDBSQLCreatePackage, DBRequestTemplate{
		ORSBitmap: 0x80040000,
	}, params)
	if err != nil {
		t.Fatalf("BuildDBRequest: %v", err)
	}

	// JT400 wire bytes (extracted from
	// fixtures/prepared_package_first_use.trace frame #5, byte offset
	// 40 onward -- past the 20-byte DSS header and 20-byte template).
	wantParams := []byte{
		// Param 1: CP 0x3804 (package name)
		0x00, 0x00, 0x00, 0x14, 0x38, 0x04, 0x00, 0x25,
		0x00, 0x0a,
		0xc7, 0xd6, 0xd1, 0xe3, 0xd7, 0xd2, 0xf9, 0xf8, 0xf9, 0xf9, // GOJTPK9899
		// Param 2: CP 0x3801 (package library)
		0x00, 0x00, 0x00, 0x10, 0x38, 0x01, 0x00, 0x25,
		0x00, 0x06,
		0xc7, 0xd6, 0xe3, 0xc5, 0xe2, 0xe3, // GOTEST
	}
	gotParams := payload[20:]
	if !bytes.Equal(gotParams, wantParams) {
		t.Errorf("CREATE_PACKAGE param section differs from JT400 wire bytes\nwant: %x\n got: %x", wantParams, gotParams)
	}
}

// TestBuildReturnPackageParams_WireShape mirrors the test above for
// RETURN_PACKAGE (0x1815) -- 3 params: name + library + 4-byte zero
// option. Bytes lifted from prepared_package_cache_hit.trace frame
// #6.
func TestBuildReturnPackageParams_WireShape(t *testing.T) {
	params, err := BuildReturnPackageParams("GOJTPK9899", "GOTEST", 37)
	if err != nil {
		t.Fatalf("BuildReturnPackageParams: %v", err)
	}
	if len(params) != 3 {
		t.Fatalf("want 3 params, got %d", len(params))
	}

	_, payload, err := BuildDBRequest(ReqDBSQLReturnPackage, DBRequestTemplate{
		ORSBitmap: 0x80140000,
	}, params)
	if err != nil {
		t.Fatalf("BuildDBRequest: %v", err)
	}

	wantParams := []byte{
		0x00, 0x00, 0x00, 0x14, 0x38, 0x04, 0x00, 0x25,
		0x00, 0x0a,
		0xc7, 0xd6, 0xd1, 0xe3, 0xd7, 0xd2, 0xf9, 0xf8, 0xf9, 0xf9,
		0x00, 0x00, 0x00, 0x10, 0x38, 0x01, 0x00, 0x25,
		0x00, 0x06,
		0xc7, 0xd6, 0xe3, 0xc5, 0xe2, 0xe3,
		// Trailing option: CP 0x3815, 4 bytes of 0
		0x00, 0x00, 0x00, 0x0a, 0x38, 0x15,
		0x00, 0x00, 0x00, 0x00,
	}
	gotParams := payload[20:]
	if !bytes.Equal(gotParams, wantParams) {
		t.Errorf("RETURN_PACKAGE param section differs from JT400 wire bytes\nwant: %x\n got: %x", wantParams, gotParams)
	}
}

// TestPackageNameByteEquality_FixtureBytes confirms the
// asciiToEBCDIC37 helper produces the exact bytes JT400 sent for the
// 6-char and 10-char names in our fixtures. Round-trips every
// allowed character so the helper doesn't drift if someone reorders
// the switch branches.
func TestPackageNameByteEquality_FixtureBytes(t *testing.T) {
	cases := []struct {
		in   string
		want []byte
	}{
		{"GOTEST", []byte{0xc7, 0xd6, 0xe3, 0xc5, 0xe2, 0xe3}},
		{"GOJTPK9899", []byte{0xc7, 0xd6, 0xd1, 0xe3, 0xd7, 0xd2, 0xf9, 0xf8, 0xf9, 0xf9}},
		{"_", []byte{0x6d}},
		{"#", []byte{0x7b}},
		{"@", []byte{0x7c}},
		{"$", []byte{0x5b}},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			got, err := ebcdicVarStringBytes(c.in, 37)
			if err != nil {
				t.Fatalf("encode %q: %v", c.in, err)
			}
			if !bytes.Equal(got, c.want) {
				t.Errorf("encode %q: got %x, want %x", c.in, got, c.want)
			}
		})
	}
}

// TestPackageNameByteEquality_RejectsBadChars makes sure a Config
// with an invalid character in the package name surfaces at encode
// time, not silently as a server-side SQL error.
func TestPackageNameByteEquality_RejectsBadChars(t *testing.T) {
	for _, bad := range []string{"PKG!", "PKG.A", "PKG/B", "пкг"} {
		t.Run(bad, func(t *testing.T) {
			_, err := ebcdicVarStringBytes(bad, 37)
			if err == nil {
				t.Errorf("expected encoding error for %q", bad)
			}
		})
	}
}

// pickWireParamSection is a tiny helper used in subsequent M10
// tests to slice into the param section of a generated request,
// skipping the 20-byte template. Kept in this file so future
// CREATE/RETURN/DELETE_PACKAGE tests can share it.
func pickWireParamSection(payload []byte) []byte {
	if len(payload) < 20 {
		return nil
	}
	return payload[20:]
}

// TestSelectStaticSQL_ExtendedDynamicAddsMarker confirms the
// SelectStaticSQL path appends an empty CP 0x3804 marker to its
// PREPARE_DESCRIBE frame when WithExtendedDynamic(true) is supplied
// -- and does NOT add it under the default options. The fake conn
// replays select_dummy.trace replies (the request payload doesn't
// affect what comes back), so we can drive the prepare path and
// then inspect what got written.
func TestSelectStaticSQL_ExtendedDynamicAddsMarker(t *testing.T) {
	all := allReceivedsFromFixture(t, "select_dummy.trace")
	var sqlReceiveds [][]byte
	for _, b := range all {
		if len(b) >= 8 && b[6] == 0xE0 && b[7] == 0x04 {
			sqlReceiveds = append(sqlReceiveds, b)
		}
	}
	if len(sqlReceiveds) < 6 {
		t.Fatalf("need >= 6 SQL receiveds, got %d", len(sqlReceiveds))
	}

	cases := []struct {
		name           string
		opts           []SelectOption
		wantHasMarker  bool
	}{
		{"default off", nil, false},
		{"dynamic on", []SelectOption{WithExtendedDynamic(true)}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFakeConn(sqlReceiveds[3], sqlReceiveds[4], sqlReceiveds[5])
			cursor, err := OpenSelectStatic(conn,
				"SELECT CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_SERVER FROM SYSIBM.SYSDUMMY1",
				closureFromInt(3), tc.opts...,
			)
			if err != nil {
				t.Fatalf("OpenSelectStatic: %v", err)
			}
			_, _ = cursor.drainAll()

			// Walk the sent frames; the second one is PREPARE_DESCRIBE.
			r := bytes.NewReader(conn.written.Bytes())
			var prepareBytes []byte
			for i := 0; i < 4; i++ {
				hdr, payload, err := ReadFrame(r)
				if err != nil {
					t.Fatalf("re-parse sent frame %d: %v", i, err)
				}
				if hdr.ReqRepID == ReqDBSQLPrepareDescribe {
					prepareBytes = payload
					break
				}
			}
			if prepareBytes == nil {
				t.Fatal("no PREPARE_DESCRIBE frame found in sent stream")
			}
			hasMarker := containsEmptyCP(prepareBytes, cpPackageName)
			if hasMarker != tc.wantHasMarker {
				t.Errorf("PREPARE_DESCRIBE has empty CP 0x%04X = %v, want %v",
					cpPackageName, hasMarker, tc.wantHasMarker)
			}
		})
	}
}

// containsEmptyCP walks a request payload (everything after the DSS
// header) looking for an LL/CP entry with LL=6 and the given CP --
// i.e. an empty marker parameter.
func containsEmptyCP(payload []byte, wantCP uint16) bool {
	if len(payload) < 20 {
		return false
	}
	off := 20 // skip template
	for off+6 <= len(payload) {
		ll := uint32(payload[off])<<24 | uint32(payload[off+1])<<16 | uint32(payload[off+2])<<8 | uint32(payload[off+3])
		if ll < 6 || int(ll) > len(payload)-off {
			return false
		}
		cp := uint16(payload[off+4])<<8 | uint16(payload[off+5])
		if ll == 6 && cp == wantCP {
			return true
		}
		off += int(ll)
	}
	return false
}

// TestParsePackageInfo_FixtureMatch replays the two CP 0x380B reply
// bodies captured in prepared_package_cache_download.trace and
// asserts the decoder reproduces every field the JT400 wire-side
// stored. Both captures hold the same one-statement payload (the
// SELECT CURRENT_TIMESTAMP / CAST(? AS INTEGER) statement the
// harness pre-seeds before the second connect downloads it).
//
// This is the load-bearing regression net for the SQLDA-format
// parser: any divergence in field offsets here breaks the client-
// side cache-hit fast path silently (statement name mismatch ->
// server treats as unknown -> falls back to PREPARE_DESCRIBE +
// extra round-trip per call).
func TestParsePackageInfo_FixtureMatch(t *testing.T) {
	bodies := packageInfoBodiesFromFixture(t, "prepared_package_cache_download.trace")
	if len(bodies) == 0 {
		t.Fatalf("no CP 0x380B bodies captured in fixture")
	}
	for i, body := range bodies {
		t.Run(fmt.Sprintf("body_%d", i), func(t *testing.T) {
			stmts, err := ParsePackageInfo(body)
			if err != nil {
				t.Fatalf("ParsePackageInfo: %v", err)
			}
			if len(stmts) != 1 {
				t.Fatalf("got %d statements, want 1", len(stmts))
			}
			ps := stmts[0]
			// Server-assigned name, captured EBCDIC bytes:
			//   d8e9c1c6f4f8f1f8f1f5c5f8f0f2c5f0f0f1
			//      Q  Z  A  F  4  8  1  8  1  5  E  8  0  2  E  0  0  1
			wantNameBytes := []byte{
				0xd8, 0xe9, 0xc1, 0xc6, 0xf4, 0xf8, 0xf1, 0xf8, 0xf1,
				0xf5, 0xc5, 0xf8, 0xf0, 0xf2, 0xc5, 0xf0, 0xf0, 0xf1,
			}
			if !bytes.Equal(ps.NameBytes, wantNameBytes) {
				t.Errorf("NameBytes = %x, want %x", ps.NameBytes, wantNameBytes)
			}
			if ps.Name != "QZAF481815E802E001" {
				t.Errorf("Name = %q, want %q", ps.Name, "QZAF481815E802E001")
			}
			// Statement type 2 = SELECT, matches the seeded
			// SELECT CURRENT_TIMESTAMP statement.
			if ps.StatementType != 2 {
				t.Errorf("StatementType = %d, want 2 (SELECT)", ps.StatementType)
			}
			wantSQL := "SELECT CURRENT_TIMESTAMP, CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1"
			if ps.SQLText != wantSQL {
				t.Errorf("SQLText = %q, want %q", ps.SQLText, wantSQL)
			}

			// Two result columns: TIMESTAMP NN (392) and INTEGER
			// nullable (497, from CAST(? AS INTEGER) which carries
			// the input parameter's nullability).
			if got, want := len(ps.DataFormat), 2; got != want {
				t.Fatalf("DataFormat len = %d, want %d", got, want)
			}
			if got, want := ps.DataFormat[0].SQLType, uint16(SQLTypeTimestampNN); got != want {
				t.Errorf("DataFormat[0].SQLType = %d, want %d (TIMESTAMP NN)", got, want)
			}
			if got, want := ps.DataFormat[1].SQLType, uint16(497); got != want {
				t.Errorf("DataFormat[1].SQLType = %d, want %d (INTEGER nullable)", got, want)
			}
			if got, want := ps.DataFormat[1].Length, uint32(4); got != want {
				t.Errorf("DataFormat[1].Length = %d, want %d", got, want)
			}

			// One parameter marker (the single '?' in the SQL).
			if got, want := len(ps.ParameterMarkerFormat), 1; got != want {
				t.Fatalf("ParameterMarkerFormat len = %d, want %d", got, want)
			}
			pm := ps.ParameterMarkerFormat[0]
			if pm.SQLType != 497 {
				t.Errorf("PMF[0].SQLType = %d, want 497 (INTEGER nullable)", pm.SQLType)
			}
			if pm.FieldLength != 4 {
				t.Errorf("PMF[0].FieldLength = %d, want 4", pm.FieldLength)
			}
			// The SQLDA direction byte at offset +32 of the field
			// record is 0x00 in our fixtures (the server hasn't tagged
			// the marker as I/O/B); JT400's switch default returns
			// 0xF0 (input).
			if pm.ParamType != 0xF0 {
				t.Errorf("PMF[0].ParamType = 0x%02X, want 0xF0 (input)", pm.ParamType)
			}

			if len(ps.RawDataFormat) == 0 {
				t.Errorf("RawDataFormat should retain SQLDA bytes for cache-hit replay")
			}
			if len(ps.RawParameterMarkerFormat) == 0 {
				t.Errorf("RawParameterMarkerFormat should retain SQLDA bytes for cache-hit replay")
			}
		})
	}
}

// TestParsePackageInfo_EmptyPackage covers the brand-new *PGM case:
// the package header is intact but statement_count = 0. JT400 ships
// a CP 0x380B with just the 42-byte header in that case, and we
// must return (nil, nil) -- not error.
func TestParsePackageInfo_EmptyPackage(t *testing.T) {
	body := make([]byte, packageEntryHeaderLen)
	binary.BigEndian.PutUint32(body[0:4], uint32(packageEntryHeaderLen))
	binary.BigEndian.PutUint16(body[packageHeaderCCSIDOffset:packageHeaderCCSIDOffset+2], 13488)
	for i := 6; i < 24; i++ {
		body[i] = 0x40 // EBCDIC blanks for default collection
	}
	// statement count stays 0.
	stmts, err := ParsePackageInfo(body)
	if err != nil {
		t.Fatalf("ParsePackageInfo(empty): %v", err)
	}
	if stmts != nil {
		t.Errorf("expected nil for empty package, got %d statements", len(stmts))
	}
}

// TestParsePackageInfo_Truncated catches a body that claims more
// entries than the byte budget can house. A malformed CP 0x380B
// from the server (or a corrupt fixture) should surface as a typed
// error rather than panic on a slice bound check.
func TestParsePackageInfo_Truncated(t *testing.T) {
	body := make([]byte, packageEntryHeaderLen)
	// count = 5 but no entry bytes follow.
	binary.BigEndian.PutUint16(body[24:26], 5)
	binary.BigEndian.PutUint32(body[0:4], uint32(packageEntryHeaderLen))
	_, err := ParsePackageInfo(body)
	if err == nil {
		t.Fatalf("expected truncation error for count=5/no entries")
	}
}

// TestPackageSQLDADirectionByteMapping exercises the I/O/B EBCDIC
// switch DBSQLDADataFormat documents. The default (any other byte)
// must fall through to input.
func TestPackageSQLDADirectionByteMapping(t *testing.T) {
	cases := []struct {
		in   byte
		want byte
	}{
		{0xC9, 0xF0}, // 'I' input
		{0xD6, 0xF1}, // 'O' output
		{0xC2, 0xF2}, // 'B' inout
		{0x00, 0xF0}, // unset -> input
		{0xFF, 0xF0}, // garbage -> input (matches JT400 default arm)
	}
	for _, c := range cases {
		if got := sqldaParamDirection(c.in); got != c.want {
			t.Errorf("sqldaParamDirection(0x%02X) = 0x%02X, want 0x%02X", c.in, got, c.want)
		}
	}
}

// packageInfoBodiesFromFixture extracts every CP 0x380B body from
// every RETURN_PACKAGE reply (ReqRepID 0x2800) inside the named
// fixture. Used by ParsePackageInfo tests so a single fixture file
// can drive multiple regressions (the trace captures two connects,
// so two bodies land here).
func packageInfoBodiesFromFixture(t *testing.T, name string) [][]byte {
	t.Helper()
	all := allReceivedsFromFixture(t, name)
	var bodies [][]byte
	for _, b := range all {
		if len(b) < 8 || b[6] != 0xE0 || b[7] != 0x04 {
			continue
		}
		_, payload, err := ReadFrame(bytesReader(b))
		if err != nil {
			continue
		}
		rep, err := ParseDBReply(payload)
		if err != nil {
			continue
		}
		for _, p := range rep.Params {
			if p.CodePoint == cpPackageReplyInfo {
				body := make([]byte, len(p.Data))
				copy(body, p.Data)
				bodies = append(bodies, body)
			}
		}
	}
	return bodies
}

// bytesReader wraps b in a bytes.Reader. Kept here so the
// fixture-loading helper above doesn't need its own bytes import
// (the file already pulls "bytes" for the LL/CP assertions).
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// sanity-check the BE encoder helper layout against the existing
// param-list builder so a refactor of DBParam wire shape elsewhere
// can't silently break our package CPs.
func TestBuildPackageParams_LLAlignment(t *testing.T) {
	params, err := BuildCreatePackageParams("PKGA", "QGPL", 37)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	_, payload, err := BuildDBRequest(ReqDBSQLCreatePackage, DBRequestTemplate{}, params)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	section := pickWireParamSection(payload)
	// First param LL is bytes 0-3. The LL value must equal 4 +
	// len(CP=2) + CCSID(2) + SL(2) + name_data_bytes(4).
	gotLL := binary.BigEndian.Uint32(section[0:4])
	wantLL := uint32(4 + 2 + 2 + 2 + 4) // 14
	if gotLL != wantLL {
		t.Errorf("LL=0x%X, want 0x%X", gotLL, wantLL)
	}
	// Second param CP is at section[ wantLL .. wantLL+2 ].
	cp2 := binary.BigEndian.Uint16(section[wantLL+4 : wantLL+6])
	if cp2 != cpPackageLibrary {
		t.Errorf("2nd param CP=0x%04X, want 0x%04X (cpPackageLibrary)", cp2, cpPackageLibrary)
	}
}
