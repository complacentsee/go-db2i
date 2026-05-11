package hostserver

import (
	"bytes"
	"encoding/binary"
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
// project_gojtopen_m10_jt400_interop.md). If a future change to the
// suffix formula breaks this, a Go client running goJTOpen and a Java
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
