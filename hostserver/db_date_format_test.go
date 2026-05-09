package hostserver

import (
	"testing"
)

// TestEncodeDateStringForFormatPositive walks every supported IBM i
// date format and pins the wire output for one ISO input. The wire
// patterns mirror IBM i's documented format names (CL DSPSYSVAL
// SYSVAL(QDATFMT) reference); a regression here would mean we'd
// silently send the server bytes it can't parse for that session's
// negotiated date format.
func TestEncodeDateStringForFormatPositive(t *testing.T) {
	const iso = "2026-05-08"
	cases := []struct {
		name   string
		format byte
		want   string
	}{
		{"ISO", DateFormatISO, "2026-05-08"},
		{"JIS", DateFormatJIS, "2026-05-08"},
		{"USA", DateFormatUSA, "05/08/2026"},
		{"EUR", DateFormatEUR, "08.05.2026"},
		{"MDY", DateFormatMDY, "05/08/26"},
		{"DMY", DateFormatDMY, "08/05/26"},
		{"YMD", DateFormatYMD, "26-05-08"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeDateStringForFormat(iso, tc.format)
			if err != nil {
				t.Fatalf("encodeDateStringForFormat: %v", err)
			}
			if got != tc.want {
				t.Errorf("format %s: got %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

// TestEncodeDateStringForFormatRejectsJOB confirms callers can't
// emit *JOB directly -- the format must be resolved to a concrete
// pattern first (ISO/USA/EUR/JIS/MDY/DMY/YMD). *JOB is a session-
// level "let the server pick" indicator, not a per-bind directive.
func TestEncodeDateStringForFormatRejectsJOB(t *testing.T) {
	if _, err := encodeDateStringForFormat("2026-05-08", DateFormatJOB); err == nil {
		t.Error("expected error for *JOB, got nil")
	}
}

// TestEncodeDateStringForFormatRejectsBadInput confirms non-ISO
// input strings are rejected uniformly across formats.
func TestEncodeDateStringForFormatRejectsBadInput(t *testing.T) {
	bad := []string{
		"2026-5-8",     // unpadded
		"05/08/2026",   // already USA-formatted
		"2026-05",      // truncated
		"",             // empty
		"not-a-date-x", // garbage
	}
	for _, in := range bad {
		if _, err := encodeDateStringForFormat(in, DateFormatISO); err == nil {
			t.Errorf("expected error for %q, got nil", in)
		}
	}
}

// TestEncodeDateStringForFormatRejectsUnknownFormat confirms an
// unmapped format byte is rejected explicitly rather than silently
// falling through to a default.
func TestEncodeDateStringForFormatRejectsUnknownFormat(t *testing.T) {
	if _, err := encodeDateStringForFormat("2026-05-08", 0xAA); err == nil {
		t.Error("expected error for unknown format byte, got nil")
	}
}

// TestEncodeDateStringWrapper confirms the legacy fieldLen-based
// wrapper still picks ISO for fieldLen=10 and YMD for fieldLen=8 --
// existing call sites in db_prepared.go that bind by FieldLength
// keep their semantics.
func TestEncodeDateStringWrapper(t *testing.T) {
	t.Run("len10=ISO", func(t *testing.T) {
		got, err := encodeDateString("2026-05-08", 10)
		if err != nil {
			t.Fatalf("encodeDateString: %v", err)
		}
		if got != "2026-05-08" {
			t.Errorf("got %q, want 2026-05-08", got)
		}
	})
	t.Run("len8=YMD", func(t *testing.T) {
		got, err := encodeDateString("2026-05-08", 8)
		if err != nil {
			t.Fatalf("encodeDateString: %v", err)
		}
		if got != "26-05-08" {
			t.Errorf("got %q, want 26-05-08", got)
		}
	})
	t.Run("len12=err", func(t *testing.T) {
		if _, err := encodeDateString("2026-05-08", 12); err == nil {
			t.Error("expected error for fieldLen 12, got nil")
		}
	})
}

// TestEncodeDateForParamHonoursFormat is the regression test for
// the USA-format DATE bind quirk: with a non-zero DateFormat, the
// bind path must emit format-specific wire bytes rather than always
// ISO. Pre-fix the USA case landed YYYY-MM-DD on the wire and the
// server rejected the bind because its parser was configured for
// MM/DD/YYYY.
func TestEncodeDateForParamHonoursFormat(t *testing.T) {
	const iso = "2026-05-08"
	cases := []struct {
		name     string
		format   byte
		fieldLen int
		want     string
	}{
		{"USA", DateFormatUSA, 10, "05/08/2026"},
		{"EUR", DateFormatEUR, 10, "08.05.2026"},
		{"JIS", DateFormatJIS, 10, "2026-05-08"},
		{"MDY", DateFormatMDY, 8, "05/08/26"},
		{"DMY", DateFormatDMY, 8, "08/05/26"},
		{"YMD", DateFormatYMD, 8, "26-05-08"},
		{"ISO", DateFormatISO, 10, "2026-05-08"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeDateForParam(iso, tc.fieldLen, tc.format)
			if err != nil {
				t.Fatalf("encodeDateForParam: %v", err)
			}
			if got != tc.want {
				t.Errorf("format %s: got %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

// TestEncodeDateForParamFallsBackToLegacy confirms that the zero
// DateFormat keeps the old length-only behaviour intact, so existing
// callers that haven't been updated to set PreparedParam.DateFormat
// continue to work unchanged.
func TestEncodeDateForParamFallsBackToLegacy(t *testing.T) {
	t.Run("len10 zero format", func(t *testing.T) {
		got, err := encodeDateForParam("2026-05-08", 10, 0)
		if err != nil {
			t.Fatalf("encodeDateForParam: %v", err)
		}
		if got != "2026-05-08" {
			t.Errorf("got %q, want 2026-05-08 (legacy ISO path)", got)
		}
	})
	t.Run("len8 zero format", func(t *testing.T) {
		got, err := encodeDateForParam("2026-05-08", 8, 0)
		if err != nil {
			t.Fatalf("encodeDateForParam: %v", err)
		}
		if got != "26-05-08" {
			t.Errorf("got %q, want 26-05-08 (legacy YMD path)", got)
		}
	})
	t.Run("JOB treated like zero", func(t *testing.T) {
		got, err := encodeDateForParam("2026-05-08", 10, DateFormatJOB)
		if err != nil {
			t.Fatalf("encodeDateForParam: %v", err)
		}
		if got != "2026-05-08" {
			t.Errorf("got %q, want 2026-05-08 (JOB == let server pick, send ISO)", got)
		}
	})
}

// TestEncodeDateForParamRejectsLengthMismatch confirms the helper
// catches a programmer error where the caller asks for USA (10 chars)
// but allocated only 8 bytes of FieldLength -- the server would have
// silently truncated the wire bytes otherwise.
func TestEncodeDateForParamRejectsLengthMismatch(t *testing.T) {
	cases := []struct {
		name     string
		format   byte
		fieldLen int
	}{
		{"USA wants 10, given 8", DateFormatUSA, 8},
		{"YMD wants 8, given 10", DateFormatYMD, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := encodeDateForParam("2026-05-08", tc.fieldLen, tc.format); err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
