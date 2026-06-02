package hostserver

import (
	"testing"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// Issue #25: DATE/TIME decode used to shape-sniff the wire string and
// default the ambiguous 8-char "NN/NN/NN" shape to MDY, mis-parsing
// DMY job dates; TIME decode returned the wire string verbatim, so
// non-HMS server formats (USA AM/PM, dotted ISO/EUR) were not
// normalised to canonical "hh:mm:ss". These tests pin the negotiated-
// format decode path (dateStringToISO / timeStringToISO), mirroring
// JT400's SQLDate.stringToDate / SQLTime.stringToTime switches on the
// negotiated format rather than the string shape.

// TestDateStringToISONegotiated walks every negotiated date format and
// confirms the wire string decodes to ISO by format, not by shape. The
// load-bearing case is the MDY/DMY pair: identical "05/08/26" wire
// bytes must decode to different ISO dates depending on the negotiated
// format.
func TestDateStringToISONegotiated(t *testing.T) {
	cases := []struct {
		name   string
		wire   string
		format byte
		want   string
	}{
		{"USA", "05/08/2026", DateFormatUSA, "2026-05-08"},
		{"EUR", "08.05.2026", DateFormatEUR, "2026-05-08"},
		{"ISO", "2026-05-08", DateFormatISO, "2026-05-08"},
		{"JIS", "2026-05-08", DateFormatJIS, "2026-05-08"},

		// MDY vs DMY: same 8-char wire shape, opposite field order.
		{"MDY", "05/08/26", DateFormatMDY, "2026-05-08"}, // May 8
		{"DMY", "05/08/26", DateFormatDMY, "2026-08-05"}, // Aug 5

		// YMD 8-char with 1940 century boundary.
		{"YMD 20xx", "26-05-08", DateFormatYMD, "2026-05-08"},
		{"YMD 19xx", "40-01-01", DateFormatYMD, "1940-01-01"},

		// MDY/DMY century boundary.
		{"MDY 19xx", "12/31/99", DateFormatMDY, "1999-12-31"},
		{"DMY 19xx", "31/12/99", DateFormatDMY, "1999-12-31"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dateStringToISO(tc.wire, tc.format); got != tc.want {
				t.Errorf("dateStringToISO(%q, 0x%02X) = %q, want %q", tc.wire, tc.format, got, tc.want)
			}
		})
	}
}

// TestDateStringToISOFallsBackToShapeSniff confirms that an unset date
// format (DateFormatJOB / zero) keeps the legacy shape-sniffing path,
// so existing JOB-default callers are unchanged.
func TestDateStringToISOFallsBackToShapeSniff(t *testing.T) {
	cases := []struct {
		name   string
		wire   string
		format byte
		want   string
	}{
		{"JOB sniffs ISO", "2026-05-08", DateFormatJOB, "2026-05-08"},
		{"JOB sniffs YMD", "26-05-08", DateFormatJOB, "2026-05-08"},
		{"zero sniffs USA", "05/08/2026", 0, "2026-05-08"},
		// Length that doesn't match the negotiated format also degrades
		// to the shape-sniffer rather than slicing out of range.
		{"USA fmt, ISO wire", "2026-05-08", DateFormatUSA, "2026-05-08"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dateStringToISO(tc.wire, tc.format); got != tc.want {
				t.Errorf("dateStringToISO(%q, 0x%02X) = %q, want %q", tc.wire, tc.format, got, tc.want)
			}
		})
	}
}

// TestTimeStringToISONegotiated walks the time formats that need
// normalisation. HMS is already canonical; USA ships 12-hour AM/PM
// without seconds; ISO/EUR/JIS ship dotted "HH.MM.SS".
func TestTimeStringToISONegotiated(t *testing.T) {
	cases := []struct {
		name   string
		wire   string
		format int8
		want   string
	}{
		// HMS (index 0) and unset (-1): already "HH:MM:SS" verbatim.
		{"HMS", "13:45:09", 0, "13:45:09"},
		{"unset", "13:45:09", -1, "13:45:09"},

		// USA (index 1): "HH:MM AM" / "HH:MM PM", 12-hour clock.
		{"USA 1pm", "01:45 PM", 1, "13:45:00"},
		{"USA 1am", "01:45 AM", 1, "01:45:00"},
		{"USA noon", "12:00 PM", 1, "12:00:00"},
		{"USA midnight", "12:00 AM", 1, "00:00:00"},

		// ISO (2) / EUR (3) / JIS (4): dotted "HH.MM.SS" -> "HH:MM:SS".
		{"ISO dotted", "23.59.59", 2, "23:59:59"},
		{"EUR dotted", "23.59.59", 3, "23:59:59"},
		{"JIS dotted", "23.59.59", 4, "23:59:59"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := timeStringToISO(tc.wire, tc.format); got != tc.want {
				t.Errorf("timeStringToISO(%q, %d) = %q, want %q", tc.wire, tc.format, got, tc.want)
			}
		})
	}
}

// TestDecodeColumnDateNegotiatedFormat is the end-to-end regression for
// the cited bug: the same DATE wire bytes ("05/08/26") decoded through
// decodeColumn must yield different ISO dates for MDY vs DMY once the
// negotiated format is stamped on the column. Pre-fix decodeColumn
// always sniffed the shape and landed on MDY (May 8) for both.
func TestDecodeColumnDateNegotiatedFormat(t *testing.T) {
	wire, err := ebcdic.CCSID37.Encode("05/08/26")
	if err != nil {
		t.Fatalf("encode date: %v", err)
	}
	cases := []struct {
		name   string
		format byte
		want   string
	}{
		{"MDY", DateFormatMDY, "2026-05-08"},
		{"DMY", DateFormatDMY, "2026-08-05"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col := SelectColumn{SQLType: SQLTypeDateNN, Length: uint32(len(wire)), Name: "D", DateFormat: tc.format, TimeFormat: -1}
			got, n, err := decodeColumn(wire, col)
			if err != nil {
				t.Fatalf("decodeColumn: %v", err)
			}
			if n != len(wire) {
				t.Errorf("consumed = %d, want %d", n, len(wire))
			}
			if s, _ := got.(string); s != tc.want {
				t.Errorf("decoded = %q, want %q", s, tc.want)
			}
		})
	}
}

// TestDecodeColumnTimeNegotiatedFormat confirms a USA-format TIME
// column ("01:45 PM") normalises to canonical "13:45:00" through
// decodeColumn, where pre-fix it would have returned the raw wire
// string verbatim.
func TestDecodeColumnTimeNegotiatedFormat(t *testing.T) {
	wire, err := ebcdic.CCSID37.Encode("01:45 PM")
	if err != nil {
		t.Fatalf("encode time: %v", err)
	}
	col := SelectColumn{SQLType: SQLTypeTimeNN, Length: uint32(len(wire)), Name: "T", DateFormat: 0, TimeFormat: 1}
	got, n, err := decodeColumn(wire, col)
	if err != nil {
		t.Fatalf("decodeColumn: %v", err)
	}
	if n != len(wire) {
		t.Errorf("consumed = %d, want %d", n, len(wire))
	}
	if s, _ := got.(string); s != "13:45:00" {
		t.Errorf("decoded = %q, want %q", s, "13:45:00")
	}
}
