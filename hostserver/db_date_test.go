package hostserver

import "testing"

// TestYmdToISODate covers all date-format wire shapes the decoder
// auto-detects. Caller is responsible for setting
// DBAttributesOptions.DateFormat if they want to force a specific
// wire format from the server; this function is the safety net for
// the JOB-default path where the server picks the format and we
// have to figure it out from bytes alone.
func TestYmdToISODate(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// ISO -- already canonical.
		{"2026-05-08", "2026-05-08"},
		// JIS uses identical wire shape to ISO.
		{"2099-12-31", "2099-12-31"},

		// YMD: 2-digit year, 1940 century boundary.
		{"26-05-08", "2026-05-08"},
		{"39-12-31", "2039-12-31"},
		{"40-01-01", "1940-01-01"},
		{"99-12-31", "1999-12-31"},

		// USA: MM/DD/YYYY.
		{"05/08/2026", "2026-05-08"},
		{"12/31/1999", "1999-12-31"},

		// EUR: DD.MM.YYYY.
		{"08.05.2026", "2026-05-08"},
		{"31.12.1999", "1999-12-31"},

		// MDY: MM/DD/YY (US 8-char). DMY collides on shape.
		{"05/08/26", "2026-05-08"},
		{"12/31/99", "1999-12-31"},

		// Unrecognised shape -- pass through.
		{"hello", "hello"},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := ymdToISODate(tc.in); got != tc.want {
				t.Errorf("ymdToISODate(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
