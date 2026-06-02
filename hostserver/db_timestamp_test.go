package hostserver

import "testing"

// TestIbmTimestampToISO covers every TIMESTAMP precision the server
// can emit. IBM i sends "YYYY-MM-DD-HH.MM.SS[.frac]" where the
// fractional tail is precision-many digits (0..12); TIMESTAMP(0)
// omits the separator and tail entirely. Before the precision fix
// only the 26-char TIMESTAMP(6) shape was rewritten -- every other
// precision fell through raw and then failed to Scan into time.Time.
func TestIbmTimestampToISO(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		// TIMESTAMP(0): 19 chars, no fractional separator.
		{"prec0", "2026-05-08-12.34.56", "2026-05-08T12:34:56"},
		// TIMESTAMP(3): 23 chars.
		{"prec3", "2026-05-08-12.34.56.123", "2026-05-08T12:34:56.123"},
		// TIMESTAMP(6): 26 chars -- the originally-supported shape.
		{"prec6", "2026-05-08-12.34.56.123456", "2026-05-08T12:34:56.123456"},
		// TIMESTAMP(9): 29 chars.
		{"prec9", "2026-05-08-12.34.56.123456789", "2026-05-08T12:34:56.123456789"},
		// TIMESTAMP(12): 32 chars -- max IBM i precision (picoseconds).
		{"prec12", "2026-05-08-12.34.56.123456789012", "2026-05-08T12:34:56.123456789012"},

		// Bad shapes pass through unchanged.
		{"too_short", "2026-05-08", "2026-05-08"},
		{"missing_sep_at_19", "2026-05-08-12.34.56X123", "2026-05-08-12.34.56X123"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ibmTimestampToISO(tc.in); got != tc.want {
				t.Errorf("ibmTimestampToISO(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
