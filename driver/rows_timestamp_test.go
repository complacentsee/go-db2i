package driver

import (
	"testing"
	"time"
)

// TestParseTemporalISOTimestampPrecisions pins the TIMESTAMP decode
// path across every precision IBM i can emit. hostserver rewrites the
// wire form to ISO "YYYY-MM-DDTHH:MM:SS[.frac]"; parseTemporalISO must
// turn each of those into the matching time.Time. Before the precision
// fix only TIMESTAMP(6) round-tripped; the others arrived raw and
// errored here.
func TestParseTemporalISOTimestampPrecisions(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want time.Time
	}{
		{"prec0", "2026-05-08T12:34:56", time.Date(2026, 5, 8, 12, 34, 56, 0, time.UTC)},
		{"prec3", "2026-05-08T12:34:56.123", time.Date(2026, 5, 8, 12, 34, 56, 123000000, time.UTC)},
		{"prec6", "2026-05-08T12:34:56.123456", time.Date(2026, 5, 8, 12, 34, 56, 123456000, time.UTC)},
		{"prec9", "2026-05-08T12:34:56.123456789", time.Date(2026, 5, 8, 12, 34, 56, 123456789, time.UTC)},
		// TIMESTAMP(12): time.Time is nanosecond-resolution, so the
		// trailing picosecond digits are truncated -- the same loss
		// java.sql.Timestamp takes in JTOpen (setNanos(picos/1000)).
		{"prec12", "2026-05-08T12:34:56.123456789012", time.Date(2026, 5, 8, 12, 34, 56, 123456789, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTemporalISO(393, tc.in)
			if err != nil {
				t.Fatalf("parseTemporalISO(393, %q) error: %v", tc.in, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("parseTemporalISO(393, %q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
