package hostserver

import (
	"testing"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// TestTimeBindReshapeForDateTimeCacheHit pins issue #23's cache-hit
// fix: a time.Time bind always arrives as the 26-char IBM timestamp
// "YYYY-MM-DD-HH.MM.SS.ffffff" (driver/stmt.go), but on a cache-hit
// the parameter shape comes from the *PGM-stored PMF
// (preparedParamsFromCached). When that PMF declares DATE or TIME the
// 26-char value must be reshaped down to the column width before the
// 10/8-char encoders run -- otherwise encodeDateForParam /
// encodeTimeString reject it on length. We drive the production
// EncodeDBExtendedData path (the same call ExecutePreparedCached
// makes) and decode the data block back from CCSID-37 to assert the
// wire form.
func TestTimeBindReshapeForDateTimeCacheHit(t *testing.T) {
	// As produced by the time.Time bind site for
	// 2026-05-07 14:23:45.123456.
	const tsBind = "2026-05-07-14.23.45.123456"

	cases := []struct {
		name     string
		sqlType  uint16
		fieldLen uint32
		want     string // expected decoded wire form
	}{
		{"DATE NN", 384, 10, "2026-05-07"},
		{"DATE nullable", 385, 10, "2026-05-07"},
		// TIME wire uses JT400's period separator "HH.MM.SS" (issue #40;
		// encodeTimeString matches SQLTime.convertToRawBytes byte-for-byte).
		{"TIME NN", 388, 8, "14.23.45"},
		{"TIME nullable", 389, 8, "14.23.45"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := []PreparedParam{{
				SQLType:     tc.sqlType,
				FieldLength: tc.fieldLen,
				CCSID:       37,
			}}
			payload, err := EncodeDBExtendedData(params, []any{tsBind})
			if err != nil {
				t.Fatalf("EncodeDBExtendedData (cache-hit reshape failed): %v", err)
			}
			// Layout: 20-byte header + 1 col * 2-byte indicator, then
			// the data block of FieldLength bytes.
			const dataOff = 20 + 2
			ebc := payload[dataOff : dataOff+int(tc.fieldLen)]
			got, err := ebcdic.CCSID37.Decode(ebc)
			if err != nil {
				t.Fatalf("decode CCSID-37: %v", err)
			}
			if got != tc.want {
				t.Errorf("decoded wire = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestReshapeTimestampHelpers covers the slice helpers directly,
// including the pass-through for inputs that are already a bare
// date / time string (the non-time.Time bind path).
func TestReshapeTimestampHelpers(t *testing.T) {
	const ts = "2026-05-07-14.23.45.123456"
	if got := reshapeTimestampForDate(ts); got != "2026-05-07" {
		t.Errorf("reshapeTimestampForDate = %q, want %q", got, "2026-05-07")
	}
	if got := reshapeTimestampForTime(ts); got != "14.23.45" {
		t.Errorf("reshapeTimestampForTime = %q, want %q", got, "14.23.45")
	}
	// Pass-through: a bare 10-char date is untouched.
	if got := reshapeTimestampForDate("2026-05-07"); got != "2026-05-07" {
		t.Errorf("reshapeTimestampForDate(date) = %q, want unchanged", got)
	}
	// Pass-through: a bare 8-char time is untouched.
	if got := reshapeTimestampForTime("14:23:45"); got != "14:23:45" {
		t.Errorf("reshapeTimestampForTime(time) = %q, want unchanged", got)
	}
}
