package wirelog

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fixturesDir = "../../testdata/jtopen-fixtures/fixtures"

func TestParseConnectOnly(t *testing.T) {
	frames := loadFrames(t, "connect_only.trace")
	if len(frames) == 0 {
		t.Fatalf("expected frames, got 0")
	}
	var sent, recv int
	for _, f := range frames {
		switch f.Direction {
		case Sent:
			sent++
		case Received:
			recv++
		}
	}
	if sent == 0 || recv == 0 {
		t.Errorf("expected both directions, got sent=%d recv=%d", sent, recv)
	}
}

// TestSentFrameLengthHeader sanity-checks that for sent frames in
// connect_only the first 4 bytes (host-server DSS length field) match
// the actual byte count we extracted. If the parser ever drops bytes
// or merges frames this fires loudly.
func TestSentFrameLengthHeader(t *testing.T) {
	frames := loadFrames(t, "connect_only.trace")
	for i, f := range frames {
		if f.Direction != Sent || len(f.Bytes) < 4 {
			continue
		}
		got := binary.BigEndian.Uint32(f.Bytes[:4])
		if int(got) != len(f.Bytes) {
			t.Errorf("frame %d: header length %d != bytes %d", i, got, len(f.Bytes))
		}
	}
}

// TestParseAllFixtures asserts every committed .trace parses cleanly
// and produces at least one Sent + one Received frame -- a structural
// invariant of any captured connection (sign-on alone exchanges several
// pairs).
func TestParseAllFixtures(t *testing.T) {
	entries, err := os.ReadDir(fixturesDir)
	if err != nil {
		t.Skipf("fixtures not present: %v", err)
	}
	var checked int
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".trace") {
			continue
		}
		t.Run(e.Name(), func(t *testing.T) {
			frames := loadFrames(t, e.Name())
			if len(frames) == 0 {
				t.Errorf("no frames parsed")
				return
			}
			var sent, recv int
			for _, f := range frames {
				switch f.Direction {
				case Sent:
					sent++
				case Received:
					recv++
				}
			}
			if sent == 0 {
				t.Errorf("no sent frames")
			}
			if recv == 0 {
				t.Errorf("no received frames")
			}
		})
		checked++
	}
	if checked == 0 {
		t.Skip("no .trace fixtures yet")
	}
}

func loadFrames(t *testing.T, name string) []Frame {
	t.Helper()
	path := filepath.Join(fixturesDir, name)
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("fixture %s not present: %v", name, err)
	}
	defer f.Close()
	frames, err := ParseJTOpenTrace(f)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return frames
}
