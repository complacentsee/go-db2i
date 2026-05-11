package wirelog

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FuzzParseJTOpenTrace exercises the text-mode JT400 trace parser
// against adversarial input. The corpus is seeded with every
// committed .trace fixture plus a handful of hand-built shapes that
// cover the parser's syntactic edges (mixed-case direction tokens,
// runaway hex rows, mid-frame non-hex text, multi-frame). The
// invariant: any input is either parsed into a Frame slice without
// panic, or rejected with a typed error. ParseJTOpenTrace must not
// hang, panic, or allocate without bound.
func FuzzParseJTOpenTrace(f *testing.F) {
	for _, seed := range traceFuzzSeeds() {
		f.Add(seed)
	}
	entries, err := os.ReadDir(fixturesDir)
	if err == nil {
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".trace") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(fixturesDir, e.Name()))
			if err != nil {
				continue
			}
			f.Add(b)
		}
	}

	f.Fuzz(func(t *testing.T, in []byte) {
		frames, err := ParseJTOpenTrace(bytes.NewReader(in))
		if err != nil {
			// Errors are allowed -- the contract is only "do not
			// panic". Consolidate / Sents / Receiveds still have to
			// behave on the returned (possibly nil) slice though.
			_ = Consolidate(frames)
			_ = Sents(frames)
			_ = Receiveds(frames)
			return
		}
		// On success, downstream consolidation must not panic. The
		// consolidated slice should be <= the raw count and preserve
		// the (Direction, ConnID) of the first frame.
		cons := Consolidate(frames)
		if len(cons) > len(frames) {
			t.Fatalf("Consolidate grew the frame count: %d > %d", len(cons), len(frames))
		}
		if len(frames) > 0 && len(cons) > 0 {
			if cons[0].Direction != frames[0].Direction || cons[0].ConnID != frames[0].ConnID {
				t.Fatalf("Consolidate reordered first frame: %+v vs %+v", cons[0], frames[0])
			}
		}
		_ = Sents(frames)
		_ = Receiveds(frames)
	})
}

func traceFuzzSeeds() [][]byte {
	return [][]byte{
		[]byte(""),
		[]byte("\n"),
		[]byte("Data stream sent (connID=1)\n00 01 02 03\n"),
		[]byte("Data stream sent (connID=1)\n00 01\nData stream data received (connID=1)\nAA BB\n"),
		// Mid-frame non-hex text -- should flush.
		[]byte("Data stream sent (connID=1)\n00 01\n  stack trace\n02 03\n"),
		// Hex with odd-length token -- rejected by tryParseHexLine.
		[]byte("Data stream sent (connID=1)\n0\n"),
		// Two-char non-hex token -- rejected.
		[]byte("Data stream sent (connID=1)\nZZ\n"),
		// Multi-byte connID parses but is out of uint32 range.
		[]byte("Data stream sent (connID=99999999999999999999)\n00\n"),
		// Hex row before any header -- ignored.
		[]byte("00 01 02 03\nData stream sent (connID=1)\n0A\n"),
	}
}
