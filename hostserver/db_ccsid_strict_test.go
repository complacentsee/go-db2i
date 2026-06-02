package hostserver

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// resetCCSIDPolicy clears the process-level strict flag and per-CCSID
// warn-once map so each subtest starts from a clean slate. Tests that
// touch the same CCSID would otherwise see a no-op Once on the second
// run.
func resetCCSIDPolicy(t *testing.T) {
	t.Helper()
	SetCCSIDStrict(false)
	ccsidWarnOnce.Range(func(k, _ any) bool {
		ccsidWarnOnce.Delete(k)
		return true
	})
	t.Cleanup(func() { SetCCSIDStrict(false) })
}

// TestEbcdicForCCSIDKnownNeverWarns pins that the implemented CCSIDs
// (37 US, 273 German stand-in) resolve to their real codec without a
// warning or strict error, in both default and strict modes.
func TestEbcdicForCCSIDKnownNeverWarns(t *testing.T) {
	resetCCSIDPolicy(t)

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for _, strict := range []bool{false, true} {
		SetCCSIDStrict(strict)
		for ccsid, want := range map[uint16]uint32{37: 37, 273: 273} {
			conv := ebcdicForCCSID(ccsid)
			if conv.CCSID() != want {
				t.Errorf("strict=%v: ebcdicForCCSID(%d).CCSID() = %d, want %d", strict, ccsid, conv.CCSID(), want)
			}
			// A known CCSID must round-trip without erroring even
			// under strict mode.
			if _, err := conv.Encode("AB"); err != nil {
				t.Errorf("strict=%v: ebcdicForCCSID(%d).Encode: unexpected error %v", strict, ccsid, err)
			}
		}
	}
	if buf.Len() != 0 {
		t.Errorf("known CCSIDs emitted unexpected log output: %q", buf.String())
	}
}

// TestEbcdicForCCSIDUnknownWarnsOnceByDefault is the non-breaking
// default: an unknown CCSID falls back to the CCSID-37 byte table and
// emits exactly one slog.Warn per CCSID, no error.
func TestEbcdicForCCSIDUnknownWarnsOnceByDefault(t *testing.T) {
	resetCCSIDPolicy(t)

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	const unknown uint16 = 5026 // Japanese -- not implemented

	// Three calls; the warning must fire exactly once.
	for i := 0; i < 3; i++ {
		conv := ebcdicForCCSID(unknown)
		if conv.CCSID() != 37 {
			t.Fatalf("default fallback for CCSID %d resolved to CCSID %d, want the 37 stand-in", unknown, conv.CCSID())
		}
		// Default path must NOT error -- it stands in CCSID 37.
		if _, err := conv.Decode([]byte{0xC1, 0xC2}); err != nil {
			t.Fatalf("default fallback Decode errored: %v", err)
		}
	}

	got := strings.Count(buf.String(), "unsupported CCSID")
	if got != 1 {
		t.Errorf("warn fired %d times, want exactly 1 (warn-once per CCSID); log:\n%s", got, buf.String())
	}
	if !strings.Contains(buf.String(), "ccsid=5026") {
		t.Errorf("warn missing offending ccsid; log:\n%s", buf.String())
	}
}

// TestEbcdicForCCSIDStrictHardErrors pins that ?charset-strict=true
// promotes the unknown-CCSID fallback to a hard error surfaced through
// the returned codec's Decode/Encode (matching JT400's
// UnsupportedEncodingException). No silent CCSID-37 stand-in.
func TestEbcdicForCCSIDStrictHardErrors(t *testing.T) {
	resetCCSIDPolicy(t)
	SetCCSIDStrict(true)

	const unknown uint16 = 1140 // Euro -- not implemented

	conv := ebcdicForCCSID(unknown)

	if _, err := conv.Decode([]byte{0xC1}); err == nil {
		t.Fatal("strict mode: Decode of unsupported CCSID returned nil error, want UnsupportedEncodingException-style error")
	} else if !strings.Contains(err.Error(), "1140") || !strings.Contains(err.Error(), "charset-strict") {
		t.Errorf("strict Decode error missing CCSID or knob hint: %v", err)
	}

	if _, err := conv.Encode("A"); err == nil {
		t.Fatal("strict mode: Encode of unsupported CCSID returned nil error, want error")
	}
}

// TestEbcdicForCCSIDStrictSpecial confirms the strict codec reports the
// requesting CCSID via CCSID() so diagnostics can identify the column.
func TestEbcdicForCCSIDStrictSpecial(t *testing.T) {
	resetCCSIDPolicy(t)
	SetCCSIDStrict(true)

	conv := ebcdicForCCSID(500)
	if _, ok := conv.(unsupportedCCSIDCodec); !ok {
		t.Fatalf("strict mode returned %T, want unsupportedCCSIDCodec", conv)
	}
	if conv.CCSID() != 500 {
		t.Errorf("unsupportedCCSIDCodec.CCSID() = %d, want 500", conv.CCSID())
	}
	// Sanity: a known CCSID under strict mode is still the real codec.
	if got := ebcdicForCCSID(37); got != ebcdic.Codec(ebcdic.CCSID37) {
		t.Errorf("strict mode clobbered the CCSID 37 codec: %T", got)
	}
}
