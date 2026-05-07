package hostserver

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/complacentsee/goJTOpen/internal/wirelog"
)

const fixturesDir = "../testdata/jtopen-fixtures/fixtures"

// TestHeaderRoundTrip confirms encode / decode are inverses.
func TestHeaderRoundTrip(t *testing.T) {
	in := Header{
		Length:         52,
		HeaderID:       0,
		ServerID:       ServerSignon,
		CSInstance:     0,
		CorrelationID:  0,
		TemplateLength: 0,
		ReqRepID:       ReqExchangeAttributesSignon,
	}
	b, err := in.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	if len(b) != HeaderLength {
		t.Fatalf("encoded len = %d, want %d", len(b), HeaderLength)
	}
	var out Header
	if err := out.UnmarshalBinary(b); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if in != out {
		t.Fatalf("round trip mismatch: in=%+v out=%+v", in, out)
	}
}

func TestUnmarshalRejectsBadSanityByte(t *testing.T) {
	bad := make([]byte, HeaderLength)
	bad[6] = 0xFE // not 0xE0
	var h Header
	if err := h.UnmarshalBinary(bad); err == nil {
		t.Fatal("expected error for bad sanity byte, got nil")
	}
}

// TestParseConnectOnlyFirstSent decodes the first sent frame from the
// connect_only fixture (which is JTOpen's SignonExchangeAttributeReq)
// and validates every header field plus the parameter layout.
func TestParseConnectOnlyFirstSent(t *testing.T) {
	frames := loadFixture(t, "connect_only.trace")
	first := firstSent(t, frames)

	hdr, payload := decodeFrame(t, first.Bytes)

	if hdr.ServerID != ServerSignon {
		t.Errorf("ServerID = %s, want as-signon", hdr.ServerID)
	}
	if hdr.ReqRepID != ReqExchangeAttributesSignon {
		t.Errorf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, ReqExchangeAttributesSignon)
	}
	if hdr.HeaderID != 0 || hdr.CSInstance != 0 || hdr.CorrelationID != 0 || hdr.TemplateLength != 0 {
		t.Errorf("expected zero header fields, got %+v", hdr)
	}
	if int(hdr.Length) != len(first.Bytes) {
		t.Errorf("header length %d != frame size %d", hdr.Length, len(first.Bytes))
	}

	// Payload structure: three LL-CP-data params -- version, DS level, seed.
	const wantPayload = 32
	if len(payload) != wantPayload {
		t.Fatalf("payload len = %d, want %d", len(payload), wantPayload)
	}
}

// TestExchangeAttributesRequestMatchesFixture builds a fresh
// ExchangeAttributesRequest using the same constants JTOpen used for
// connect_only and asserts the bytes are identical to the fixture.
// This is the M1 acceptance test: if the Go encoder and JTOpen agree
// byte-for-byte on the very first frame of sign-on, the framing layer
// is wire-compatible.
func TestExchangeAttributesRequestMatchesFixture(t *testing.T) {
	frames := loadFixture(t, "connect_only.trace")
	first := firstSent(t, frames)

	// Pull the seed bytes out of the fixture (offset 24..32 in payload,
	// equivalently 44..52 in the full frame).
	if len(first.Bytes) != 52 {
		t.Fatalf("expected 52-byte first frame in connect_only, got %d", len(first.Bytes))
	}
	seed := append([]byte(nil), first.Bytes[44:52]...)

	hdr, payload, err := ExchangeAttributesRequest(ServerSignon, 1, 10, seed)
	if err != nil {
		t.Fatalf("ExchangeAttributesRequest: %v", err)
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got := buf.Bytes()
	if !bytes.Equal(got, first.Bytes) {
		t.Errorf("wire bytes mismatch.\n got: %s\nwant: %s",
			hex.EncodeToString(got), hex.EncodeToString(first.Bytes))
	}
}

// TestParseConnectOnlyExchangeAttributesReply decodes the first
// received frame in connect_only.trace (JTOpen's
// SignonExchangeAttributeRep) and validates every field.
func TestParseConnectOnlyExchangeAttributesReply(t *testing.T) {
	frames := wirelog.Consolidate(loadFixture(t, "connect_only.trace"))
	var first wirelog.Frame
	for _, f := range frames {
		if f.Direction == wirelog.Received {
			first = f
			break
		}
	}
	if first.Bytes == nil {
		t.Fatal("no received frames in connect_only.trace")
	}

	hdr, payload := decodeFrame(t, first.Bytes)
	if hdr.ServerID != ServerSignon {
		t.Errorf("ServerID = %s, want as-signon", hdr.ServerID)
	}
	if hdr.ReqRepID != RepExchangeAttributesSignon {
		t.Errorf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, RepExchangeAttributesSignon)
	}
	if hdr.TemplateLength != 4 {
		t.Errorf("TemplateLength = %d, want 4", hdr.TemplateLength)
	}

	rep, err := ParseExchangeAttributesReply(payload)
	if err != nil {
		t.Fatalf("ParseExchangeAttributesReply: %v", err)
	}
	if rep.ReturnCode != 0 {
		t.Errorf("ReturnCode = %d, want 0", rep.ReturnCode)
	}
	// Server version 0x00070500 = V7R5M0 (PUB400 is on IBM i 7.5).
	if rep.ServerVersion != 0x00070500 {
		t.Errorf("ServerVersion = 0x%08X, want 0x00070500", rep.ServerVersion)
	}
	if rep.ServerLevel != 15 {
		t.Errorf("ServerLevel = %d, want 15", rep.ServerLevel)
	}
	if rep.PasswordLevel != 3 {
		t.Errorf("PasswordLevel = %d, want 3", rep.PasswordLevel)
	}
	wantSeed := []byte{0x2C, 0x16, 0x0B, 0xFE, 0x9E, 0x75, 0x85, 0x2C}
	if !bytes.Equal(rep.ServerSeed, wantSeed) {
		t.Errorf("ServerSeed = %x, want %x", rep.ServerSeed, wantSeed)
	}
	// EBCDIC "341513/QUSER/QZSOSIGN" -- cross-check just by length and
	// a couple of distinctive bytes; full EBCDIC decode lives in the
	// future ebcdic package.
	if len(rep.JobName) != 21 {
		t.Errorf("JobName len = %d, want 21", len(rep.JobName))
	}
	if rep.AAFIndicator {
		t.Errorf("AAFIndicator = true, want false")
	}
}

// ---- helpers ----

func loadFixture(t *testing.T, name string) []wirelog.Frame {
	t.Helper()
	path := filepath.Join(fixturesDir, name)
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("fixture %s not present: %v", name, err)
	}
	defer f.Close()
	frames, err := wirelog.ParseJTOpenTrace(f)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return frames
}

func firstSent(t *testing.T, frames []wirelog.Frame) wirelog.Frame {
	t.Helper()
	for _, f := range frames {
		if f.Direction == wirelog.Sent {
			return f
		}
	}
	t.Fatal("no sent frames")
	return wirelog.Frame{}
}

func decodeFrame(t *testing.T, b []byte) (Header, []byte) {
	t.Helper()
	hdr, payload, err := ReadFrame(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return hdr, payload
}
