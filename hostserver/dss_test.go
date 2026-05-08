package hostserver

import (
	"bytes"
	"encoding/binary"
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

// TestSignonInfoRequestMatchesFixture: same wire-compatibility test as
// TestExchangeAttributesRequestMatchesFixture but for the second sent
// frame in connect_only -- JTOpen's SignonInfoReq. We pull the
// 20-byte SHA-1 encrypted password out of the fixture (we don't know
// the plaintext or the algorithm yet -- that lands in M1's auth
// package) and feed it back into our SignonInfoRequest builder; the
// resulting bytes must match JTOpen's output byte-for-byte.
func TestSignonInfoRequestMatchesFixture(t *testing.T) {
	frames := wirelog.Consolidate(loadFixture(t, "connect_only.trace"))

	var sent []wirelog.Frame
	for _, f := range frames {
		if f.Direction == wirelog.Sent {
			sent = append(sent, f)
		}
	}
	if len(sent) < 2 {
		t.Fatalf("expected >= 2 sent frames in connect_only, got %d", len(sent))
	}

	second := sent[1]
	if int(binary.BigEndian.Uint32(second.Bytes[0:4])) != len(second.Bytes) {
		t.Fatalf("second sent frame length header doesn't match bytes: %d vs %d",
			binary.BigEndian.Uint32(second.Bytes[0:4]), len(second.Bytes))
	}

	hdr, payload := decodeFrame(t, second.Bytes)
	if hdr.ReqRepID != ReqSignonInfo {
		t.Fatalf("ReqRepID = 0x%04X, want 0x%04X (SignonInfo)", hdr.ReqRepID, ReqSignonInfo)
	}

	// Extract the encrypted password from the fixture so we can feed it
	// back into our builder. Parameters in the payload after the
	// 1-byte template:
	//   offset 1:  CCSID param (10 bytes)
	//   offset 11: auth bytes param: LL (4) | CP (2) | password (LL-6)
	//   ...
	if len(payload) < 17 {
		t.Fatalf("payload too short: %d", len(payload))
	}
	authLL := binary.BigEndian.Uint32(payload[11:15])
	authCP := binary.BigEndian.Uint16(payload[15:17])
	if authCP != cpPassword {
		t.Fatalf("auth CP = 0x%04X, want password 0x%04X", authCP, cpPassword)
	}
	authBytes := append([]byte(nil), payload[17:17+(authLL-6)]...)
	if len(authBytes) != 20 {
		t.Fatalf("expected 20-byte SHA-1 password, got %d", len(authBytes))
	}

	// Reconstruct.
	req, reqPayload, err := SignonInfoRequest(
		AuthSchemePassword,
		"AFTRAEGE1",
		authBytes,
		15,   // serverLevel
		1200, // clientCCSID = UTF-16 BE
		nil,
	)
	if err != nil {
		t.Fatalf("SignonInfoRequest: %v", err)
	}
	// Match the fixture's correlation ID (this is the second request on
	// the connection, JTOpen counted from 0 for exchange-attributes).
	req.CorrelationID = 1

	var buf bytes.Buffer
	if err := WriteFrame(&buf, req, reqPayload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got := buf.Bytes()
	if !bytes.Equal(got, second.Bytes) {
		t.Errorf("wire bytes mismatch.\n got: %s\nwant: %s",
			hex.EncodeToString(got), hex.EncodeToString(second.Bytes))
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
	// ServerSeed is per-session random; structural assertion only.
	if len(rep.ServerSeed) != 8 {
		t.Errorf("ServerSeed length = %d, want 8", len(rep.ServerSeed))
	}
	if bytes.Equal(rep.ServerSeed, make([]byte, 8)) {
		t.Errorf("ServerSeed is all zeros (unlikely valid)")
	}
	// JobName is "<jobnumber>/QUSER/QZSOSIGN" in EBCDIC; the job
	// number rotates per-capture so the total length isn't stable
	// (could be 20-22 bytes depending on job-number digit count).
	// Just confirm it's non-empty.
	if len(rep.JobName) == 0 {
		t.Errorf("JobName is empty")
	}
	if rep.AAFIndicator {
		t.Errorf("AAFIndicator = true, want false")
	}
}

// TestParseConnectOnlySignonInfoReply decodes the second received
// frame (consolidated) in connect_only.trace -- the SignonInfoRep
// reply -- and asserts dates, server CCSID, and password expiration
// warning match what we hand-decoded from the fixture bytes.
func TestParseConnectOnlySignonInfoReply(t *testing.T) {
	frames := wirelog.Consolidate(loadFixture(t, "connect_only.trace"))

	var receiveds []wirelog.Frame
	for _, f := range frames {
		if f.Direction == wirelog.Received {
			receiveds = append(receiveds, f)
		}
	}
	if len(receiveds) < 2 {
		t.Fatalf("expected >= 2 received frames, got %d", len(receiveds))
	}
	second := receiveds[1]

	hdr, payload := decodeFrame(t, second.Bytes)
	if hdr.ServerID != ServerSignon {
		t.Errorf("ServerID = %s, want as-signon", hdr.ServerID)
	}
	if hdr.ReqRepID != RepSignonInfo {
		t.Errorf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, RepSignonInfo)
	}
	if hdr.CorrelationID != 1 {
		t.Errorf("CorrelationID = %d, want 1", hdr.CorrelationID)
	}

	rep, err := ParseSignonInfoReply(payload)
	if err != nil {
		t.Fatalf("ParseSignonInfoReply: %v", err)
	}
	if rep.ReturnCode != 0 {
		t.Errorf("ReturnCode = %d, want 0", rep.ReturnCode)
	}
	// Sign-on dates are stamped server-side at capture time, so
	// pinning them breaks every fixture re-capture. Assert
	// non-zero + chronological invariant (current >= last) instead.
	if rep.CurrentSignonDate.IsZero() {
		t.Errorf("CurrentSignonDate is zero, want non-zero")
	}
	if rep.LastSignonDate.IsZero() {
		t.Errorf("LastSignonDate is zero, want non-zero")
	}
	if rep.CurrentSignonDate.Before(rep.LastSignonDate) {
		t.Errorf("CurrentSignonDate (%v) before LastSignonDate (%v)",
			rep.CurrentSignonDate, rep.LastSignonDate)
	}
	if rep.ExpirationDate.IsZero() {
		t.Errorf("ExpirationDate is zero, want non-zero")
	}
	if rep.ServerCCSID != 273 {
		t.Errorf("ServerCCSID = %d, want 273 (German)", rep.ServerCCSID)
	}
	if rep.PWDExpirationWarningDays != 7 {
		t.Errorf("PWDExpirationWarningDays = %d, want 7", rep.PWDExpirationWarningDays)
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
