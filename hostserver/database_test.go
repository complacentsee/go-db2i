package hostserver

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/complacentsee/go-db2i/internal/wirelog"
)

// findAsDatabaseConnID picks the trace connID that carries the
// as-database (port 8471) traffic, by inspecting each unique
// connection's first Received DSS frame and matching ServerID ==
// ServerDatabase (0xE004). JTOpen opens one TCP socket per
// host-server service, so the database half always lives on its
// own connID distinct from the as-signon connection.
//
// We derive this dynamically rather than hardcoding a constant,
// because every fixture re-capture generates fresh random connIDs.
// Returns 0 if no as-database connection is found in the fixture.
func findAsDatabaseConnID(frames []wirelog.Frame) uint32 {
	// Track first-received-frame per connID, in connID-discovery order.
	type firstFrame struct {
		connID uint32
		bytes  []byte
	}
	var firstByConn []firstFrame
	seen := map[uint32]bool{}
	for _, f := range frames {
		if f.Direction != wirelog.Received {
			continue
		}
		if seen[f.ConnID] {
			continue
		}
		seen[f.ConnID] = true
		firstByConn = append(firstByConn, firstFrame{connID: f.ConnID, bytes: f.Bytes})
	}
	for _, ff := range firstByConn {
		if len(ff.bytes) < HeaderLength {
			continue
		}
		var hdr Header
		if err := hdr.UnmarshalBinary(ff.bytes[:HeaderLength]); err != nil {
			continue
		}
		if hdr.ServerID == ServerDatabase {
			return ff.connID
		}
	}
	return 0
}

// dbReceivedsFromFixture pulls only the as-database receive frames
// from a fixture in their on-wire order.
func dbReceivedsFromFixture(t *testing.T, name string) []wirelog.Frame {
	t.Helper()
	frames := wirelog.Consolidate(loadFixture(t, name))
	dbConnID := findAsDatabaseConnID(frames)
	if dbConnID == 0 {
		t.Fatalf("no as-database (ServerID=0xE004) connection found in %s", name)
	}
	var out []wirelog.Frame
	for _, f := range frames {
		if f.Direction == wirelog.Received && f.ConnID == dbConnID {
			out = append(out, f)
		}
	}
	if len(out) < 2 {
		t.Fatalf("need >= 2 as-database received frames in %s, got %d", name, len(out))
	}
	return out
}

// dbSentsFromFixture pulls only the as-database send frames.
func dbSentsFromFixture(t *testing.T, name string) []wirelog.Frame {
	t.Helper()
	frames := wirelog.Consolidate(loadFixture(t, name))
	dbConnID := findAsDatabaseConnID(frames)
	if dbConnID == 0 {
		t.Fatalf("no as-database (ServerID=0xE004) connection found in %s", name)
	}
	var out []wirelog.Frame
	for _, f := range frames {
		if f.Direction == wirelog.Sent && f.ConnID == dbConnID {
			out = append(out, f)
		}
	}
	if len(out) < 2 {
		t.Fatalf("need >= 2 as-database sent frames in %s, got %d", name, len(out))
	}
	return out
}

// TestXChgRandSeedRequestBytesMatchFixture confirms the encoder
// produces a 28-byte frame whose every field but the 8-byte seed is
// byte-equal to JTOpen's. The seed itself is supplied by the test (so
// we don't have to mock crypto/rand) and round-tripped through
// ReadFrame.
func TestXChgRandSeedRequestBytesMatchFixture(t *testing.T) {
	fixture := dbSentsFromFixture(t, "connect_only.trace")[0].Bytes
	if len(fixture) != 28 {
		t.Fatalf("fixture sent #1 (xchg-rand-seed req) is %d bytes, want 28", len(fixture))
	}

	// Pull the seed JTOpen actually sent so we can re-emit the same
	// frame and compare byte-for-byte.
	clientSeed := append([]byte(nil), fixture[20:28]...)

	hdr, payload, err := XChgRandSeedRequest(ServerDatabase, clientSeed)
	if err != nil {
		t.Fatalf("XChgRandSeedRequest: %v", err)
	}

	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got := buf.Bytes()
	if !bytes.Equal(got, fixture) {
		t.Errorf("XChgRandSeedRequest bytes differ:\n got %s\nwant %s",
			hex.EncodeToString(got), hex.EncodeToString(fixture))
	}
}

// TestParseXChgRandSeedReplyAgainstFixture decodes the recorded
// 0xF001 reply for the database service and validates RC, server
// seed, password level (carried in HeaderID), and the absence of
// optional CPs.
func TestParseXChgRandSeedReplyAgainstFixture(t *testing.T) {
	fixture := dbReceivedsFromFixture(t, "connect_only.trace")[0].Bytes
	hdr, payload, err := ReadFrame(bytes.NewReader(fixture))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if hdr.ReqRepID != RepXChgRandSeed {
		t.Fatalf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, RepXChgRandSeed)
	}
	rep, err := ParseXChgRandSeedReply(hdr.HeaderID, payload)
	if err != nil {
		t.Fatalf("ParseXChgRandSeedReply: %v", err)
	}
	if rep.ReturnCode != 0 {
		t.Errorf("ReturnCode = %d, want 0", rep.ReturnCode)
	}
	if rep.PasswordLevel != 3 {
		t.Errorf("PasswordLevel = %d, want 3 (SHA-1)", rep.PasswordLevel)
	}
	if len(rep.ServerSeed) != 8 {
		t.Errorf("ServerSeed length = %d, want 8", len(rep.ServerSeed))
	}
	// ServerSeed is per-session random; structural assertion only
	// (length + non-zero), no value pin -- otherwise every re-capture
	// breaks this test.
	if bytes.Equal(rep.ServerSeed, make([]byte, 8)) {
		t.Errorf("ServerSeed is all zeros (unlikely valid)")
	}
	if rep.AAFIndicator {
		t.Errorf("AAFIndicator = true, want false (no MFA on PUB400)")
	}
}

// TestStartServerRequestEncodesValidFrame builds a 0x7002 frame with
// the same shape JTOpen sends and confirms the result re-parses
// through ReadFrame without losing any structure. We can't do a
// byte-for-byte compare against the fixture because the SHA-1
// substitute password depends on a fresh client seed, but we can
// confirm the user-ID + auth-bytes layout.
func TestStartServerRequestEncodesValidFrame(t *testing.T) {
	authBytes, _ := hex.DecodeString("1a5a48d41ad37507756a7734" + "8b81286" + "69f5c4aae")
	if len(authBytes) != 20 {
		t.Fatalf("test author bug: authBytes length %d, want 20", len(authBytes))
	}
	hdr, payload, err := StartServerRequest(ServerDatabase, AuthSchemePassword, "AFTRAEGE1", authBytes)
	if err != nil {
		t.Fatalf("StartServerRequest: %v", err)
	}
	if hdr.ReqRepID != ReqStartServer {
		t.Fatalf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, ReqStartServer)
	}
	if hdr.HeaderID>>8 != clientAttrsStartServer {
		t.Errorf("HeaderID high byte = 0x%02X, want 0x%02X", hdr.HeaderID>>8, clientAttrsStartServer)
	}
	if hdr.TemplateLength != 2 {
		t.Errorf("TemplateLength = %d, want 2", hdr.TemplateLength)
	}
	// Template byte 0 = 0x03 (SHA-1, 20 bytes). Template byte 1 = 0x01.
	if payload[0] != 0x03 {
		t.Errorf("template byte 0 = 0x%02X, want 0x03", payload[0])
	}
	if payload[1] != 0x01 {
		t.Errorf("template byte 1 = 0x%02X, want 0x01 (send-reply true)", payload[1])
	}

	// Round-trip through Read/Write to confirm framing.
	var buf bytes.Buffer
	if err := WriteFrame(&buf, hdr, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, payload2, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame round trip: %v", err)
	}
	if got.Length != hdr.Length {
		t.Errorf("round-tripped Length = %d, want %d", got.Length, hdr.Length)
	}
	if !bytes.Equal(payload, payload2) {
		t.Errorf("round-tripped payload differs")
	}

	// Verify the user-ID CP is at the expected offset and contains
	// "AFTRAEGE1 " in EBCDIC CCSID 37.
	be := binary.BigEndian
	off := 2 + 6 + len(authBytes)
	if int(be.Uint32(payload[off:off+4])) != 16 {
		t.Errorf("user-ID LL = %d, want 16", be.Uint32(payload[off:off+4]))
	}
	if be.Uint16(payload[off+4:off+6]) != cpUserID {
		t.Errorf("user-ID CP = 0x%04X, want 0x%04X", be.Uint16(payload[off+4:off+6]), cpUserID)
	}
	wantUser, _ := hex.DecodeString("c1c6e3d9c1c5c7c5f140") // AFTRAEGE1 + EBCDIC space
	if !bytes.Equal(payload[off+6:off+16], wantUser) {
		t.Errorf("user-ID bytes = %x, want %x", payload[off+6:off+16], wantUser)
	}
}

// TestParseStartServerReplyAgainstFixture decodes the recorded
// 0xF002 reply and validates RC + the prestart-job EBCDIC bytes.
func TestParseStartServerReplyAgainstFixture(t *testing.T) {
	fixture := dbReceivedsFromFixture(t, "connect_only.trace")[1].Bytes
	hdr, payload, err := ReadFrame(bytes.NewReader(fixture))
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if hdr.ReqRepID != RepStartServer {
		t.Fatalf("ReqRepID = 0x%04X, want 0x%04X", hdr.ReqRepID, RepStartServer)
	}
	rep, err := ParseStartServerReply(payload)
	if err != nil {
		t.Fatalf("ParseStartServerReply: %v", err)
	}
	if rep.ReturnCode != 0 {
		t.Errorf("ReturnCode = %d, want 0", rep.ReturnCode)
	}
	// Job name is "<jobnumber>/QUSER/QZDASOINIT" at capture time;
	// the job number rotates per-capture so we assert structurally
	// (non-empty, contains the EBCDIC bytes for "QUSER" and
	// "QZDASOINIT") rather than pinning the prefix.
	if len(rep.JobName) == 0 {
		t.Errorf("JobName is empty, want non-empty")
	}
	wantQUSER, _ := hex.DecodeString("d8e4e2c5d9") // EBCDIC "QUSER"
	if !bytes.Contains(rep.JobName, wantQUSER) {
		t.Errorf("JobName %x does not contain EBCDIC 'QUSER' (%x)", rep.JobName, wantQUSER)
	}
	wantQZDA, _ := hex.DecodeString("d8e9c4c1e2d6c9d5c9e3") // EBCDIC "QZDASOINIT"
	if !bytes.Contains(rep.JobName, wantQZDA) {
		t.Errorf("JobName %x does not contain EBCDIC 'QZDASOINIT' (%x)", rep.JobName, wantQZDA)
	}
}

// TestStartDatabaseServiceAgainstConnectOnlyFixture replays both
// reply frames captured from the database connection at a known
// successful sign-on. It validates the same trio that
// TestSignOnAgainstConnectOnlyFixture validates for the signon
// service:
//
//   - the orchestrator accepts the captured replies as-is;
//   - parsed XChgRandSeedReply / StartServerReply look right;
//   - the requests StartDatabaseService writes parse cleanly back.
func TestStartDatabaseServiceAgainstConnectOnlyFixture(t *testing.T) {
	receiveds := dbReceivedsFromFixture(t, "connect_only.trace")

	conn := newFakeConn(receiveds[0].Bytes, receiveds[1].Bytes)
	xs, ss, err := StartDatabaseService(conn, "AFTRAEGE1", "any-password-the-test-doesnt-care")
	if err != nil {
		t.Fatalf("StartDatabaseService: %v", err)
	}

	if xs.ReturnCode != 0 {
		t.Errorf("XChgRandSeedReply.ReturnCode = %d, want 0", xs.ReturnCode)
	}
	if xs.PasswordLevel != 3 {
		t.Errorf("XChgRandSeedReply.PasswordLevel = %d, want 3", xs.PasswordLevel)
	}
	if ss.ReturnCode != 0 {
		t.Errorf("StartServerReply.ReturnCode = %d, want 0", ss.ReturnCode)
	}
	// JobName: per-capture variable; structural-only assertion as
	// in TestParseStartServerReplyAgainstFixture.
	if len(ss.JobName) == 0 {
		t.Errorf("StartServerReply.JobName is empty")
	}

	// Re-parse the requests StartDatabaseService produced.
	r := bytes.NewReader(conn.written.Bytes())

	hdr1, payload1, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("re-parse first sent frame: %v", err)
	}
	if hdr1.ReqRepID != ReqXChgRandSeed {
		t.Errorf("first sent ReqRepID = 0x%04X, want 0x%04X", hdr1.ReqRepID, ReqXChgRandSeed)
	}
	if len(payload1) != 8 {
		t.Errorf("first sent payload = %d bytes, want 8", len(payload1))
	}

	hdr2, payload2, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("re-parse second sent frame: %v", err)
	}
	if hdr2.ReqRepID != ReqStartServer {
		t.Errorf("second sent ReqRepID = 0x%04X, want 0x%04X", hdr2.ReqRepID, ReqStartServer)
	}
	// Template (2) + auth CP (6 + 20) + user-ID CP (16) = 44.
	if len(payload2) != 44 {
		t.Errorf("second sent payload = %d bytes, want 44", len(payload2))
	}
}
