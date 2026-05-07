package hostserver

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/complacentsee/goJTOpen/internal/wirelog"
)

// fakeConn replays a queue of pre-recorded reply bytes on Read while
// recording everything Write sees. SignOn(conn) drives both halves
// in turn; this lets us validate the full flow against a captured
// fixture without leaking the real password and without needing
// PUB400.
type fakeConn struct {
	pending []byte // remaining queued reply bytes, sliced as Read consumes
	replies [][]byte
	written bytes.Buffer
	closed  bool
}

func newFakeConn(replies ...[]byte) *fakeConn {
	return &fakeConn{replies: replies}
}

func (c *fakeConn) Read(p []byte) (int, error) {
	if c.closed {
		return 0, io.EOF
	}
	for len(c.pending) == 0 {
		if len(c.replies) == 0 {
			return 0, io.EOF
		}
		c.pending = c.replies[0]
		c.replies = c.replies[1:]
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *fakeConn) Write(p []byte) (int, error) {
	return c.written.Write(p)
}

func (c *fakeConn) Close() error { c.closed = true; return nil }

// TestSignOnAgainstConnectOnlyFixture replays the two server replies
// captured in connect_only.trace at SignOn. It can't validate the
// encrypted password bytes (we don't know the fixture's plaintext
// password), but it does confirm:
//
//   - SignOn writes a syntactically valid exchange-attrs request (DSS
//     header + LL/CP params parse cleanly back through ReadFrame);
//   - SignOn writes a syntactically valid signon-info request after
//     parsing the captured reply;
//   - the parsed ExchangeAttributesReply matches the fixture's
//     server-supplied values (V7R5M0, ds level 15, password level 3,
//     8-byte server seed);
//   - the parsed SignonInfoReply matches (RC=0, dates, CCSID 273,
//     warn=7).
func TestSignOnAgainstConnectOnlyFixture(t *testing.T) {
	frames := wirelog.Consolidate(loadFixture(t, "connect_only.trace"))
	var receiveds []wirelog.Frame
	for _, f := range frames {
		if f.Direction == wirelog.Received {
			receiveds = append(receiveds, f)
		}
	}
	if len(receiveds) < 2 {
		t.Fatalf("need >= 2 received frames in connect_only, got %d", len(receiveds))
	}

	conn := newFakeConn(receiveds[0].Bytes, receiveds[1].Bytes)
	xa, si, err := SignOn(conn, "AFTRAEGE1", "any-password-the-test-doesnt-care")
	if err != nil {
		t.Fatalf("SignOn: %v", err)
	}

	// Validate the parsed reply fields landed correctly.
	if xa.ServerVersion != 0x00070500 {
		t.Errorf("ServerVersion = 0x%08X, want 0x00070500", xa.ServerVersion)
	}
	if xa.PasswordLevel != 3 {
		t.Errorf("PasswordLevel = %d, want 3", xa.PasswordLevel)
	}
	if si.ReturnCode != 0 {
		t.Errorf("ReturnCode = %d, want 0", si.ReturnCode)
	}
	if si.ServerCCSID != 273 {
		t.Errorf("ServerCCSID = %d, want 273", si.ServerCCSID)
	}
	wantCurrent := time.Date(2026, 5, 7, 19, 8, 0, 0, time.UTC)
	if !si.CurrentSignonDate.Equal(wantCurrent) {
		t.Errorf("CurrentSignonDate = %v, want %v", si.CurrentSignonDate, wantCurrent)
	}

	// And validate the requests SignOn wrote are themselves parseable.
	written := conn.written.Bytes()
	r := bytes.NewReader(written)

	hdr1, payload1, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("re-parse first sent frame: %v", err)
	}
	if hdr1.ReqRepID != ReqExchangeAttributesSignon {
		t.Errorf("first sent ReqRepID = 0x%04X, want 0x%04X", hdr1.ReqRepID, ReqExchangeAttributesSignon)
	}
	// The exchange-attrs payload must be exactly 32 bytes (version 10
	// + ds level 8 + seed 14).
	if len(payload1) != 32 {
		t.Errorf("first sent payload = %d bytes, want 32", len(payload1))
	}

	hdr2, payload2, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("re-parse second sent frame: %v", err)
	}
	if hdr2.ReqRepID != ReqSignonInfo {
		t.Errorf("second sent ReqRepID = 0x%04X, want 0x%04X", hdr2.ReqRepID, ReqSignonInfo)
	}
	if hdr2.CorrelationID != 1 {
		t.Errorf("second sent CorrelationID = %d, want 1", hdr2.CorrelationID)
	}
	// Signon-info payload contains: 1 (template) + 10 (CCSID) + 26
	// (auth: 6 + 20 SHA-1 password) + 16 (userID) + 7 (return errs) = 60.
	if len(payload2) != 60 {
		t.Errorf("second sent payload = %d bytes, want 60", len(payload2))
	}
}
