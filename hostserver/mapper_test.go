package hostserver

import "testing"

func TestServerMapName(t *testing.T) {
	cases := []struct {
		service ServerID
		secure  bool
		want    string
	}{
		{ServerDatabase, false, "as-database"},
		{ServerDatabase, true, "as-database-s"},
		{ServerSignon, false, "as-signon"},
		{ServerSignon, true, "as-signon-s"},
	}
	for _, c := range cases {
		if got := ServerMapName(c.service, c.secure); got != c.want {
			t.Errorf("ServerMapName(%v, %v) = %q, want %q", c.service, c.secure, got, c.want)
		}
	}
}

func TestServerMapPortSuccess(t *testing.T) {
	// 0x2B '+' status, then 8471 (0x00002117) big-endian.
	conn := newFakeConn([]byte{0x2B, 0x00, 0x00, 0x21, 0x17})
	port, err := ServerMapPort(conn, ServerDatabase, false)
	if err != nil {
		t.Fatalf("ServerMapPort: unexpected error: %v", err)
	}
	if port != 8471 {
		t.Errorf("resolved port = %d, want 8471", port)
	}
	// The request on the wire is the bare ASCII service name -- no
	// length prefix, no terminator, no padding.
	if got := conn.written.String(); got != "as-database" {
		t.Errorf("request bytes = %q, want %q", got, "as-database")
	}
}

func TestServerMapPortSecureSuffix(t *testing.T) {
	// 9471 (0x000024FF) big-endian on a secure lookup.
	conn := newFakeConn([]byte{0x2B, 0x00, 0x00, 0x24, 0xFF})
	port, err := ServerMapPort(conn, ServerDatabase, true)
	if err != nil {
		t.Fatalf("ServerMapPort: unexpected error: %v", err)
	}
	if port != 9471 {
		t.Errorf("resolved port = %d, want 9471", port)
	}
	if got := conn.written.String(); got != "as-database-s" {
		t.Errorf("secure request bytes = %q, want %q", got, "as-database-s")
	}
}

func TestServerMapPortFailureStatus(t *testing.T) {
	// ASCII '-' (0x2D) = service-not-found; trailing bytes ignored.
	conn := newFakeConn([]byte{0x2D, 0x00, 0x00, 0x00, 0x00})
	if _, err := ServerMapPort(conn, ServerSignon, false); err == nil {
		t.Fatal("ServerMapPort returned nil error on '-' status; want failure")
	}
}

func TestServerMapPortShortRead(t *testing.T) {
	// Fewer than 5 reply bytes is a dropped connection, not a partial
	// success.
	conn := newFakeConn([]byte{0x2B, 0x00})
	if _, err := ServerMapPort(conn, ServerDatabase, false); err == nil {
		t.Fatal("ServerMapPort returned nil error on short reply; want failure")
	}
}

func TestServerMapPortRejectsOutOfRangePort(t *testing.T) {
	// Success status but a zero port -- must be rejected, not returned.
	conn := newFakeConn([]byte{0x2B, 0x00, 0x00, 0x00, 0x00})
	if _, err := ServerMapPort(conn, ServerDatabase, false); err == nil {
		t.Fatal("ServerMapPort returned nil error on port 0; want range failure")
	}
}
