package hostserver

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/complacentsee/go-db2i/ebcdic"
	"github.com/complacentsee/go-db2i/internal/wirelog"
)

// TestNDBAddLibraryListMultiWireShape pins the byte layout of the
// CP 0x3813 list-of-libraries parameter when sending more than one
// library in a single ADD_LIBRARY_LIST frame. The first library is
// tagged with EBCDIC 'C' (0xC3, "current SQL schema") and the rest
// with EBCDIC 'L' (0xD3, "append to back of *LIBL"). Mirrors JT400's
// JDLibraryList behaviour for a comma-separated libraries= list
// without an explicit *LIBL token.
func TestNDBAddLibraryListMultiWireShape(t *testing.T) {
	// Use the captured NDB reply from select_dummy.trace as a
	// believable server response; the reply shape is independent of
	// how many libraries the client sent.
	const ndbServerID ServerID = 0xE005
	frames := wirelog.Consolidate(loadFixture(t, "select_dummy.trace"))
	var ndbRecv []byte
	for _, f := range frames {
		if f.Direction != wirelog.Received || len(f.Bytes) < 8 {
			continue
		}
		if ServerID(binary.BigEndian.Uint16(f.Bytes[6:8])) == ndbServerID {
			ndbRecv = f.Bytes
			break
		}
	}
	if ndbRecv == nil {
		t.Fatalf("no NDB-service reply found in select_dummy.trace")
	}

	conn := newFakeConn(ndbRecv)
	libs := []string{"GOTEST", "GOSPROCS", "QGPL"}
	if err := NDBAddLibraryListMulti(conn, libs, 2); err != nil {
		t.Fatalf("NDBAddLibraryListMulti: %v", err)
	}
	got := conn.written.Bytes()

	// Build the expected CP 0x3813 inner payload byte-by-byte so a
	// future encoder bug yields a precise offset diff.
	var inner bytes.Buffer
	var two [2]byte
	binary.BigEndian.PutUint16(two[:], 273) // CCSID
	inner.Write(two[:])
	binary.BigEndian.PutUint16(two[:], uint16(len(libs))) // numLibraries
	inner.Write(two[:])
	for i, name := range libs {
		nameEbcdic, err := ebcdic.CCSID37.Encode(name)
		if err != nil {
			t.Fatalf("encode %q: %v", name, err)
		}
		if i == 0 {
			inner.WriteByte(0xC3) // 'C' indicator
		} else {
			inner.WriteByte(0xD3) // 'L' indicator
		}
		binary.BigEndian.PutUint16(two[:], uint16(len(nameEbcdic)))
		inner.Write(two[:])
		inner.Write(nameEbcdic)
	}

	// Locate the CP 0x3813 frame inside the sent bytes. DSS header
	// is 20 bytes, then the SQL-request template is 20 bytes
	// (matches BuildDBRequest layout), then per-param the layout is
	// LL(4) + CP(2) + payload. With one param, the param starts at
	// offset 40.
	const paramOffset = 40
	if len(got) < paramOffset+6 {
		t.Fatalf("sent %d bytes too short for a param-bearing frame", len(got))
	}
	gotCP := binary.BigEndian.Uint16(got[paramOffset+4 : paramOffset+6])
	if gotCP != 0x3813 {
		t.Fatalf("expected CP 0x3813 at offset %d, got 0x%04X", paramOffset, gotCP)
	}
	gotLL := binary.BigEndian.Uint32(got[paramOffset : paramOffset+4])
	wantLL := uint32(6 + inner.Len())
	if gotLL != wantLL {
		t.Errorf("CP 0x3813 LL = %d, want %d", gotLL, wantLL)
	}
	gotInner := got[paramOffset+6:]
	if !bytes.Equal(gotInner, inner.Bytes()) {
		t.Errorf("CP 0x3813 inner payload mismatch")
		t.Errorf("  got:  %x", gotInner)
		t.Errorf("  want: %x", inner.Bytes())
	}

	// Also verify the ServerID was flipped to NDB (0xE005) -- a
	// regression here means SQL service gets the frame.
	gotSrv := ServerID(binary.BigEndian.Uint16(got[6:8]))
	if gotSrv != ndbServerID {
		t.Errorf("ServerID = 0x%04X, want 0x%04X (NDB)", gotSrv, ndbServerID)
	}
}

// TestNDBAddLibraryListEmptyRejected confirms passing zero
// libraries returns a clear error rather than emitting an empty
// CP 0x3813 (which the server would reject).
func TestNDBAddLibraryListEmptyRejected(t *testing.T) {
	conn := newFakeConn() // no reply needed; we fail before writing
	err := NDBAddLibraryListMulti(conn, nil, 2)
	if err == nil {
		t.Fatalf("expected error for empty library list, got nil")
	}
}
