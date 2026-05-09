package driver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// fakeNetConn satisfies net.Conn with Read/Write backed by a queue
// of pre-recorded reply bytes. Anything else (LocalAddr, deadlines,
// etc.) is a stub -- LOBReader only ever does Read/Write through
// hostserver.RetrieveLOBData on the as-database socket.
type fakeNetConn struct {
	pending []byte
	replies [][]byte
	written bytes.Buffer
	closed  bool
}

func newFakeNetConn(replies ...[]byte) *fakeNetConn {
	return &fakeNetConn{replies: replies}
}

func (c *fakeNetConn) Read(p []byte) (int, error) {
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

func (c *fakeNetConn) Write(p []byte) (int, error)        { return c.written.Write(p) }
func (c *fakeNetConn) Close() error                       { c.closed = true; return nil }
func (c *fakeNetConn) LocalAddr() net.Addr                { return nil }
func (c *fakeNetConn) RemoteAddr() net.Addr               { return nil }
func (c *fakeNetConn) SetDeadline(t time.Time) error      { return nil }
func (c *fakeNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fakeNetConn) SetWriteDeadline(t time.Time) error { return nil }

// buildLOBDataReply assembles a 0x2800 (DBReply) frame whose CP
// payload mimics what RETRIEVE_LOB_DATA returns: CP 0x380F (data:
// CCSID + actualLen + bytes) and CP 0x3810 (current length: SL=4 +
// uint32 BE). corr is the request correlation we're answering.
func buildLOBDataReply(corr uint32, ccsid uint16, totalLen uint32, payload []byte) []byte {
	hdr := hostserver.Header{
		ServerID:       hostserver.ServerDatabase,
		CorrelationID:  corr,
		TemplateLength: 20,
		ReqRepID:       hostserver.RepDBReply,
	}
	tpl := make([]byte, 20)
	// 0x380F payload: CCSID(2) + actualLen(4) + bytes
	dataPayload := make([]byte, 6+len(payload))
	binary.BigEndian.PutUint16(dataPayload[0:2], ccsid)
	binary.BigEndian.PutUint32(dataPayload[2:6], uint32(len(payload)))
	copy(dataPayload[6:], payload)
	// 0x3810 payload: SL(2)=4 + length(4)
	lenPayload := make([]byte, 2+4)
	binary.BigEndian.PutUint16(lenPayload[0:2], 4)
	binary.BigEndian.PutUint32(lenPayload[2:6], totalLen)

	body := append(tpl, lenLLCPData(0x380F, dataPayload)...)
	body = append(body, lenLLCPData(0x3810, lenPayload)...)

	hdr.Length = uint32(20 + len(body))
	var buf bytes.Buffer
	if err := hostserver.WriteFrame(&buf, hdr, body); err != nil {
		panic(fmt.Sprintf("buildLOBDataReply: %v", err))
	}
	return buf.Bytes()
}

func lenLLCPData(cp uint16, data []byte) []byte {
	out := make([]byte, 6+len(data))
	binary.BigEndian.PutUint32(out[0:4], uint32(6+len(data)))
	binary.BigEndian.PutUint16(out[4:6], cp)
	copy(out[6:], data)
	return out
}

func newTestConn(replies ...[]byte) *Conn {
	cfg := DefaultConfig()
	return &Conn{conn: newFakeNetConn(replies...), cfg: &cfg}
}

// TestLOBReaderReadStreamsChunks confirms that LOBReader pulls one
// chunk per RETRIEVE_LOB_DATA round trip and yields the
// concatenation through io.ReadAll.
func TestLOBReaderReadStreamsChunks(t *testing.T) {
	// Two-chunk LOB of 16 + 16 bytes; 32 total.
	chunk1 := []byte("AAAAAAAAAAAAAAAA")
	chunk2 := []byte("BBBBBBBBBBBBBBBB")
	// LOBReader uses Conn.nextCorr() which atomically adds 100 per
	// call, so the first three corr IDs are 100, 200, 300.
	r1 := buildLOBDataReply(100, 65535, 32, chunk1)
	r2 := buildLOBDataReply(200, 65535, 32, chunk2)
	// After 32 bytes consumed, an empty third reply signals EOF
	// (server returns zero bytes when offset >= total).
	r3 := buildLOBDataReply(300, 65535, 32, nil)

	conn := newTestConn(r1, r2, r3)
	reader := &LOBReader{
		conn:      conn,
		loc:       hostserver.LOBLocator{Handle: 0xDEADBEEF, SQLType: 961, CCSID: 65535},
		colIdx:    0,
		chunkSize: 16, // force per-Read round trip
	}

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := append(append([]byte(nil), chunk1...), chunk2...)
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
	if reader.Length() != 32 {
		t.Errorf("Length = %d, want 32", reader.Length())
	}
}

// TestLOBReaderRespectsChunkBoundaryWithSmallBuffer confirms that
// when the caller's Read buffer is smaller than the chunk we pulled
// from the server, the leftover bytes are buffered for the next
// Read instead of dropped.
func TestLOBReaderRespectsChunkBoundaryWithSmallBuffer(t *testing.T) {
	chunk := []byte("0123456789ABCDEF") // 16 bytes
	r1 := buildLOBDataReply(100, 65535, 16, chunk)
	r2 := buildLOBDataReply(200, 65535, 16, nil)

	conn := newTestConn(r1, r2)
	reader := &LOBReader{
		conn:      conn,
		loc:       hostserver.LOBLocator{Handle: 1, SQLType: 961, CCSID: 65535},
		colIdx:    0,
		chunkSize: 16,
	}

	// Read 4 bytes at a time -- forces buffer-management code path.
	out := make([]byte, 0, 16)
	buf := make([]byte, 4)
	for {
		n, err := reader.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if !bytes.Equal(out, chunk) {
		t.Errorf("got %q, want %q", out, chunk)
	}
}

// TestLOBReaderCloseStopsReads confirms post-Close Reads return an
// error (not silently zero bytes), and that Close is idempotent.
func TestLOBReaderCloseStopsReads(t *testing.T) {
	conn := newTestConn()
	reader := &LOBReader{
		conn:      conn,
		loc:       hostserver.LOBLocator{Handle: 1, SQLType: 961},
		colIdx:    0,
		chunkSize: DefaultLOBChunkSize,
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Idempotent.
	if err := reader.Close(); err != nil {
		t.Fatalf("Close (second): %v", err)
	}
	buf := make([]byte, 4)
	if _, err := reader.Read(buf); err == nil {
		t.Error("Read after Close returned nil error")
	}
}

// TestLOBReaderCCSIDAndSQLTypeAccessors confirm the metadata the
// caller can pull off the reader before/after the first Read.
func TestLOBReaderCCSIDAndSQLTypeAccessors(t *testing.T) {
	reader := &LOBReader{
		loc: hostserver.LOBLocator{
			Handle:    42,
			SQLType:   964, // CLOB
			MaxLength: 1024,
			CCSID:     1208,
		},
	}
	if reader.CCSID() != 1208 {
		t.Errorf("CCSID = %d, want 1208", reader.CCSID())
	}
	if reader.SQLType() != 964 {
		t.Errorf("SQLType = %d, want 964", reader.SQLType())
	}
	if reader.Length() != 0 {
		t.Errorf("Length = %d, want 0 (pre-Read)", reader.Length())
	}
}
