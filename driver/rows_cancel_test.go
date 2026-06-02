package driver

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/complacentsee/go-db2i/hostserver"
)

// TestStreamingSelectCancelMidIteration is the offline regression for
// issue #30: a multi-batch streaming SELECT must honour ctx
// cancellation while the caller iterates Rows.Next, not just during
// the initial OPEN. QueryContext arms the deadline then tears it down
// via defer cleanup() before the caller ever calls Next, so before the
// fix the continuation FETCH read blocked forever on a hung-but-alive
// server even after the ctx was canceled.
//
// The test stands up a net.Pipe-backed *Conn and a tiny in-process
// "server" goroutine that completes the OPEN handshake (CREATE_RPB ->
// PREPARE_DESCRIBE reply with one INTEGER column -> OPEN_DESCRIBE_FETCH
// reply with ZERO rows but no end-of-data signal, i.e. a non-exhausted
// cursor) and then goes silent on the continuation FETCH -- modelling a
// server that accepted the request but never answers. The first
// Rows.Next must therefore issue a continuation FETCH, block on the
// pipe read, and -- once the ctx is canceled -- return promptly with
// context.Canceled rather than hanging.
func TestStreamingSelectCancelMidIteration(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	defer clientEnd.Close()
	defer serverEnd.Close()

	// Server goroutine: drive the OPEN handshake, then read (and drop)
	// the continuation FETCH without replying. Each reply echoes the
	// request's correlation ID so ReadDBReplyMatching pairs them.
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		// 1) CREATE_RPB -- fire-and-forget, no reply expected.
		if _, err := readRequestFrame(serverEnd); err != nil {
			return
		}
		// 2) PREPARE_DESCRIBE -- reply with one INTEGER column.
		prepHdr, err := readRequestFrame(serverEnd)
		if err != nil {
			return
		}
		if err := writeReply(serverEnd, prepHdr.CorrelationID, oneIntColumnFormatReply()); err != nil {
			return
		}
		// 3) OPEN_DESCRIBE_FETCH -- success (EC=0/RC=0), no row-data CP,
		//    so the cursor opens with zero pending rows and is NOT
		//    exhausted: the first Next() must issue a continuation FETCH.
		openHdr, err := readRequestFrame(serverEnd)
		if err != nil {
			return
		}
		if err := writeReply(serverEnd, openHdr.CorrelationID, nil); err != nil {
			return
		}
		// 4) Continuation FETCH -- read it but never reply. The client
		//    read blocks here until the canceled ctx drives SetDeadline
		//    on the pipe and unblocks it.
		_, _ = readRequestFrame(serverEnd)
		<-serverDone
	}()

	cfg := DefaultConfig()
	conn := &Conn{conn: clientEnd, cfg: &cfg}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cursor, err := hostserver.OpenSelectStatic(clientEnd, "SELECT C FROM T", conn.nextCorrFunc())
	if err != nil {
		t.Fatalf("OpenSelectStatic: %v", err)
	}
	if got := len(cursor.Columns()); got != 1 {
		t.Fatalf("cursor columns = %d, want 1", got)
	}

	rows := &Rows{cursor: cursor, conn: conn, ctx: ctx}

	// Cancel shortly after Next blocks on the continuation FETCH.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		start := time.Now()
		dest := make([]driver.Value, 1)
		err := rows.Next(dest)
		done <- result{err: err, elapsed: time.Since(start)}
	}()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Rows.Next err = %v, want errors.Is(..., context.Canceled)", r.err)
		}
		// Sanity: it returned because of the cancel (~50ms), not some
		// unrelated immediate error. Mostly we care it didn't hang.
		if r.elapsed > 2*time.Second {
			t.Errorf("Rows.Next took %v; expected prompt return after cancel", r.elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Rows.Next did not return after ctx cancel -- streaming SELECT ignored cancellation (issue #30)")
	}
}

// readRequestFrame reads one DSS request frame from r and returns its
// header. Used by the in-test server to step through the OPEN
// handshake.
func readRequestFrame(r io.Reader) (hostserver.Header, error) {
	hdr, _, err := hostserver.ReadFrame(r)
	return hdr, err
}

// writeReply writes a 0x2800 DBReply frame whose 20-byte template is
// all zeros (ErrorClass=0, ReturnCode=0 -> success, non-exhausted) and
// whose body is the supplied CP-param bytes (may be nil for a reply
// that carries no parameters). corr echoes the request being answered.
func writeReply(w io.Writer, corr uint32, body []byte) error {
	payload := make([]byte, 20+len(body))
	copy(payload[20:], body)
	hdr := hostserver.Header{
		ServerID:      hostserver.ServerDatabase,
		CorrelationID: corr,
		ReqRepID:      hostserver.RepDBReply,
	}
	return hostserver.WriteFrame(w, hdr, payload)
}

// oneIntColumnFormatReply builds the CP 0x3812 (super-extended data
// format) body describing a single non-nullable INTEGER column, the
// minimum a PREPARE_DESCRIBE reply needs so OpenSelectStatic parses a
// usable result-set descriptor. Layout per parseSuperExtendedDataFormat:
// 16-byte header (numFields at offset 4) + one 48-byte field record
// (SQL type at offset 2, field length at offset 4).
func oneIntColumnFormatReply() []byte {
	const headerLen = 16
	const perField = 48
	data := make([]byte, headerLen+perField)
	binary.BigEndian.PutUint32(data[4:8], 1) // number of fields
	base := headerLen
	binary.BigEndian.PutUint16(data[base+2:base+4], 496) // INTEGER NN
	binary.BigEndian.PutUint32(data[base+4:base+8], 4)   // field length (4 bytes)
	// scale/precision/ccsid/name all zero -- fine for an INTEGER column.
	return cpParam(0x3812, data)
}

// cpParam encodes one LL/CP/data parameter: LL(4, total incl. header)
// + CP(2) + data. Mirrors the wire layout ParseDBReply walks.
func cpParam(cp uint16, data []byte) []byte {
	out := make([]byte, 6+len(data))
	binary.BigEndian.PutUint32(out[0:4], uint32(6+len(data)))
	binary.BigEndian.PutUint16(out[4:6], cp)
	copy(out[6:], data)
	return out
}
