package driver

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// DefaultLOBChunkSize is the per-`Read` round-trip size LOBReader
// requests from the server when the caller's buffer is larger.
// 32 KB matches the default block-fetch buffer used elsewhere in
// the driver and is well below the IBM i wire-format int32 cap.
const DefaultLOBChunkSize = 32 * 1024

// LOBReader streams a BLOB / CLOB / DBCLOB column from the server
// via successive RETRIEVE_LOB_DATA round trips. Returned from
// Rows.Next when the connection was opened with the DSN option
// `?lob=stream` -- otherwise the driver materialises the full LOB
// into []byte (BLOB) or string (CLOB / DBCLOB) at Scan time, the
// historical behaviour.
//
// Lifecycle:
//
//	rows, err := db.Query("SELECT B FROM T WHERE ID = ?", id)
//	for rows.Next() {
//	    var r *gojtopen.LOBReader
//	    rows.Scan(&r)
//	    defer r.Close()
//	    io.Copy(dst, r)
//	}
//
// The locator backing the reader stays valid only while the
// producing cursor is open. Reading after Rows.Next has advanced
// to a later row is undefined; reading after Rows.Close returns an
// error from the server. Callers that need to keep LOB content
// past the cursor lifetime must copy the bytes through io.ReadAll
// (or similar) before advancing.
//
// CCSID semantics for character LOBs:
//
//	BLOB    (SQLType 960/961) -- bytes are binary regardless of CCSID tag
//	CLOB    (SQLType 964/965) -- bytes are characters in CCSID:
//	                                65535  raw passthrough
//	                                1208   UTF-8 (no transcode)
//	                                else   EBCDIC (caller transcodes)
//	DBCLOB  (SQLType 968/969) -- bytes are 16-bit codepoints (UCS-2 BE / UTF-16 BE)
//
// The LOBReader emits the bytes the server transmitted; transcoding
// is the caller's responsibility (see ebcdic.CCSID37.Decode for
// EBCDIC, encoding/utf16 for DBCLOB).
type LOBReader struct {
	conn      *Conn
	loc       hostserver.LOBLocator
	colIdx    int
	offset    int64 // bytes already returned to the caller
	totalLen  int64 // server-reported total LOB length; 0 until first Read
	chunkSize int
	pending   []byte // bytes pulled from server but not yet consumed
	closed    bool
	sticky    error // once non-nil, all further Read calls return this
}

// CCSID is the column's CCSID as reported in the SELECT result-set
// metadata. 65535 means "binary / FOR BIT DATA"; other values
// describe how to interpret the bytes for CLOB / DBCLOB.
func (r *LOBReader) CCSID() uint16 { return r.loc.CCSID }

// SQLType is the IBM i SQL type code for the LOB column (960/961
// = BLOB, 964/965 = CLOB, 968/969 = DBCLOB; the even/odd parity
// distinguishes NN from nullable).
func (r *LOBReader) SQLType() uint16 { return r.loc.SQLType }

// Length returns the total LOB size in bytes as reported by the
// server. Returns 0 until the first Read has populated it (the
// server returns the current length as part of the data reply, so
// no extra round trip is required -- but we don't issue any I/O
// from the getter).
func (r *LOBReader) Length() int64 { return r.totalLen }

// Read pulls bytes from the LOB into p. Implements io.Reader.
//
// On the first call (or any call after the local buffer drains)
// Read issues one RETRIEVE_LOB_DATA round trip, asking for up to
// chunkSize bytes (32 KB by default; bounded by len(p) so a small
// caller buffer doesn't waste a 32 KB pull). Any bytes the server
// returns past len(p) are buffered for the next Read call so we
// don't drop data on the floor.
//
// Returns io.EOF once the server-reported total length is reached.
// Connection-level errors are wrapped through Conn.classifyConnErr
// so the pool retires the conn on a dead socket.
func (r *LOBReader) Read(p []byte) (int, error) {
	if r.sticky != nil {
		return 0, r.sticky
	}
	if r.closed {
		return 0, fmt.Errorf("gojtopen: LOBReader.Read after Close")
	}
	if len(p) == 0 {
		return 0, nil
	}

	// Serve from buffer first.
	if len(r.pending) > 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}

	// Already at known end?
	if r.totalLen > 0 && r.offset >= r.totalLen {
		return 0, io.EOF
	}

	// Pull a fresh chunk. Cap the request at chunkSize so we
	// don't ask the server for more than we'd buffer locally on
	// callers that pass huge p slices.
	chunk := r.chunkSize
	if chunk <= 0 {
		chunk = DefaultLOBChunkSize
	}
	requestSize := int64(chunk)
	// If the LOB length is known and we're near the end, narrow
	// the request so the server doesn't need to scan past EOF.
	if r.totalLen > 0 {
		if remaining := r.totalLen - r.offset; remaining < requestSize {
			requestSize = remaining
		}
	}
	if requestSize <= 0 {
		return 0, io.EOF
	}

	// Wire offset/size are in characters for graphic (DBCLOB)
	// LOBs, bytes for everything else. The reader tracks bytes
	// internally so the EOF math doesn't have to know about
	// graphic-ness; we halve at the wire boundary instead. Both
	// values must stay 2-byte aligned for graphic so the server
	// doesn't return half-codepoints.
	wireOffset := r.offset
	wireSize := requestSize
	if r.loc.SQLType == 968 || r.loc.SQLType == 969 {
		if wireOffset%2 != 0 {
			r.sticky = fmt.Errorf("gojtopen: LOBReader: graphic LOB offset %d not 2-byte aligned", wireOffset)
			return 0, r.sticky
		}
		wireOffset /= 2
		// Round wireSize down to even bytes -- requesting an
		// odd byte count from a graphic LOB would split a
		// codepoint at the seam.
		if wireSize%2 != 0 {
			wireSize--
			if wireSize <= 0 {
				return 0, io.EOF
			}
		}
		wireSize /= 2
	}
	data, err := hostserver.RetrieveLOBData(
		r.conn.conn,
		r.loc.Handle,
		wireOffset,
		wireSize,
		r.colIdx,
		r.conn.nextCorr(),
	)
	if err != nil {
		r.sticky = r.conn.classifyConnErr(err)
		return 0, r.sticky
	}
	if r.conn.log != nil {
		r.conn.log.LogAttrs(context.Background(), slog.LevelDebug, "gojtopen: RETRIEVE_LOB_DATA",
			slog.Uint64("handle", uint64(r.loc.Handle)),
			slog.Int("col", r.colIdx),
			slog.Int64("offset", r.offset),
			slog.Int64("requested", requestSize),
			slog.Int("returned", len(data.Bytes)),
		)
	}

	// Capture the total length from the first reply that carries it.
	// The server reports CurrentLength in characters for graphic
	// LOBs (DBCLOB / SQL types 968/969); we track it in bytes so
	// the chunked-Read EOF math (`offset >= totalLen`) doesn't bail
	// halfway through a UTF-16 LOB.
	if r.totalLen == 0 && data.CurrentLength > 0 {
		r.totalLen = int64(data.CurrentLength)
		if r.loc.SQLType == 968 || r.loc.SQLType == 969 {
			r.totalLen *= 2
		}
	}

	if len(data.Bytes) == 0 {
		// Server signalled end-of-data with an empty payload.
		return 0, io.EOF
	}

	r.offset += int64(len(data.Bytes))
	n := copy(p, data.Bytes)
	if n < len(data.Bytes) {
		// Buffer the leftover for the next Read.
		r.pending = append(r.pending[:0], data.Bytes[n:]...)
	}
	// Do NOT return io.EOF together with n>0 here -- the caller
	// might pass a buffer exactly large enough for one chunk and
	// we'd suppress data on subsequent reads if pending isn't
	// drained yet. The next Read will pick up at offset and
	// return EOF once the server's chunk-zero reply arrives or
	// totalLen is reached.
	return n, nil
}

// Close marks the reader inactive. It does NOT issue any wire
// frame -- the server-side locator is dropped automatically when
// the producing cursor's RPB DELETE runs (Rows.Close). Subsequent
// Read calls return an error.
//
// Idempotent.
func (r *LOBReader) Close() error {
	r.closed = true
	r.pending = nil
	return nil
}
