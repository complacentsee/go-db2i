package driver

import (
	"fmt"
	"io"

	"github.com/complacentsee/go-db2i/hostserver"
)

// LOBValue is the bind-side counterpart of *LOBReader. Pass one to
// db.Exec / Stmt.Exec / Stmt.Query when binding a BLOB / CLOB /
// DBCLOB column and you want the driver to upload the bytes via
// WRITE_LOB_DATA frames rather than inlining a small []byte / string.
//
// Two modes:
//
//	// Materialised — driver ships Bytes in a single WRITE_LOB_DATA
//	// frame. Equivalent to passing []byte directly except that
//	// LOBValue makes the LOB intent explicit at the call site.
//	db.Exec("INSERT INTO t (id, b) VALUES (?, ?)", 1,
//	    &db2i.LOBValue{Bytes: payload})
//
//	// Streamed — driver chunks the Reader into LOBStreamChunkSize
//	// (32 KiB by default) frames at advancing offsets. Length is
//	// MANDATORY because the IBM i host server allocates a locator
//	// per-prepared-statement and needs the total size declared at
//	// PREPARE_DESCRIBE; the driver passes Length to the server
//	// implicitly via the LOB column's declared CCSID/MaxSize.
//	f, _ := os.Open("video.mp4")
//	stat, _ := f.Stat()
//	db.Exec("INSERT INTO t (id, b) VALUES (?, ?)", 1,
//	    &db2i.LOBValue{Reader: f, Length: stat.Size()})
//
// Bytes and Reader are mutually exclusive. If both are set Bytes
// wins (the streamed path is opt-in).
//
// CCSID semantics:
//
//   - For BLOB columns (SQL types 960/961) the driver ships the
//     bytes verbatim, regardless of CCSID.
//   - For CLOB columns (SQL types 964/965) Bytes/Reader content is
//     treated as already encoded in the column's declared CCSID
//     (the server tags it 0xFFFF on the wire, matching JT400's
//     pattern). Pass a Go string instead if you want the driver to
//     transcode for you via internal/ebcdic.
//   - For DBCLOB columns (968/969) Bytes/Reader content must be
//     UTF-16 BE (a.k.a. UCS-2 BE); the driver halves the byte count
//     for the requested-size CP per JT400's graphic-LOB convention.
//     Go strings are also accepted -- the driver runs them through
//     unicode/utf16 to produce the wire bytes.
//
// LOBValue itself doesn't track byte counts as it streams; if Reader
// returns fewer than Length bytes the driver returns an error rather
// than guessing -- the host server has already pre-allocated buffer
// for the declared length and a short upload would leak that buffer
// across the rest of the connection's life.
type LOBValue struct {
	// Bytes is the materialised LOB content. Mutually exclusive
	// with Reader; if both are non-nil Bytes wins.
	Bytes []byte

	// Reader is the streamed source. Length must be set when
	// Reader is non-nil. The driver Reads exactly Length bytes
	// across one or more WRITE_LOB_DATA frames; supplying a
	// shorter or longer stream is an error.
	Reader io.Reader

	// Length is the total byte count for the streamed path. Unused
	// when Bytes is set (the byte count comes from len(Bytes)).
	Length int64
}

// LOBLength reports the byte count the driver will upload. Implements
// the hostserver.LOBStream interface so the bind dispatcher can
// stream this value across multiple frames.
func (v *LOBValue) LOBLength() int64 {
	if v == nil {
		return 0
	}
	if v.Bytes != nil {
		return int64(len(v.Bytes))
	}
	return v.Length
}

// LOBNextChunk pulls bytes from Reader for the bind dispatcher. The
// hostserver layer invokes it repeatedly with shrinking buffers
// until LOBLength bytes have been read or io.EOF is returned.
//
// For the Bytes path this method is never called -- the dispatcher
// detects len(Bytes)>0 and routes through hostserver.WriteLOBData
// directly.
func (v *LOBValue) LOBNextChunk(buf []byte) (int, error) {
	if v == nil || v.Reader == nil {
		return 0, io.EOF
	}
	return v.Reader.Read(buf)
}

// resolveLOBValue picks the right hostserver-level value for a
// *LOBValue. The Bytes path returns the byte slice directly; the
// streamed path returns the *LOBValue (which satisfies
// hostserver.LOBStream). Callers who want bytes irrespective of
// the value's mode should use ToBytes.
//
// Currently used at bind time so bindArgsToPreparedParams can
// produce a value that the hostserver LOB dispatcher recognises.
func resolveLOBValue(v *LOBValue) (any, error) {
	if v == nil {
		return nil, nil
	}
	if v.Bytes != nil {
		return v.Bytes, nil
	}
	if v.Reader == nil {
		return nil, fmt.Errorf("db2i.LOBValue: neither Bytes nor Reader is set")
	}
	if v.Length < 0 {
		return nil, fmt.Errorf("db2i.LOBValue: Length must be non-negative when Reader is set, got %d", v.Length)
	}
	return hostserver.LOBStream(v), nil
}
