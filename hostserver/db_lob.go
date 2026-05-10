package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"unicode/utf16"
)

// RETRIEVE_LOB_DATA function ID. Per JT400's
// DBSQLRequestDS.FUNCTIONID_RETRIEVE_LOB_DATA -- given a LOB
// locator handle plus an offset/length, asks the server to return
// the bytes from that range. Used to materialise BLOB / CLOB /
// DBCLOB columns whose values arrived as locators in the SELECT
// result row instead of inlined.
const ReqDBSQLRetrieveLOBData uint16 = 0x1816

// WRITE_LOB_DATA function ID. Per JT400's
// DBSQLRequestDS.FUNCTIONID_WRITE_LOB_DATA -- uploads bytes for a
// LOB locator that the server allocated during PREPARE_DESCRIBE.
// Each frame writes a slice of the LOB at the given offset; the
// truncate indicator on the last frame finalises the value before
// EXECUTE references the locator from CP 0x381F.
const ReqDBSQLWriteLOBData uint16 = 0x1817

// LOB-related code points (parameter blocks).
//
// Request side (read):
//
//	0x3818 LOB locator handle              (uint32 BE)
//	0x3819 Requested size (bytes)          (uint32 BE)
//	0x381A Start offset (bytes)            (uint32 BE)
//	0x381B Compression indicator           (byte: 0xF0 off / 0xF1 on)
//	0x3821 Return current length indicator (byte: 0xF1 yes)
//	0x3828 Column index (optional)         (uint32 BE)
//
// Request side (write — additional):
//
//	0x3822 LOB Truncation Indicator        (byte: 0xF0 truncate / 0xF1 don't)
//	0x381D LOB Data (write)                (CCSID(2)+Len(4)+payload; CCSID always 0xFFFF)
//
// Reply side:
//
//	0x380F LOB data (read)                 (CCSID(2)+actualLen(4)+payload bytes)
//	0x3810 Current LOB length              (SL(2)+(uint32 BE if SL=4, uint64 BE if SL=8))
//
// 0x381D vs 0x380F is the only place the read and write CPs differ;
// the byte layout is identical (CCSID(2)+Len(4)+bytes), so the same
// helper structure encodes both sides. JT400 always tags 0x381D
// content as CCSID 0xFFFF (binary) and pre-encodes CLOB strings to
// the column's declared CCSID before placing the bytes.
const (
	cpDBLOBLocatorHandle     uint16 = 0x3818
	cpDBRequestedSize        uint16 = 0x3819
	cpDBStartOffset          uint16 = 0x381A
	cpDBCompressionIndicator uint16 = 0x381B
	cpDBLOBTruncation        uint16 = 0x3822 // write side: 0xF0 truncate / 0xF1 don't
	cpDBLOBDataWrite         uint16 = 0x381D // write side: CCSID(2)+Len(4)+bytes
	cpDBReturnCurrentLen     uint16 = 0x3821
	cpDBLOBColumnIndex       uint16 = 0x3828
	cpDBLOBData              uint16 = 0x380F // read side: same shape as 0x381D
	cpDBCurrentLOBLength     uint16 = 0x3810
)

// LOBLocator is a placeholder value emitted by the result-data
// decoder when a SELECT row carries a BLOB / CLOB / DBCLOB column
// as a server-side locator instead of inlining the bytes. The
// driver layer uses Handle + the connection's RetrieveLOBData to
// materialise the actual content when the caller scans the column;
// callers using the hostserver layer directly issue their own
// RetrieveLOBData calls with this Handle.
//
// SQLType identifies the locator flavour:
//
//	960 / 961   BLOB locator   (CCSID 65535 -- binary)
//	964 / 965   CLOB locator   (CCSID is the column's char encoding)
//	968 / 969   DBCLOB locator (CCSID 13488 -- UCS-2 BE, double-byte)
//
// MaxLength is the column's declared maximum LOB size (in bytes for
// BLOB, characters for CLOB, double-bytes for DBCLOB). The actual
// content length is whatever RetrieveLOBData returns in
// LOBData.CurrentLength.
type LOBLocator struct {
	Handle    uint32
	SQLType   uint16
	MaxLength uint32
	CCSID     uint16
}

// LOBData is the result of a successful RETRIEVE_LOB_DATA round
// trip. Bytes carries the actual LOB payload returned by the server
// (truncated to the requested size); CurrentLength is the LOB's
// total length on the server, useful for callers that want to
// stream more chunks.
//
// CCSID is the encoding tag the server stamped on the data:
//   65535       BLOB / FOR BIT DATA -- bytes are binary
//   1208        CLOB tagged as UTF-8
//   13488       DBCLOB tagged as UCS-2 BE
//   else        CLOB tagged with an SBCS / EBCDIC table; caller
//               transcodes via ebcdic.* converters as appropriate.
//
// Bytes is always returned verbatim from the wire -- no transcode.
// Callers who want a Go string for a CLOB need to decode it
// themselves once they've consumed all chunks; the driver layer
// does that transparently when a BLOB / CLOB column is scanned via
// database/sql.
type LOBData struct {
	CCSID         uint16
	CurrentLength uint64
	Bytes         []byte
}

// RetrieveLOBData runs a single RETRIEVE_LOB_DATA round trip:
// asks the server for `size` bytes starting at `offset` of the
// locator identified by `handle`. nextCorrelation is the request
// correlation ID; one frame consumed.
//
// Callers needing more than one chunk re-call this with an updated
// offset; the locator stays valid for the life of the cursor /
// statement that produced it. The locator is automatically dropped
// when the producing statement is freed (Cursor.Close / RPB DELETE).
//
// Empty result: when the requested range is beyond the LOB or the
// LOB itself is empty, Bytes is nil and CurrentLength reflects
// whatever the server reported (commonly 0).
func RetrieveLOBData(conn io.ReadWriter, handle uint32, offset, size int64, columnIndex int, nextCorrelation uint32) (*LOBData, error) {
	if offset < 0 || size < 0 {
		return nil, fmt.Errorf("hostserver: RetrieveLOBData: offset/size must be >= 0 (got %d, %d)", offset, size)
	}
	// Server currently caps at int32 for both fields; saturate.
	if offset > 0x7FFFFFFF {
		offset = 0x7FFFFFFF
	}
	if size > 0x7FFFFFFF {
		size = 0x7FFFFFFF
	}
	tpl := DBRequestTemplate{
		// ReturnData + ResultData. The RLE-on-reply bit
		// (0x00040000 = ORS_BITMAP_REPLY_RLE_COMPRESSION per JT400)
		// is intentionally NOT set: enabling it on V7R6 caused the
		// server to wrap the entire reply in CP 0x3832 (the
		// whole-datastream RLE wrapper, distinct from per-CP RLE),
		// which our parseDBReply doesn't unwrap. The decompressor
		// (decompressRLE1) and parseLOBReply's per-CP RLE handling
		// landed in M7-7 as the foundation for whole-datastream
		// RLE; turning the bit back on requires the CP 0x3832
		// reader to land first. See docs/lob-known-gaps.md §4 for
		// the residual work.
		//
		// SQLCA omitted -- locator retrieval is straight wire data,
		// no statement-level error machinery to surface.
		ORSBitmap:                 ORSReturnData | ORSResultData,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 0,
	}
	params := []DBParam{
		dbParamUint32(cpDBLOBLocatorHandle, handle),
		dbParamUint32(cpDBRequestedSize, uint32(size)),
		dbParamUint32(cpDBStartOffset, uint32(offset)),
		DBParamByte(cpDBCompressionIndicator, 0xF0), // off
		DBParamByte(cpDBReturnCurrentLen, 0xF1),     // yes, please
	}
	if columnIndex >= 0 {
		params = append(params, dbParamUint32(cpDBLOBColumnIndex, uint32(columnIndex)))
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLRetrieveLOBData, tpl, params)
	if err != nil {
		return nil, fmt.Errorf("hostserver: build RETRIEVE_LOB_DATA: %w", err)
	}
	hdr.CorrelationID = nextCorrelation
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send RETRIEVE_LOB_DATA: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorrelation, 4)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read RETRIEVE_LOB_DATA reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: RETRIEVE_LOB_DATA reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse RETRIEVE_LOB_DATA reply: %w", err)
	}
	if dbErr := makeDb2Error(rep, "RETRIEVE_LOB_DATA"); dbErr != nil {
		return nil, dbErr
	}
	return parseLOBReply(rep)
}

// parseLOBReply walks the CP list of a RETRIEVE_LOB_DATA reply and
// extracts the LOB data + current length. Either CP may be absent:
// no current-length CP just means the server didn't include it; an
// absent LOB-data CP means an empty result.
func parseLOBReply(rep *DBReply) (*LOBData, error) {
	out := &LOBData{}
	be := binary.BigEndian
	for _, p := range rep.Params {
		switch p.CodePoint {
		case cpDBLOBData:
			// Layout: CCSID(2) + actualLen(4) + payload bytes.
			//
			// actualLen is the *uncompressed* byte count.  When the
			// server applies RLE-1 compression to the payload (which
			// it may or may not do -- a request-side RLE bit asks
			// for it but the server skips compression on
			// incompressible data) the on-wire payload is shorter
			// than actualLen.  We trigger decompression iff
			// `actualLen != len(payload)`, which is reliable because
			// RLE-1 with mismatched lengths can never coincide with
			// raw data of the same length (raw payload by definition
			// has actualLen == len(payload)).
			//
			// For BLOB / CLOB, actualLen equals the uncompressed
			// payload byte count.  For graphic LOBs (DBCLOB),
			// actualLen is reported in *characters* and the wire
			// uncompressed payload is twice that; JT400 mirrors
			// this by calling DBLobData.adjustForGraphic to double
			// actualLen after retrieve.  We sidestep the
			// graphic/non-graphic ambiguity by using the wire byte
			// count for the equality check (graphic payloads are
			// even-length and len(payload) == 2*actualLen, so the
			// "compressed" trigger is simply `len(payload) <
			// actualLen` rather than `!= actualLen`).
			if len(p.Data) < 6 {
				return nil, fmt.Errorf("hostserver: LOB data CP too short: %d bytes", len(p.Data))
			}
			out.CCSID = be.Uint16(p.Data[0:2])
			actualLen := int(be.Uint32(p.Data[2:6]))
			payload := p.Data[6:]
			// Compute the byte count we'd expect for an uncompressed
			// payload. For BLOB / CLOB the actualLen field is bytes;
			// for graphic LOBs (CCSID 13488 UCS-2 BE or 1200 UTF-16
			// BE) it is the character count and the uncompressed
			// payload is twice as many bytes.
			expectedRaw := actualLen
			if out.CCSID == 13488 || out.CCSID == 1200 {
				expectedRaw = actualLen * 2
			}
			switch {
			case len(payload) == expectedRaw:
				// Raw passthrough. Copy so the caller can hold the
				// bytes after the underlying reply buffer is
				// recycled.
				out.Bytes = append([]byte(nil), payload...)
			case len(payload) < expectedRaw:
				// RLE-1 compressed: decompress to expectedRaw bytes.
				// The server skips compression on incompressible
				// data, so the only way the wire payload is shorter
				// than the declared size is RLE.
				decompressed, err := decompressRLE1(payload, expectedRaw)
				if err != nil {
					return nil, fmt.Errorf("hostserver: decompress LOB payload (actualLen=%d ccsid=%d wire=%d expected=%d): %w",
						actualLen, out.CCSID, len(payload), expectedRaw, err)
				}
				out.Bytes = decompressed
			default:
				// len(payload) > expectedRaw: server delivered more
				// bytes than the actualLen suggests. Treat as raw
				// passthrough; this matches JT400's tolerance
				// (DBLobData uses the parsed length field as
				// authoritative for decompression and otherwise
				// trusts the wire bytes).
				out.Bytes = append([]byte(nil), payload...)
			}
		case cpDBCurrentLOBLength:
			// Layout: SL(2) + value (4 or 8 bytes BE).
			if len(p.Data) < 2 {
				continue
			}
			sl := int(be.Uint16(p.Data[0:2]))
			switch sl {
			case 4:
				if len(p.Data) >= 6 {
					out.CurrentLength = uint64(be.Uint32(p.Data[2:6]))
				}
			case 8:
				if len(p.Data) >= 10 {
					out.CurrentLength = be.Uint64(p.Data[2:10])
				}
			}
		}
	}
	return out, nil
}

// bindLOBParameters walks paramShapes/values alongside the
// server-supplied parameter marker format. For each parameter the
// server declared as a LOB column it:
//
//  1. Resolves the user's value to wire bytes (transcoding strings
//     for CLOB CCSIDs, passing []byte verbatim, or pulling chunks
//     from a LOBStream).
//  2. Issues one or more WRITE_LOB_DATA frames against the
//     server-allocated locator handle.
//  3. Rewrites paramShapes[i] to the locator-bind shape (SQLType
//     from server, FieldLength=4, CCSID from server) and
//     paramValues[i] to the locator handle so the subsequent
//     CHANGE_DESCRIPTOR + EXECUTE encode the 4-byte handle in the
//     SQLDA value slot, matching the JT400 wire pattern documented
//     in docs/lob-bind-wire-protocol.md.
//
// Non-LOB parameters and a nil pmf (statement with no `?` markers
// or an old-server reply that didn't include CP 0x3813) are
// no-ops. nextCorr mints fresh correlation IDs for every WRITE_LOB_DATA
// frame the function emits.
func bindLOBParameters(conn io.ReadWriter, shapes []PreparedParam, values []any, pmf []ParameterMarkerField, nextCorr func() uint32) error {
	if len(pmf) == 0 {
		return nil
	}
	if len(pmf) != len(shapes) {
		return fmt.Errorf("hostserver: parameter marker format declares %d fields but caller supplied %d shapes", len(pmf), len(shapes))
	}
	for i, p := range pmf {
		if !p.IsLOB() {
			continue
		}
		if values[i] == nil {
			// NULL-bind a LOB column: no WRITE_LOB_DATA, leave the
			// indicator block to flag null. Override the shape to
			// the locator shape so CHANGE_DESCRIPTOR / EXECUTE
			// agree with the server.
			shapes[i] = PreparedParam{
				SQLType:     p.SQLType,
				FieldLength: 4,
				CCSID:       p.CCSID,
			}
			continue
		}
		if err := bindOneLOB(conn, p, values, i, nextCorr); err != nil {
			return fmt.Errorf("param %d: %w", i, err)
		}
		shapes[i] = PreparedParam{
			SQLType:     p.SQLType,
			FieldLength: 4,
			CCSID:       p.CCSID,
		}
	}
	return nil
}

// bindOneLOB ships the bytes for a single LOB parameter and
// rewrites values[i] to the locator handle. Split out so the
// per-parameter type switch stays readable.
//
// DBCLOB note: per JT400's JDLobLocator.writeData, the
// `requestedSize` CP carries the *character* count for graphic
// LOBs, not the byte count -- `lengthToUse = graphic_ ? length / 2
// : length`. We mirror that via lobRequestedSize so DBCLOB binds
// don't overstate their size (which silently caused IBM i to
// allocate a buffer twice the needed size and report SQL -302
// "string truncation" downstream of EXECUTE).
func bindOneLOB(conn io.ReadWriter, p ParameterMarkerField, values []any, i int, nextCorr func() uint32) error {
	switch v := values[i].(type) {
	case []byte:
		// BLOB ships verbatim. CLOB ships verbatim too --
		// caller already transcoded. DBCLOB bytes must already
		// be UCS-2 BE / UTF-16 BE; the dispatcher passes
		// len(data)/2 as requestedSize.
		if err := writeLOBBytesChunked(conn, p, v, LOBBlockSize, nextCorr); err != nil {
			return err
		}
	case string:
		// CLOB: pre-encode the Go string to the column's declared
		// CCSID and ship CCSID-tagged 0xFFFF (binary) to the
		// server, mirroring JT400's SQLClobLocator.writeToServer.
		// The real per-CCSID codecs in package ebcdic (CCSID37,
		// CCSID273, ...) handle the 17 code-points where EBCDIC
		// SBCS tables disagree (e.g. "!" is 0x5A in CCSID 37 but
		// 0x4F in CCSID 273); the read path in driver/rows.go
		// must decode through the same codec or the round-trip
		// silently corrupts those characters.
		//
		// DBCLOB string bind encodes the Go runes to UTF-16 BE
		// bytes; surrogate-pair runes therefore consume 4 bytes
		// = 2 wire characters, which matches the IBM i CCSID
		// 1200 / 13488 wire layout.
		if p.SQLType == 960 || p.SQLType == 961 {
			return fmt.Errorf("string value for BLOB locator (SQL type %d); pass []byte instead", p.SQLType)
		}
		var bytes []byte
		switch {
		case p.SQLType == 968 || p.SQLType == 969:
			// DBCLOB: pick encoder by column CCSID.
			//
			//   1200  -> UTF-16 BE; surrogate pairs allowed (the
			//           default DBCLOB encoding negotiated by JT400)
			//   13488 -> strict UCS-2 BE; surrogate pairs forbidden
			//           server-side (SQL-330 "character cannot be
			//           converted"). Substitute non-BMP runes with
			//           U+003F ('?') to mirror JT400's behaviour and
			//           emit a one-shot warn so callers notice.
			//   else  -> assume UTF-16 BE for forward compat; the
			//           server will reject if the column actually
			//           wants something stricter.
			if p.CCSID == 13488 {
				bytes = encodeUCS2BE(v)
			} else {
				bytes = encodeUTF16BE(v)
			}
		case p.CCSID == ccsidBinary:
			bytes = []byte(v)
		case p.CCSID == 1208:
			// UTF-8 column: ship UTF-8 bytes verbatim. Server
			// stores them as-is.
			bytes = []byte(v)
		default:
			conv := ebcdicForCCSID(p.CCSID)
			b, err := conv.Encode(v)
			if err != nil {
				return fmt.Errorf("encode CLOB string for CCSID %d: %w", p.CCSID, err)
			}
			bytes = b
		}
		if err := writeLOBBytesChunked(conn, p, bytes, LOBBlockSize, nextCorr); err != nil {
			return err
		}
	case LOBStream:
		// Streamed bind: chunk the source into LOBStreamChunkSize
		// frames with advancing offset. Truncate=true on the last
		// frame so the server finalises the LOB before EXECUTE.
		//
		// Each chunk's requestedSize honours the same
		// byte-vs-character convention as the single-frame
		// branches; for DBCLOB the chunk boundary must be
		// 2-byte aligned (no half-codepoint at the seam) -- which
		// LOBStreamChunkSize satisfies because it's even, but the
		// caller's Reader has to return even-byte chunks too.
		length := v.LOBLength()
		if length < 0 {
			return fmt.Errorf("LOBStream Length must be non-negative, got %d", length)
		}
		// Empty LOB: still emit one zero-length frame so the
		// server materialises an empty value (otherwise the
		// EXECUTE would carry the locator handle but no content
		// was uploaded). Matches JT400's behaviour for
		// `setBytes(2, new byte[0])`.
		if length == 0 {
			if err := WriteLOBData(conn, p.LOBLocator, 0, 0, nil, true, false, nextCorr()); err != nil {
				return err
			}
			break
		}
		buf := make([]byte, LOBStreamChunkSize)
		var sent int64
		for sent < length {
			want := length - sent
			if want > int64(len(buf)) {
				want = int64(len(buf))
			}
			n, readErr := v.LOBNextChunk(buf[:want])
			if n > 0 {
				truncate := sent+int64(n) >= length
				// Per-chunk offset/size for graphic LOBs is
				// also in characters, not bytes.
				offCharOrByte := lobOffsetCount(p.SQLType, sent)
				reqChunk := lobRequestedSize(p.SQLType, n)
				if err := WriteLOBData(conn, p.LOBLocator, offCharOrByte, reqChunk, buf[:n], truncate, false, nextCorr()); err != nil {
					return fmt.Errorf("WriteLOBData at offset %d: %w", sent, err)
				}
				sent += int64(n)
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				return fmt.Errorf("LOBStream read at offset %d: %w", sent, readErr)
			}
		}
		if sent < length {
			return fmt.Errorf("LOBStream produced %d bytes; declared Length was %d", sent, length)
		}
	default:
		return fmt.Errorf("LOB bind value must be []byte, string, or LOBStream; got %T", v)
	}
	values[i] = p.LOBLocator
	return nil
}

// writeLOBBytesChunked uploads `data` for `p.LOBLocator` as a
// sequence of WRITE_LOB_DATA frames sized at chunkSize (or smaller
// for the trailing fragment), with truncate=true only on the last
// frame. See LOBBlockSize for the rationale and measured impact;
// short version: single-frame multi-megabyte WRITE_LOB_DATA exposes
// a server-side performance cliff that JT400 sidesteps by chunking
// via SQLLocatorBase.readInputStream + LOB_BLOCK_SIZE, so we do too.
//
// For graphic LOBs (DBCLOB, SQL types 968/969) the per-chunk
// requestedSize and startOffset are reported in *characters* via
// lobRequestedSize / lobOffsetCount, mirroring the LOBStream branch.
// The DBCLOB chunk boundary must be 2-byte aligned (no half
// codepoint at the seam); LOBBlockSize is even, and string CLOBs are
// pre-encoded as UTF-16 BE which always produces even-byte input.
func writeLOBBytesChunked(conn io.ReadWriter, p ParameterMarkerField, data []byte, chunkSize int, nextCorr func() uint32) error {
	if chunkSize <= 0 {
		chunkSize = LOBBlockSize
	}
	// Empty LOB: emit one zero-length frame with truncate=true so
	// the server materialises an empty value. Without this, EXECUTE
	// would carry the locator handle but no content. Matches
	// JT400's setBytes(2, new byte[0]) behaviour and the LOBStream
	// length==0 branch above.
	if len(data) == 0 {
		return WriteLOBData(conn, p.LOBLocator, 0, 0, nil, true, false, nextCorr())
	}
	for off := 0; off < len(data); {
		end := off + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[off:end]
		isLast := end == len(data)
		offCharOrByte := lobOffsetCount(p.SQLType, int64(off))
		reqChunk := lobRequestedSize(p.SQLType, len(chunk))
		if err := WriteLOBData(conn, p.LOBLocator, offCharOrByte, reqChunk, chunk, isLast, false, nextCorr()); err != nil {
			return fmt.Errorf("WriteLOBData at offset %d: %w", off, err)
		}
		off = end
	}
	return nil
}

// lobRequestedSize returns the value the WRITE_LOB_DATA "requested
// size" CP (0x3819) should carry for a parameter of the given SQL
// type when the caller has byteCount bytes of payload. BLOB / CLOB
// pass through unchanged; DBCLOB (SQL types 968/969) is graphic on
// the wire and reports the *character* count (byteCount / 2).
func lobRequestedSize(sqlType uint16, byteCount int) uint32 {
	if sqlType == 968 || sqlType == 969 {
		return uint32(byteCount / 2)
	}
	return uint32(byteCount)
}

// lobOffsetCount returns the Start Offset CP value for a graphic
// (DBCLOB) LOB by halving the byte offset, mirroring lobRequestedSize.
// Non-graphic LOBs keep the byte offset.
func lobOffsetCount(sqlType uint16, byteOffset int64) int64 {
	if sqlType == 968 || sqlType == 969 {
		return byteOffset / 2
	}
	return byteOffset
}

// encodeUTF16BE encodes a Go string as UTF-16 big-endian bytes,
// matching IBM i's CCSID 1200 (UTF-16 BE) and 13488 (UCS-2 BE)
// graphic encodings. Runes outside the BMP turn into surrogate
// pairs; the caller's wire char-count math should treat each
// surrogate as one IBM i graphic character (i.e. byteCount / 2,
// which is what lobRequestedSize already does).
func encodeUTF16BE(s string) []byte {
	codes := utf16.Encode([]rune(s))
	out := make([]byte, 2*len(codes))
	for i, c := range codes {
		out[2*i] = byte(c >> 8)
		out[2*i+1] = byte(c)
	}
	return out
}

// NonBMPRuneError is the typed error encodeUCS2BEStrict returns when
// it sees a codepoint outside the Basic Multilingual Plane while
// encoding for a CCSID that forbids surrogate pairs (CCSID 13488 is
// the only such target today). The default substitute path used by
// bindOneLOB does not produce this error; callers that opt into
// strict mode by calling encodeUCS2BEStrict directly receive it so
// they can surface a meaningful message instead of letting the
// server reply with SQL-330.
type NonBMPRuneError struct {
	CCSID uint16 // column CCSID that forbids surrogates (13488)
	Rune  rune   // first offending rune
	Index int    // rune index in the input string
}

func (e *NonBMPRuneError) Error() string {
	return fmt.Sprintf("hostserver: rune U+%04X at index %d is outside the BMP and not representable in CCSID %d (UCS-2 BE)", e.Rune, e.Index, e.CCSID)
}

// ucs2NonBMPWarnOnce rate-limits the substitute warning to one
// emission per process. Per-process granularity (rather than per
// *sql.DB as the M7 plan originally sketched) is intentional: the
// hostserver layer has no stable handle to the database/sql.DB the
// call originated from, and a single warning is enough to alert a
// caller that their data is being silently transcoded -- they can
// switch to encodeUCS2BEStrict via a future opt-in flag if they
// need per-rune visibility.
var ucs2NonBMPWarnOnce sync.Once

// encodeUCS2BE encodes s as strict UCS-2 BE, the on-wire encoding for
// CCSID 13488 DBCLOB columns. Runes outside the BMP (any codepoint
// > 0xFFFF) are substituted with U+003F (`?`), mirroring JT400's
// SQLDBClobLocator.writeToServer behaviour where non-representable
// characters are replaced rather than rejected. The first such
// substitution emits a one-shot slog.Warn so callers notice their
// data is being transcoded.
//
// Surrogate code units (U+D800..U+DFFF) cannot reach this function
// from a Go string literal: Go re-encodes lone surrogates as U+FFFD
// at string-construction time, which is BMP and round-trips
// verbatim.
func encodeUCS2BE(s string) []byte {
	runes := []rune(s)
	out := make([]byte, 2*len(runes))
	for i, r := range runes {
		c := uint16(r)
		if r > 0xFFFF {
			c = 0x003F
			ucs2NonBMPWarnOnce.Do(func() {
				slog.Warn("goJTOpen: non-BMP rune substituted with '?' for CCSID 13488 DBCLOB bind",
					"ccsid", 13488,
					"rune", fmt.Sprintf("U+%04X", r),
					"note", "first occurrence; subsequent substitutions silent",
				)
			})
		}
		out[2*i] = byte(c >> 8)
		out[2*i+1] = byte(c)
	}
	return out
}

// encodeUCS2BEStrict is the opt-in counterpart to encodeUCS2BE. It
// returns a *NonBMPRuneError on the first non-BMP rune and emits no
// bytes; callers who would rather surface a typed error than let the
// substitute path silently corrupt their data route through this
// helper. Today it is only reachable via direct call from package
// tests; a future DSN flag (`?dbclob-strict=true` or similar) will
// wire it into bindOneLOB.
func encodeUCS2BEStrict(s string) ([]byte, error) {
	runes := []rune(s)
	for i, r := range runes {
		if r > 0xFFFF {
			return nil, &NonBMPRuneError{CCSID: 13488, Rune: r, Index: i}
		}
	}
	out := make([]byte, 2*len(runes))
	for i, r := range runes {
		c := uint16(r)
		out[2*i] = byte(c >> 8)
		out[2*i+1] = byte(c)
	}
	return out, nil
}

// rleEscapeByte is the single-byte sentinel JT400's RLE-1 encoder
// uses to introduce both runs and escaped literals. See
// JTOpen/src/main/java/com/ibm/as400/access/JDUtilities.java
// (`private static final byte escape = (byte) 0x1B`). The wire
// format on the reply side is:
//
//	literal byte         -> emit byte, advance 1
//	0x1B 0x1B            -> emit one literal 0x1B, advance 2
//	0x1B value count(4)  -> emit `value` repeated count times,
//	                        advance 6
//
// Count is a 4-byte big-endian int32 (matches JT400's
// BinaryConverter.byteArrayToInt). Servers skip RLE on
// incompressible data; parseLOBReply only invokes this helper when
// the on-wire payload is shorter than the declared `actualLen`.
const rleEscapeByte = 0x1B

// decompressRLE1 expands an RLE-1 encoded byte stream into a
// pre-allocated buffer of `expectedLen` bytes. Mirrors
// JDUtilities.decompress in JT400 byte-for-byte. Returns an error if
// the input is malformed (escape byte at end without a payload, or
// run length that overflows the destination).
//
// `expectedLen` is the uncompressed byte count parseLOBReply derived
// from the CP header (with the graphic-LOB doubling already
// applied). The helper allocates the output buffer at this size and
// returns it sliced to the actual decompressed length so a
// short-but-not-error return surfaces visibly.
func decompressRLE1(src []byte, expectedLen int) ([]byte, error) {
	if expectedLen < 0 {
		return nil, fmt.Errorf("hostserver: decompressRLE1: negative expectedLen %d", expectedLen)
	}
	out := make([]byte, expectedLen)
	j := 0
	for i := 0; i < len(src); {
		if src[i] != rleEscapeByte {
			if j >= len(out) {
				return nil, fmt.Errorf("hostserver: decompressRLE1: output overflow (literal at src[%d], dst index %d, expected %d bytes)", i, j, expectedLen)
			}
			out[j] = src[i]
			i++
			j++
			continue
		}
		// Escape sequence -- need at least one trailing byte.
		if i+1 >= len(src) {
			return nil, fmt.Errorf("hostserver: decompressRLE1: truncated escape at src[%d] (need >= 2 bytes)", i)
		}
		if src[i+1] == rleEscapeByte {
			// Escaped literal 0x1B.
			if j >= len(out) {
				return nil, fmt.Errorf("hostserver: decompressRLE1: output overflow (escaped 0x1B at src[%d], dst index %d, expected %d)", i, j, expectedLen)
			}
			out[j] = rleEscapeByte
			i += 2
			j++
			continue
		}
		// Run: 0x1B value count_BE_int32.
		if i+6 > len(src) {
			return nil, fmt.Errorf("hostserver: decompressRLE1: truncated run header at src[%d] (need 6 bytes, have %d)", i, len(src)-i)
		}
		value := src[i+1]
		count := int(int32(binary.BigEndian.Uint32(src[i+2 : i+6])))
		if count < 0 {
			return nil, fmt.Errorf("hostserver: decompressRLE1: negative run length %d at src[%d]", count, i)
		}
		if j+count > len(out) {
			return nil, fmt.Errorf("hostserver: decompressRLE1: run overflow (run of %d at src[%d], dst index %d, expected %d)", count, i, j, expectedLen)
		}
		for k := 0; k < count; k++ {
			out[j+k] = value
		}
		j += count
		i += 6
	}
	return out[:j], nil
}

// dbParamUint32 builds a 4-byte big-endian uint32 LL/CP/Data block.
// JT400 uses this shape for several integer-valued LOB parameters
// (locator handle, requested size, start offset, column index).
func dbParamUint32(cp uint16, v uint32) DBParam {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return DBParam{CodePoint: cp, Data: b}
}

// LOBStream is the interface a streamed bind value satisfies. The
// driver-layer LOBValue (driver/lob_value.go) implements it for the
// Reader-driven path where the caller cannot hand us the full byte
// array up front.
//
// LOBLength is the total payload size in bytes (for BLOB), in
// characters for CLOB after caller-side CCSID transcoding, or in
// double-bytes for DBCLOB. LOBNextChunk fills buf with the next
// portion of the payload and returns the byte count read; the
// dispatcher invokes it repeatedly until LOBLength bytes have been
// pulled or it returns io.EOF.
type LOBStream interface {
	LOBLength() int64
	LOBNextChunk(buf []byte) (n int, err error)
}

// LOBStreamChunkSize is the per-frame byte size used when uploading
// a LOBStream. 32 KB matches the read-side DefaultLOBChunkSize and
// is well below the wire-format int32 cap.
const LOBStreamChunkSize = 32 * 1024

// LOBBlockSize is the per-frame byte size used when chunking a
// `[]byte` or `string` LOB bind. It mirrors JT400's
// AS400JDBCPreparedStatement.LOB_BLOCK_SIZE = 1_000_000, with the
// JT400 source comment "@pdc Match Native JDBC Driver" -- so this is
// also the block size IBM's own native JDBC driver uses.
//
// Background. Single-frame WRITE_LOB_DATA frames carrying many
// megabytes are exposed to a server-side performance cliff on
// IBM i V7R6: a 64 MiB single-frame BLOB+CLOB INSERT measured
// 195s on a quiet 1-CPU LPAR (~0.33 KB/ms, two orders of magnitude
// below GbE wire speed), and 128 MiB hit our previous 10-minute
// context timeout. Chunking the upload at 1 MB frames -- matching
// JT400's stream-bind path -- avoids the worst of that cliff
// (64 MiB BLOB+CLOB drops to ~115s) without measurably hurting small
// LOBs. It does NOT fully linearise large-LOB throughput though; the
// IBM i host server still exhibits super-linear scaling above ~16
// MiB regardless of chunk strategy, so 128 MiB+ writes remain slow.
// Tracking the residual server-side cost is a separate line of work.
const LOBBlockSize = 1_000_000

// IsLOBSQLType reports whether the IBM i SQL type code refers to a
// LOB locator that needs the WRITE_LOB_DATA / RETRIEVE_LOB_DATA path
// rather than inline value transfer:
//
//	960 / 961  BLOB locator
//	964 / 965  CLOB locator
//	968 / 969  DBCLOB locator
//
// The even/odd parity distinguishes NN (not nullable) from
// nullable; both forms route through the locator path.
func IsLOBSQLType(sqlType uint16) bool {
	switch sqlType {
	case 960, 961, 964, 965, 968, 969:
		return true
	}
	return false
}

// WriteLOBData uploads a slice of LOB content tied to handle. Mirrors
// JT400's JDLobLocator.writeData: emits one WRITE_LOB_DATA (0x1817)
// frame with truncation, locator handle, requested-size, start-offset,
// compression, and the data CP set. Returns nil on success or a
// *Db2Error when the server flags the upload.
//
// Single-frame default: callers with a full byte slice in hand pass
// offset=0, truncate=true, and the entire content; JT400 ships any
// size in one DSS frame (a 64 KiB BLOB went out as a single
// 65 632-byte frame in our captured fixture). Stream callers issue
// multiple WriteLOBData calls with advancing offset and
// truncate=false on every call except the last.
//
// `requestedSize` is the byte count for BLOB / character count for
// CLOB / character count for DBCLOB (caller passing bytes for a
// DBCLOB should pass len(data)/2; JT400 does the same). For BLOB
// and CLOB binds it equals len(data).
//
// `compressed` should be false today; the server handshake doesn't
// negotiate compression and JT400's writeData always passes 0xF0.
//
// nextCorr is the request correlation ID; one frame consumed.
//
// CCSID tag on the data CP is fixed at 0xFFFF (binary) -- callers
// that want the server to transcode their bytes (e.g. UTF-8 source
// for an EBCDIC CLOB column) use WriteLOBDataCCSID instead.
func WriteLOBData(conn io.ReadWriter, handle uint32, offset int64, requestedSize uint32, data []byte, truncate, compressed bool, nextCorr uint32) error {
	return WriteLOBDataCCSID(conn, handle, offset, requestedSize, data, ccsidBinary, truncate, compressed, nextCorr)
}

// WriteLOBDataCCSID is the CCSID-aware variant of WriteLOBData. The
// CP 0x381D payload is tagged with `dataCCSID`; the server uses that
// tag to decide whether/how to transcode the bytes to the column's
// declared CCSID.
//
// Common values:
//
//	0xFFFF  binary -- bytes pass through unchanged. Use this for
//	        BLOB binds and for CLOB binds where the caller has
//	        already encoded the string in the column's CCSID.
//	1208    UTF-8 -- supported on V7R3+ servers. The server
//	        transcodes UTF-8 to the column CCSID; avoids needing
//	        a Go-side codec for every EBCDIC table.
//	273     German EBCDIC SBCS (when calling code already has
//	        bytes in that exact encoding). Same shape applies for
//	        any single-byte EBCDIC CCSID.
//
// The function does NOT validate that the bytes are well-formed in
// dataCCSID; that's the caller's responsibility.
func WriteLOBDataCCSID(conn io.ReadWriter, handle uint32, offset int64, requestedSize uint32, data []byte, dataCCSID uint16, truncate, compressed bool, nextCorr uint32) error {
	if offset < 0 {
		return fmt.Errorf("hostserver: WriteLOBData: offset must be >= 0 (got %d)", offset)
	}
	if offset > 0x7FFFFFFF {
		return fmt.Errorf("hostserver: WriteLOBData: offset %d exceeds 31-bit limit", offset)
	}

	// Compose the CP 0x381D payload: CCSID(2) + Len(4) + payload.
	dataCP := make([]byte, 6+len(data))
	binary.BigEndian.PutUint16(dataCP[0:2], dataCCSID)
	binary.BigEndian.PutUint32(dataCP[2:6], uint32(len(data)))
	copy(dataCP[6:], data)

	truncByte := byte(0xF1) // don't truncate
	if truncate {
		truncByte = 0xF0
	}
	compByte := byte(0xF0)
	if compressed {
		compByte = 0xF1
	}

	tpl := DBRequestTemplate{
		// JT400 uses ReturnData + ResultData on writeData; we follow.
		// SQLCA omitted -- LOB write errors come back via the standard
		// errorClass/returnCode in the reply template, not a SQLCA CP.
		ORSBitmap:                 ORSReturnData | ORSResultData | 0x00040000,
		ReturnORSHandle:           1,
		FillORSHandle:             1,
		BasedOnORSHandle:          0,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 0,
	}
	params := []DBParam{
		DBParamByte(cpDBLOBTruncation, truncByte),
		dbParamUint32(cpDBLOBLocatorHandle, handle),
		dbParamUint32(cpDBRequestedSize, requestedSize),
		dbParamUint32(cpDBStartOffset, uint32(offset)),
		DBParamByte(cpDBCompressionIndicator, compByte),
		{CodePoint: cpDBLOBDataWrite, Data: dataCP},
	}
	hdr, payload, err := BuildDBRequest(ReqDBSQLWriteLOBData, tpl, params)
	if err != nil {
		return fmt.Errorf("hostserver: build WRITE_LOB_DATA: %w", err)
	}
	hdr.CorrelationID = nextCorr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send WRITE_LOB_DATA: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, nextCorr, 4)
	if err != nil {
		return fmt.Errorf("hostserver: read WRITE_LOB_DATA reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return fmt.Errorf("hostserver: WRITE_LOB_DATA reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return fmt.Errorf("hostserver: parse WRITE_LOB_DATA reply: %w", err)
	}
	if dbErr := makeDb2Error(rep, "WRITE_LOB_DATA"); dbErr != nil {
		return dbErr
	}
	return nil
}
