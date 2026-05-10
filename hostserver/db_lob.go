package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
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
		// ReturnData + ResultData + RLE. SQLCA omitted -- locator
		// retrieval is straight wire data, no statement-level error
		// machinery to surface.
		ORSBitmap:                 ORSReturnData | ORSResultData | 0x00040000,
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
			if len(p.Data) < 6 {
				return nil, fmt.Errorf("hostserver: LOB data CP too short: %d bytes", len(p.Data))
			}
			out.CCSID = be.Uint16(p.Data[0:2])
			actualLen := int(be.Uint32(p.Data[2:6]))
			payload := p.Data[6:]
			if actualLen > len(payload) {
				return nil, fmt.Errorf("hostserver: LOB data actual-len %d exceeds payload %d", actualLen, len(payload))
			}
			// Copy so the caller can hold the bytes after the
			// underlying reply buffer is recycled.
			out.Bytes = append([]byte(nil), payload[:actualLen]...)
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
func bindOneLOB(conn io.ReadWriter, p ParameterMarkerField, values []any, i int, nextCorr func() uint32) error {
	switch v := values[i].(type) {
	case []byte:
		// BLOB ships verbatim. CLOB ships verbatim too --
		// caller already transcoded. The single-frame branch
		// matches JT400's default path; even a 1 MiB byte slice
		// goes out as one DSS frame.
		err := WriteLOBData(conn, p.LOBLocator, 0, uint32(len(v)), v, true, false, nextCorr())
		if err != nil {
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
		switch p.SQLType {
		case 960, 961:
			return fmt.Errorf("string value for BLOB locator (SQL type %d); pass []byte instead", p.SQLType)
		}
		var bytes []byte
		switch p.CCSID {
		case ccsidBinary:
			bytes = []byte(v)
		case 1208:
			// UTF-8 column: ship UTF-8 bytes verbatim. Server
			// stores them as-is.
			bytes = []byte(v)
		case 13488:
			return fmt.Errorf("DBCLOB string bind via CCSID 13488 not yet supported (use []byte with UCS-2 BE bytes)")
		default:
			conv := ebcdicForCCSID(p.CCSID)
			b, err := conv.Encode(v)
			if err != nil {
				return fmt.Errorf("encode CLOB string for CCSID %d: %w", p.CCSID, err)
			}
			bytes = b
		}
		err := WriteLOBData(conn, p.LOBLocator, 0, uint32(len(bytes)), bytes, true, false, nextCorr())
		if err != nil {
			return err
		}
	case LOBStream:
		// Streamed bind: chunk the source into LOBStreamChunkSize
		// frames with advancing offset. Truncate=true on the last
		// frame so the server finalises the LOB before EXECUTE.
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
				if err := WriteLOBData(conn, p.LOBLocator, sent, uint32(n), buf[:n], truncate, false, nextCorr()); err != nil {
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
