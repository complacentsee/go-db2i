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

// LOB-related code points (parameter blocks).
//
// Request side:
//
//	0x3818 LOB locator handle              (uint32 BE)
//	0x3819 Requested size (bytes)          (uint32 BE)
//	0x381A Start offset (bytes)            (uint32 BE)
//	0x381B Compression indicator           (byte: 0xF0 off / 0xF1 on)
//	0x3821 Return current length indicator (byte: 0xF1 yes)
//	0x3828 Column index (optional)         (uint32 BE)
//
// Reply side:
//
//	0x380F LOB data: CCSID(2) + actualLen(4) + payload bytes
//	0x3810 Current LOB length: SL(2) + (uint32 BE if SL=4, uint64 BE if SL=8)
const (
	cpDBLOBLocatorHandle    uint16 = 0x3818
	cpDBRequestedSize       uint16 = 0x3819
	cpDBStartOffset         uint16 = 0x381A
	cpDBCompressionIndicator uint16 = 0x381B
	cpDBReturnCurrentLen    uint16 = 0x3821
	cpDBLOBColumnIndex      uint16 = 0x3828
	cpDBLOBData             uint16 = 0x380F
	cpDBCurrentLOBLength    uint16 = 0x3810
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

// dbParamUint32 builds a 4-byte big-endian uint32 LL/CP/Data block.
// JT400 uses this shape for several integer-valued LOB parameters
// (locator handle, requested size, start offset, column index).
func dbParamUint32(cp uint16, v uint32) DBParam {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return DBParam{CodePoint: cp, Data: b}
}
