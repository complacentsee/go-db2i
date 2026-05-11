package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// NDB (Native Database) request IDs that share the as-database TCP
// connection with the SQL service. PUB400 V7R5 routes by the
// ServerID field in the DSS header, so all three (SQL=0xE004,
// NDB=0xE005, ROI=0xE006) ride the same socket on port 8471.
//
// Without an NDB ADD_LIBRARY_LIST somewhere between
// SET_SQL_ATTRIBUTES and the first PREPARE_DESCRIBE, PUB400
// V7R5 returns SQL -401 ("operands not compatible") for any
// PREPARE on the SQL service. Empirically, even adding the
// user's home library that's *already* on the job's library
// list is enough to flip this off; the NDB call is treated as a
// session-init handshake more than an actual library mutation.
const (
	ReqDBNDBAddLibraryList uint16 = 0x180C
)

// NDBAddLibraryList sends one NDB-service ADD_LIBRARY_LIST frame to
// conn. library is one EBCDIC library name (10 chars max,
// space-padded by JTOpen but the NDB protocol just takes the
// declared length).
//
// JTOpen hardcodes the indicator byte to ASCII 'C' (0x43),
// converted to EBCDIC 0xC3, with no public name for the meaning.
// We pass the same byte; if any future caller needs a different
// indicator (e.g. 'F' for front-of-list, 'B' for back),
// generalise this helper then.
func NDBAddLibraryList(conn io.ReadWriter, library string, correlationID uint32) error {
	libBytes, err := ebcdic.CCSID37.Encode(library)
	if err != nil {
		return fmt.Errorf("hostserver: encode library name: %w", err)
	}
	if len(libBytes) == 0 || len(libBytes) > 0xFFFF {
		return fmt.Errorf("hostserver: library name length %d out of range", len(libBytes))
	}

	// Library-list param layout (mirrors DBBaseRequestDS
	// addParameter(int, ConvTable, char[], String[])):
	//   CCSID(2) + numLibraries(2) +
	//   per library: indicator(1) + length(2) + name bytes
	libParam := make([]byte, 4+1+2+len(libBytes))
	binary.BigEndian.PutUint16(libParam[0:2], 273) // CCSID
	binary.BigEndian.PutUint16(libParam[2:4], 1)   // numLibraries = 1
	libParam[4] = 0xC3                             // EBCDIC 'C' indicator
	binary.BigEndian.PutUint16(libParam[5:7], uint16(len(libBytes)))
	copy(libParam[7:], libBytes)

	tpl := DBRequestTemplate{
		ORSBitmap: ORSReturnData | ORSDataCompression, // return data + RLE
	}
	hdr, payload, err := BuildDBRequest(ReqDBNDBAddLibraryList, tpl, []DBParam{
		{CodePoint: 0x3813, Data: libParam},
	})
	if err != nil {
		return fmt.Errorf("hostserver: build NDB ADD_LIBRARY_LIST: %w", err)
	}
	// Override ServerID to NDB (BuildDBRequest defaults to SQL).
	hdr.ServerID = 0xE005 // as-database NDB sub-service
	hdr.CorrelationID = correlationID

	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send NDB ADD_LIBRARY_LIST: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, correlationID, 8)
	if err != nil {
		return fmt.Errorf("hostserver: read NDB ADD_LIBRARY_LIST reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return fmt.Errorf("hostserver: NDB reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return fmt.Errorf("hostserver: parse NDB reply: %w", err)
	}
	// JTOpen tolerates errorClass=5 returnCode=1301 ("library not
	// added; already in *LIBL or doesn't exist") and treats it as
	// a warning. We do the same -- the goal is to flip the SQL
	// service's session state, not to actually mutate *LIBL.
	if rep.ErrorClass != 0 && !(rep.ErrorClass == 5 && rep.ReturnCode == 1301) {
		return fmt.Errorf("hostserver: NDB ADD_LIBRARY_LIST errorClass=%d returnCode=%d", rep.ErrorClass, rep.ReturnCode)
	}
	return nil
}
