package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/complacentsee/go-db2i/ebcdic"
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
// conn for a single library. Thin wrapper around
// NDBAddLibraryListMulti preserved for callers that only need one
// library; equivalent to NDBAddLibraryListMulti(conn, []string{library},
// correlationID).
func NDBAddLibraryList(conn io.ReadWriter, library string, correlationID uint32) error {
	return NDBAddLibraryListMulti(conn, []string{library}, correlationID)
}

// NDBAddLibraryListMulti sends one NDB-service ADD_LIBRARY_LIST frame
// carrying N libraries. The first library is tagged with indicator
// 'C' (EBCDIC 0xC3, "current SQL schema") and the rest with 'L'
// (EBCDIC 0xD3, "append to back of *LIBL"). This matches JT400's
// JDLibraryList behaviour when the user supplies a comma-separated
// libraries= list without an explicit *LIBL token and the default
// schema is the first item -- the common migration shape.
//
// The CP 0x3813 layout mirrors JT400's
// DBBaseRequestDS.addParameter(int, ConvTable, char[], String[]):
//
//	CCSID(2) + numLibraries(2) +
//	per library: indicator(1) + length(2) + name bytes
//
// JTOpen tolerates errorClass=5 returnCode=1301 ("library not added;
// already in *LIBL or doesn't exist") and treats it as a warning.
// We do the same -- the goal is to flip the SQL service's session
// state, not to actually mutate *LIBL.
func NDBAddLibraryListMulti(conn io.ReadWriter, libraries []string, correlationID uint32) error {
	if len(libraries) == 0 {
		return fmt.Errorf("hostserver: NDBAddLibraryListMulti called with zero libraries")
	}
	if len(libraries) > 0xFFFF {
		return fmt.Errorf("hostserver: too many libraries %d (max 65535)", len(libraries))
	}

	encoded := make([][]byte, len(libraries))
	total := 4 // CCSID(2) + numLibraries(2)
	for i, lib := range libraries {
		libBytes, err := ebcdic.CCSID37.Encode(lib)
		if err != nil {
			return fmt.Errorf("hostserver: encode library name %q: %w", lib, err)
		}
		if len(libBytes) == 0 || len(libBytes) > 0xFFFF {
			return fmt.Errorf("hostserver: library name %q length %d out of range", lib, len(libBytes))
		}
		encoded[i] = libBytes
		total += 3 + len(libBytes) // indicator(1) + length(2) + name
	}

	libParam := make([]byte, total)
	binary.BigEndian.PutUint16(libParam[0:2], 273)                       // CCSID
	binary.BigEndian.PutUint16(libParam[2:4], uint16(len(libraries)))    // numLibraries
	off := 4
	for i, libBytes := range encoded {
		if i == 0 {
			libParam[off] = 0xC3 // EBCDIC 'C' -- current SQL schema
		} else {
			libParam[off] = 0xD3 // EBCDIC 'L' -- append to back of *LIBL
		}
		binary.BigEndian.PutUint16(libParam[off+1:off+3], uint16(len(libBytes)))
		copy(libParam[off+3:], libBytes)
		off += 3 + len(libBytes)
	}

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
	if rep.ErrorClass != 0 && !(rep.ErrorClass == 5 && rep.ReturnCode == 1301) {
		return fmt.Errorf("hostserver: NDB ADD_LIBRARY_LIST errorClass=%d returnCode=%d", rep.ErrorClass, rep.ReturnCode)
	}
	return nil
}
