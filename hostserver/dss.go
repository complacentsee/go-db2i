package hostserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// HeaderLength is the size of the fixed DSS header in bytes.
const HeaderLength = 20

// ServerID identifies which IBM i host server a frame belongs to.
// Wire values are always 0xE0xx -- the high byte is JTOpen's sanity
// marker (see ClientAccessDataStream.construct).
type ServerID uint16

// Service identifiers from JTOpen's AS400Server.getServerId.
const (
	ServerCentral   ServerID = 0xE000
	ServerFile      ServerID = 0xE002
	ServerNetPrint  ServerID = 0xE003
	ServerDatabase  ServerID = 0xE004
	ServerDataQueue ServerID = 0xE007
	ServerCommand   ServerID = 0xE008
	ServerSignon    ServerID = 0xE009
)

// String returns the JTOpen service name for s, e.g. "as-signon".
func (s ServerID) String() string {
	switch s {
	case ServerCentral:
		return "as-central"
	case ServerFile:
		return "as-file"
	case ServerNetPrint:
		return "as-netprt"
	case ServerDatabase:
		return "as-database"
	case ServerDataQueue:
		return "as-dtaq"
	case ServerCommand:
		return "as-rmtcmd"
	case ServerSignon:
		return "as-signon"
	}
	return fmt.Sprintf("ServerID(0x%04X)", uint16(s))
}

// Header is the 20-byte DSS frame header that prefixes every host-
// server datastream. All multi-byte fields are big-endian.
//
//	bytes  field             notes
//	0..3   Length            total frame size including this header
//	4..5   HeaderID          0 for client requests; varies for replies
//	6..7   ServerID          0xE0xx; high byte 0xE0 is a sanity marker
//	8..11  CSInstance        client/server instance
//	12..15 CorrelationID     matches a request to its reply
//	16..17 TemplateLength    bytes of fixed-format template after header
//	18..19 ReqRepID          request or reply identifier
type Header struct {
	Length         uint32
	HeaderID       uint16
	ServerID       ServerID
	CSInstance     uint32
	CorrelationID  uint32
	TemplateLength uint16
	ReqRepID       uint16
}

// MarshalBinary returns a 20-byte big-endian encoding of h.
func (h Header) MarshalBinary() ([]byte, error) {
	out := make([]byte, HeaderLength)
	h.appendTo(out)
	return out, nil
}

// appendTo writes h into b[0:20]. Caller guarantees len(b) >= 20.
func (h Header) appendTo(b []byte) {
	be := binary.BigEndian
	be.PutUint32(b[0:4], h.Length)
	be.PutUint16(b[4:6], h.HeaderID)
	be.PutUint16(b[6:8], uint16(h.ServerID))
	be.PutUint32(b[8:12], h.CSInstance)
	be.PutUint32(b[12:16], h.CorrelationID)
	be.PutUint16(b[16:18], h.TemplateLength)
	be.PutUint16(b[18:20], h.ReqRepID)
}

// UnmarshalBinary parses h from b. b must be at least 20 bytes;
// extra bytes are ignored.
func (h *Header) UnmarshalBinary(b []byte) error {
	if len(b) < HeaderLength {
		return fmt.Errorf("hostserver: DSS header needs %d bytes, got %d", HeaderLength, len(b))
	}
	be := binary.BigEndian
	h.Length = be.Uint32(b[0:4])
	h.HeaderID = be.Uint16(b[4:6])
	h.ServerID = ServerID(be.Uint16(b[6:8]))
	h.CSInstance = be.Uint32(b[8:12])
	h.CorrelationID = be.Uint32(b[12:16])
	h.TemplateLength = be.Uint16(b[16:18])
	h.ReqRepID = be.Uint16(b[18:20])
	if b[6] != 0xE0 {
		return fmt.Errorf("hostserver: bad DSS header sanity byte 0x%02X (expected 0xE0)", b[6])
	}
	return nil
}

// ErrShortFrame is returned when a frame's Length field promises more
// bytes than are available on the wire.
var ErrShortFrame = errors.New("hostserver: short DSS frame")

// ReadFrame reads one complete DSS frame from r and returns the parsed
// header plus the payload bytes that followed it (Length-20 bytes).
//
// It does not retain a reference to r after returning. A frame whose
// Length field is < 20 returns an error without consuming further
// bytes.
func ReadFrame(r io.Reader) (Header, []byte, error) {
	var headerBuf [HeaderLength]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return Header{}, nil, err
	}
	var hdr Header
	if err := hdr.UnmarshalBinary(headerBuf[:]); err != nil {
		return Header{}, nil, err
	}
	if hdr.Length < HeaderLength {
		return hdr, nil, fmt.Errorf("hostserver: header Length %d < HeaderLength %d", hdr.Length, HeaderLength)
	}
	payload := make([]byte, hdr.Length-HeaderLength)
	if len(payload) > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return hdr, nil, ErrShortFrame
			}
			return hdr, nil, err
		}
	}
	return hdr, payload, nil
}

// WriteFrame writes hdr followed by payload to w. hdr.Length is
// overwritten with HeaderLength + len(payload).
func WriteFrame(w io.Writer, hdr Header, payload []byte) error {
	hdr.Length = uint32(HeaderLength + len(payload))
	buf := make([]byte, hdr.Length)
	hdr.appendTo(buf[:HeaderLength])
	copy(buf[HeaderLength:], payload)
	_, err := w.Write(buf)
	return err
}
