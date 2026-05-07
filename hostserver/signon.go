package hostserver

import (
	"encoding/binary"
	"fmt"
)

// Sign-on service request/reply identifiers (DSS Header.ReqRepID).
const (
	ReqExchangeAttributesSignon  uint16 = 0x7003 // sent over ServerSignon
	ReqExchangeAttributesHostCnn uint16 = 0x7103 // sent over the HOSTCNN service
)

// Code points (CP) for parameters carried inside an exchange-attributes
// frame. Layout per parameter is LL (uint32) | CP (uint16) | data.
const (
	cpClientVersion       uint16 = 0x1101 // 4-byte uint32 client version
	cpClientDatastreamLvl uint16 = 0x1102 // 2-byte uint16 datastream level
	cpClientSeed          uint16 = 0x1103 // 8-byte client random seed
)

// ExchangeAttributesRequest builds a SIGNON / HOSTCNN exchange-
// attributes request payload (everything after the 20-byte DSS
// header). Together with the matching [Header] it forms one wire frame.
//
//   - server:        ServerSignon (most common) or the HOSTCNN service.
//   - clientVersion: client protocol version. JTOpen always sends 1.
//   - clientDSLevel: datastream level. 10 selects password level 4
//     (SHA-512 + PBKDF2 + SHA-256 salt); 5 selects password level 2 or 3
//     (SHA-1 token).
//   - clientSeed:    optional 8-byte client random seed used in the
//     password challenge. nil omits the parameter (server cannot then
//     ask for an encrypted password reply).
//
// Returns the payload bytes and a Header pre-filled for sending. The
// caller is expected to apply correlation IDs etc. before writing.
func ExchangeAttributesRequest(server ServerID, clientVersion uint32, clientDSLevel uint16, clientSeed []byte) (Header, []byte, error) {
	if server != ServerSignon && server != ServerID(0xE00B) /* HOSTCNN */ {
		return Header{}, nil, fmt.Errorf("hostserver: ExchangeAttributesRequest only valid for SIGNON or HOSTCNN, got %s", server)
	}
	if clientSeed != nil && len(clientSeed) != 8 {
		return Header{}, nil, fmt.Errorf("hostserver: client seed must be 8 bytes, got %d", len(clientSeed))
	}

	// Param sizes (LL includes its own 4-byte length field):
	//   client version:     4 + 2 + 4 = 10
	//   client DS level:    4 + 2 + 2 = 8
	//   client seed:        4 + 2 + 8 = 14 (optional)
	payloadLen := 10 + 8
	if clientSeed != nil {
		payloadLen += 14
	}
	payload := make([]byte, payloadLen)
	be := binary.BigEndian

	// Client version.
	be.PutUint32(payload[0:4], 10)
	be.PutUint16(payload[4:6], cpClientVersion)
	be.PutUint32(payload[6:10], clientVersion)

	// Client datastream level.
	be.PutUint32(payload[10:14], 8)
	be.PutUint16(payload[14:16], cpClientDatastreamLvl)
	be.PutUint16(payload[16:18], clientDSLevel)

	// Client seed (optional).
	if clientSeed != nil {
		be.PutUint32(payload[18:22], 14)
		be.PutUint16(payload[22:24], cpClientSeed)
		copy(payload[24:32], clientSeed)
	}

	reqID := ReqExchangeAttributesSignon
	if server != ServerSignon {
		reqID = ReqExchangeAttributesHostCnn
	}
	hdr := Header{
		Length:   uint32(HeaderLength + payloadLen),
		ServerID: server,
		ReqRepID: reqID,
	}
	return hdr, payload, nil
}
