package hostserver

import (
	"encoding/binary"
	"fmt"
)

// Sign-on service request/reply identifiers (DSS Header.ReqRepID).
const (
	ReqExchangeAttributesSignon  uint16 = 0x7003 // sent over ServerSignon
	ReqExchangeAttributesHostCnn uint16 = 0x7103 // sent over the HOSTCNN service

	RepExchangeAttributesSignon  uint16 = 0xF003 // signon's reply to 0x7003
	RepExchangeAttributesHostCnn uint16 = 0xF103 // hostcnn's reply to 0x7103
)

// Code points (CP) for parameters carried inside an exchange-attributes
// frame. Layout per parameter is LL (uint32) | CP (uint16) | data.
const (
	cpClientVersion       uint16 = 0x1101 // 4-byte uint32 client version
	cpClientDatastreamLvl uint16 = 0x1102 // 2-byte uint16 datastream level
	cpClientSeed          uint16 = 0x1103 // 8-byte client random seed
	// Reply-side reuses 0x1101 / 0x1102 for server version + level, plus:
	cpServerSeed    uint16 = 0x1103 // 8-byte server random seed
	cpPasswordLevel uint16 = 0x1119 // 1-byte server password level (0..4)
	cpJobName       uint16 = 0x111F // 4-byte CCSID + EBCDIC job name
	cpAAFIndicator  uint16 = 0x112E // 1-byte additional-auth-factor flag
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

// ExchangeAttributesReply is the parsed form of a
// 0xF003 / 0xF103 reply payload from the as-signon (or as-hostcnn)
// service.
//
// The first three fields are at fixed offsets (RC, server version,
// server level); everything else is a variable-length parameter
// addressed by code point. Optional CPs that the server doesn't send
// leave their corresponding field at the zero value.
type ExchangeAttributesReply struct {
	// ReturnCode is the operation status; 0 = success. Non-zero values
	// have meanings documented in the IBM i Information Center under
	// "Sign-on server return codes".
	ReturnCode uint32

	// ServerVersion is the IBM i VRM packed as a uint32, e.g.
	// 0x00070500 = V7R5M0. The high byte is reserved.
	ServerVersion uint32

	// ServerLevel is the negotiated host-server datastream level.
	// JTOpen requests 10 (password level 4); the server picks a value
	// no higher than what it supports (commonly 15 on V7R5+).
	ServerLevel uint16

	// ServerSeed is an 8-byte random nonce the client mixes into the
	// password encryption challenge. Empty if CP 0x1103 was absent.
	ServerSeed []byte

	// PasswordLevel selects the encryption scheme:
	//   0, 1: DES
	//   2, 3: SHA-1
	//   4:    PBKDF2-HMAC-SHA-512 + SHA-256-salted token
	// 0 if CP 0x1119 was absent (treat as level 0).
	PasswordLevel uint8

	// JobName is the EBCDIC byte sequence for the prestart job that's
	// servicing this connection (e.g. "341513/QUSER/QZSOSIGN"). nil
	// if CP 0x111F was absent. Decode through the CCSID indicated by
	// JobNameCCSID.
	JobName      []byte
	JobNameCCSID uint32

	// AAFIndicator is true when the server is asking for an
	// additional authentication factor (e.g. MFA token). False if
	// CP 0x112E is absent or carries 0.
	AAFIndicator bool
}

// ParseExchangeAttributesReply decodes the payload of a 0xF003 / 0xF103
// frame (i.e., the bytes after the 20-byte DSS header). It rejects
// malformed length-prefixed parameters but is forgiving about unknown
// CPs -- they are silently skipped so a newer server doesn't break
// older clients.
func ParseExchangeAttributesReply(payload []byte) (*ExchangeAttributesReply, error) {
	const fixedLen = 22 // RC + version param (10) + level param (8)
	if len(payload) < fixedLen {
		return nil, fmt.Errorf("hostserver: exchange-attributes reply too short: %d bytes (want >= %d)", len(payload), fixedLen)
	}
	be := binary.BigEndian

	rep := &ExchangeAttributesReply{
		ReturnCode:    be.Uint32(payload[0:4]),
		ServerVersion: be.Uint32(payload[10:14]), // skip LL+CP of version param
		ServerLevel:   be.Uint16(payload[20:22]), // skip LL+CP of level param
	}

	// Variable LL-CP-data parameters start at offset 22.
	for pos := fixedLen; pos+6 <= len(payload); {
		ll := be.Uint32(payload[pos : pos+4])
		if ll < 6 || pos+int(ll) > len(payload) {
			return nil, fmt.Errorf("hostserver: bad LL %d at payload offset %d (frame len %d)", ll, pos, len(payload))
		}
		cp := be.Uint16(payload[pos+4 : pos+6])
		data := payload[pos+6 : pos+int(ll)]
		switch cp {
		case cpServerSeed:
			if len(data) != 8 {
				return nil, fmt.Errorf("hostserver: server seed must be 8 bytes, got %d", len(data))
			}
			rep.ServerSeed = append([]byte(nil), data...)
		case cpPasswordLevel:
			if len(data) < 1 {
				return nil, fmt.Errorf("hostserver: password level CP empty")
			}
			rep.PasswordLevel = data[0]
		case cpJobName:
			if len(data) < 4 {
				return nil, fmt.Errorf("hostserver: job name CP too short: %d bytes", len(data))
			}
			rep.JobNameCCSID = be.Uint32(data[0:4])
			rep.JobName = append([]byte(nil), data[4:]...)
		case cpAAFIndicator:
			if len(data) >= 1 {
				rep.AAFIndicator = data[0] == 0x01
			}
		}
		pos += int(ll)
	}
	return rep, nil
}
