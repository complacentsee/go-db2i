package hostserver

import (
	"encoding/binary"
	"fmt"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// Sign-on service request/reply identifiers (DSS Header.ReqRepID).
const (
	ReqExchangeAttributesSignon  uint16 = 0x7003 // sent over ServerSignon
	ReqExchangeAttributesHostCnn uint16 = 0x7103 // sent over the HOSTCNN service
	ReqSignonInfo                uint16 = 0x7004 // user/password authentication

	RepExchangeAttributesSignon  uint16 = 0xF003 // signon's reply to 0x7003
	RepExchangeAttributesHostCnn uint16 = 0xF103 // hostcnn's reply to 0x7103
	RepSignonInfo                uint16 = 0xF004 // signon's reply to 0x7004
)

// AuthScheme classifies what kind of credential a SignonInfoRequest carries.
type AuthScheme int

const (
	// AuthSchemePassword sends an encrypted password. The exact
	// encryption depends on the server's password level (DES, SHA-1,
	// or PBKDF2-SHA-512) negotiated via [ExchangeAttributesReply].
	AuthSchemePassword AuthScheme = iota
	// AuthSchemeGSSToken carries a Kerberos GSS-API token.
	AuthSchemeGSSToken
	// AuthSchemeIdentityToken carries an IBM i identity token.
	AuthSchemeIdentityToken
	// AuthSchemeProfileToken carries a profile token created by an
	// earlier authenticated session (or by a privileged process).
	AuthSchemeProfileToken
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

	// Code points used by SignonInfoRequest (0x7004).
	cpClientCCSID         uint16 = 0x1113 // 4-byte uint32
	cpUserID              uint16 = 0x1104 // 10-byte EBCDIC user ID (CCSID 37)
	cpPassword            uint16 = 0x1105 // encrypted password (8 / 20 / variable bytes)
	cpAuthToken           uint16 = 0x1115 // GSS / identity / profile token
	cpReturnErrorMessages uint16 = 0x1128 // 1-byte flag (set when serverLevel >= 5)
	cpAddAuthFactor       uint16 = 0x112F // 4-byte CCSID + MFA token (serverLevel >= 18)
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

// SignonInfoRequest builds a SIGNON-service signon-info frame
// (ReqRepID 0x7004) -- the message that carries the actual user
// credential. authBytes must already be encrypted; the encryption
// algorithm to use depends on the password level returned by the
// preceding [ParseExchangeAttributesReply] (lives in a separate
// package).
//
// Inputs:
//   - scheme:        AuthSchemePassword for the password path; the
//     others select the corresponding token CP.
//   - userID:        Unicode user name. Encoded to 10 bytes EBCDIC
//     CCSID 37, space-padded. May be empty for token-only schemes;
//     omits the 0x1104 parameter in that case.
//   - authBytes:     already-encrypted password OR token bytes. For
//     AuthSchemePassword, length determines the template byte: 8 ->
//     0x01 (DES), 20 -> 0x03 (SHA-1), other -> 0x07 (PBKDF2-SHA-512).
//   - serverLevel:   datastream level from the prior reply. >= 5 adds
//     the "return error messages" flag CP. >= 18 enables the AAF CP
//     when addAuthFactor is non-empty.
//   - clientCCSID:   client CCSID claimed in CP 0x1113. JTOpen always
//     sends 1200 (UTF-16 BE).
//   - addAuthFactor: optional MFA token. nil/empty omits CP 0x112F.
//
// Returns the Header (Length set, CorrelationID left at zero -- caller
// owns sequencing) and the payload bytes. Use [WriteFrame] to write
// the whole thing to a Writer.
func SignonInfoRequest(
	scheme AuthScheme,
	userID string,
	authBytes []byte,
	serverLevel uint16,
	clientCCSID uint32,
	addAuthFactor []byte,
) (Header, []byte, error) {
	if len(authBytes) == 0 {
		return Header{}, nil, fmt.Errorf("hostserver: SignonInfoRequest authBytes empty")
	}

	// Encode user ID to 10 bytes EBCDIC CCSID 37 + space pad.
	var userIDBytes []byte
	if userID != "" {
		encoded, err := ebcdic.CCSID37.Encode(userID)
		if err != nil {
			return Header{}, nil, fmt.Errorf("hostserver: encode user ID: %w", err)
		}
		if len(encoded) > 10 {
			return Header{}, nil, fmt.Errorf("hostserver: user ID %q encodes to %d bytes (max 10)", userID, len(encoded))
		}
		userIDBytes = make([]byte, 10)
		copy(userIDBytes, encoded)
		for i := len(encoded); i < 10; i++ {
			userIDBytes[i] = 0x40 // EBCDIC space
		}
	}

	// Template byte at payload offset 0 (= frame offset 20).
	templateByte := authSchemeTemplateByte(scheme, len(authBytes))

	// Compute size up front so we can allocate one buffer.
	// 1 (template) + 10 (CCSID param) + (6+len(authBytes)) (auth)
	//             + (16 if userID)    + (7 if serverLevel>=5)
	//             + (10+len(addAuthFactor) if serverLevel>=18 && len>0)
	size := 1 + 10 + 6 + len(authBytes)
	if userIDBytes != nil {
		size += 16
	}
	if serverLevel >= 5 {
		size += 7
	}
	if serverLevel >= 18 && len(addAuthFactor) > 0 {
		size += 10 + len(addAuthFactor)
	}

	payload := make([]byte, size)
	be := binary.BigEndian

	payload[0] = templateByte

	// Client CCSID param.
	be.PutUint32(payload[1:5], 10)
	be.PutUint16(payload[5:7], cpClientCCSID)
	be.PutUint32(payload[7:11], clientCCSID)

	// Auth bytes param (CP 0x1105 for password, 0x1115 for token).
	be.PutUint32(payload[11:15], uint32(6+len(authBytes)))
	authCP := cpPassword
	if scheme != AuthSchemePassword {
		authCP = cpAuthToken
	}
	be.PutUint16(payload[15:17], authCP)
	copy(payload[17:17+len(authBytes)], authBytes)
	off := 17 + len(authBytes)

	// User ID param.
	if userIDBytes != nil {
		be.PutUint32(payload[off:off+4], 16)
		be.PutUint16(payload[off+4:off+6], cpUserID)
		copy(payload[off+6:off+16], userIDBytes)
		off += 16
	}

	// Return-error-messages param.
	if serverLevel >= 5 {
		be.PutUint32(payload[off:off+4], 7)
		be.PutUint16(payload[off+4:off+6], cpReturnErrorMessages)
		payload[off+6] = 0x01
		off += 7
	}

	// Additional-auth-factor (MFA) param.
	if serverLevel >= 18 && len(addAuthFactor) > 0 {
		ll := uint32(10 + len(addAuthFactor))
		be.PutUint32(payload[off:off+4], ll)
		be.PutUint16(payload[off+4:off+6], cpAddAuthFactor)
		be.PutUint32(payload[off+6:off+10], 1208) // UTF-8
		copy(payload[off+10:off+10+len(addAuthFactor)], addAuthFactor)
		off += 10 + len(addAuthFactor)
	}

	if off != size {
		// Defensive: catch a math mistake during construction.
		return Header{}, nil, fmt.Errorf("hostserver: SignonInfoRequest size mismatch: wrote %d, planned %d", off, size)
	}

	hdr := Header{
		Length:         uint32(HeaderLength + size),
		ServerID:       ServerSignon,
		TemplateLength: 1,
		ReqRepID:       ReqSignonInfo,
	}
	return hdr, payload, nil
}

// authSchemeTemplateByte encodes the auth scheme + password length
// into the single template byte at payload offset 0. Mirrors JTOpen's
// SignonInfoReq logic: identity token wins over default 0x02, GSS
// overrides, and password length further refines for the password
// scheme.
func authSchemeTemplateByte(scheme AuthScheme, authLen int) byte {
	switch scheme {
	case AuthSchemeGSSToken:
		return 0x05
	case AuthSchemeIdentityToken:
		return 0x06
	case AuthSchemePassword:
		switch authLen {
		case 8:
			return 0x01 // DES
		case 20:
			return 0x03 // SHA-1
		default:
			return 0x07 // PBKDF2-SHA-512
		}
	}
	return 0x02
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
