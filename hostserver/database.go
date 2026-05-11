package hostserver

import (
	"encoding/binary"
	"fmt"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// Database-service request / reply identifiers (DSS Header.ReqRepID).
//
// Unlike the as-signon service (which negotiates via 0x7003/0xF003
// exchange-attributes), as-database opens with a smaller pair of
// flows: 0x7001/0xF001 swaps random seeds, then 0x7002/0xF002 starts
// the server prestart job for this user. Both of these are also used
// by the other host-server services (rmtcmd, dtaq, file, etc.) -- the
// constants live here because as-database is the first service in
// go-db2i that drives them.
const (
	ReqXChgRandSeed uint16 = 0x7001
	ReqStartServer  uint16 = 0x7002

	RepXChgRandSeed uint16 = 0xF001
	RepStartServer  uint16 = 0xF002
)

// Client-attributes byte values that go in HeaderID's high byte for
// the as-database flows. JTOpen documents them inline in
// AS400XChgRandSeedDS / AS400StrSvrDS:
//
//	0x01: SHA-1 password encryption
//	0x02: server password level 4 (PBKDF2-SHA-512)
//	0x03: client supports the additional-authentication-factor flow
//	      (also implies SHA-1 + level-4 acceptance)
//
// For StartServer the byte means something different (0x02 = "I want
// the prestart-job info back in the reply"); kept as a separate name.
const (
	clientAttrsXChgRandSeed = 0x03 // SHA-1 + lvl4 + AAF
	clientAttrsStartServer  = 0x02 // wants job info in reply
)

// XChgRandSeedRequest builds the 28-byte 0x7001 frame that a host-
// server client sends right after TCP-connect (and before any
// authentication) to exchange random seeds and probe the server's
// password-encryption capabilities. server identifies the host-server
// service: typically [ServerDatabase] for the database service on
// port 8471, but the same flow is used by the other services too.
//
// clientSeed must be exactly 8 bytes. JTOpen uses the
// current-millis low/high split as its source of "random"; go-db2i
// callers should pass crypto/rand bytes via the orchestrator
// in [StartDatabaseService].
func XChgRandSeedRequest(server ServerID, clientSeed []byte) (Header, []byte, error) {
	if len(clientSeed) != 8 {
		return Header{}, nil, fmt.Errorf("hostserver: XChgRandSeedRequest client seed must be 8 bytes, got %d", len(clientSeed))
	}
	payload := make([]byte, 8)
	copy(payload, clientSeed)

	hdr := Header{
		Length:         uint32(HeaderLength + 8),
		HeaderID:       uint16(clientAttrsXChgRandSeed) << 8, // byte 4 = 0x03, byte 5 = 0x00
		ServerID:       server,
		TemplateLength: 8,
		ReqRepID:       ReqXChgRandSeed,
	}
	return hdr, payload, nil
}

// XChgRandSeedReply is the parsed form of a 0xF001 frame.
//
// Per JTOpen's AS400XChgRandSeedReplyDS, the server seed lives in the
// 12-byte template (RC + 8-byte seed) and the password level is
// carried in the second byte of the HeaderID field (data_[5] in
// JTOpen). Optional CPs follow the template; we surface
// AAFIndicator from CP 0x112E if present.
type XChgRandSeedReply struct {
	// ReturnCode is 0 on success.
	ReturnCode uint32
	// ServerSeed is the 8-byte nonce the client mixes into its
	// password encryption. Always present on a successful reply.
	ServerSeed []byte
	// PasswordLevel is the encryption scheme the server requires:
	//   0, 1: DES (pre-V5R1)
	//   2, 3: SHA-1
	//   4:    PBKDF2-HMAC-SHA-512 (V7R1+)
	// Carried in HeaderID's low byte; mirrors JTOpen's
	// getServerAttributes() return value.
	PasswordLevel uint8
	// AAFIndicator is true iff the server includes CP 0x112E with
	// data byte 0x01 -- a request for the additional-auth-factor
	// flow (MFA token).
	AAFIndicator bool
}

// ParseXChgRandSeedReply decodes the 12-byte fixed template plus any
// optional CP parameters from a 0xF001 payload. headerID is the
// HeaderID field of the DSS frame -- needed because the password
// level is carried there, not in the payload.
func ParseXChgRandSeedReply(headerID uint16, payload []byte) (*XChgRandSeedReply, error) {
	const fixedLen = 12 // RC(4) + ServerSeed(8)
	if len(payload) < fixedLen {
		return nil, fmt.Errorf("hostserver: xchg-rand-seed reply too short: %d bytes (want >= %d)", len(payload), fixedLen)
	}
	be := binary.BigEndian
	rep := &XChgRandSeedReply{
		ReturnCode:    be.Uint32(payload[0:4]),
		ServerSeed:    append([]byte(nil), payload[4:12]...),
		PasswordLevel: uint8(headerID & 0xFF),
	}
	for pos := fixedLen; pos+6 <= len(payload); {
		ll := be.Uint32(payload[pos : pos+4])
		if ll < 6 || pos+int(ll) > len(payload) {
			return nil, fmt.Errorf("hostserver: bad LL %d at xchg-rand-seed payload offset %d", ll, pos)
		}
		cp := be.Uint16(payload[pos+4 : pos+6])
		data := payload[pos+6 : pos+int(ll)]
		switch cp {
		case cpAAFIndicator:
			if len(data) >= 1 {
				rep.AAFIndicator = data[0] == 0x01
			}
		}
		pos += int(ll)
	}
	return rep, nil
}

// StartServerRequest builds a 0x7002 frame -- the second leg of the
// host-server handshake on the database (and other) services. It
// carries the actual credential the user is signing on with.
//
// authBytes is the already-encrypted password (or token) bytes; for
// SHA-1 (password level 2/3) that's a 20-byte substitute computed by
// auth.EncryptPasswordSHA1 against the client+server seeds from the
// preceding XChgRandSeed exchange. The length determines the auth
// scheme byte at template offset 0:
//
//	scheme==Password,  len 8  -> 0x01 (DES)
//	scheme==Password,  len 20 -> 0x03 (SHA-1)
//	scheme==Password,  other  -> 0x07 (PBKDF2-SHA-512)
//	scheme==GSSToken          -> 0x05
//	scheme==IdentityToken     -> 0x06
//	otherwise                 -> 0x02
//
// userID is encoded to 10 bytes EBCDIC CCSID 37 + space pad (the same
// as the signon service). Empty userID omits CP 0x1104 -- a token-only
// flow.
//
// The optional MFA / verification-ID / client-IP CPs from JTOpen's
// 7-arg AS400StrSvrDS constructor are not surfaced here yet; PUB400
// and the production target don't require them. They land when M3+
// drags a real authentication-token path through the codebase.
func StartServerRequest(server ServerID, scheme AuthScheme, userID string, authBytes []byte) (Header, []byte, error) {
	if len(authBytes) == 0 {
		return Header{}, nil, fmt.Errorf("hostserver: StartServerRequest authBytes empty")
	}

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

	// Template (2 bytes): scheme byte + send-reply flag.
	// Then LL/CP params:
	//   password / token: LL(4) + CP(2) + auth bytes
	//   user ID  (opt):    LL(4) + CP(2) + 10 bytes
	size := 2 + 6 + len(authBytes)
	if userIDBytes != nil {
		size += 16
	}
	payload := make([]byte, size)
	be := binary.BigEndian

	payload[0] = authSchemeTemplateByte(scheme, len(authBytes))
	payload[1] = 0x01 // send-reply true (matches JTOpen)

	off := 2
	be.PutUint32(payload[off:off+4], uint32(6+len(authBytes)))
	authCP := cpPassword
	if scheme != AuthSchemePassword {
		authCP = cpAuthToken
	}
	be.PutUint16(payload[off+4:off+6], authCP)
	copy(payload[off+6:], authBytes)
	off += 6 + len(authBytes)

	if userIDBytes != nil {
		be.PutUint32(payload[off:off+4], 16)
		be.PutUint16(payload[off+4:off+6], cpUserID)
		copy(payload[off+6:off+16], userIDBytes)
		off += 16
	}

	if off != size {
		return Header{}, nil, fmt.Errorf("hostserver: StartServerRequest size mismatch: wrote %d, planned %d", off, size)
	}

	hdr := Header{
		Length:         uint32(HeaderLength + size),
		HeaderID:       uint16(clientAttrsStartServer) << 8, // byte 4 = 0x02
		ServerID:       server,
		TemplateLength: 2,
		ReqRepID:       ReqStartServer,
	}
	return hdr, payload, nil
}

// StartServerReply is the parsed form of a 0xF002 frame. The fixed
// template carries only the RC; the user ID and prestart-job name
// arrive as optional CPs in the variable section.
type StartServerReply struct {
	// ReturnCode is 0 on success. Non-zero values are documented in
	// the IBM i sign-on / database server reference and are
	// authentication-failure-shaped.
	ReturnCode uint32
	// UserID is the canonical 10-byte EBCDIC user ID the server
	// registered for this connection (CP 0x1104). nil if absent.
	UserID []byte
	// JobName is the EBCDIC bytes of the prestart job that's now
	// servicing this connection (e.g. "344425/QUSER/QZDASOINIT").
	// CCSID is in JobNameCCSID; zero CCSID means "no CCSID prefix
	// in the CP" (some servers send 4 bytes of zero there).
	JobName      []byte
	JobNameCCSID uint32
}

// ParseStartServerReply decodes a 0xF002 payload. The fixed-template
// length is 4 bytes (just the RC); CPs follow per the LL/CP/data
// scheme.
//
// Per JTOpen's AS400StrSvrReplyDS.findCP, the CP scan starts at frame
// offset 24 (which is payload offset 4). We tolerate both the
// 10-byte-prefix layout that AS400StrSvrReplyDS expects (LL+CP+4
// CCSID-bytes+data) and a no-CCSID fallback for the user-ID CP --
// JTOpen's getUserIdBytes uses get32bit(offset)-10 unconditionally,
// so we match.
func ParseStartServerReply(payload []byte) (*StartServerReply, error) {
	const fixedLen = 4 // RC
	if len(payload) < fixedLen {
		return nil, fmt.Errorf("hostserver: start-server reply too short: %d bytes (want >= %d)", len(payload), fixedLen)
	}
	be := binary.BigEndian
	rep := &StartServerReply{
		ReturnCode: be.Uint32(payload[0:4]),
	}

	for pos := fixedLen; pos+6 <= len(payload); {
		ll := be.Uint32(payload[pos : pos+4])
		if ll < 6 || pos+int(ll) > len(payload) {
			return nil, fmt.Errorf("hostserver: bad LL %d at start-server payload offset %d", ll, pos)
		}
		cp := be.Uint16(payload[pos+4 : pos+6])
		data := payload[pos+6 : pos+int(ll)]
		switch cp {
		case cpUserID:
			// JTOpen reads (LL-10) bytes from offset+10, i.e. it
			// skips 4 bytes after the CP (treats them as CCSID
			// even though for user IDs they're zeros). We do the
			// same so that a 16-byte LL gives 6 bytes of user
			// ID; in practice servers send LL=20 with 10 bytes.
			if len(data) >= 4 {
				rep.UserID = append([]byte(nil), data[4:]...)
			}
		case cpJobName:
			if len(data) < 4 {
				return nil, fmt.Errorf("hostserver: job name CP too short: %d bytes", len(data))
			}
			rep.JobNameCCSID = be.Uint32(data[0:4])
			rep.JobName = append([]byte(nil), data[4:]...)
		}
		pos += int(ll)
	}
	return rep, nil
}
