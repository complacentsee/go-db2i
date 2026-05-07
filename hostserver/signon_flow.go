package hostserver

import (
	"crypto/rand"
	"fmt"
	"io"

	"github.com/complacentsee/goJTOpen/auth"
)

// SignOn runs the as-signon service handshake on conn and returns
// both halves of the negotiation: the [ExchangeAttributesReply] from
// step 1 (server VRM, datastream level, server seed, password level,
// job name) and the [SignonInfoReply] from step 2 (RC, sign-on dates,
// server CCSID, password expiration).
//
// The flow:
//
//  1. Generate an 8-byte random client seed (crypto/rand).
//  2. Send [ExchangeAttributesRequest] with the seed; parse reply.
//  3. Use the server's seed + password level to encrypt password.
//  4. Send [SignonInfoRequest]; parse reply.
//
// conn is any io.ReadWriter -- typically a *net.TCPConn from the
// caller. SignOn does not close conn; the caller owns lifecycle.
//
// Only password levels 2 and 3 (SHA-1) are wired up at the moment.
// Levels 0/1 (DES) and 4 (PBKDF2-HMAC-SHA-512) return an error
// describing the gap.
func SignOn(conn io.ReadWriter, userID, password string) (
	*ExchangeAttributesReply,
	*SignonInfoReply,
	error,
) {
	clientSeed := make([]byte, 8)
	if _, err := rand.Read(clientSeed); err != nil {
		return nil, nil, fmt.Errorf("hostserver: generate client seed: %w", err)
	}

	// Step 1: exchange attributes.
	xaHdr, xaPayload, err := ExchangeAttributesRequest(ServerSignon, 1, 10, clientSeed)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: build exchange-attrs req: %w", err)
	}
	if err := WriteFrame(conn, xaHdr, xaPayload); err != nil {
		return nil, nil, fmt.Errorf("hostserver: send exchange-attrs req: %w", err)
	}
	xaRepHdr, xaRepPayload, err := ReadFrame(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: read exchange-attrs reply: %w", err)
	}
	if xaRepHdr.ReqRepID != RepExchangeAttributesSignon {
		return nil, nil, fmt.Errorf("hostserver: unexpected reply ReqRepID 0x%04X (want 0x%04X)",
			xaRepHdr.ReqRepID, RepExchangeAttributesSignon)
	}
	xa, err := ParseExchangeAttributesReply(xaRepPayload)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: parse exchange-attrs reply: %w", err)
	}
	if xa.ReturnCode != 0 {
		return xa, nil, fmt.Errorf("hostserver: exchange-attrs RC=%d", xa.ReturnCode)
	}
	if len(xa.ServerSeed) != 8 {
		return xa, nil, fmt.Errorf("hostserver: server seed missing or wrong length: %d bytes", len(xa.ServerSeed))
	}

	// Step 2: encrypt password using the negotiated level.
	var encrypted []byte
	switch xa.PasswordLevel {
	case 2, 3:
		encrypted, err = auth.EncryptPasswordSHA1(userID, password, clientSeed, xa.ServerSeed)
		if err != nil {
			return xa, nil, fmt.Errorf("hostserver: encrypt password (level %d, SHA-1): %w", xa.PasswordLevel, err)
		}
	case 0, 1:
		return xa, nil, fmt.Errorf("hostserver: server password level %d (DES) not implemented", xa.PasswordLevel)
	case 4:
		return xa, nil, fmt.Errorf("hostserver: server password level 4 (PBKDF2-SHA-512) not implemented")
	default:
		return xa, nil, fmt.Errorf("hostserver: unknown server password level %d", xa.PasswordLevel)
	}

	// Step 3: send signon-info request.
	siHdr, siPayload, err := SignonInfoRequest(
		AuthSchemePassword,
		userID,
		encrypted,
		xa.ServerLevel,
		1200, // client CCSID = UTF-16 BE, matches JTOpen
		nil,  // no MFA factor
	)
	if err != nil {
		return xa, nil, fmt.Errorf("hostserver: build signon-info req: %w", err)
	}
	siHdr.CorrelationID = 1 // exchange-attrs was 0
	if err := WriteFrame(conn, siHdr, siPayload); err != nil {
		return xa, nil, fmt.Errorf("hostserver: send signon-info req: %w", err)
	}
	siRepHdr, siRepPayload, err := ReadFrame(conn)
	if err != nil {
		return xa, nil, fmt.Errorf("hostserver: read signon-info reply: %w", err)
	}
	if siRepHdr.ReqRepID != RepSignonInfo {
		return xa, nil, fmt.Errorf("hostserver: unexpected reply ReqRepID 0x%04X (want 0x%04X)",
			siRepHdr.ReqRepID, RepSignonInfo)
	}
	si, err := ParseSignonInfoReply(siRepPayload)
	if err != nil {
		return xa, nil, fmt.Errorf("hostserver: parse signon-info reply: %w", err)
	}
	if si.ReturnCode != 0 {
		return xa, si, fmt.Errorf("hostserver: signon-info RC=%d (auth failed)", si.ReturnCode)
	}

	return xa, si, nil
}
