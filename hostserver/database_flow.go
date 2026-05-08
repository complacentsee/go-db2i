package hostserver

import (
	"crypto/rand"
	"fmt"
	"io"

	"github.com/complacentsee/goJTOpen/auth"
)

// StartDatabaseService runs the as-database (port 8471) handshake on
// conn -- the second host-server connection a JTOpen client opens
// after as-signon. It performs:
//
//  1. 0x7001 XChgRandSeed: client seed (8 bytes) -> server seed +
//     password level. Same conceptual flow as the as-signon
//     exchange-attributes, but a smaller, simpler frame; the
//     password level lives in the reply's HeaderID byte instead of
//     a separate CP.
//  2. 0x7002 StartServer: encrypted password + EBCDIC user ID
//     -> RC + prestart job name (e.g. "344425/QUSER/QZDASOINIT").
//
// On success, the connection is authenticated and the next frame
// the client sends should be a database-server-attributes request
// (0x1F80) -- but that's M2 territory.
//
// Only password levels 2 and 3 (SHA-1) are wired up; 0/1 (DES) and
// 4 (PBKDF2-HMAC-SHA-512) return a clearly named error pointing at
// the unimplemented auth package.
//
// conn lifecycle (open + close) is the caller's responsibility.
func StartDatabaseService(conn io.ReadWriter, userID, password string) (
	*XChgRandSeedReply,
	*StartServerReply,
	error,
) {
	clientSeed := make([]byte, 8)
	if _, err := rand.Read(clientSeed); err != nil {
		return nil, nil, fmt.Errorf("hostserver: generate client seed: %w", err)
	}

	// Step 1: exchange random seeds.
	xsHdr, xsPayload, err := XChgRandSeedRequest(ServerDatabase, clientSeed)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: build xchg-rand-seed req: %w", err)
	}
	if err := WriteFrame(conn, xsHdr, xsPayload); err != nil {
		return nil, nil, fmt.Errorf("hostserver: send xchg-rand-seed req: %w", err)
	}
	xsRepHdr, xsRepPayload, err := ReadFrame(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: read xchg-rand-seed reply: %w", err)
	}
	if xsRepHdr.ReqRepID != RepXChgRandSeed {
		return nil, nil, fmt.Errorf("hostserver: unexpected reply ReqRepID 0x%04X (want 0x%04X)",
			xsRepHdr.ReqRepID, RepXChgRandSeed)
	}
	xs, err := ParseXChgRandSeedReply(xsRepHdr.HeaderID, xsRepPayload)
	if err != nil {
		return nil, nil, fmt.Errorf("hostserver: parse xchg-rand-seed reply: %w", err)
	}
	if xs.ReturnCode != 0 {
		return xs, nil, fmt.Errorf("hostserver: xchg-rand-seed RC=%d", xs.ReturnCode)
	}
	if len(xs.ServerSeed) != 8 {
		return xs, nil, fmt.Errorf("hostserver: server seed missing or wrong length: %d bytes", len(xs.ServerSeed))
	}

	// Step 2: encrypt password using the negotiated level.
	var encrypted []byte
	switch xs.PasswordLevel {
	case 2, 3:
		encrypted, err = auth.EncryptPasswordSHA1(userID, password, clientSeed, xs.ServerSeed)
		if err != nil {
			return xs, nil, fmt.Errorf("hostserver: encrypt password (level %d, SHA-1): %w", xs.PasswordLevel, err)
		}
	case 0, 1:
		// DES path -- spec-validated against JT400, NOT wire-validated;
		// auth.EncryptPasswordDES emits a one-shot stderr warning on
		// first use to make the gap visible.
		encrypted, err = auth.EncryptPasswordDES(userID, password, clientSeed, xs.ServerSeed)
		if err != nil {
			return xs, nil, fmt.Errorf("hostserver: encrypt password (level %d, DES): %w", xs.PasswordLevel, err)
		}
	case 4:
		// PBKDF2-HMAC-SHA-512 path -- same spec-validated-only caveat.
		encrypted, err = auth.EncryptPasswordPBKDF2(userID, password, clientSeed, xs.ServerSeed)
		if err != nil {
			return xs, nil, fmt.Errorf("hostserver: encrypt password (level 4, PBKDF2): %w", err)
		}
	default:
		return xs, nil, fmt.Errorf("hostserver: unknown server password level %d", xs.PasswordLevel)
	}

	// Step 3: start server (auth round trip).
	ssHdr, ssPayload, err := StartServerRequest(ServerDatabase, AuthSchemePassword, userID, encrypted)
	if err != nil {
		return xs, nil, fmt.Errorf("hostserver: build start-server req: %w", err)
	}
	if err := WriteFrame(conn, ssHdr, ssPayload); err != nil {
		return xs, nil, fmt.Errorf("hostserver: send start-server req: %w", err)
	}
	ssRepHdr, ssRepPayload, err := ReadFrame(conn)
	if err != nil {
		return xs, nil, fmt.Errorf("hostserver: read start-server reply: %w", err)
	}
	if ssRepHdr.ReqRepID != RepStartServer {
		return xs, nil, fmt.Errorf("hostserver: unexpected reply ReqRepID 0x%04X (want 0x%04X)",
			ssRepHdr.ReqRepID, RepStartServer)
	}
	ss, err := ParseStartServerReply(ssRepPayload)
	if err != nil {
		return xs, nil, fmt.Errorf("hostserver: parse start-server reply: %w", err)
	}
	if ss.ReturnCode != 0 {
		return xs, ss, fmt.Errorf("hostserver: start-server RC=%d (auth failed)", ss.ReturnCode)
	}

	return xs, ss, nil
}
