package auth

import (
	"crypto/sha1"
	"fmt"
	"strings"
	"unicode/utf16"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// EncryptPasswordSHA1 returns the 20-byte encrypted password the
// go-db2i client sends in a SignonInfoRequest when the server's
// password level is 2 or 3.
//
// Algorithm (mirrors com.ibm.as400.access.AS400ImplRemote in JTOpen):
//
//  1. token = SHA-1(userID-utf16-be || password-utf16-be)
//  2. encrypted = SHA-1(token || serverSeed || clientSeed
//     || userID-utf16-be || sequence)
//
// Both userID-utf16-be and password-utf16-be are obtained by:
//
//   - userID first round-trips through CCSID 37 (EBCDIC) padded with
//     0x40 to 10 bytes, then back to Unicode -- this normalises the
//     character set the same way JTOpen does, so two callers passing
//     "USER" (4 chars) and "USER      " (10 chars w/ space pad)
//     produce identical bytes;
//   - password is trimmed of leading + trailing Unicode spaces, then
//     each rune is encoded big-endian into 2 bytes (no BOM).
//
// sequence is the literal byte slice {0, 0, 0, 0, 0, 0, 0, 1}.
//
// Returns an error if the userID or password contain characters that
// don't round-trip through CCSID 37.
//
// Inputs are validated minimally (an empty password or one starting
// with '*' is rejected, mirroring the JTOpen guards). All buffer
// allocations are heap-resident; callers concerned about plaintext-
// password residue in memory should overwrite their own copies.
func EncryptPasswordSHA1(userID, password string, clientSeed, serverSeed []byte) ([]byte, error) {
	if password == "" {
		return nil, fmt.Errorf("auth: password is empty")
	}
	if strings.HasPrefix(password, "*") {
		return nil, fmt.Errorf("auth: password starts with '*' (reserved)")
	}
	if len(clientSeed) != 8 {
		return nil, fmt.Errorf("auth: clientSeed must be 8 bytes, got %d", len(clientSeed))
	}
	if len(serverSeed) != 8 {
		return nil, fmt.Errorf("auth: serverSeed must be 8 bytes, got %d", len(serverSeed))
	}

	userIDBytes, err := userIDToUTF16BE(userID)
	if err != nil {
		return nil, err
	}
	passwordBytes := utf16BE(strings.TrimSpace(password))

	// token = SHA-1(userIDBytes || passwordBytes)
	h := sha1.New()
	h.Write(userIDBytes)
	h.Write(passwordBytes)
	token := h.Sum(nil)

	// encrypted = SHA-1(token || serverSeed || clientSeed || userIDBytes || sequence)
	sequence := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	h.Reset()
	h.Write(token)
	h.Write(serverSeed)
	h.Write(clientSeed)
	h.Write(userIDBytes)
	h.Write(sequence)
	return h.Sum(nil), nil
}

// userIDToUTF16BE applies the EBCDIC -> Unicode -> UTF-16 BE pipeline
// JTOpen uses for userID hashing inputs. A pure-Unicode userID would
// round-trip identically, but going through CCSID 37 also pads to the
// 10-byte field IBM i expects.
func userIDToUTF16BE(userID string) ([]byte, error) {
	encoded, err := ebcdic.CCSID37.Encode(userID)
	if err != nil {
		return nil, fmt.Errorf("auth: encode user ID: %w", err)
	}
	if len(encoded) > 10 {
		return nil, fmt.Errorf("auth: user ID encodes to %d bytes (max 10)", len(encoded))
	}
	padded := make([]byte, 10)
	copy(padded, encoded)
	for i := len(encoded); i < 10; i++ {
		padded[i] = 0x40 // EBCDIC space
	}
	roundTrip, err := ebcdic.CCSID37.Decode(padded)
	if err != nil {
		return nil, fmt.Errorf("auth: decode user ID: %w", err)
	}
	return utf16BE(roundTrip), nil
}

// utf16BE encodes s as UTF-16 big-endian without a BOM.
func utf16BE(s string) []byte {
	codes := utf16.Encode([]rune(s))
	out := make([]byte, len(codes)*2)
	for i, c := range codes {
		out[2*i] = byte(c >> 8)
		out[2*i+1] = byte(c)
	}
	return out
}
