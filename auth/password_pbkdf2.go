package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"strings"
)

// EncryptPasswordPBKDF2 returns the 64-byte encrypted password the
// goJTOpen client sends in a SignonInfoRequest when the server's
// password level is 4 (QPWDLVL=4, V7R1+).
//
// Algorithm (mirrors AS400ImplRemote.generatePwdTokenForPasswordLevel4 +
// generateSha512Substitute in JTOpen):
//
//  1. Build a 14-character salt input:
//     a. Uppercase the userID, blank-pad to 10 chars (first 10 chars).
//     b. Append the LAST 4 chars of the password (right-padded to 4
//        with spaces if shorter).
//  2. UTF-16 BE encode -> 28 bytes.
//  3. SHA-256(28-byte input) -> 32-byte salt.
//  4. PBKDF2-HMAC-SHA-512(password = UTF-8(password),
//                         salt = 32-byte salt,
//                         iter = 10022,
//                         keyLen = 64) -> 64-byte token PW_TOKEN.
//     NOTE the UTF-8 here -- JT400's spec comment says "Unicode" but
//     the actual Java implementation goes through PBEKeySpec, whose
//     PBKDF2KeyImpl encodes the char[] as UTF-8 (sun.security.pkcs.PBEKey
//     pre-Java-8 was Latin-1; current behaviour is UTF-8). The IBM i
//     server matches that, not the spec text. Going UTF-16BE here
//     produces the right SHAPE of bytes but the wrong VALUE -- server
//     returns SQL -3008 / errClass 8 ("password incorrect"). Live-
//     validated against IBM i 7.6 on IBM Cloud Power VS, 2026-05-08.
//  5. PW_SUB = SHA-512(PW_TOKEN || serverSeed || clientSeed
//                      || UTF-16BE(10-char-padded-userID) || sequence).
//     UTF-16BE here is correct (matches both spec and JT400's actual
//     getBytes("utf-16be") call).
//
// sequence is the literal {0,0,0,0,0,0,0,1}.
func EncryptPasswordPBKDF2(userID, password string, clientSeed, serverSeed []byte) ([]byte, error) {
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

	// PBKDF2 is now wire-validated (see commit history); no warning
	// needed. DES (levels 0/1) is still spec-only -- see
	// EncryptPasswordDES.

	// Step 1+2: build 14-char salt input, UTF-16BE encode.
	saltInput := buildPBKDF2SaltInput(userID, password)

	// Step 3: SHA-256 -> 32-byte salt.
	saltDigest := sha256.Sum256(saltInput)
	salt := saltDigest[:]

	// Step 4: PBKDF2-HMAC-SHA-512, 10022 iterations, 64-byte output.
	// JT400's spec comment says "Data = Unicode password value" but
	// the actual Java implementation uses PBEKeySpec, whose
	// PBKDF2KeyImpl.getPasswordBytes() encodes the char[] as UTF-8.
	// The IBM i server matches that, not the spec text -- so we
	// pass UTF-8 here, not UTF-16BE. (UTF-16BE is still correct for
	// the salt construction in step 1+2 above.) Live-validated 2026-05-08.
	pwToken := pbkdf2HMACSHA512([]byte(password), salt, 10022, 64)

	// Step 5: SHA-512 substitute.
	userIDBytes, err := paddedUserIDUTF16BE(userID)
	if err != nil {
		return nil, err
	}
	sequence := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	h := sha512.New()
	h.Write(pwToken)
	h.Write(serverSeed)
	h.Write(clientSeed)
	h.Write(userIDBytes)
	h.Write(sequence)
	return h.Sum(nil), nil
}

// buildPBKDF2SaltInput constructs the 28-byte UTF-16BE salt input
// per JT400's generateSaltForPasswordLevel4. The 14-char layout:
//
//	bytes 0-19   : uppercase userID, blank-padded to 10 chars (UTF-16BE)
//	bytes 20-27  : last 4 chars of password, blank-padded to 4 (UTF-16BE)
//
// JT400 explicitly takes the LAST 4 chars of the password, not the
// first 4 -- this is documented in the IBM password-level-4 spec and
// is the easiest detail to get wrong by inspection.
func buildPBKDF2SaltInput(userID, password string) []byte {
	saltChars := make([]rune, 14)
	for i := range saltChars {
		saltChars[i] = ' '
	}
	upper := strings.ToUpper(userID)
	upperRunes := []rune(upper)
	if len(upperRunes) > 10 {
		upperRunes = upperRunes[:10]
	}
	copy(saltChars[:10], upperRunes)

	pwRunes := []rune(password)
	if len(pwRunes) > 0 {
		start := len(pwRunes) - 4
		if start < 0 {
			start = 0
		}
		// Copy the last 4 chars (or fewer) into positions 10..13.
		copy(saltChars[10:14], pwRunes[start:])
	}

	return utf16BERunes(saltChars)
}

// paddedUserIDUTF16BE returns the 20-byte UTF-16BE encoding of the
// userID, **uppercased** and padded to 10 chars with U+0020 spaces.
// JT400's generateSha512Substitute uses this exact encoding for the
// userID component of the substitute hash; JT400 normalises userId_
// to upper case at AS400.connect() (see AS400.java toUpperCase
// calls). We do the equivalent here so callers can pass any case
// and the salt+substitute paths see consistent inputs.
func paddedUserIDUTF16BE(userID string) ([]byte, error) {
	runes := []rune(strings.ToUpper(userID))
	if len(runes) > 10 {
		return nil, fmt.Errorf("auth: userID %q is %d chars (max 10)", userID, len(runes))
	}
	padded := make([]rune, 10)
	for i := range padded {
		padded[i] = ' '
	}
	copy(padded, runes)
	return utf16BERunes(padded), nil
}

// utf16BERunes encodes a slice of runes as UTF-16 BE without a BOM.
// All inputs to this file are BMP-only (ASCII or EBCDIC-derived
// padding), so we don't bother with surrogate pairs -- if a future
// caller needs them, switch to unicode/utf16.Encode + manual BE
// emission.
func utf16BERunes(runes []rune) []byte {
	out := make([]byte, len(runes)*2)
	for i, r := range runes {
		out[2*i] = byte(r >> 8)
		out[2*i+1] = byte(r)
	}
	return out
}

// pbkdf2HMACSHA512 derives keyLen bytes from password+salt using
// PBKDF2 with HMAC-SHA-512 per RFC 8018 §5.2. Hand-rolled to avoid
// pulling in golang.org/x/crypto for one function; equivalent to
// pbkdf2.Key(password, salt, iter, keyLen, sha512.New).
func pbkdf2HMACSHA512(password, salt []byte, iter, keyLen int) []byte {
	hashLen := sha512.Size // 64
	numBlocks := (keyLen + hashLen - 1) / hashLen
	out := make([]byte, 0, numBlocks*hashLen)
	prf := hmac.New(sha512.New, password)
	for block := 1; block <= numBlocks; block++ {
		out = append(out, pbkdf2Block(prf, salt, iter, block)...)
	}
	return out[:keyLen]
}

// pbkdf2Block computes one PBKDF2 output block: F(P, S, c, i).
// `prf` is reset and re-keyed by HMAC under the hood.
func pbkdf2Block(prf hash.Hash, salt []byte, iter, blockIdx int) []byte {
	// U_1 = PRF(P, S || INT(i))
	prf.Reset()
	prf.Write(salt)
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], uint32(blockIdx))
	prf.Write(idx[:])
	u := prf.Sum(nil)
	out := make([]byte, len(u))
	copy(out, u)
	for j := 1; j < iter; j++ {
		prf.Reset()
		prf.Write(u)
		u = prf.Sum(nil)
		for k := range out {
			out[k] ^= u[k]
		}
	}
	return out
}
