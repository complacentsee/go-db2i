package auth

import (
	"crypto/des"
	"fmt"

	"github.com/complacentsee/go-db2i/ebcdic"
)

// EncryptPasswordDES returns the 8-byte encrypted password the
// go-db2i client sends in a SignonInfoRequest when the server's
// password level is 0 or 1 (QPWDLVL=0 or 1, pre-V5R1 IBM i and
// modern systems running in legacy-compatible mode).
//
// Algorithm (mirrors AS400ImplRemote.encryptPassword + generateToken
// + generatePasswordSubstitute in JTOpen):
//
//	token = generateToken(userID, password)
//	    -- 8-byte DES key derived from EBCDIC userID + password.
//
//	encrypted = DES_chain(token, ...)
//	    -- 5 chained single-block DES encryptions with XOR
//	    -- mixing of userID, sequence, server seed, and client seed.
//
// The full chain is in the body comments. JT400 calls this a "MAC
// (DES)" but the per-step structure is custom -- not RFC 4493 nor a
// standard CBC-MAC. We mirror it byte-for-byte.
//
// userID and password come in as Go strings; this function encodes
// them through CCSID 37 (EBCDIC) and 0x40-pads to 10 bytes (the
// IBM i field width). Both must be at most 10 chars after EBCDIC
// encoding; longer inputs return an error.
//
// !!! NOT WIRE-VALIDATED !!! See warning.go. PUB400 is QPWDLVL=3 and
// will not issue level-0/1 challenges, so this implementation has
// only ever been exercised against JT400 source. DES is effectively
// dead on modern IBM i deployments; this code exists so the driver
// doesn't fail-fast against a legacy system, but consider it
// "best-effort spec implementation" rather than "shipped and tested."
func EncryptPasswordDES(userID, password string, clientSeed, serverSeed []byte) ([]byte, error) {
	if password == "" {
		return nil, fmt.Errorf("auth: password is empty")
	}
	if len(clientSeed) != 8 {
		return nil, fmt.Errorf("auth: clientSeed must be 8 bytes, got %d", len(clientSeed))
	}
	if len(serverSeed) != 8 {
		return nil, fmt.Errorf("auth: serverSeed must be 8 bytes, got %d", len(serverSeed))
	}

	unvalidatedAlgorithmWarning("DES (level 0/1)")

	userIDEbcdic, err := encodeEBCDICPadded10(userID)
	if err != nil {
		return nil, fmt.Errorf("auth: encode userID for DES: %w", err)
	}
	passwordEbcdic, err := encodeEBCDICPadded10(password)
	if err != nil {
		return nil, fmt.Errorf("auth: encode password for DES: %w", err)
	}

	token, err := generateDESToken(userIDEbcdic, passwordEbcdic)
	if err != nil {
		return nil, err
	}
	defer zero(token)
	defer zero(passwordEbcdic)

	return generateDESPasswordSubstitute(userIDEbcdic, token, clientSeed, serverSeed)
}

// generateDESToken derives the 8-byte DES key from a 10-byte EBCDIC
// userID and password. Mirrors JTOpen's generateToken().
//
// For passwords <= 8 chars: token = DES(shifted-password-XOR-0x55, userID-folded).
// For passwords > 8 chars:  token = DES(shifted-pwd[0:8], userID) ^ DES(shifted-pwd[8:], userID).
//
// userID > 8 chars is "folded": low 2 bytes are XOR-blended back into bytes 0..7.
func generateDESToken(userIDEbcdic, passwordEbcdic []byte) ([]byte, error) {
	workUserID := make([]byte, 10)
	copy(workUserID, userIDEbcdic)

	uidLen := ebcdicStrLen(workUserID)
	if uidLen > 8 {
		// Fold 2-byte tail back into the 8-byte head, 2 bits per
		// byte at staggered positions. Per JTOpen, this allows
		// 10-char user IDs to still produce an 8-byte DES key.
		workUserID[0] ^= workUserID[8] & 0xC0
		workUserID[1] ^= (workUserID[8] & 0x30) << 2
		workUserID[2] ^= (workUserID[8] & 0x0C) << 4
		workUserID[3] ^= (workUserID[8] & 0x03) << 6
		workUserID[4] ^= workUserID[9] & 0xC0
		workUserID[5] ^= (workUserID[9] & 0x30) << 2
		workUserID[6] ^= (workUserID[9] & 0x0C) << 4
		workUserID[7] ^= (workUserID[9] & 0x03) << 6
	}

	pwdLen := ebcdicStrLen(passwordEbcdic)

	if pwdLen <= 8 {
		work := make([]byte, 10)
		for i := range work {
			work[i] = 0x40
		}
		copy(work, passwordEbcdic[:pwdLen])
		xorWith0x55AndLshift(work[:8])
		return desEncryptBlock(work[:8], workUserID[:8])
	}

	// Long password path: split, encrypt each half, XOR the results.
	work1 := make([]byte, 10)
	work2 := make([]byte, 10)
	for i := range work1 {
		work1[i] = 0x40
		work2[i] = 0x40
	}
	copy(work1, passwordEbcdic[:8])
	copy(work2, passwordEbcdic[8:pwdLen])

	xorWith0x55AndLshift(work1[:8])
	half1, err := desEncryptBlock(work1[:8], workUserID[:8])
	if err != nil {
		return nil, err
	}
	xorWith0x55AndLshift(work2[:8])
	half2, err := desEncryptBlock(work2[:8], workUserID[:8])
	if err != nil {
		return nil, err
	}
	out := make([]byte, 8)
	for i := range out {
		out[i] = half1[i] ^ half2[i]
	}
	return out, nil
}

// generateDESPasswordSubstitute runs the 5-block chained-DES MAC
// JTOpen calls a "password substitute." Mirrors
// generatePasswordSubstitute() byte-for-byte. RDr = serverSeed,
// RDs = clientSeed, sequence is fixed {0,...,0,1}.
func generateDESPasswordSubstitute(userIDEbcdic, token, clientSeed, serverSeed []byte) ([]byte, error) {
	sequence := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	rdrSeq := make([]byte, 8)
	addArray8(sequence, serverSeed, rdrSeq)

	// Block 1: DES(token, RDrSeq).
	b1, err := desEncryptBlock(token, rdrSeq)
	if err != nil {
		return nil, err
	}

	// Block 2: DES(token, b1 ^ clientSeed) -- discarded (used for verify).
	tmp := make([]byte, 8)
	for i := range tmp {
		tmp[i] = b1[i] ^ clientSeed[i]
	}
	b2, err := desEncryptBlock(token, tmp)
	if err != nil {
		return nil, err
	}

	// Block 3: DES(token, userID[0:8] ^ RDrSeq ^ b2).
	for i := range tmp {
		tmp[i] = userIDEbcdic[i] ^ rdrSeq[i] ^ b2[i]
	}
	b3, err := desEncryptBlock(token, tmp)
	if err != nil {
		return nil, err
	}

	// Block 4: pad userID[8:] to 8 bytes (right-fill EBCDIC blanks),
	// then DES(token, padded ^ RDrSeq ^ b3).
	padded := []byte{0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40, 0x40}
	padded[0] = userIDEbcdic[8]
	padded[1] = userIDEbcdic[9]
	for i := range tmp {
		tmp[i] = padded[i] ^ rdrSeq[i] ^ b3[i]
	}
	b4, err := desEncryptBlock(token, tmp)
	if err != nil {
		return nil, err
	}

	// Block 5: DES(token, b4 ^ sequence) -- the encrypted password.
	for i := range tmp {
		tmp[i] = b4[i] ^ sequence[i]
	}
	return desEncryptBlock(token, tmp)
}

// desEncryptBlock encrypts one 8-byte block under the given 8-byte
// DES key. Go's crypto/des doesn't validate key parity, so the
// IBM-style derived keys (which haven't been parity-adjusted) work
// without a fixup pass.
func desEncryptBlock(key, plaintext []byte) ([]byte, error) {
	block, err := des.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("auth: DES key: %w", err)
	}
	out := make([]byte, 8)
	block.Encrypt(out, plaintext)
	return out, nil
}

// xorWith0x55AndLshift applies the password-pre-DES transform JTOpen
// uses: XOR each byte with 0x55, then shift the whole 8-byte word
// left by 1 bit (big-endian). This converts an EBCDIC password into
// something that survives DES's parity-bit dropping.
func xorWith0x55AndLshift(b []byte) {
	for i := range b {
		b[i] ^= 0x55
	}
	for i := 0; i < 7; i++ {
		b[i] = (b[i] << 1) | ((b[i+1] & 0x80) >> 7)
	}
	b[7] <<= 1
}

// addArray8 sets dst = a + b as an 8-byte big-endian unsigned
// integer with carry propagation. Wraps modulo 2^64. Mirrors
// JTOpen's addArray with length=8.
func addArray8(a, b, dst []byte) {
	carry := uint16(0)
	for i := 7; i >= 0; i-- {
		t := uint16(a[i]) + uint16(b[i]) + carry
		carry = t >> 8
		dst[i] = byte(t)
	}
}

// ebcdicStrLen returns the index of the first 0x40 (EBCDIC space)
// or 0x00 in b, capped at len(b). Mirrors JTOpen's ebcdicStrLen.
func ebcdicStrLen(b []byte) int {
	for i, c := range b {
		if c == 0x40 || c == 0x00 {
			return i
		}
	}
	return len(b)
}

// encodeEBCDICPadded10 returns the 10-byte EBCDIC (CCSID 37) encoding
// of s, right-padded with 0x40 if shorter. Returns an error if the
// EBCDIC encoding exceeds 10 bytes.
func encodeEBCDICPadded10(s string) ([]byte, error) {
	encoded, err := ebcdic.CCSID37.Encode(s)
	if err != nil {
		return nil, err
	}
	if len(encoded) > 10 {
		return nil, fmt.Errorf("auth: input %q encodes to %d EBCDIC bytes (max 10)", s, len(encoded))
	}
	out := make([]byte, 10)
	copy(out, encoded)
	for i := len(encoded); i < 10; i++ {
		out[i] = 0x40
	}
	return out, nil
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
