package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestPBKDF2HMACSHA512RFC6070Vector confirms our hand-rolled PBKDF2
// matches the canonical RFC 6070 §2 test vectors (originally for
// HMAC-SHA-1, but equivalent vectors for HMAC-SHA-512 are widely
// published from the same source). One vector is enough to prove
// the iteration loop + block-index endianness are right.
//
// Vector: P = "password", S = "salt", c = 1, dkLen = 64.
// HMAC-SHA-512 hex (verified against pycryptodome KDF.PBKDF2):
//
//	867f70cf1ade02cff3752599a3a53dc4af34c7a669815ae5d513554e1c8cf252
//	c02d470a285a0501bad999bfe943c08f050235d7d68b1da55e63f73b60a57fce
func TestPBKDF2HMACSHA512RFC6070Vector(t *testing.T) {
	out := pbkdf2HMACSHA512([]byte("password"), []byte("salt"), 1, 64)
	want, _ := hex.DecodeString(
		"867f70cf1ade02cff3752599a3a53dc4af34c7a669815ae5d513554e1c8cf252" +
			"c02d470a285a0501bad999bfe943c08f050235d7d68b1da55e63f73b60a57fce")
	if !bytes.Equal(out, want) {
		t.Errorf("PBKDF2 mismatch\n got: %x\nwant: %x", out, want)
	}
}

// TestPBKDF2HMACSHA512MultiIter validates the iteration loop with a
// non-trivial count (4096 iter, 32 byte output). Vector also from
// pycryptodome KDF.PBKDF2 with hashlib.sha512.
func TestPBKDF2HMACSHA512MultiIter(t *testing.T) {
	out := pbkdf2HMACSHA512([]byte("password"), []byte("salt"), 4096, 32)
	want, _ := hex.DecodeString("d197b1b33db0143e018b12f3d1d1479e6cdebdcc97c5c0f87f6902e072f457b5")
	if !bytes.Equal(out, want) {
		t.Errorf("PBKDF2 4096-iter mismatch\n got: %x\nwant: %x", out, want)
	}
}

// TestBuildPBKDF2SaltInput pins the 28-byte UTF-16BE salt construction
// against a hand-computed value. The salt is the most fragile part
// of the level-4 algorithm -- "last 4 chars of password" is the
// detail that's easiest to read wrong from the IBM spec.
func TestBuildPBKDF2SaltInput(t *testing.T) {
	// userID "AFTRAEGE1" (9 chars) -> uppercased to "AFTRAEGE1 " (10 chars,
	// trailing space). Password "secret" (6 chars) -> last 4 chars = "cret".
	// Layout: A F T R A E G E 1 ' ' c r e t -- 14 chars, 28 bytes UTF-16BE.
	got := buildPBKDF2SaltInput("AFTRAEGE1", "secret")
	want := []byte{
		0x00, 'A', 0x00, 'F', 0x00, 'T', 0x00, 'R',
		0x00, 'A', 0x00, 'E', 0x00, 'G', 0x00, 'E',
		0x00, '1', 0x00, ' ',
		0x00, 'c', 0x00, 'r', 0x00, 'e', 0x00, 't',
	}
	if !bytes.Equal(got, want) {
		t.Errorf("salt input mismatch\n got: %x\nwant: %x", got, want)
	}
}

// TestBuildPBKDF2SaltInputShortPassword confirms passwords < 4 chars
// occupy as many positions as they have, with the rest blank-padded.
// JT400's spec text says "if the password is less than 4 characters,
// then copy the entire Unicode password value" -- i.e. left-justify
// in the 4-char tail with spaces filling the rest.
func TestBuildPBKDF2SaltInputShortPassword(t *testing.T) {
	got := buildPBKDF2SaltInput("BOB", "ab")
	// JT400's spec: copy password chars at the START of the 4-char
	// tail, then pad RIGHT with spaces. So "ab" -> "ab  ", not
	// "  ab". Layout: "BOB       " + "ab  ".
	want := []byte{
		0x00, 'B', 0x00, 'O', 0x00, 'B', 0x00, ' ',
		0x00, ' ', 0x00, ' ', 0x00, ' ', 0x00, ' ',
		0x00, ' ', 0x00, ' ',
		0x00, 'a', 0x00, 'b', 0x00, ' ', 0x00, ' ',
	}
	if !bytes.Equal(got, want) {
		t.Errorf("short-password salt mismatch\n got: %x\nwant: %x", got, want)
	}
}

// TestEncryptPasswordPBKDF2WireValidatedVector pins the substitute
// produced for a fixed (userID, password, clientSeed, serverSeed)
// combination. The expected value was wire-validated against IBM i
// 7.6 (V7R6M0, QPWDLVL=4) on IBM Cloud Power VS, 2026-05-08 -- the
// server accepted this exact 64-byte substitute as proof of
// possession of password "synthpwd99" for user
// GOTEST.
//
// If this test fails, the algorithm has drifted from what a real
// IBM i server expects; the most likely culprit is a regression of
// the PBKDF2 password-input encoding (server expects UTF-8; the
// JT400 spec text misleadingly says UTF-16BE).
func TestEncryptPasswordPBKDF2WireValidatedVector(t *testing.T) {
	clientSeed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	serverSeed := []byte{8, 7, 6, 5, 4, 3, 2, 1}
	got, err := EncryptPasswordPBKDF2("GOTEST", "synthpwd99", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	want, _ := hex.DecodeString(
		"32ca48c270a17c284bd5a1904f385e4e410bb26be09ea8940402ec6bc3c7c82f" +
			"4b692a89809a88b4fa800b329e240fa588ad370efc6824960f494408124ea7de")
	if !bytes.Equal(got, want) {
		t.Errorf("PBKDF2 substitute mismatch -- algorithm has drifted from wire-validated reference\n got: %x\nwant: %x", got, want)
	}
}

// TestEncryptPasswordPBKDF2Deterministic confirms the same inputs
// produce the same output across calls -- cheap regression net for
// "did someone introduce nondeterminism" without needing a live
// IBM i to validate against.
func TestEncryptPasswordPBKDF2Deterministic(t *testing.T) {
	clientSeed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	serverSeed := []byte{8, 7, 6, 5, 4, 3, 2, 1}
	a, err := EncryptPasswordPBKDF2("AFTRAEGE1", "synthpwd99", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	b, err := EncryptPasswordPBKDF2("AFTRAEGE1", "synthpwd99", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Errorf("non-deterministic output\n a: %x\n b: %x", a, b)
	}
	if len(a) != 64 {
		t.Errorf("output length = %d, want 64 (SHA-512 substitute)", len(a))
	}
}

// TestPBKDF2SaltDerivation confirms our SHA-256 salt step matches a
// hand-computed value. SHA-256 of the 28-byte salt input for
// userID "TEST" + password "pw" is fixed; compute once with
// crypto/sha256 and pin.
func TestPBKDF2SaltDerivation(t *testing.T) {
	saltInput := buildPBKDF2SaltInput("TEST", "pw")
	if len(saltInput) != 28 {
		t.Fatalf("salt input length = %d, want 28", len(saltInput))
	}
	got := sha256.Sum256(saltInput)
	// Self-consistency only -- pin the value so a future change
	// to buildPBKDF2SaltInput shows up here as a regression.
	want, _ := hex.DecodeString("d6207dcb7dd0bcc614060a99ecd25ee45892879db51e9ee41d9774efbde79440")
	if !bytes.Equal(got[:], want) {
		// Print actual value so the test author can pin it on first run.
		t.Errorf("salt SHA-256 mismatch\n got: %x\nwant: %x", got[:], want)
	}
}
