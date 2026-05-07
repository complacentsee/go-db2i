package auth

import (
	"bytes"
	"crypto/sha1"
	"testing"
)

// TestEncryptPasswordSHA1Determinism checks the basic shape -- 20
// bytes out, deterministic for the same inputs, different output for
// different inputs. We can't assert the actual encrypted bytes here
// without leaking a real PUB400 password; the smoketest will validate
// against a live server when M1 reaches that step.
func TestEncryptPasswordSHA1Determinism(t *testing.T) {
	clientSeed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	serverSeed := []byte{8, 7, 6, 5, 4, 3, 2, 1}

	got1, err := EncryptPasswordSHA1("AFTRAEGE1", "test-password", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("EncryptPasswordSHA1: %v", err)
	}
	if len(got1) != 20 {
		t.Errorf("encrypted password = %d bytes, want 20", len(got1))
	}

	got2, err := EncryptPasswordSHA1("AFTRAEGE1", "test-password", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if !bytes.Equal(got1, got2) {
		t.Error("EncryptPasswordSHA1 not deterministic: same inputs gave different outputs")
	}

	// Different password -> different output.
	got3, err := EncryptPasswordSHA1("AFTRAEGE1", "test-password2", clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("third call: %v", err)
	}
	if bytes.Equal(got1, got3) {
		t.Error("different passwords produced identical encryption (collision or bug)")
	}
}

func TestEncryptPasswordSHA1RejectsBadInputs(t *testing.T) {
	clientSeed := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	serverSeed := []byte{8, 7, 6, 5, 4, 3, 2, 1}

	cases := []struct {
		name              string
		userID, password  string
		client, server    []byte
		wantErr           bool
	}{
		{"empty password", "USER", "", clientSeed, serverSeed, true},
		{"star password", "USER", "*BLANK", clientSeed, serverSeed, true},
		{"short client seed", "USER", "secret", []byte{1, 2}, serverSeed, true},
		{"short server seed", "USER", "secret", clientSeed, []byte{1, 2}, true},
		{"valid", "USER", "secret", clientSeed, serverSeed, false},
	}
	for _, c := range cases {
		_, err := EncryptPasswordSHA1(c.userID, c.password, c.client, c.server)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", c.name, err, c.wantErr)
		}
	}
}

// TestEncryptPasswordSHA1MatchesHandComputed validates the algorithm
// against an independent implementation: we manually compute the two
// SHA-1 hashes JTOpen specifies, then compare to what
// EncryptPasswordSHA1 produced. This catches accidental algorithm
// drift (e.g., wrong concatenation order, wrong sequence bytes)
// without needing a real IBM i.
func TestEncryptPasswordSHA1MatchesHandComputed(t *testing.T) {
	const userID = "USER" // 4 chars; gets padded to 10 with EBCDIC spaces
	const password = "secret"
	clientSeed := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11}
	serverSeed := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}

	// Hand-compute userID-utf16be padded to 10 chars.
	userIDPadded := []byte{
		0, 'U', 0, 'S', 0, 'E', 0, 'R',
		0, ' ', 0, ' ', 0, ' ', 0, ' ',
		0, ' ', 0, ' ',
	}
	passwordBytes := []byte{0, 's', 0, 'e', 0, 'c', 0, 'r', 0, 'e', 0, 't'}
	sequence := []byte{0, 0, 0, 0, 0, 0, 0, 1}

	h := sha1.New()
	h.Write(userIDPadded)
	h.Write(passwordBytes)
	token := h.Sum(nil)

	h.Reset()
	h.Write(token)
	h.Write(serverSeed)
	h.Write(clientSeed)
	h.Write(userIDPadded)
	h.Write(sequence)
	want := h.Sum(nil)

	got, err := EncryptPasswordSHA1(userID, password, clientSeed, serverSeed)
	if err != nil {
		t.Fatalf("EncryptPasswordSHA1: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("\n got:  %x\nwant: %x", got, want)
	}
}
