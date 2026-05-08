package hostserver

import (
	"errors"
	"strings"
	"testing"
)

// TestWrapSignonRCNilOnZero confirms the convenience wrapper turns
// success (RC=0) into nil so callers can use `if err := wrap...; err != nil`
// without thinking about the zero case.
func TestWrapSignonRCNilOnZero(t *testing.T) {
	if got := wrapSignonRC(0); got != nil {
		t.Errorf("wrapSignonRC(0) = %v, want nil", got)
	}
}

// TestSignonErrorCategoryAndSubcode pins the high/low 16-bit split
// against a known RC layout. 0x0003000B = "password incorrect" --
// category 3 (password), subcode 0x000B.
func TestSignonErrorCategoryAndSubcode(t *testing.T) {
	e := &SignonError{ReturnCode: 0x0003000B}
	if got := e.Category(); got != 0x0003 {
		t.Errorf("Category = 0x%04X, want 0x0003", got)
	}
	if got := e.Subcode(); got != 0x000B {
		t.Errorf("Subcode = 0x%04X, want 0x000B", got)
	}
}

// TestSignonErrorMessageKnown checks a handful of common codes
// produce the documented message string. The exact wording isn't
// load-bearing, so we use substring matches against the IBM-i
// documented terms.
func TestSignonErrorMessageKnown(t *testing.T) {
	cases := []struct {
		rc       uint32
		contains string
	}{
		{0x00020001, "user ID not found"},
		{0x00020002, "user ID disabled"},
		{0x0003000B, "password incorrect"},
		{0x0003000D, "password expired"},
		{0x00030010, "*NONE"},
		{0x0001000B, "invalid sign-on request"},
		{0x00040000, "general security error"},
	}
	for _, tc := range cases {
		e := &SignonError{ReturnCode: tc.rc}
		got := e.Error()
		if !strings.Contains(got, tc.contains) {
			t.Errorf("Error() for RC=0x%08X = %q, want contains %q",
				tc.rc, got, tc.contains)
		}
	}
}

// TestSignonErrorMessageUnknown confirms unknown codes get the
// fallback rendering (raw hex + category/subcode breakdown). This
// is the path real production sees most often -- the IBM i RC
// vocabulary is large and only the common ones are enumerated.
func TestSignonErrorMessageUnknown(t *testing.T) {
	e := &SignonError{ReturnCode: 0x07FF1234}
	msg := e.Error()
	if !strings.Contains(msg, "0x07FF1234") {
		t.Errorf("Error() should include raw RC hex, got %q", msg)
	}
	if !strings.Contains(msg, "category=0x07FF") || !strings.Contains(msg, "subcode=0x1234") {
		t.Errorf("Error() should include category/subcode breakdown, got %q", msg)
	}
}

// TestSignonErrorIsSentinel exercises errors.Is matching against the
// exported sentinels. This is the API a real driver/caller uses to
// branch on common auth failures without parsing the RC.
func TestSignonErrorIsSentinel(t *testing.T) {
	cases := []struct {
		rc       uint32
		sentinel error
	}{
		{0x0003000B, ErrPasswordIncorrect},
		{0x00020001, ErrUserIDUnknown},
		{0x00020002, ErrUserIDDisabled},
		{0x0003000D, ErrPasswordExpired},
		{0x00030010, ErrPasswordIsNone},
	}
	for _, tc := range cases {
		err := wrapSignonRC(tc.rc)
		if !errors.Is(err, tc.sentinel) {
			t.Errorf("errors.Is(wrapSignonRC(0x%08X), %v) = false, want true",
				tc.rc, tc.sentinel)
		}
	}
}

// TestSignonErrorIsNoSentinelForUnknown confirms Unwrap returns nil
// (rather than a stub error) for codes without a dedicated sentinel,
// so errors.Is doesn't false-positive against arbitrary sentinels.
func TestSignonErrorIsNoSentinelForUnknown(t *testing.T) {
	err := wrapSignonRC(0x07FF1234)
	if errors.Is(err, ErrPasswordIncorrect) {
		t.Errorf("errors.Is should not match arbitrary RC against ErrPasswordIncorrect")
	}
}
