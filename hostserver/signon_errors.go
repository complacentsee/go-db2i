package hostserver

import (
	"errors"
	"fmt"
)

// SignonError wraps a non-zero ReturnCode from a sign-on reply
// (XChgRandSeed, ExchangeAttributes, SignonInfo) into a typed value
// callers can switch on without re-parsing magic numbers. Mirrors the
// subset of return codes JT400 surfaces via AS400SecurityException --
// not every documented code is enumerated here, but the common
// auth-failure paths (bad password, disabled profile, expired
// password) are spelled out so a real driver can map them to
// useful application-layer behaviour.
//
// The full IBM i sign-on RC vocabulary is documented in JTOpen's
// AS400ImplRemote.returnSecurityException; codes not in
// signonErrorMessages render as "unknown" with the raw hex.
type SignonError struct {
	// ReturnCode is the raw 4-byte RC the server reported.
	ReturnCode uint32
}

// Category returns the high 16 bits of ReturnCode -- the IBM i
// "error class" grouping. 0x0001 = bad request data, 0x0002 =
// userID problems, 0x0003 = password problems, 0x0004 = general
// security, etc.
func (e *SignonError) Category() uint16 {
	return uint16(e.ReturnCode >> 16)
}

// Subcode returns the low 16 bits of ReturnCode -- the per-category
// detail. e.g. category=0x0003 subcode=0x000B = "password incorrect."
func (e *SignonError) Subcode() uint16 {
	return uint16(e.ReturnCode)
}

func (e *SignonError) Error() string {
	if msg := signonErrorMessage(e.ReturnCode); msg != "" {
		return fmt.Sprintf("hostserver: signon RC=0x%08X: %s", e.ReturnCode, msg)
	}
	return fmt.Sprintf("hostserver: signon RC=0x%08X (category=0x%04X subcode=0x%04X, see JTOpen AS400SecurityException for full vocabulary)",
		e.ReturnCode, e.Category(), e.Subcode())
}

// Sentinel errors for the most common auth-failure paths. Callers
// can use errors.Is(err, ErrPasswordIncorrect) without parsing the
// raw RC.
var (
	ErrUserIDUnknown      = errors.New("user ID not found on system")
	ErrUserIDDisabled     = errors.New("user ID disabled")
	ErrPasswordIncorrect  = errors.New("password incorrect")
	ErrPasswordExpired    = errors.New("password expired")
	ErrPasswordIsNone     = errors.New("password is *NONE")
	ErrUserIDLengthBad    = errors.New("user ID length not valid")
	ErrPasswordLengthBad  = errors.New("password length not valid")
	ErrSignonRequestBad   = errors.New("sign-on request not valid")
	ErrPasswordWillExpire = errors.New("password incorrect; profile will be disabled on next attempt")
)

// Unwrap lets errors.Is / errors.As walk through to the sentinel
// for the categorised codes. Returns nil for codes without a
// dedicated sentinel.
func (e *SignonError) Unwrap() error {
	switch e.ReturnCode {
	case 0x00020001:
		return ErrUserIDUnknown
	case 0x00020002:
		return ErrUserIDDisabled
	case 0x0003000B:
		return ErrPasswordIncorrect
	case 0x0003000C:
		return ErrPasswordWillExpire
	case 0x0003000D:
		return ErrPasswordExpired
	case 0x00030010:
		return ErrPasswordIsNone
	case 0x00010007:
		return ErrUserIDLengthBad
	case 0x00010008:
		return ErrPasswordLengthBad
	case 0x0001000B:
		return ErrSignonRequestBad
	}
	return nil
}

// signonErrorMessage maps a sign-on RC to a short human-readable
// description. Empty string for codes we don't enumerate (caller
// renders as "unknown" with the raw hex).
func signonErrorMessage(rc uint32) string {
	switch rc {
	// 0x0001xxxx: request data / protocol problems
	case 0x00010001:
		return "invalid exchange-random-seeds request"
	case 0x00010002:
		return "invalid server ID"
	case 0x00010003:
		return "invalid request ID"
	case 0x00010004:
		return "invalid random seed (zero or too large)"
	case 0x00010005:
		return "random seed required when doing password substitution"
	case 0x00010006:
		return "invalid password encrypt indicator"
	case 0x00010007:
		return "invalid user ID length"
	case 0x00010008:
		return "invalid password length"
	case 0x00010009, 0x0001000A:
		return "request data error (invalid client version, DS level, or missing required field)"
	case 0x0001000B:
		return "invalid sign-on request (missing user ID, password, or token)"

	// 0x0002xxxx: user ID problems
	case 0x00020001:
		return "user ID not found on system"
	case 0x00020002:
		return "user ID disabled"
	case 0x00020003:
		return "user profile mismatch"

	// 0x0003xxxx: password problems
	case 0x0003000B:
		return "password incorrect"
	case 0x0003000C:
		return "password incorrect; profile will be disabled on next attempt"
	case 0x0003000D:
		return "password expired"
	case 0x0003000E:
		return "pre-V2R2 encrypted password (legacy DES-only)"
	case 0x00030010:
		return "password is *NONE (account cannot sign on)"
	case 0x00030011:
		return "password validation program rejected the request"
	case 0x00030012:
		return "password change not allowed at this time"

	// 0x0004xxxx: general security
	case 0x00040000:
		return "general security error (often QUSER password expired, CCSID issue, or QSYGETPH/QWTSETP failure)"
	}
	return ""
}

// wrapSignonRC returns a *SignonError if rc is non-zero, else nil.
// Convenience helper for callers that want one-line "if err :=
// wrapSignonRC(rep.ReturnCode); err != nil { return err }" usage.
func wrapSignonRC(rc uint32) error {
	if rc == 0 {
		return nil
	}
	return &SignonError{ReturnCode: rc}
}
