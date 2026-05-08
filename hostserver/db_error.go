package hostserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// Db2Error is the typed form of a server-side SQL error. Callers can
// `errors.As(err, &dbErr)` and inspect SQLState / SQLCode to drive
// retry / classification logic instead of regexing the formatted
// string.
//
// SQLState is the 5-character SQLSTATE the IBM i Db2 returns; "00000"
// means "no error" but we never construct a Db2Error in that case.
// "01xxx" is a warning (we don't currently surface those as errors --
// SelectStaticSQL etc. swallow them via isSQLWarning). "02000" is the
// not-found / end-of-data sentinel and we generally swallow that too,
// but exposed as IsNotFound() for the rare caller that wants to see
// it directly.
//
// SQLCode is the IBM SQL error code (negative = error, 0 = ok,
// positive = warning). Examples relevant to labelverification-gw:
//   -204  table not found
//   -206  column not found
//   -302  numeric value out of range / truncation
//   -407  null insert into NOT NULL column
//   -530  foreign-key violation
//   -803  unique-key violation (duplicate row)
//   -911  deadlock / lock timeout
//   -913  unsuccessful execution / lock timeout warning
//
// MessageID is the 8-char IBM message identifier (e.g. "SQL0204")
// pulled from the SQLERRP slot. Useful for log greping but not part
// of the standard exception interface.
//
// ErrorClass is the legacy DB-reply error class (0=ok, 1=warning,
// 2=application/SQL error, 3+=system errors). Surface for parity
// with the prior fmt.Errorf format; new callers should prefer
// SQLState / SQLCode predicates.
//
// MessageTokens are the substitution variables the server inserts
// into the message text (table name, column name, etc.). Each token
// is a Go string already EBCDIC-decoded. Empty when the server didn't
// supply any (DDL errors, system errors).
type Db2Error struct {
	SQLState      string
	SQLCode       int32
	MessageID     string
	Message       string
	ErrorClass    uint16
	MessageTokens []string

	// Op identifies the host-server operation that produced the
	// error (e.g. "PREPARE_DESCRIBE", "EXECUTE", "FETCH"). Set by
	// the wrapping helper at each call site so log lines can point
	// to the right wire frame.
	Op string
}

// Error formats the error in a single-line, log-friendly way:
//
//	hostserver: <Op> SQL<sqlcode> <sqlstate> <message-id>: <message>
//
// When Message is empty (server returned only the codes), falls back
// to a numeric form. Designed to be readable and grep-friendly; do
// not parse this string -- use the typed fields instead.
func (e *Db2Error) Error() string {
	var b strings.Builder
	b.WriteString("hostserver: ")
	if e.Op != "" {
		b.WriteString(e.Op)
		b.WriteString(" ")
	}
	fmt.Fprintf(&b, "SQL%d %s", e.SQLCode, e.SQLState)
	if e.MessageID != "" {
		fmt.Fprintf(&b, " %s", e.MessageID)
	}
	if e.ErrorClass != 0 {
		fmt.Fprintf(&b, " errorClass=0x%04X", e.ErrorClass)
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	return b.String()
}

// IsNotFound reports SQLSTATE 02xxx (no-data) or SQLCODE +100. Engines
// can scan/QueryRow loops can use this to distinguish "no row matched"
// from a real error without looking at sql.ErrNoRows.
func (e *Db2Error) IsNotFound() bool {
	return e.SQLCode == 100 || strings.HasPrefix(e.SQLState, "02")
}

// IsConstraintViolation reports SQLSTATE 23xxx (integrity constraint
// violation) -- duplicate keys, foreign-key violations, NOT NULL
// violations, check-constraint failures. Engine retry logic should
// generally surface these to the caller rather than retry.
func (e *Db2Error) IsConstraintViolation() bool {
	return strings.HasPrefix(e.SQLState, "23")
}

// IsConnectionLost reports SQLSTATE 08xxx (connection exception) --
// server dropped the connection, can't reach the DB, etc. Callers
// should treat the underlying database/sql conn as dead and let the
// pool create a fresh one.
func (e *Db2Error) IsConnectionLost() bool {
	return strings.HasPrefix(e.SQLState, "08")
}

// IsLockTimeout reports SQLCODE -911 / -913 (deadlock or lock
// timeout). Retry-friendly with backoff -- distinct from a real
// constraint violation.
func (e *Db2Error) IsLockTimeout() bool {
	return e.SQLCode == -911 || e.SQLCode == -913
}

// makeDb2Error builds a *Db2Error from a parsed reply, decoding the
// CP 0x3807 SQLCA when present. Returns nil for success (rc == 0)
// and for any positive return code that isSQLWarning recognises --
// the latter regardless of ErrorClass, because the IBM i server
// frequently flags warnings (SQL +100 / +700 / +8001) with a
// non-zero error class even when the operation succeeded. Callers
// that need to act on a specific warning (e.g. EXECUTE_IMMEDIATE
// special-casing +100 to return rows-affected=0) should check rc
// before calling this.
//
// op is the operation label (e.g. "PREPARE_DESCRIBE") embedded in
// the formatted Error() string and on the returned Db2Error.Op for
// callers that want to dispatch on it.
func makeDb2Error(rep *DBReply, op string) *Db2Error {
	rc := int32(rep.ReturnCode)
	if rc == 0 || isSQLWarning(rep.ReturnCode) {
		return nil
	}
	e := &Db2Error{
		SQLCode:    rc,
		ErrorClass: rep.ErrorClass,
		Op:         op,
	}
	for _, p := range rep.Params {
		if p.CodePoint == cpDBSQLCA {
			fillSQLCAFields(e, p.Data)
			break
		}
	}
	return e
}

// fillSQLCAFields decodes the JT400 SQLCA layout (DBReplySQLCA) into
// e. Layout (offsets into CP 0x3807 data):
//
//	0..5    SQLCAID + Eyecatcher bits
//	6..7    SQLCABC (SQLCA byte count)  -- if length <= 6 the rest is empty
//	8..11   SQLCABC tail (Java reads it)
//	12..15  SQLCODE                       (int32 BE)
//	16..17  SQLERRML                      (uint16 BE; ERRMC byte length)
//	18..(18+SQLERRML-1)
//	         SQLERRMC                      (variable EBCDIC: token list,
//	                                       each tagged with its 2-byte
//	                                       length prefix per JT400's
//	                                       getErrmc(int) walker)
//	88..95   SQLERRP                       (8 EBCDIC bytes; message ID
//	                                       prefix like "SQL0204 ")
//	96..119  SQLERRD[1..6]                 (six int32 BE -- rows-affected
//	                                       at SQLERRD[2], offset 100)
//	120..130 SQLWARN[0..10]               (11 single-char warning flags)
//	131..135 SQLSTATE                      (5 EBCDIC bytes)
//
// Empty / absent SQLCA leaves e fields zero-valued; callers can still
// see SQLCode / ErrorClass from the template.
func fillSQLCAFields(e *Db2Error, data []byte) {
	if len(data) < 16 {
		return
	}
	be := binary.BigEndian
	// SQLCABC at offset 6 -- if zero/short, nothing past byte 6.
	// Mirror JT400's "length <= 6" guard.
	if len(data) <= 6 {
		return
	}
	// SQLCODE -- prefer the SQLCA-supplied value when present;
	// the template's ReturnCode usually matches but the SQLCA is
	// authoritative for cases where the server bumps the code in
	// post-execute logic.
	if len(data) >= 16 {
		if v := int32(be.Uint32(data[12:16])); v != 0 || e.SQLCode == 0 {
			e.SQLCode = v
		}
	}
	// SQLERRP (message id prefix) -- 8 bytes EBCDIC starting at 88.
	if len(data) >= 96 {
		if id, err := ebcdic.CCSID37.Decode(data[88:96]); err == nil {
			e.MessageID = strings.TrimRight(id, " ")
		}
	}
	// SQLSTATE -- 5 EBCDIC bytes at offset 131.
	if len(data) >= 136 {
		if s, err := ebcdic.CCSID37.Decode(data[131:136]); err == nil {
			e.SQLState = strings.TrimRight(s, " ")
		}
	}
	// SQLERRMC -- variable-length token list at offset 18.
	if len(data) >= 18 {
		errml := int(be.Uint16(data[16:18]))
		if errml > 0 && 18+errml <= len(data) {
			e.MessageTokens, e.Message = parseSQLERRMC(data[18 : 18+errml])
		}
	}
}

// parseSQLERRMC walks the SQLERRMC token list. JT400's
// DBReplySQLCA.getErrmc(int n) walks tokens by reading a 2-byte
// length prefix and that-many EBCDIC bytes per token. When no length
// prefixes are present (some V5R3- replies), the entire ERRMC is
// returned as a single message string -- detect by checking whether
// the first 2 bytes form a plausible length within the buffer.
//
// Returns (tokens, joinedMessage) where joinedMessage is the tokens
// concatenated with " | " separators -- fine for log lines, callers
// that need structure should use the tokens slice directly.
func parseSQLERRMC(data []byte) ([]string, string) {
	if len(data) < 2 {
		s, _ := ebcdic.CCSID37.Decode(data)
		s = strings.TrimRight(s, " \x00")
		if s == "" {
			return nil, ""
		}
		return []string{s}, s
	}
	// Heuristic for the "no length prefixes" flavour: if the first
	// 2 bytes treated as a length would consume the entire buffer
	// (modulo trailing space padding), it's a tagged token list of
	// one entry. Otherwise treat the whole thing as a single
	// pre-decoded EBCDIC string -- matches what the older capture
	// fixtures show.
	be := binary.BigEndian
	first := int(be.Uint16(data[0:2]))
	if first <= 0 || first+2 > len(data) {
		s, _ := ebcdic.CCSID37.Decode(data)
		s = strings.TrimRight(s, " \x00")
		if s == "" {
			return nil, ""
		}
		return []string{s}, s
	}
	var tokens []string
	for i := 0; i+2 <= len(data); {
		ll := int(be.Uint16(data[i : i+2]))
		i += 2
		if ll <= 0 || i+ll > len(data) {
			break
		}
		tok, err := ebcdic.CCSID37.Decode(data[i : i+ll])
		if err == nil {
			tok = strings.TrimRight(tok, " \x00")
			if tok != "" {
				tokens = append(tokens, tok)
			}
		}
		i += ll
	}
	return tokens, strings.Join(tokens, " | ")
}

// IsDb2Error tests whether err (or anything wrapped by it) is a
// *Db2Error. Convenience wrapper for the common errors.As pattern.
func IsDb2Error(err error) bool {
	var d *Db2Error
	return errors.As(err, &d)
}
