package hostserver

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/complacentsee/goJTOpen/ebcdic"
)

// buildSQLCA assembles a CP 0x3807 SQLCA payload at JT400's offsets.
// Used by tests to verify the SQLCA parser without depending on a
// captured fixture.
//
//	0..5    SQLCAID + eyecatcher (bytes ignored by parser)
//	6..7    SQLCABC (length tail)
//	12..15  SQLCODE (int32 BE)
//	16..17  SQLERRML (uint16 BE; ERRMC byte length)
//	18..    SQLERRMC (token list)
//	88..95  SQLERRP (8 EBCDIC; message id)
//	131..   SQLSTATE (5 EBCDIC)
//	136     -- end of standard SQLCA
func buildSQLCA(t *testing.T, sqlcode int32, sqlstate string, msgID string, tokens []string) []byte {
	t.Helper()
	buf := make([]byte, 136)
	be := binary.BigEndian

	// Mark length so the parser doesn't bail at the length-<=6 guard.
	be.PutUint16(buf[6:8], 100)
	be.PutUint32(buf[12:16], uint32(sqlcode))

	// Build SQLERRMC token list: each token is uint16 BE length
	// followed by EBCDIC bytes.
	errmc := []byte{}
	for _, tok := range tokens {
		ebc, err := ebcdic.CCSID37.Encode(tok)
		if err != nil {
			t.Fatalf("encode token %q: %v", tok, err)
		}
		ll := make([]byte, 2)
		be.PutUint16(ll, uint16(len(ebc)))
		errmc = append(errmc, ll...)
		errmc = append(errmc, ebc...)
	}
	be.PutUint16(buf[16:18], uint16(len(errmc)))
	if 18+len(errmc) > len(buf) {
		buf = append(buf, make([]byte, 18+len(errmc)-len(buf))...)
	}
	copy(buf[18:18+len(errmc)], errmc)

	// SQLERRP at offset 88 (8 EBCDIC bytes; pad with EBCDIC space 0x40).
	if msgID != "" {
		ebc, err := ebcdic.CCSID37.Encode(msgID)
		if err != nil {
			t.Fatalf("encode msgID: %v", err)
		}
		copy(buf[88:96], ebc)
		for i := 88 + len(ebc); i < 96; i++ {
			buf[i] = 0x40
		}
	}

	// SQLSTATE at offset 131 (5 EBCDIC bytes).
	if sqlstate != "" {
		ebc, err := ebcdic.CCSID37.Encode(sqlstate)
		if err != nil {
			t.Fatalf("encode sqlstate: %v", err)
		}
		copy(buf[131:136], ebc)
	}
	return buf
}

func TestMakeDb2ErrorSwallowsSuccessAndWarnings(t *testing.T) {
	cases := []struct {
		name string
		rep  *DBReply
	}{
		{"rc=0 errorClass=0", &DBReply{ReturnCode: 0, ErrorClass: 0}},
		{"rc=+100 errorClass=1 (no rows)", &DBReply{ReturnCode: 100, ErrorClass: 1}},
		{"rc=+700 errorClass=2 (cursor warning)", &DBReply{ReturnCode: 700, ErrorClass: 2}},
		{"rc=+8001 errorClass=0 (set-attrs OK)", &DBReply{ReturnCode: 8001, ErrorClass: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := makeDb2Error(tc.rep, "OP"); got != nil {
				t.Errorf("makeDb2Error(%+v) = %v, want nil", tc.rep, got)
			}
		})
	}
}

func TestMakeDb2ErrorParsesSQLCA(t *testing.T) {
	sqlca := buildSQLCA(t, -204, "42704", "SQL0204", []string{"GOTEST", "DOESNOTEXIST"})
	rcSigned := int32(-204)
	rep := &DBReply{
		ReturnCode: uint32(rcSigned),
		ErrorClass: 2,
		Params: []DBParam{
			{CodePoint: cpDBSQLCA, Data: sqlca},
		},
	}
	dbErr := makeDb2Error(rep, "PREPARE_DESCRIBE")
	if dbErr == nil {
		t.Fatal("makeDb2Error returned nil for rc=-204 errorClass=2")
	}
	if dbErr.SQLCode != -204 {
		t.Errorf("SQLCode = %d, want -204", dbErr.SQLCode)
	}
	if dbErr.SQLState != "42704" {
		t.Errorf("SQLState = %q, want %q", dbErr.SQLState, "42704")
	}
	if dbErr.MessageID != "SQL0204" {
		t.Errorf("MessageID = %q, want SQL0204", dbErr.MessageID)
	}
	if dbErr.Op != "PREPARE_DESCRIBE" {
		t.Errorf("Op = %q, want PREPARE_DESCRIBE", dbErr.Op)
	}
	if len(dbErr.MessageTokens) != 2 || dbErr.MessageTokens[0] != "GOTEST" || dbErr.MessageTokens[1] != "DOESNOTEXIST" {
		t.Errorf("MessageTokens = %v, want [GOTEST DOESNOTEXIST]", dbErr.MessageTokens)
	}
	// Error() should be human-readable and include the salient fields.
	msg := dbErr.Error()
	for _, want := range []string{"PREPARE_DESCRIBE", "SQL-204", "42704", "SQL0204"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, missing substring %q", msg, want)
		}
	}
}

func TestDb2ErrorPredicates(t *testing.T) {
	cases := []struct {
		name        string
		err         *Db2Error
		notFound    bool
		constraint  bool
		connLost    bool
		lockTimeout bool
	}{
		{"-204 table-not-found", &Db2Error{SQLCode: -204, SQLState: "42704"}, false, false, false, false},
		{"-803 dup-key", &Db2Error{SQLCode: -803, SQLState: "23505"}, false, true, false, false},
		{"-911 deadlock", &Db2Error{SQLCode: -911, SQLState: "40001"}, false, false, false, true},
		{"-913 lock-timeout", &Db2Error{SQLCode: -913, SQLState: "57033"}, false, false, false, true},
		{"+100 not-found", &Db2Error{SQLCode: 100, SQLState: "02000"}, true, false, false, false},
		{"08001 conn-lost", &Db2Error{SQLCode: -30080, SQLState: "08001"}, false, false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.IsNotFound(); got != tc.notFound {
				t.Errorf("IsNotFound = %v, want %v", got, tc.notFound)
			}
			if got := tc.err.IsConstraintViolation(); got != tc.constraint {
				t.Errorf("IsConstraintViolation = %v, want %v", got, tc.constraint)
			}
			if got := tc.err.IsConnectionLost(); got != tc.connLost {
				t.Errorf("IsConnectionLost = %v, want %v", got, tc.connLost)
			}
			if got := tc.err.IsLockTimeout(); got != tc.lockTimeout {
				t.Errorf("IsLockTimeout = %v, want %v", got, tc.lockTimeout)
			}
		})
	}
}

func TestDb2ErrorErrorsAs(t *testing.T) {
	src := &Db2Error{SQLCode: -204, SQLState: "42704", Op: "PREPARE_DESCRIBE"}
	wrapped := errors.New("outer: " + src.Error())
	// Plain wrapping with errors.New loses type info -- this just
	// confirms that direct returns work with errors.As for callers.
	var got *Db2Error
	if !errors.As(src, &got) {
		t.Fatal("errors.As failed on direct *Db2Error")
	}
	if got.SQLCode != -204 {
		t.Errorf("got.SQLCode = %d, want -204", got.SQLCode)
	}
	// And wrap-via-fmt also works (the typical hostserver call site
	// pattern).
	wrapped2 := errWrap("RC", src)
	if !errors.As(wrapped2, &got) {
		t.Fatalf("errors.As failed on wrapped Db2Error (%v)", wrapped2)
	}

	// Sanity: a non-Db2Error error doesn't trigger.
	if errors.As(wrapped, &got) {
		t.Error("errors.As wrongly matched a plain error")
	}
}

// errWrap is a tiny helper that mirrors fmt.Errorf("...: %w", err)
// for use in TestDb2ErrorErrorsAs without pulling in fmt-only test
// scaffolding.
func errWrap(label string, err error) error {
	return &wrappedErr{label: label, inner: err}
}

type wrappedErr struct {
	label string
	inner error
}

func (w *wrappedErr) Error() string { return w.label + ": " + w.inner.Error() }
func (w *wrappedErr) Unwrap() error { return w.inner }
