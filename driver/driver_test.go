package driver

import (
	"strings"
	"testing"

	"github.com/complacentsee/goJTOpen/hostserver"
)

func TestParseDSNBasic(t *testing.T) {
	cfg, err := parseDSN("gojtopen://USR:PWD@host.example.com:8471/?library=MYLIB")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.User != "USR" {
		t.Errorf("User = %q, want USR", cfg.User)
	}
	if cfg.Password != "PWD" {
		t.Errorf("Password not preserved")
	}
	if cfg.Host != "host.example.com" {
		t.Errorf("Host = %q", cfg.Host)
	}
	if cfg.DBPort != 8471 {
		t.Errorf("DBPort = %d, want 8471", cfg.DBPort)
	}
	if cfg.SignonPort != 8476 {
		t.Errorf("SignonPort = %d, want 8476 (default)", cfg.SignonPort)
	}
	if cfg.Library != "MYLIB" {
		t.Errorf("Library = %q, want MYLIB", cfg.Library)
	}
	if cfg.DateFormat != hostserver.DateFormatJOB {
		t.Errorf("DateFormat = 0x%02X, want JOB (0x%02X)", cfg.DateFormat, hostserver.DateFormatJOB)
	}
	if cfg.Isolation != hostserver.IsolationCommitNone {
		t.Errorf("Isolation = %d, want CommitNone (%d) for autocommit-permissive default",
			cfg.Isolation, hostserver.IsolationCommitNone)
	}
}

func TestParseDSNCustomPort(t *testing.T) {
	cfg, err := parseDSN("gojtopen://u:p@h:9999/?signon-port=18476")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.DBPort != 9999 {
		t.Errorf("DBPort = %d, want 9999", cfg.DBPort)
	}
	if cfg.SignonPort != 18476 {
		t.Errorf("SignonPort = %d, want 18476", cfg.SignonPort)
	}
}

func TestParseDSNDateFormats(t *testing.T) {
	cases := []struct {
		key  string
		want byte
	}{
		{"job", hostserver.DateFormatJOB},
		{"iso", hostserver.DateFormatISO},
		{"usa", hostserver.DateFormatUSA},
		{"eur", hostserver.DateFormatEUR},
		{"jis", hostserver.DateFormatJIS},
		{"mdy", hostserver.DateFormatMDY},
		{"dmy", hostserver.DateFormatDMY},
		{"ymd", hostserver.DateFormatYMD},
	}
	for _, tc := range cases {
		cfg, err := parseDSN("gojtopen://u:p@h/?date=" + tc.key)
		if err != nil {
			t.Errorf("date=%s: %v", tc.key, err)
			continue
		}
		if cfg.DateFormat != tc.want {
			t.Errorf("date=%s: format = 0x%02X, want 0x%02X", tc.key, cfg.DateFormat, tc.want)
		}
	}
}

func TestParseDSNIsolations(t *testing.T) {
	cases := []struct {
		key  string
		want int16
	}{
		{"none", hostserver.IsolationCommitNone},
		{"cs", hostserver.IsolationReadCommitted},
		{"all", hostserver.IsolationAllCS},
		{"rr", hostserver.IsolationRepeatableRd},
		{"rs", hostserver.IsolationSerializable},
	}
	for _, tc := range cases {
		cfg, err := parseDSN("gojtopen://u:p@h/?isolation=" + tc.key)
		if err != nil {
			t.Errorf("isolation=%s: %v", tc.key, err)
			continue
		}
		if cfg.Isolation != tc.want {
			t.Errorf("isolation=%s: level = %d, want %d", tc.key, cfg.Isolation, tc.want)
		}
	}
}

func TestParseDSNRejectsBadInputs(t *testing.T) {
	cases := map[string]string{
		"wrong scheme":     "postgres://u:p@h/db",
		"missing user":     "gojtopen://h:8471/",
		"missing host":     "gojtopen://u:p@",
		"bad port":         "gojtopen://u:p@h:notnumeric/",
		"bad date format":  "gojtopen://u:p@h/?date=bogus",
		"bad isolation":    "gojtopen://u:p@h/?isolation=bogus",
		"bad signon-port":  "gojtopen://u:p@h/?signon-port=notanumber",
	}
	for name, dsn := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDSN(dsn); err == nil {
				t.Errorf("expected error for %q, got nil", dsn)
			}
		})
	}
}

func TestParseDSNUppercasesLibrary(t *testing.T) {
	// IBM i schema lookups are case-insensitive but the wire format
	// expects EBCDIC uppercase. Normalising at the DSN boundary
	// keeps every downstream caller from doing it.
	cfg, err := parseDSN("gojtopen://u:p@h/?library=mylib")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.Library != "MYLIB" {
		t.Errorf("Library = %q, want MYLIB (uppercased)", cfg.Library)
	}
}

func TestParseDSNErrorWrapped(t *testing.T) {
	// Confirm errors mention the actual offending value so operators
	// have something to act on.
	_, err := parseDSN("gojtopen://u:p@h/?date=fooBAR")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "fooBAR") {
		t.Errorf("error %q does not include offending value 'fooBAR'", err)
	}
}
