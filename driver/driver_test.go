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

// TestParseDSNTLSDefaultPorts confirms that tls=true switches the
// default ports to the SSL host-server pair (9476 / 9471) -- and
// that an explicit URL port / signon-port still wins.
func TestParseDSNTLSDefaultPorts(t *testing.T) {
	t.Run("plain DSN keeps 8471/8476", func(t *testing.T) {
		cfg, err := parseDSN("gojtopen://u:p@h/?library=MYLIB")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if cfg.TLS {
			t.Error("TLS = true, want false")
		}
		if cfg.DBPort != 8471 || cfg.SignonPort != 8476 {
			t.Errorf("ports = %d/%d, want 8471/8476", cfg.DBPort, cfg.SignonPort)
		}
	})
	t.Run("tls=true flips to 9471/9476", func(t *testing.T) {
		cfg, err := parseDSN("gojtopen://u:p@h/?tls=true")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if !cfg.TLS {
			t.Error("TLS = false, want true")
		}
		if cfg.DBPort != 9471 {
			t.Errorf("DBPort = %d, want 9471", cfg.DBPort)
		}
		if cfg.SignonPort != 9476 {
			t.Errorf("SignonPort = %d, want 9476", cfg.SignonPort)
		}
	})
	t.Run("tls=true + explicit port wins", func(t *testing.T) {
		cfg, err := parseDSN("gojtopen://u:p@h:13471/?tls=on&signon-port=13476")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if cfg.DBPort != 13471 {
			t.Errorf("DBPort = %d, want 13471", cfg.DBPort)
		}
		if cfg.SignonPort != 13476 {
			t.Errorf("SignonPort = %d, want 13476", cfg.SignonPort)
		}
	})
}

// TestParseDSNTLSKnobs covers tls-insecure-skip-verify and
// tls-server-name parsing.
func TestParseDSNTLSKnobs(t *testing.T) {
	cfg, err := parseDSN("gojtopen://u:p@h/?tls=true&tls-insecure-skip-verify=true&tls-server-name=ibmi.internal")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if !cfg.TLS {
		t.Error("TLS = false, want true")
	}
	if !cfg.TLSInsecureSkipVerify {
		t.Error("TLSInsecureSkipVerify = false, want true")
	}
	if cfg.TLSServerName != "ibmi.internal" {
		t.Errorf("TLSServerName = %q, want ibmi.internal", cfg.TLSServerName)
	}
}

// TestParseDSNTLSBoolAliases confirms parseBool accepts the
// URL-friendly aliases (yes/no/on/off) alongside Go's strconv
// shapes (true/false/1/0/T/F).
func TestParseDSNTLSBoolAliases(t *testing.T) {
	cases := map[string]bool{
		"true":  true,
		"false": false,
		"1":     true,
		"0":     false,
		"T":     true,
		"F":     false,
		"yes":   true,
		"no":    false,
		"on":    true,
		"off":   false,
	}
	for v, want := range cases {
		t.Run(v, func(t *testing.T) {
			cfg, err := parseDSN("gojtopen://u:p@h/?tls=" + v)
			if err != nil {
				t.Fatalf("parseDSN tls=%s: %v", v, err)
			}
			if cfg.TLS != want {
				t.Errorf("tls=%s -> %v, want %v", v, cfg.TLS, want)
			}
		})
	}
}

// TestParseDSNTLSRejectsBogus confirms invalid tls values surface
// a clear error rather than silently defaulting to false.
func TestParseDSNTLSRejectsBogus(t *testing.T) {
	if _, err := parseDSN("gojtopen://u:p@h/?tls=notabool"); err == nil {
		t.Fatal("expected error for tls=notabool, got nil")
	}
}
