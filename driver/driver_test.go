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
		"wrong scheme":      "postgres://u:p@h/db",
		"missing user":      "gojtopen://h:8471/",
		"empty user":        "gojtopen://@h:8471/",
		"missing host":      "gojtopen://u:p@",
		"bad port":          "gojtopen://u:p@h:notnumeric/",
		"port zero":         "gojtopen://u:p@h:0/",
		"port over 65535":   "gojtopen://u:p@h:99999/",
		"bad date format":   "gojtopen://u:p@h/?date=bogus",
		"bad isolation":     "gojtopen://u:p@h/?isolation=bogus",
		"bad signon-port":   "gojtopen://u:p@h/?signon-port=notanumber",
		"signon-port zero":  "gojtopen://u:p@h/?signon-port=0",
		"bad lob mode":      "gojtopen://u:p@h/?lob=bogus",
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

// TestParseDSNLOBMode covers the lob= DSN key. Default (key absent)
// is materialise; lob=stream flips to the LOBReader path; both
// "materialise" and "materialize" spellings are accepted to keep
// US/UK spelling debates out of failing tests.
func TestParseDSNLOBMode(t *testing.T) {
	t.Run("default is materialise", func(t *testing.T) {
		cfg, err := parseDSN("gojtopen://u:p@h/")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if cfg.LOBStream {
			t.Error("LOBStream = true, want false (default)")
		}
	})
	cases := map[string]bool{
		"materialise": false,
		"materialize": false,
		"stream":      true,
	}
	for v, want := range cases {
		t.Run("lob="+v, func(t *testing.T) {
			cfg, err := parseDSN("gojtopen://u:p@h/?lob=" + v)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			if cfg.LOBStream != want {
				t.Errorf("lob=%s -> LOBStream=%v, want %v", v, cfg.LOBStream, want)
			}
		})
	}
}

func TestParseDSNCCSID(t *testing.T) {
	t.Run("default zero", func(t *testing.T) {
		cfg, err := parseDSN("gojtopen://u:p@h/")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if cfg.CCSID != 0 {
			t.Errorf("default CCSID = %d, want 0 (auto-pick)", cfg.CCSID)
		}
	})
	for _, tc := range []struct {
		raw  string
		want uint16
	}{
		{"1208", 1208},
		{"37", 37},
		{"273", 273},
		{"65535", 65535},
	} {
		t.Run("ccsid="+tc.raw, func(t *testing.T) {
			cfg, err := parseDSN("gojtopen://u:p@h/?ccsid=" + tc.raw)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			if cfg.CCSID != tc.want {
				t.Errorf("CCSID = %d, want %d", cfg.CCSID, tc.want)
			}
		})
	}
	t.Run("rejects out-of-range", func(t *testing.T) {
		// 16-bit max is 65535; anything larger must be rejected so a
		// typo doesn't silently truncate.
		_, err := parseDSN("gojtopen://u:p@h/?ccsid=99999")
		if err == nil {
			t.Errorf("ccsid=99999 should be rejected (overflow uint16)")
		}
	})
	t.Run("rejects non-numeric", func(t *testing.T) {
		_, err := parseDSN("gojtopen://u:p@h/?ccsid=utf8")
		if err == nil {
			t.Errorf("ccsid=utf8 should be rejected (not an integer)")
		}
	})
}

// TestParseDSNLOBThreshold covers the M7-5 ?lob-threshold=N knob:
// default zero (hostserver substitutes the historical 32768),
// explicit values, the 15728640 documented cap, and rejection of
// non-numeric / overflowing inputs.
func TestParseDSNLOBThreshold(t *testing.T) {
	t.Run("default zero", func(t *testing.T) {
		cfg, err := parseDSN("gojtopen://u:p@h/")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if cfg.LOBThreshold != 0 {
			t.Errorf("default LOBThreshold = %d, want 0", cfg.LOBThreshold)
		}
	})
	for _, tc := range []struct {
		raw  string
		want uint32
	}{
		{"0", 0},
		{"1", 1},
		{"32768", 32768},
		{"65536", 65536},
		{"15728640", 15728640}, // server-documented cap
	} {
		t.Run("lob-threshold="+tc.raw, func(t *testing.T) {
			cfg, err := parseDSN("gojtopen://u:p@h/?lob-threshold=" + tc.raw)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			if cfg.LOBThreshold != tc.want {
				t.Errorf("LOBThreshold = %d, want %d", cfg.LOBThreshold, tc.want)
			}
		})
	}
	t.Run("rejects out-of-range", func(t *testing.T) {
		// uint32 max is 4294967295; anything larger must be rejected.
		_, err := parseDSN("gojtopen://u:p@h/?lob-threshold=4294967296")
		if err == nil {
			t.Errorf("lob-threshold=4294967296 should be rejected (overflow uint32)")
		}
	})
	t.Run("rejects non-numeric", func(t *testing.T) {
		_, err := parseDSN("gojtopen://u:p@h/?lob-threshold=32k")
		if err == nil {
			t.Errorf("lob-threshold=32k should be rejected (not an integer)")
		}
	})
}

// TestParseDSNExtendedMetadata covers the M4 ?extended-metadata=true
// knob: default false, explicit true/false, the bool aliases, and
// rejection of bogus input.
func TestParseDSNExtendedMetadata(t *testing.T) {
	t.Run("default false", func(t *testing.T) {
		cfg, err := parseDSN("gojtopen://u:p@h/")
		if err != nil {
			t.Fatalf("parseDSN: %v", err)
		}
		if cfg.ExtendedMetadata {
			t.Error("default ExtendedMetadata = true, want false")
		}
	})
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"true", true},
		{"false", false},
		{"yes", true},
		{"no", false},
		{"on", true},
		{"off", false},
		{"1", true},
		{"0", false},
	} {
		t.Run("extended-metadata="+tc.raw, func(t *testing.T) {
			cfg, err := parseDSN("gojtopen://u:p@h/?extended-metadata=" + tc.raw)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			if cfg.ExtendedMetadata != tc.want {
				t.Errorf("ExtendedMetadata = %v, want %v", cfg.ExtendedMetadata, tc.want)
			}
		})
	}
	t.Run("rejects bogus", func(t *testing.T) {
		if _, err := parseDSN("gojtopen://u:p@h/?extended-metadata=maybe"); err == nil {
			t.Error("extended-metadata=maybe should be rejected")
		}
	})
}

// TestParseDSN_DefaultsM10 confirms the package-cache defaults a
// blank DSN inherits from DefaultConfig: no extended-dynamic, no
// cache, library QGPL, warning mode, default criteria, CCSID 13488.
// These have to match JT400's JDProperties.java defaults or a
// caller expecting JT400-compatible behaviour will silently drift.
func TestParseDSN_DefaultsM10(t *testing.T) {
	cfg, err := parseDSN("gojtopen://u:p@h/")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.ExtendedDynamic {
		t.Errorf("ExtendedDynamic = true, want false")
	}
	if cfg.PackageName != "" {
		t.Errorf("PackageName = %q, want empty", cfg.PackageName)
	}
	if cfg.PackageLibrary != "QGPL" {
		t.Errorf("PackageLibrary = %q, want QGPL", cfg.PackageLibrary)
	}
	if cfg.PackageCache {
		t.Errorf("PackageCache = true, want false")
	}
	if cfg.PackageError != "warning" {
		t.Errorf("PackageError = %q, want warning", cfg.PackageError)
	}
	if cfg.PackageCriteria != "default" {
		t.Errorf("PackageCriteria = %q, want default", cfg.PackageCriteria)
	}
	if cfg.PackageCCSID != 13488 {
		t.Errorf("PackageCCSID = %d, want 13488", cfg.PackageCCSID)
	}
}

// TestParseDSN_ExtendedDynamicHappyPath wires the seven main keys
// together the way an operator who migrated from JT400 would.
func TestParseDSN_ExtendedDynamicHappyPath(t *testing.T) {
	cfg, err := parseDSN("gojtopen://u:p@h/?extended-dynamic=true&package=APP&package-library=MYLIB&package-cache=true&package-error=exception&package-criteria=select&package-ccsid=1200")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if !cfg.ExtendedDynamic {
		t.Error("ExtendedDynamic = false, want true")
	}
	if cfg.PackageName != "APP" {
		t.Errorf("PackageName = %q, want APP", cfg.PackageName)
	}
	if cfg.PackageLibrary != "MYLIB" {
		t.Errorf("PackageLibrary = %q, want MYLIB", cfg.PackageLibrary)
	}
	if !cfg.PackageCache {
		t.Error("PackageCache = false, want true")
	}
	if cfg.PackageError != "exception" {
		t.Errorf("PackageError = %q, want exception", cfg.PackageError)
	}
	if cfg.PackageCriteria != "select" {
		t.Errorf("PackageCriteria = %q, want select", cfg.PackageCriteria)
	}
	if cfg.PackageCCSID != 1200 {
		t.Errorf("PackageCCSID = %d, want 1200", cfg.PackageCCSID)
	}
}

func TestParseDSN_PackageNameCanon(t *testing.T) {
	cases := []struct {
		raw, want string
	}{
		{"app", "APP"},
		{"My Pkg", "MY_PKG"},
		{"PKG123", "PKG123"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			cfg, err := parseDSN("gojtopen://u:p@h/?extended-dynamic=true&package=" + tc.raw)
			if err != nil {
				t.Fatalf("parseDSN: %v", err)
			}
			if cfg.PackageName != tc.want {
				t.Errorf("PackageName = %q, want %q", cfg.PackageName, tc.want)
			}
		})
	}
}

func TestParseDSN_PackageCCSIDSystem(t *testing.T) {
	cfg, err := parseDSN("gojtopen://u:p@h/?package-ccsid=system")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.PackageCCSID != 0 {
		t.Errorf("PackageCCSID = %d, want 0 (system)", cfg.PackageCCSID)
	}
}

func TestParseDSN_PackageAddTrueAccepted(t *testing.T) {
	if _, err := parseDSN("gojtopen://u:p@h/?package-add=true"); err != nil {
		t.Errorf("package-add=true should be silently accepted: %v", err)
	}
	if _, err := parseDSN("gojtopen://u:p@h/?package-add=false"); err == nil {
		t.Error("package-add=false should be rejected (always-add semantics)")
	}
}

func TestParseDSN_PackageClearAccepted(t *testing.T) {
	// shape-validate only; the connect path emits the warn-log
	for _, v := range []string{"true", "false"} {
		t.Run(v, func(t *testing.T) {
			if _, err := parseDSN("gojtopen://u:p@h/?package-clear=" + v); err != nil {
				t.Errorf("package-clear=%s should be accepted: %v", v, err)
			}
		})
	}
	if _, err := parseDSN("gojtopen://u:p@h/?package-clear=maybe"); err == nil {
		t.Error("package-clear=maybe should be rejected")
	}
}

func TestParseDSN_M10RejectsBadValues(t *testing.T) {
	cases := map[string]string{
		"bogus extended-dynamic":  "gojtopen://u:p@h/?extended-dynamic=maybe",
		"package name >10 chars":  "gojtopen://u:p@h/?extended-dynamic=true&package=ELEVENCHARS",
		"package bad char dot":    "gojtopen://u:p@h/?extended-dynamic=true&package=A.B",
		"package bad char slash":  "gojtopen://u:p@h/?extended-dynamic=true&package=A/B",
		"package empty":           "gojtopen://u:p@h/?extended-dynamic=true&package=",
		"package-library too lng": "gojtopen://u:p@h/?package-library=ELEVENCHARS",
		"package-cache bogus":     "gojtopen://u:p@h/?package-cache=maybe",
		"package-error bogus":     "gojtopen://u:p@h/?package-error=fatal",
		"package-criteria bogus":  "gojtopen://u:p@h/?package-criteria=all",
		"package-ccsid 1208":      "gojtopen://u:p@h/?package-ccsid=1208",
		"package-ccsid -1":        "gojtopen://u:p@h/?package-ccsid=-1",
		"cache without extended":  "gojtopen://u:p@h/?package-cache=true",
		"extended-dyn no name":    "gojtopen://u:p@h/?extended-dynamic=true",
	}
	for name, dsn := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseDSN(dsn); err == nil {
				t.Errorf("expected error for %q, got nil", dsn)
			}
		})
	}
}

// TestParseDSN_PackageCCSIDRejectMentionsM11 makes sure the rejection
// message for an unsupported CCSID points operators at the M11+
// deferral, not just "invalid". Future contributors might widen the
// accepted set; the test pins the message until that lands.
func TestParseDSN_PackageCCSIDRejectMentionsM11(t *testing.T) {
	_, err := parseDSN("gojtopen://u:p@h/?package-ccsid=1208")
	if err == nil {
		t.Fatal("expected error for package-ccsid=1208")
	}
	if !strings.Contains(err.Error(), "M11+") {
		t.Errorf("error %q does not mention M11+ deferral", err)
	}
}
