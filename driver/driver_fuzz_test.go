package driver

import (
	"strings"
	"testing"
)

// FuzzParseDSN walks adversarial DSN strings through the parser.
// The contract: parseDSN either returns a non-nil *Config with a
// non-empty Host and a positive DBPort / SignonPort, or it returns
// a typed error. No panics; no nil-config returns paired with a nil
// error. The seed corpus covers every documented DSN knob plus the
// negative cases pinned in TestParseDSNRejectsBadInputs.
func FuzzParseDSN(f *testing.F) {
	for _, seed := range parseDSNFuzzSeeds() {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in string) {
		// Skip the all-NUL / very large inputs the URL parser
		// rejects up front -- they're not interesting and slow the
		// fuzzer down.
		if strings.IndexByte(in, 0) >= 0 {
			t.Skip()
		}
		if len(in) > 4096 {
			t.Skip()
		}
		cfg, err := parseDSN(in)
		if err != nil {
			if cfg != nil {
				t.Fatalf("parseDSN(%q): err=%v but cfg!=nil", in, err)
			}
			return
		}
		if cfg == nil {
			t.Fatalf("parseDSN(%q): (nil, nil) -- contract requires non-nil cfg on success", in)
		}
		if cfg.Host == "" {
			t.Fatalf("parseDSN(%q): empty Host on success", in)
		}
		if cfg.DBPort <= 0 || cfg.DBPort > 65535 {
			t.Fatalf("parseDSN(%q): DBPort %d out of range", in, cfg.DBPort)
		}
		if cfg.SignonPort <= 0 || cfg.SignonPort > 65535 {
			t.Fatalf("parseDSN(%q): SignonPort %d out of range", in, cfg.SignonPort)
		}
	})
}

func parseDSNFuzzSeeds() []string {
	return []string{
		"gojtopen://u:p@h/",
		"gojtopen://USR:PWD@host.example.com:8471/?library=MYLIB",
		"gojtopen://u:p@h:9999/?signon-port=18476",
		"gojtopen://u:p@h/?date=iso",
		"gojtopen://u:p@h/?date=eur",
		"gojtopen://u:p@h/?isolation=cs",
		"gojtopen://u:p@h/?isolation=rr",
		"gojtopen://u:p@h/?lob=stream",
		"gojtopen://u:p@h/?lob=materialise",
		"gojtopen://u:p@h/?ccsid=1208",
		"gojtopen://u:p@h/?ccsid=37",
		"gojtopen://u:p@h/?lob-threshold=0",
		"gojtopen://u:p@h/?lob-threshold=15728640",
		"gojtopen://u:p@h/?extended-metadata=true",
		"gojtopen://u:p@h/?extended-metadata=false",
		"gojtopen://u:p@h/?tls=true",
		"gojtopen://u:p@h/?tls=true&tls-insecure-skip-verify=yes",
		"gojtopen://u:p@h/?tls=true&tls-server-name=ibm.example.com",
		"gojtopen://u:p@h/?tls=on",
		"gojtopen://u:p@h/?tls=off",
		// Negative cases pinned from TestParseDSNRejectsBadInputs.
		"http://u:p@h/",                            // wrong scheme
		"gojtopen://h/",                            // missing user info
		"gojtopen://u:p@/",                         // missing host
		"gojtopen://u:p@h:notaport/",               // non-numeric port
		"gojtopen://u:p@h/?date=bogus",             // unknown date
		"gojtopen://u:p@h/?isolation=bogus",        // unknown isolation
		"gojtopen://u:p@h/?lob=bogus",              // unknown lob mode
		"gojtopen://u:p@h/?ccsid=notanumber",       // bad ccsid
		"gojtopen://u:p@h/?ccsid=65537",            // ccsid out of uint16
		"gojtopen://u:p@h/?lob-threshold=notanumber",
		"gojtopen://u:p@h/?extended-metadata=maybe",
		"gojtopen://u:p@h/?tls=maybe",
		"gojtopen://u:p@h/?tls=true&signon-port=notnumeric",
		// Mixed-case + URL-encoded variations.
		"gojtopen://U%20SER:P%2FWD@h/?library=lower",
		"gojtopen://u:p@[::1]:8471/",
		"gojtopen://u:p@h.example.com/?date=JOB&isolation=NONE",
	}
}
