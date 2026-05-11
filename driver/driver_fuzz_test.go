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
		"db2i://u:p@h/",
		"db2i://USR:PWD@host.example.com:8471/?library=MYLIB",
		"db2i://u:p@h:9999/?signon-port=18476",
		"db2i://u:p@h/?date=iso",
		"db2i://u:p@h/?date=eur",
		"db2i://u:p@h/?isolation=cs",
		"db2i://u:p@h/?isolation=rr",
		"db2i://u:p@h/?lob=stream",
		"db2i://u:p@h/?lob=materialise",
		"db2i://u:p@h/?ccsid=1208",
		"db2i://u:p@h/?ccsid=37",
		"db2i://u:p@h/?lob-threshold=0",
		"db2i://u:p@h/?lob-threshold=15728640",
		"db2i://u:p@h/?extended-metadata=true",
		"db2i://u:p@h/?extended-metadata=false",
		"db2i://u:p@h/?tls=true",
		"db2i://u:p@h/?tls=true&tls-insecure-skip-verify=yes",
		"db2i://u:p@h/?tls=true&tls-server-name=ibm.example.com",
		"db2i://u:p@h/?tls=on",
		"db2i://u:p@h/?tls=off",
		// Negative cases pinned from TestParseDSNRejectsBadInputs.
		"http://u:p@h/",                            // wrong scheme
		"db2i://h/",                            // missing user info
		"db2i://u:p@/",                         // missing host
		"db2i://u:p@h:notaport/",               // non-numeric port
		"db2i://u:p@h/?date=bogus",             // unknown date
		"db2i://u:p@h/?isolation=bogus",        // unknown isolation
		"db2i://u:p@h/?lob=bogus",              // unknown lob mode
		"db2i://u:p@h/?ccsid=notanumber",       // bad ccsid
		"db2i://u:p@h/?ccsid=65537",            // ccsid out of uint16
		"db2i://u:p@h/?lob-threshold=notanumber",
		"db2i://u:p@h/?extended-metadata=maybe",
		"db2i://u:p@h/?tls=maybe",
		"db2i://u:p@h/?tls=true&signon-port=notnumeric",
		// Mixed-case + URL-encoded variations.
		"db2i://U%20SER:P%2FWD@h/?library=lower",
		"db2i://u:p@[::1]:8471/",
		"db2i://u:p@h.example.com/?date=JOB&isolation=NONE",
		// M10 package-cache surface (9 knobs total).
		"db2i://u:p@h/?extended-dynamic=true&package=APP",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-library=MYLIB",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-cache=true",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-error=warning",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-error=exception",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-error=none",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-criteria=default",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-criteria=select",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-ccsid=13488",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-ccsid=1200",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-ccsid=system",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-add=true",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-clear=true",
		"db2i://u:p@h/?extended-dynamic=true&package=APP&package-clear=false",
		"db2i://u:p@h/?extended-dynamic=true&package=My%20Pkg",       // canon path
		"db2i://u:p@h/?extended-dynamic=true&package=PKG%23%40%24_2", // # @ $ _
		// M10 rejection cases pinned from TestParseDSN_M10RejectsBadValues.
		"db2i://u:p@h/?extended-dynamic=maybe",
		"db2i://u:p@h/?extended-dynamic=true&package=TOOLONGNAME",
		"db2i://u:p@h/?extended-dynamic=true&package=A.B",
		"db2i://u:p@h/?extended-dynamic=true&package=A/B",
		"db2i://u:p@h/?extended-dynamic=true&package=",
		"db2i://u:p@h/?package-library=ELEVENCHARS",
		"db2i://u:p@h/?package-cache=maybe",
		"db2i://u:p@h/?extended-dynamic=true&package=A&package-error=fatal",
		"db2i://u:p@h/?extended-dynamic=true&package=A&package-criteria=all",
		"db2i://u:p@h/?package-ccsid=1208",
		"db2i://u:p@h/?package-ccsid=-1",
		"db2i://u:p@h/?package-cache=true",
		"db2i://u:p@h/?extended-dynamic=true",
		"db2i://u:p@h/?package-add=false",
	}
}
