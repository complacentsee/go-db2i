package driver

import (
	"os"
	"regexp"
	"sort"
	"testing"
)

// documentedDSNKeys is the canonical roster of DSN query keys the
// parser accepts. It MUST stay in lock-step with the
// "Supported (N DSN keys)" list in MIGRATING.md. The test below
// extracts the keys the parser actually reads from driver.go and
// asserts the two sets are identical, so a new q.Get("...") key
// added without updating the docs (or this roster) fails the build.
var documentedDSNKeys = []string{
	"block-size",
	"ccsid",
	"date",
	"date-separator",
	"decimal-separator",
	"extended-dynamic",
	"extended-metadata",
	"isolation",
	"libraries",
	"library",
	"lob",
	"lob-threshold",
	"login-timeout",
	"naming",
	"package",
	"package-add",
	"package-cache",
	"package-ccsid",
	"package-clear",
	"package-criteria",
	"package-error",
	"package-library",
	"query-optimize-goal",
	"signon-port",
	"socket-timeout",
	"time-format",
	"time-separator",
	"tls",
	"tls-insecure-skip-verify",
	"tls-server-name",
}

// qGetKeyRe matches q.Get("some-key") calls in the parser source.
var qGetKeyRe = regexp.MustCompile(`q\.Get\("([a-z0-9-]+)"\)`)

// TestDocumentedDSNKeysMatchParser asserts the documented DSN-key
// roster equals the set of keys the parser reads. This is the
// offline acceptance check for the docs-hygiene pass: it keeps the
// MIGRATING.md "Supported (N DSN keys)" count honest by failing
// whenever the parser gains or loses a q.Get("...") key without a
// matching roster update.
func TestDocumentedDSNKeysMatchParser(t *testing.T) {
	src, err := os.ReadFile("driver.go")
	if err != nil {
		t.Fatalf("read driver.go: %v", err)
	}

	parserKeys := map[string]bool{}
	for _, m := range qGetKeyRe.FindAllStringSubmatch(string(src), -1) {
		parserKeys[m[1]] = true
	}
	if len(parserKeys) == 0 {
		t.Fatal("no q.Get(...) keys found in driver.go; regex out of date?")
	}

	documented := map[string]bool{}
	for _, k := range documentedDSNKeys {
		documented[k] = true
	}

	var missingFromDocs, missingFromParser []string
	for k := range parserKeys {
		if !documented[k] {
			missingFromDocs = append(missingFromDocs, k)
		}
	}
	for k := range documented {
		if !parserKeys[k] {
			missingFromParser = append(missingFromParser, k)
		}
	}
	sort.Strings(missingFromDocs)
	sort.Strings(missingFromParser)

	if len(missingFromDocs) > 0 {
		t.Errorf("parser reads %d undocumented DSN key(s): %v -- add them to documentedDSNKeys and MIGRATING.md", len(missingFromDocs), missingFromDocs)
	}
	if len(missingFromParser) > 0 {
		t.Errorf("documentedDSNKeys list %d key(s) the parser no longer reads: %v -- remove them from the roster and MIGRATING.md", len(missingFromParser), missingFromParser)
	}
	if len(parserKeys) != len(documentedDSNKeys) {
		t.Errorf("parser key count = %d, documented roster count = %d; MIGRATING.md says 'Supported (30 DSN keys)' -- keep all three in sync", len(parserKeys), len(documentedDSNKeys))
	}
}
