package auth

import (
	"fmt"
	"os"
	"sync"
)

// unvalidatedAlgorithmWarning prints a one-shot stderr warning the
// first time a password algorithm without live wire validation is
// used in this process. The warning is keyed on `algoTag` so each
// algorithm prints exactly once regardless of how many connections
// open in parallel.
//
// Why this exists: PUB400 (the only public IBM i we can validate
// against) is locked at QPWDLVL=3, so the level 0/1 (DES) and
// level 4 (PBKDF2-SHA-512) implementations are spec-validated
// against JT400 source but never wire-validated. The warning makes
// the gap visible to operators so an auth failure on first
// production deploy isn't a mystery.
//
// The warning goes to stderr (not slog) because callers may not
// have a logger configured at sign-on time. It is unconditional --
// suppressing it would defeat the purpose; if a target is known
// good, the warning becomes routine and can be filtered upstream.
func unvalidatedAlgorithmWarning(algoTag string) {
	warnOnce(algoTag, func() {
		fmt.Fprintf(os.Stderr,
			"goJTOpen WARNING: password algorithm %q is spec-validated against JT400 source but has NOT been wire-validated against a live IBM i. PUB400 (our only free test target) is QPWDLVL=3 and won't issue %s challenges. If sign-on fails or the server returns SQL errClass 8, please report it -- the implementation may have a salt-construction or byte-order bug that only a real %s server can flush out.\n",
			algoTag, algoTag, algoTag)
	})
}

var (
	warnedMu sync.Mutex
	warned   = map[string]bool{}
)

func warnOnce(key string, fn func()) {
	warnedMu.Lock()
	defer warnedMu.Unlock()
	if warned[key] {
		return
	}
	warned[key] = true
	fn()
}
