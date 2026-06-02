package driver

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// TestEmitInsecureTLSWarning confirms that enabling
// tls-insecure-skip-verify produces a runtime WARN log line, matching
// the DES path's habit of warning when a security property is relaxed.
// Exercises the un-gated emit so it does not race the process-wide
// sync.Once used by warnInsecureTLS.
func TestEmitInsecureTLSWarning(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	emitInsecureTLSWarning(context.Background(), log)

	out := buf.String()
	if !strings.Contains(out, "level=WARN") {
		t.Errorf("warning not logged at WARN level:\n%s", out)
	}
	if !strings.Contains(out, "tls-insecure-skip-verify") {
		t.Errorf("warning does not name the offending knob:\n%s", out)
	}
	if !strings.Contains(out, "tls_insecure_skip_verify=true") {
		t.Errorf("warning missing structured attribute:\n%s", out)
	}
}

// TestWarnInsecureTLSOnce confirms the process-wide gate fires the
// warning exactly once even across many connections (a pool would
// otherwise log it on every dial). The gate is a package-level Once,
// so this test owns it for the run.
func TestWarnInsecureTLSOnce(t *testing.T) {
	// Reset the gate so this test is order-independent within the
	// package test binary.
	insecureTLSWarnOnce = sync.Once{}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	for i := 0; i < 3; i++ {
		warnInsecureTLS(context.Background(), log)
	}

	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Errorf("warning emitted %d times, want exactly 1:\n%s", n, buf.String())
	}
}
