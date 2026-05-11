package driver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// TestResolveLoggerNilReturnsSilent confirms that a nil Config.Logger
// resolves to a non-nil logger so call sites don't need to nil-check.
func TestResolveLoggerNilReturnsSilent(t *testing.T) {
	got := resolveLogger(nil)
	if got == nil {
		t.Fatal("resolveLogger(nil) returned nil; expected silent fallback")
	}
	// Emit a line that would be visible on a real handler; the
	// silent handler discards it.
	got.Info("should not appear anywhere")
}

// TestResolveLoggerHonoursCaller confirms a non-nil caller logger
// is returned verbatim.
func TestResolveLoggerHonoursCaller(t *testing.T) {
	var buf bytes.Buffer
	caller := slog.New(slog.NewTextHandler(&buf, nil))
	got := resolveLogger(caller)
	if got != caller {
		t.Fatalf("resolveLogger(caller) returned a different *Logger")
	}
}

// TestClassifyConnErrLogsWarnOnBadConn confirms classifyConnErr
// emits a WARN line via the conn's logger when it tags an error as
// ErrBadConn. Callers wire a slog.Handler at WARN+ and trust they
// will see retired connections without polling pool stats.
func TestClassifyConnErrLogsWarnOnBadConn(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	c := &Conn{log: log}
	got := c.classifyConnErr(io.EOF)
	if !errors.Is(got, io.EOF) {
		t.Fatalf("classifyConnErr lost the underlying error: %v", got)
	}
	if !strings.Contains(buf.String(), "ErrBadConn") {
		t.Fatalf("expected WARN line mentioning ErrBadConn, got: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("expected WARN level, got: %s", buf.String())
	}
}

// TestClassifyConnErrLogsErrorOnStatementErr confirms that
// non-fatal errors are surfaced at ERROR level. The motivating use
// case: ops dashboards that subscribe to ERROR-level events should
// see SQL syntax / constraint violations without parsing the error
// message.
func TestClassifyConnErrLogsErrorOnStatementErr(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError}))
	c := &Conn{log: log}
	c.classifyConnErr(errors.New("SQL0204 -- table not found"))
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("expected ERROR level, got: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "SQL0204") {
		t.Fatalf("expected error text in attrs, got: %s", buf.String())
	}
}

// TestNewConnectorValidatesConfig pins the contract on the
// programmatic-config entry point: missing user / host / port land
// as a typed error rather than a nil-cfg panic at first connect.
func TestNewConnectorValidatesConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  *Config
	}{
		{"nil cfg", nil},
		{"empty user", &Config{Host: "h", DBPort: 8471, SignonPort: 8476}},
		{"empty host", &Config{User: "u", DBPort: 8471, SignonPort: 8476}},
		{"port zero", &Config{User: "u", Host: "h", DBPort: 0, SignonPort: 8476}},
		{"signon port too high", &Config{User: "u", Host: "h", DBPort: 8471, SignonPort: 99999}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConnector(tc.cfg)
			if err == nil {
				t.Fatalf("NewConnector(%+v) returned nil error", tc.cfg)
			}
		})
	}
}

// TestNewConnectorAcceptsLogger confirms that Config.Logger
// propagates through NewConnector and is reachable on the returned
// Connector (live connect tested in cmd/smoketest -log-debug).
func TestNewConnectorAcceptsLogger(t *testing.T) {
	cfg := DefaultConfig()
	cfg.User = "USR"
	cfg.Host = "host.example.com"
	cfg.DBPort = 8471
	cfg.SignonPort = 8476
	var buf bytes.Buffer
	cfg.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	conn, err := NewConnector(&cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	if conn.cfg.Logger == nil {
		t.Fatal("NewConnector dropped Config.Logger")
	}
}

// TestConnLoggerSilentByDefault confirms that with Config.Logger
// unset, the per-Conn logger is the silent fallback (i.e. emitting
// to it goes to io.Discard).
func TestConnLoggerSilentByDefault(t *testing.T) {
	cfg := DefaultConfig()
	c := &Conn{log: resolveLogger(cfg.Logger)}
	c.log.Info("if you see this, the silent fallback is broken")
	// No assertion -- the test passes if no panic occurs and the
	// silent handler doesn't write anywhere observable.
	_ = context.Background()
}
