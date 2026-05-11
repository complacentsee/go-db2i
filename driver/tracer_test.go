package driver

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/complacentsee/go-db2i/hostserver"
)

// TestResolveTracerNilReturnsNoop confirms a nil Config.Tracer
// resolves to a non-nil tracer (the no-op fallback) so call sites
// don't have to nil-check.
func TestResolveTracerNilReturnsNoop(t *testing.T) {
	got := resolveTracer(nil)
	if got == nil {
		t.Fatal("resolveTracer(nil) returned nil; expected no-op fallback")
	}
	// Start a span; it should be valid but record nothing.
	_, span := got.Start(context.Background(), "smoke")
	span.End()
}

// TestStmtStartSpanEmitsConventionalAttrs runs startSpan against an
// in-memory exporter and confirms the resulting span carries the
// OTel database semantic-convention attributes the M8-4 plan calls
// out (db.system.name, db.namespace, db.user, server.address,
// server.port, db.statement.parameters.count). SQL text appears
// only when Config.LogSQL is set.
func TestStmtStartSpanEmitsConventionalAttrs(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	cfg := DefaultConfig()
	cfg.User = "USR"
	cfg.Host = "host.example.com"
	cfg.DBPort = 8471
	cfg.SignonPort = 8476
	cfg.Library = "MYLIB"
	cfg.LogSQL = true
	cfg.Tracer = tp.Tracer("db2i-test")

	conn := &Conn{cfg: &cfg, tracer: resolveTracer(cfg.Tracer)}
	stmt := &Stmt{conn: conn, query: "SELECT 1 FROM SYSIBM.SYSDUMMY1"}

	_, span := stmt.startSpan(context.Background(), "QUERY", 0)
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Name != "QUERY" {
		t.Errorf("span name = %q, want %q", s.Name, "QUERY")
	}

	attrs := attrMap(s.Attributes)
	wants := map[string]attribute.Value{
		"db.system.name":                attribute.StringValue("ibm_db2_for_i"),
		"db.operation.name":             attribute.StringValue("QUERY"),
		"db.statement.parameters.count": attribute.IntValue(0),
		"db.namespace":                  attribute.StringValue("MYLIB"),
		"db.user":                       attribute.StringValue("USR"),
		"server.address":                attribute.StringValue("host.example.com"),
		"server.port":                   attribute.IntValue(8471),
		"db.statement":                  attribute.StringValue("SELECT 1 FROM SYSIBM.SYSDUMMY1"),
	}
	for k, want := range wants {
		got, ok := attrs[k]
		if !ok {
			t.Errorf("missing attr %q", k)
			continue
		}
		if got.Type() != want.Type() || got.Emit() != want.Emit() {
			t.Errorf("attr %q = %v, want %v", k, got.Emit(), want.Emit())
		}
	}
}

// TestStmtStartSpanOmitsSQLWhenLogSQLFalse confirms that the
// db.statement attribute is not emitted when LogSQL is off, matching
// the slog gating contract.
func TestStmtStartSpanOmitsSQLWhenLogSQLFalse(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	cfg := DefaultConfig()
	cfg.User = "USR"
	cfg.Host = "h"
	cfg.DBPort = 8471
	cfg.SignonPort = 8476
	cfg.LogSQL = false
	cfg.Tracer = tp.Tracer("db2i-test")
	conn := &Conn{cfg: &cfg, tracer: resolveTracer(cfg.Tracer)}
	stmt := &Stmt{conn: conn, query: "SELECT * FROM secret.pii_view"}
	_, span := stmt.startSpan(context.Background(), "QUERY", 2)
	span.End()

	attrs := attrMap(exp.GetSpans()[0].Attributes)
	if _, ok := attrs["db.statement"]; ok {
		t.Fatalf("db.statement attribute leaked when LogSQL=false")
	}
	if got := attrs["db.statement.parameters.count"]; got.AsInt64() != 2 {
		t.Fatalf("db.statement.parameters.count = %d, want 2", got.AsInt64())
	}
}

// TestRecordSpanErrorAttachesDb2ErrorAttrs walks the *Db2Error
// branch of recordSpanError. SQLSTATE / SQLCODE / MessageID land as
// dedicated attributes so alerting rules can match on them without
// regexing the span event's message.
func TestRecordSpanErrorAttachesDb2ErrorAttrs(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	conn := &Conn{cfg: &Config{User: "U", Host: "h", DBPort: 8471, SignonPort: 8476}, tracer: tp.Tracer("db2i-test")}
	stmt := &Stmt{conn: conn, query: ""}
	_, span := stmt.startSpan(context.Background(), "EXEC", 1)
	dbErr := &hostserver.Db2Error{
		SQLState:  "23505",
		SQLCode:   -803,
		MessageID: "SQL0803",
		Message:   "duplicate key value",
	}
	stmt.recordSpanError(span, dbErr)
	span.End()

	got := exp.GetSpans()[0]
	if got.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error", got.Status.Code)
	}
	attrs := attrMap(got.Attributes)
	if v, ok := attrs["db.response.status_code"]; !ok || v.AsString() != "23505" {
		t.Errorf("db.response.status_code = %v, want 23505", v.AsString())
	}
	if v, ok := attrs["db.ibm_db2_for_i.sqlcode"]; !ok || v.AsInt64() != -803 {
		t.Errorf("db.ibm_db2_for_i.sqlcode = %v, want -803", v.AsInt64())
	}
	if v, ok := attrs["db.ibm_db2_for_i.message_id"]; !ok || v.AsString() != "SQL0803" {
		t.Errorf("db.ibm_db2_for_i.message_id = %v, want SQL0803", v.AsString())
	}
}

// TestRecordSpanErrorNonDb2ErrorStillSetsStatus pins the behaviour
// for non-typed errors (TCP, ctx cancellation): status flips to
// Error but no Db2-specific attributes are added.
func TestRecordSpanErrorNonDb2ErrorStillSetsStatus(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	conn := &Conn{cfg: &Config{User: "U", Host: "h", DBPort: 8471, SignonPort: 8476}, tracer: tp.Tracer("db2i-test")}
	stmt := &Stmt{conn: conn}
	_, span := stmt.startSpan(context.Background(), "QUERY", 0)
	stmt.recordSpanError(span, errors.New("read tcp: connection reset by peer"))
	span.End()

	got := exp.GetSpans()[0]
	if got.Status.Code != codes.Error {
		t.Errorf("status = %v, want Error", got.Status.Code)
	}
	attrs := attrMap(got.Attributes)
	if _, ok := attrs["db.response.status_code"]; ok {
		t.Errorf("db.response.status_code should not be set for non-Db2Error")
	}
}

func attrMap(kvs []attribute.KeyValue) map[string]attribute.Value {
	out := make(map[string]attribute.Value, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value
	}
	return out
}
