package driver

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/complacentsee/go-db2i/hostserver"
)

// fakeMapper is a local stand-in for the IBM i server mapper: it
// accepts a TCP connection, records the request bytes, and replies with
// a fixed 5-byte response.
type fakeMapper struct {
	ln      net.Listener
	port    int
	accepts atomic.Int32
	reply   []byte

	mu      sync.Mutex
	lastReq []byte
}

func startFakeMapper(t *testing.T, reply []byte) *fakeMapper {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fm := &fakeMapper{ln: ln, port: ln.Addr().(*net.TCPAddr).Port, reply: reply}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			fm.accepts.Add(1)
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 64)
				n, _ := c.Read(buf)
				fm.mu.Lock()
				fm.lastReq = append([]byte(nil), buf[:n]...)
				fm.mu.Unlock()
				_, _ = c.Write(fm.reply)
			}(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return fm
}

func (fm *fakeMapper) request() string {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return string(fm.lastReq)
}

// pointMapperAt redirects the resolver's well-known 449 to port for the
// duration of the test and clears the process-wide resolution cache so
// tests don't leak ports into each other.
func pointMapperAt(t *testing.T, port int) {
	t.Helper()
	prev := mapperPort
	mapperPort = port
	clearPortMapCache()
	t.Cleanup(func() {
		mapperPort = prev
		clearPortMapCache()
	})
}

func clearPortMapCache() {
	portMapCache.Range(func(k, _ any) bool {
		portMapCache.Delete(k)
		return true
	})
}

func mapperTestConfig() Config {
	cfg := DefaultConfig()
	cfg.Host = "127.0.0.1"
	return cfg
}

func TestResolveServicePortMapperSuccess(t *testing.T) {
	fm := startFakeMapper(t, []byte{0x2B, 0x00, 0x00, 0x21, 0x17}) // 8471
	pointMapperAt(t, fm.port)

	cfg := mapperTestConfig()
	got := resolveServicePort(context.Background(), &cfg, hostserver.ServerDatabase, 9999, false, time.Time{}, silentLogger)
	if got != 8471 {
		t.Errorf("resolved port = %d, want 8471 (from mapper)", got)
	}
	if req := fm.request(); req != "as-database" {
		t.Errorf("mapper request = %q, want %q", req, "as-database")
	}
}

func TestResolveServicePortSecureSendsSuffix(t *testing.T) {
	fm := startFakeMapper(t, []byte{0x2B, 0x00, 0x00, 0x24, 0xFF}) // 9471
	pointMapperAt(t, fm.port)

	cfg := mapperTestConfig()
	cfg.TLS = true
	got := resolveServicePort(context.Background(), &cfg, hostserver.ServerDatabase, 9471, false, time.Time{}, silentLogger)
	if got != 9471 {
		t.Errorf("resolved port = %d, want 9471", got)
	}
	if req := fm.request(); req != "as-database-s" {
		t.Errorf("secure mapper request = %q, want %q", req, "as-database-s")
	}
}

func TestResolveServicePortDisabledSkipsMapper(t *testing.T) {
	fm := startFakeMapper(t, []byte{0x2B, 0x00, 0x00, 0x21, 0x17})
	pointMapperAt(t, fm.port)

	cfg := mapperTestConfig()
	cfg.PortMapper = false
	got := resolveServicePort(context.Background(), &cfg, hostserver.ServerDatabase, 8471, false, time.Time{}, silentLogger)
	if got != 8471 {
		t.Errorf("resolved port = %d, want 8471 (fallback, mapper disabled)", got)
	}
	if n := fm.accepts.Load(); n != 0 {
		t.Errorf("mapper dialled %d time(s); want 0 when port-mapper disabled", n)
	}
}

func TestResolveServicePortPinnedSkipsMapper(t *testing.T) {
	fm := startFakeMapper(t, []byte{0x2B, 0x00, 0x00, 0x21, 0x17})
	pointMapperAt(t, fm.port)

	cfg := mapperTestConfig()
	got := resolveServicePort(context.Background(), &cfg, hostserver.ServerDatabase, 9999, true /*pinned*/, time.Time{}, silentLogger)
	if got != 9999 {
		t.Errorf("resolved port = %d, want 9999 (pinned)", got)
	}
	if n := fm.accepts.Load(); n != 0 {
		t.Errorf("mapper dialled %d time(s); want 0 when port pinned", n)
	}
}

func TestResolveServicePortMalformedFallsBack(t *testing.T) {
	fm := startFakeMapper(t, []byte{0x2D, 0x00, 0x00, 0x00, 0x00}) // '-' = not found
	pointMapperAt(t, fm.port)

	cfg := mapperTestConfig()
	got := resolveServicePort(context.Background(), &cfg, hostserver.ServerSignon, 8476, false, time.Time{}, silentLogger)
	if got != 8476 {
		t.Errorf("resolved port = %d, want 8476 (fallback on mapper failure)", got)
	}
}

func TestResolveServicePortRefusedFallsBack(t *testing.T) {
	// Grab a port, then close the listener so the dial is refused.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	pointMapperAt(t, port)

	cfg := mapperTestConfig()
	got := resolveServicePort(context.Background(), &cfg, hostserver.ServerDatabase, 8471, false, time.Time{}, silentLogger)
	if got != 8471 {
		t.Errorf("resolved port = %d, want 8471 (fallback on connection refused)", got)
	}
}

func TestResolveServicePortCachesResult(t *testing.T) {
	fm := startFakeMapper(t, []byte{0x2B, 0x00, 0x00, 0x21, 0x17})
	pointMapperAt(t, fm.port)

	cfg := mapperTestConfig()
	for i := 0; i < 3; i++ {
		if got := resolveServicePort(context.Background(), &cfg, hostserver.ServerDatabase, 9999, false, time.Time{}, silentLogger); got != 8471 {
			t.Fatalf("call %d: resolved port = %d, want 8471", i, got)
		}
	}
	if n := fm.accepts.Load(); n != 1 {
		t.Errorf("mapper dialled %d time(s) across 3 resolves; want 1 (cached)", n)
	}
}

func TestParseDSNPortMapperDefaultsOn(t *testing.T) {
	cfg, err := parseDSN("db2i://u:p@host/?library=MYLIB")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if !cfg.PortMapper {
		t.Error("PortMapper = false, want true by default")
	}
	if cfg.dbPortPinned || cfg.signonPortPinned {
		t.Errorf("pin flags = (%v, %v), want (false, false) with no explicit ports", cfg.dbPortPinned, cfg.signonPortPinned)
	}
}

func TestParseDSNPortMapperFalse(t *testing.T) {
	cfg, err := parseDSN("db2i://u:p@host/?port-mapper=false")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if cfg.PortMapper {
		t.Error("PortMapper = true, want false when port-mapper=false")
	}
}

func TestParseDSNPortMapperInvalid(t *testing.T) {
	if _, err := parseDSN("db2i://u:p@host/?port-mapper=maybe"); err == nil {
		t.Fatal("parseDSN accepted port-mapper=maybe; want an error")
	}
}

func TestParseDSNExplicitPortsPin(t *testing.T) {
	cfg, err := parseDSN("db2i://u:p@host:9999/?signon-port=18476")
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if !cfg.dbPortPinned {
		t.Error("dbPortPinned = false, want true with explicit :PORT")
	}
	if !cfg.signonPortPinned {
		t.Error("signonPortPinned = false, want true with explicit signon-port")
	}
}

func TestDefaultConfigPortMapperOn(t *testing.T) {
	if !DefaultConfig().PortMapper {
		t.Error("DefaultConfig().PortMapper = false, want true")
	}
}
