package driver

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// Connect implements driver.Connector. Opens both as-signon (8476)
// and as-database (8471) sockets, runs sign-on + start-database-
// service, applies SQL attributes (date format, isolation, default
// library), and primes the host-server library list.
//
// ctx may be nil (when called from the legacy driver.Driver.Open
// path) or carry a deadline / cancellation. The dial honours the
// deadline; the host-server protocol exchanges currently use a
// fixed 30s timeout (the underlying net.Conn deadline).
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	deadline, _ := contextDeadline(ctx, 30*time.Second)

	// Sign-on phase: open as-signon, perform encrypted handshake.
	signon, err := dialHostServer(c.cfg, c.cfg.SignonPort, deadline)
	if err != nil {
		return nil, fmt.Errorf("gojtopen: dial as-signon: %w", err)
	}
	signon.SetDeadline(deadline)
	xs, _, err := hostserver.SignOn(signon, c.cfg.User, c.cfg.Password)
	if err != nil {
		signon.Close()
		return nil, fmt.Errorf("gojtopen: as-signon: %w", err)
	}
	signon.Close()
	serverVRM := uint32(0)
	if xs != nil {
		serverVRM = xs.ServerVersion
	}

	// Database service phase: separate TCP socket, lives for the
	// life of the Conn.
	db, err := dialHostServer(c.cfg, c.cfg.DBPort, deadline)
	if err != nil {
		return nil, fmt.Errorf("gojtopen: dial as-database: %w", err)
	}
	db.SetDeadline(time.Time{}) // clear; per-statement deadlines are managed by callers
	if _, _, err := hostserver.StartDatabaseService(db, c.cfg.User, c.cfg.Password); err != nil {
		db.Close()
		return nil, fmt.Errorf("gojtopen: as-database start: %w", err)
	}

	opts := hostserver.DefaultDBAttributesOptions()
	opts.DateFormat = c.cfg.DateFormat
	opts.IsolationLevel = c.cfg.Isolation
	if c.cfg.Library != "" {
		opts.DefaultSQLLibrary = c.cfg.Library
	}
	if c.cfg.CCSID != 0 {
		// Override the default ClientCCSID (CP 0x3801) so the server
		// returns untagged CHAR / VARCHAR / CLOB columns in the
		// caller-requested CCSID. Tagged columns are unaffected --
		// the server stamps their CCSID on the wire and the decoder
		// dispatches per column.
		opts.ClientCCSID = c.cfg.CCSID
	}
	if c.cfg.LOBThreshold != 0 {
		// CP 0x3822 LOBFieldThreshold -- inline cutoff for LOB
		// columns. SetSQLAttributes substitutes the historical
		// 32768 default for zero.
		opts.LOBThreshold = c.cfg.LOBThreshold
	}
	if _, err := hostserver.SetSQLAttributes(db, opts); err != nil {
		db.Close()
		return nil, fmt.Errorf("gojtopen: set sql attributes: %w", err)
	}
	if c.cfg.Library != "" {
		// NDB ADD_LIBRARY_LIST is treated as a session-init
		// handshake by PUB400 V7R5; we send it whenever we have
		// a library to set so PREPARE doesn't get -401.
		if err := hostserver.NDBAddLibraryList(db, c.cfg.Library, 2); err != nil {
			db.Close()
			return nil, fmt.Errorf("gojtopen: add library list: %w", err)
		}
	}

	return &Conn{conn: db, cfg: c.cfg, serverVRM: serverVRM}, nil
}

// Conn implements driver.Conn (and the Context-aware extensions).
// One Conn = one as-database socket plus the per-connection
// correlation-ID counter the host-server protocol requires.
type Conn struct {
	conn      net.Conn
	cfg       *Config
	corrCount uint32 // monotonic, incremented atomically per request
	closed    bool

	// serverVRM is the IBM i version/release/modification packed as
	// 0x00VVRRMM, captured from the SignonInfoReply at connect time.
	// Used to gate features that require V7R5+ (CCSID 1208 string
	// binds, etc.). 0 if the connection didn't capture a value.
	serverVRM uint32
}

// preferredStringCCSID returns the CCSID the driver should tag
// VARCHAR string binds with. V7R5+ servers accept CCSID 1208 (UTF-8)
// passthrough, which preserves the full Unicode repertoire on the
// wire. Older servers want a single-byte EBCDIC table -- we use 37
// (US English) which is what most IBM i jobs default to and what
// our encoder has a built-in mapping for.
//
// The encoder's CCSID 1208 branch (db_prepared.go) writes the UTF-8
// bytes verbatim with the CCSID tag set to 1208, leaving the server
// to transcode to the column's actual CCSID on its side.
//
// Static IBM i version map:
//   0x00070500  V7R5M0  -- 1208 supported
//   0x00070400  V7R4M0  -- 1208 supported
//   0x00070300  V7R3M0  -- 1208 supported
//   < V7R3      -- fall back to CCSID 37
//
// Caller can override the auto-pick via DSN ?ccsid=N (cfg.CCSID).
// When set non-zero, that wins over the VRM-driven default. Useful
// for installs where the user knows their server speaks a specific
// SBCS variant (e.g. CCSID 1140 Euro, CCSID 5026 Japan) the auto
// path doesn't model.
func (c *Conn) preferredStringCCSID() uint16 {
	if c.cfg != nil && c.cfg.CCSID != 0 {
		return c.cfg.CCSID
	}
	if c.serverVRM >= 0x00070300 {
		return 1208
	}
	return 37
}

// nextCorr returns the next correlation ID for a host-server frame.
// The protocol wants per-request unique values; we use atomic
// increments so concurrent statements on the same Conn (which
// database/sql technically forbids but defense doesn't hurt) don't
// collide on the wire.
func (c *Conn) nextCorr() uint32 {
	// Reserve a block of 100 IDs per request -- some host-server
	// flows (PREPARE then EXECUTE then FETCH) issue several frames
	// under one logical operation and reuse adjacent IDs.
	return atomic.AddUint32(&c.corrCount, 100)
}

// nextCorrFunc returns a closure that mints fresh correlation IDs
// from the same atomic counter as nextCorr but one ID at a time
// (no block reservation). Used by streaming flows (OpenSelectStatic
// and friends) where the driver and a long-lived *hostserver.Cursor
// need to share the counter across multiple Next / Close calls.
func (c *Conn) nextCorrFunc() func() uint32 {
	return func() uint32 {
		return atomic.AddUint32(&c.corrCount, 1)
	}
}

// Prepare returns a Stmt that buffers the SQL string. Real
// PREPARE-on-the-wire happens at execute time; this matches how our
// hostserver.SelectStaticSQL and ExecuteImmediate work today.
//
// (M3 deferred: a true PREPARE that returns a server-side handle to
// reuse across executions. Lands when we add prepared-bind I/U/D.)
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	return &Stmt{conn: c, query: query}, nil
}

// Close releases the as-database socket.
func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// Begin / BeginTx start a transaction by flipping autocommit off
// (which also bundles the commitment level + locator persistence
// per JT400 -- see hostserver.AutocommitOff for the wire details).
//
// Drains the result of any prior session change before returning
// the Tx wrapper.
func (c *Conn) Begin() (driver.Tx, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if err := hostserver.AutocommitOff(c.conn, c.nextCorr()); err != nil {
		return nil, c.classifyConnErr(fmt.Errorf("gojtopen: autocommit off: %w", err))
	}
	return &Tx{conn: c}, nil
}

func contextDeadline(ctx context.Context, def time.Duration) (time.Time, bool) {
	if ctx != nil {
		if d, ok := ctx.Deadline(); ok {
			return d, true
		}
	}
	return time.Now().Add(def), false
}

func dialWithDeadline(network, addr string, deadline time.Time) (net.Conn, error) {
	timeout := time.Until(deadline)
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return net.DialTimeout(network, addr, timeout)
}

// dialHostServer dials addr <host>:<port> using TCP if cfg.TLS is
// false, or wraps with crypto/tls.Dial when true. The returned
// net.Conn is type-erased so the host-server framer doesn't need to
// know whether it's reading from a plaintext socket or a TLS record
// stream. Honours the deadline as a hard timeout for the whole
// dial+handshake (TLS handshake can be slow on first connect).
//
// IBM i SSL host-server certs are commonly self-signed and lack DNS
// SANs that match the address being connected to (notably when
// connecting via SSH-tunneled localhost), so cfg.TLSInsecureSkipVerify
// is honoured. Use sparingly -- it disables MITM protection on the
// path between the Go client and the IBM i.
func dialHostServer(cfg *Config, port int, deadline time.Time) (net.Conn, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, port)
	if !cfg.TLS {
		return dialWithDeadline("tcp", addr, deadline)
	}

	timeout := time.Until(deadline)
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	tlsCfg := &tls.Config{
		ServerName:         tlsServerName(cfg),
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
		// IBM i SSL host server (5722-SS1 OS extension) supports
		// TLS 1.2 from V7R3 onwards and TLS 1.3 from V7R4 cumulative
		// PTFs. Floor at 1.2 -- older releases want a special
		// configuration override anyway.
		MinVersion: tls.VersionTLS12,
	}
	return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, tlsCfg)
}

// tlsServerName picks the SNI / cert-verify hostname. Defaults to
// cfg.Host so a public IBM i with a properly-issued cert "just
// works"; cfg.TLSServerName overrides for the SAN-mismatch case.
func tlsServerName(cfg *Config) string {
	if cfg.TLSServerName != "" {
		return cfg.TLSServerName
	}
	return cfg.Host
}
