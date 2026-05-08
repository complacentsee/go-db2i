package driver

import (
	"context"
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
	signon, err := dialWithDeadline("tcp", fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.SignonPort), deadline)
	if err != nil {
		return nil, fmt.Errorf("gojtopen: dial as-signon: %w", err)
	}
	signon.SetDeadline(deadline)
	if _, _, err := hostserver.SignOn(signon, c.cfg.User, c.cfg.Password); err != nil {
		signon.Close()
		return nil, fmt.Errorf("gojtopen: as-signon: %w", err)
	}
	signon.Close()

	// Database service phase: separate TCP socket, lives for the
	// life of the Conn.
	db, err := dialWithDeadline("tcp", fmt.Sprintf("%s:%d", c.cfg.Host, c.cfg.DBPort), deadline)
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

	return &Conn{conn: db, cfg: c.cfg}, nil
}

// Conn implements driver.Conn (and the Context-aware extensions).
// One Conn = one as-database socket plus the per-connection
// correlation-ID counter the host-server protocol requires.
type Conn struct {
	conn      net.Conn
	cfg       *Config
	corrCount uint32 // monotonic, incremented atomically per request
	closed    bool
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
