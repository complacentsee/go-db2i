// Package driver registers goJTOpen as a database/sql driver under
// the name "gojtopen", letting Go applications talk to IBM i (Db2
// for i) over the host-server datastream protocol on ports 8471
// (as-database) and 8476 (as-signon) -- no CGo, no Java sidecar,
// no IBM client libraries.
//
// # Quick start
//
// Anonymous import wires up the registration:
//
//	import _ "github.com/complacentsee/goJTOpen/driver"
//
// Then use the standard database/sql APIs:
//
//	db, err := sql.Open("gojtopen", "gojtopen://USER:PWD@host:8471/?library=MYLIB&date=iso")
//	rows, err := db.Query("SELECT id, name FROM mylib.mytable WHERE status = ?", "OPEN")
//
// # DSN syntax
//
//	gojtopen://USER:PASSWORD@HOST[:PORT]/?key=value&key=value
//
// PORT defaults to 8471 (as-database). Recognised query-string keys:
//
//	library      Default schema for unqualified SQL names. Sent via
//	             SET_SQL_ATTRIBUTES CP 0x380F. Required if the user's
//	             job library list doesn't already contain the schema.
//	             Upper-cased on parse.
//	signon-port  Port for the as-signon service. Default 8476.
//	date         Session date format. One of job, iso, usa, eur, jis,
//	             mdy, dmy, ymd. Default job (uses the job's locale).
//	isolation    Session commitment level. One of none (*NONE), cs
//	             (*CS), all (*ALL), rs (*RS), rr (*RR). Default none
//	             (matches IBM i Db2's autocommit-permissive baseline).
//	             db.Begin() flips to *CS for the duration of the
//	             transaction.
//
// # Connection lifecycle
//
// Each pooled connection opens two TCP sockets in sequence: one to
// as-signon (8476) for password-substitute negotiation, then one to
// as-database (8471) for SQL traffic. The signon socket is closed
// once StartDatabaseService completes; the as-database socket lives
// for the lifetime of the Conn.
//
// Authentication uses the IBM i password level the server announces
// in the random-seed exchange:
//
//	0, 1  DES (spec-validated only; no live target available)
//	2, 3  SHA-1 password substitute
//	4     PBKDF2-HMAC-SHA-512 (10022 iterations, UTF-8 password)
//
// # Errors
//
// Server-side SQL errors (syntax, constraint violations, lock
// timeouts, etc.) come back as [*hostserver.Db2Error]. Use
// errors.As plus the predicate methods to drive retry logic:
//
//	var dbErr *hostserver.Db2Error
//	if errors.As(err, &dbErr) {
//	    switch {
//	    case dbErr.IsConstraintViolation(): // SQLSTATE 23xxx
//	    case dbErr.IsLockTimeout():         // SQLCODE -911 / -913
//	    case dbErr.IsConnectionLost():      // SQLSTATE 08xxx
//	    }
//	}
//
// TCP-level failures (peer drops, I/O timeouts, short frames) get
// wrapped so they satisfy errors.Is(err, driver.ErrBadConn). The
// database/sql pool then retires the dead connection and creates a
// fresh one transparently.
//
// # Context cancellation
//
// Stmt.QueryContext / Stmt.ExecContext propagate ctx to the
// underlying net.Conn via SetDeadline plus a context.AfterFunc, so a
// canceled or timed-out request unblocks the in-flight host-server
// read instead of hanging for the connection's full timeout. The
// returned error satisfies errors.Is(err, context.Canceled) /
// context.DeadlineExceeded regardless of which I/O step bailed.
//
// # Streaming Rows
//
// SELECT result sets stream batch-by-batch via continuation FETCH
// rather than buffering everything in memory. A million-row query
// pays one ~32 KB-buffer round-trip per batch, not one per row;
// memory stays bounded.
//
// # Server compatibility
//
// V7R3+ servers accept CCSID 1208 (UTF-8) string binds, preserving
// the full Unicode repertoire on the wire. Older releases fall back
// to CCSID 37 (US English EBCDIC). The choice is automatic based on
// the server VRM captured at sign-on; nothing for the caller to
// configure.
//
// V7R6 (V7R6M0) is the wire-validated target; V7R3-V7R5 should work
// at protocol parity but are not regularly tested.
//
// Requires Go 1.23+ (uses context.AfterFunc).
package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// Driver registered as "gojtopen" via sql.Register at init time.
type Driver struct{}

func init() {
	sql.Register("gojtopen", &Driver{})
}

// Open implements driver.Driver. database/sql calls it with the DSN
// the caller passed to sql.Open. We delegate to OpenConnector so the
// DSN parsing path is exercised the same regardless of whether the
// caller goes through sql.Open or sql.OpenDB(driver.OpenConnector(dsn)).
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(nil)
}

// OpenConnector parses the DSN into a Connector that can spawn fresh
// driver.Conn values on demand. Implements driver.DriverContext.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("gojtopen: parse DSN: %w", err)
	}
	return &Connector{cfg: cfg, drv: d}, nil
}

// Connector lets database/sql open multiple connections from one
// parsed DSN. Implements driver.Connector.
type Connector struct {
	cfg *Config
	drv *Driver
}

func (c *Connector) Driver() driver.Driver { return c.drv }

// Config is the parsed form of a gojtopen DSN. Public so tests can
// build connections without round-tripping through the URL parser.
type Config struct {
	User       string
	Password   string
	Host       string
	DBPort     int    // as-database service (default 8471)
	SignonPort int    // as-signon service  (default 8476)
	Library    string // default SQL schema; empty = no override
	DateFormat byte   // hostserver.DateFormat* constant; 0 = JOB
	Isolation  int16  // hostserver.Isolation* constant; -1 = default (CS)
}

// DefaultConfig returns the values used when DSN doesn't specify a
// given key. Public so callers can construct Configs programmatically
// and only override the fields they care about.
//
// Isolation defaults to *NONE (commitNone). This matches IBM i Db2's
// autocommit-permissive default and allows DML against non-journaled
// tables without a transaction wrapper. Callers that need real
// transactions opt in via db.Begin() -- the driver's Begin handler
// flips to *CS for the duration of the transaction (see
// hostserver.AutocommitOff) and back to *NONE on Commit/Rollback.
//
// Override via the DSN "isolation" query key if you specifically want
// the connection to start in a different commitment-control level.
func DefaultConfig() Config {
	return Config{
		DBPort:     8471,
		SignonPort: 8476,
		DateFormat: hostserver.DateFormatJOB,
		Isolation:  hostserver.IsolationCommitNone,
	}
}

func parseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "gojtopen" {
		return nil, fmt.Errorf("scheme %q not recognised (want gojtopen)", u.Scheme)
	}
	if u.User == nil {
		return nil, fmt.Errorf("DSN missing user info -- expected gojtopen://USER:PWD@HOST/...")
	}
	cfg := DefaultConfig()
	cfg.User = u.User.Username()
	if pwd, ok := u.User.Password(); ok {
		cfg.Password = pwd
	}
	cfg.Host = u.Hostname()
	if cfg.Host == "" {
		return nil, fmt.Errorf("DSN missing host")
	}
	if portStr := u.Port(); portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
		}
		cfg.DBPort = port
	}

	q := u.Query()
	if v := q.Get("library"); v != "" {
		cfg.Library = strings.ToUpper(v)
	}
	if v := q.Get("signon-port"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid signon-port %q: %w", v, err)
		}
		cfg.SignonPort = port
	}
	if v := q.Get("date"); v != "" {
		switch strings.ToLower(v) {
		case "job":
			cfg.DateFormat = hostserver.DateFormatJOB
		case "iso":
			cfg.DateFormat = hostserver.DateFormatISO
		case "usa":
			cfg.DateFormat = hostserver.DateFormatUSA
		case "eur":
			cfg.DateFormat = hostserver.DateFormatEUR
		case "jis":
			cfg.DateFormat = hostserver.DateFormatJIS
		case "mdy":
			cfg.DateFormat = hostserver.DateFormatMDY
		case "dmy":
			cfg.DateFormat = hostserver.DateFormatDMY
		case "ymd":
			cfg.DateFormat = hostserver.DateFormatYMD
		default:
			return nil, fmt.Errorf("invalid date format %q (want job|iso|usa|eur|jis|mdy|dmy|ymd)", v)
		}
	}
	if v := q.Get("isolation"); v != "" {
		switch strings.ToLower(v) {
		case "none":
			cfg.Isolation = hostserver.IsolationCommitNone
		case "cs":
			cfg.Isolation = hostserver.IsolationReadCommitted
		case "all":
			cfg.Isolation = hostserver.IsolationAllCS
		case "rr":
			cfg.Isolation = hostserver.IsolationRepeatableRd
		case "rs":
			cfg.Isolation = hostserver.IsolationSerializable
		default:
			return nil, fmt.Errorf("invalid isolation %q (want none|cs|all|rr|rs)", v)
		}
	}
	return &cfg, nil
}
