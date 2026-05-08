// Package driver registers goJTOpen as a database/sql driver under
// the name "gojtopen". After import (anonymous):
//
//	import _ "github.com/complacentsee/goJTOpen/driver"
//
// open with the standard sql.Open path:
//
//	db, err := sql.Open("gojtopen", "gojtopen://user:pwd@host:8471/?library=MYLIB&date=iso")
//	rows, err := db.Query("SELECT ID, NAME FROM MYLIB.MYTABLE WHERE STATUS = ?", "OPEN")
//
// DSN syntax:
//
//	gojtopen://USER:PASSWORD@HOST[:PORT]/?key=value&key=value
//
// Recognised query-string keys:
//
//	library      -- default schema for unqualified SQL names. Sent
//	                via SET_SQL_ATTRIBUTES CP 0x380F. Required if the
//	                user's job library list doesn't already contain
//	                the schema.
//	signon-port  -- port for the as-signon service (default 8476).
//	date         -- session date format (one of: job, iso, usa, eur,
//	                jis, mdy, dmy, ymd). Default: job.
//	isolation    -- session commitment level (none, cs, all, rs, rr).
//	                Default: cs (matches IBM i Db2 default).
//
// The driver opens TWO TCP connections per sql.DB.Conn: one to the
// as-signon service for password substitute negotiation, then one
// to the as-database service for SQL traffic. The signon socket is
// closed after StartDatabaseService completes; the as-database
// socket lives for the lifetime of the Conn.
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
