// Package driver registers go-db2i as a database/sql driver under
// the name "db2i", letting Go applications talk to IBM i (Db2
// for i) over the host-server datastream protocol on ports 8471
// (as-database) and 8476 (as-signon) -- no CGo, no Java sidecar,
// no IBM client libraries.
//
// # Quick start
//
// Anonymous import wires up the registration:
//
//	import _ "github.com/complacentsee/go-db2i/driver"
//
// Then use the standard database/sql APIs:
//
//	db, err := sql.Open("db2i", "db2i://USER:PWD@host:8471/?library=MYLIB&date=iso")
//	rows, err := db.Query("SELECT id, name FROM mylib.mytable WHERE status = ?", "OPEN")
//
// # DSN syntax
//
//	db2i://USER:PASSWORD@HOST[:PORT]/?key=value&key=value
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
//	lob          BLOB / CLOB / DBCLOB scan mode. One of materialise
//	             (default; full content into []byte / string at Scan
//	             time, fits the small-to-medium LOB common case) or
//	             stream (returns *db2i.LOBReader -- an io.Reader
//	             + io.Closer the caller drives via successive
//	             RETRIEVE_LOB_DATA chunks). Stream mode is the only
//	             way to handle multi-GB LOBs without exhausting Go
//	             heap. Spelt either materialise or materialize.
//	tls          true | false. Wraps both as-signon and as-database
//	             sockets in crypto/tls. Default false; when true the
//	             default ports flip to 9476 / 9471 (the IBM i SSL
//	             host-server pair) unless the caller overrode them.
//	tls-insecure-skip-verify  Disables server-cert verification.
//	             Useful for self-signed IBM i certs that lack DNS
//	             SANs; equivalent to JT400's setUseSSL with cert
//	             validation off.
//	tls-server-name  Overrides the SNI / cert-verify hostname.
//	ccsid        Application-data CCSID. Overrides the connection-
//	             level "ClientCCSID" negotiated at sign-on (CP 0x3801)
//	             and the parameter-bind CCSID tag for string values.
//	             Columns that explicitly tag a CCSID in their schema
//	             ignore this on the read side -- per-column CCSID
//	             always wins. Default 0 = auto: 13488 (UCS-2 BE)
//	             for the SET_SQL_ATTRIBUTES negotiation,
//	             1208 (UTF-8) for parameter binds on V7R3+ servers
//	             and CCSID 37 elsewhere. Common explicit values are
//	             1208 (UTF-8) and 37 (US English EBCDIC).
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
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/complacentsee/go-db2i/hostserver"
)

// Driver registered as "db2i" via sql.Register at init time.
type Driver struct{}

// registeredDriver is the singleton sql.Register handed out under
// the "db2i" name. NewConnector hands this same pointer back so
// db.Driver() and sql.Open share identity.
var registeredDriver = &Driver{}

func init() {
	sql.Register("db2i", registeredDriver)
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
		return nil, fmt.Errorf("db2i: parse DSN: %w", err)
	}
	return &Connector{cfg: cfg, drv: d}, nil
}

// Connector lets database/sql open multiple connections from one
// parsed DSN. Implements driver.Connector.
type Connector struct {
	cfg *Config
	drv *Driver
}

// NewConnector builds a Connector from a programmatically-assembled
// Config. Use this when you need to set fields that can't be
// expressed in a DSN string -- Config.Logger and Config.LogSQL are
// the main ones today. Pass the returned Connector to sql.OpenDB to
// get a *sql.DB:
//
//	cfg := db2i.DefaultConfig()
//	cfg.User, cfg.Password, cfg.Host = "USR", "PWD", "host.example.com"
//	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
//	connector, err := db2i.NewConnector(&cfg)
//	db := sql.OpenDB(connector)
//
// Returns an error if cfg lacks the minimum required fields
// (User, Host, DBPort, SignonPort).
func NewConnector(cfg *Config) (*Connector, error) {
	if cfg == nil {
		return nil, fmt.Errorf("db2i: NewConnector requires a non-nil Config")
	}
	if cfg.User == "" {
		return nil, fmt.Errorf("db2i: NewConnector: Config.User is empty")
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("db2i: NewConnector: Config.Host is empty")
	}
	if cfg.DBPort <= 0 || cfg.DBPort > 65535 {
		return nil, fmt.Errorf("db2i: NewConnector: Config.DBPort %d out of range (1..65535)", cfg.DBPort)
	}
	if cfg.SignonPort <= 0 || cfg.SignonPort > 65535 {
		return nil, fmt.Errorf("db2i: NewConnector: Config.SignonPort %d out of range (1..65535)", cfg.SignonPort)
	}
	return &Connector{cfg: cfg, drv: registeredDriver}, nil
}

// Driver returns the *Driver that produced this Connector. Used by
// database/sql for some pool-management decisions. Implements
// database/sql/driver.Connector.Driver.
func (c *Connector) Driver() driver.Driver { return c.drv }

// Config is the parsed form of a go-db2i DSN. Public so tests can
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

	// TLS controls whether the driver wraps the as-signon and
	// as-database sockets in crypto/tls. When true, the dial uses
	// tls.Dial against the configured ports; the default ports flip
	// to 9476 (signon) / 9471 (database) -- the IBM i SSL host-
	// server pair -- if the caller didn't override them.
	TLS bool

	// TLSInsecureSkipVerify disables server-cert verification. IBM i
	// host-server certs are commonly self-signed and lack DNS SANs;
	// crypto/tls otherwise rejects them. Use with care: setting this
	// true makes the connection vulnerable to MITM attack on the
	// network path between client and server.
	TLSInsecureSkipVerify bool

	// TLSServerName overrides the SNI / cert-verify hostname. When
	// empty, defaults to Host. Useful when the cert was issued for
	// a different name than the address you connect to (e.g. the
	// short hostname vs the FQDN in DNS).
	TLSServerName string

	// LOBStream switches BLOB / CLOB / DBCLOB columns from the
	// default "materialise on Scan" behaviour to the streaming
	// LOBReader API. When true, Rows.Next writes a *LOBReader into
	// the destination slot for LOB columns; callers must scan into
	// **db2i.LOBReader and Read / Close it before advancing
	// the row. The materialise default keeps existing callers
	// working unchanged.
	LOBStream bool

	// CCSID overrides the application-data CCSID negotiated at
	// SET_SQL_ATTRIBUTES (CP 0x3801, "ClientCCSID"). It tells the
	// server which CCSID to encode CHAR / VARCHAR / CLOB column data
	// in when the column itself doesn't tag a CCSID, AND the CCSID
	// the driver tags on string parameter binds.
	//
	// The default 0 means "auto-pick": 13488 (UCS-2 BE) for the
	// SET_SQL_ATTRIBUTES negotiation (matching JT400's default), and
	// 1208 (UTF-8) for parameter-bind tagging on V7R3+ servers /
	// CCSID 37 on older releases (see preferredStringCCSID).
	//
	// Common explicit values:
	//   1208   UTF-8 -- preserves the full Unicode repertoire on
	//          the wire when both client and server speak UTF-8.
	//          V7R3+ targets only.
	//   37     US English EBCDIC -- minimal SBCS subset, every IBM i
	//          job understands it. Use for legacy installs.
	//   273    German EBCDIC -- the historical PUB400 default; pin
	//          this if the server's job CCSID is 273 and you want
	//          parameter-bind to land on the same table.
	//
	// Columns that explicitly tag a CCSID in their definition (e.g.
	// `VARCHAR(64) CCSID 1208`) ignore this setting on the read side
	// -- the per-column CCSID always wins. CCSID is for the
	// "untagged" / connection-default case.
	CCSID uint16

	// LOBThreshold is the byte count below which the server returns
	// LOB columns inline in row data (and accepts inline LOB shapes
	// on bind) instead of allocating a server-side locator. Sent as
	// CP 0x3822 in SET_SQL_ATTRIBUTES; matches JT400's "lob
	// threshold" JDBC URL knob. Configured via the DSN
	// "lob-threshold" query key.
	//
	// 0 = the historical default (32768). Raising it inlines bigger
	// LOBs at the cost of buffer memory; lowering it forces the
	// locator path so RETRIEVE_LOB_DATA covers anything beyond the
	// chosen threshold.
	LOBThreshold uint32

	// ExtendedDynamic switches the connection on to JT400's
	// extended-dynamic SQL package caching. When true and PackageName
	// is non-empty, the driver instructs the server to maintain a
	// persistent *PGM that accumulates PREPAREd statements across
	// connections so a co-tenant reconnect skips the PREPARE round-
	// trip. Mirrors JT400's "extended dynamic" JDBC URL knob;
	// configured via the DSN "extended-dynamic=true" query key.
	// Default false to preserve the pre-flag wire shape byte-for-byte.
	ExtendedDynamic bool

	// PackageName is the user-chosen base for the on-wire package
	// name (1-6 chars from the IBM-i object-name charset:
	// A-Z 0-9 _ # @ $). The 10-char wire name is base + a 4-char
	// suffix derived from session options; see
	// hostserver.BuildPackageName.
	PackageName string

	// PackageLibrary is the library the package object lives in.
	// Default "QGPL". Up to 10 chars from the IBM-i object-name
	// charset.
	PackageLibrary string

	// PackageCache enables the client-side fast path. When true the
	// driver issues a RETURN_PACKAGE on connect to download the
	// server's cached statement entries, then bypasses PREPARE for
	// any cache-hit SQL on subsequent Stmt.Exec / Query calls.
	// Default false (statements still get added to the server-side
	// package via CP 0x3804 on PREPARE, but every Stmt.Prepare on
	// the client still round-trips).
	PackageCache bool

	// PackageError selects how the driver handles errors from the
	// CREATE_PACKAGE / RETURN_PACKAGE / CP 0x3804 paths. Mirrors
	// JT400's "package error" JDBC URL knob; configured via the DSN
	// "package-error=warning|exception|none" query key.
	//   "warning"   (default) slog.Warn + continue without package
	//   "exception" return the error to the database/sql caller
	//   "none"      silent drop + continue without package
	PackageError string

	// PackageCriteria filters which SQL strings the driver considers
	// for cache insertion / lookup. Mirrors JT400's
	// "package criteria" JDBC URL knob.
	//   "default" (default) only parameterised statements (the JT400
	//             rule: marker count > 0 && !isCurrentOf && various
	//             special-case shapes)
	//   "select"  default rules plus all SELECTs (broader cache)
	PackageCriteria string

	// PackageCCSID is the CCSID the server uses to write package-
	// stored SQL text on disk. JT400's default is 13488 (UCS-2 BE);
	// a value of 0 means "system default" (the connection's job
	// CCSID). The driver accepts 13488 and 1200 (UTF-16 LE); other
	// values must wait for the M11+ broader package-CCSID work.
	PackageCCSID int

	// ExtendedMetadata, when true, asks the server to include CP
	// 0x3811 (extended column descriptors) in every PREPARE_DESCRIBE
	// reply by ORing the ORSExtendedColumnDescrs (0x00020000) bit
	// into the request ORS bitmap. The driver then surfaces the
	// per-column schema name, base table name, base column name,
	// and column label through go-db2i-specific Rows methods
	// (Rows.ColumnTypeSchemaName / Rows.ColumnTypeTableName etc.).
	// Mirrors JT400's "extended metadata=true" JDBC URL knob;
	// configured via the DSN "extended-metadata=true" query key.
	// Default false to preserve the pre-flag wire shape byte-for-byte.
	ExtendedMetadata bool

	// Logger is the *slog.Logger callers want the driver to emit
	// diagnostic events through. Nil (the default) silences all
	// driver-side logging; the driver internally substitutes a
	// no-op handler so call sites never have to nil-check.
	//
	// At connect time the driver derives a child logger that carries
	// the attrs `driver=db2i`, `conn_id=<corr-base>`, and
	// `dsn_host=<host>`, so every line a single connection emits is
	// pre-tagged. Levels used:
	//   DEBUG  Per wire-level operation (PREPARE+EXECUTE,
	//          continuation FETCH boundary, RETRIEVE_LOB_DATA chunk
	//          boundary, WRITE_LOB_DATA frame boundary). Costs one
	//          line per operation -- enable for diagnosis, leave off
	//          in production.
	//   INFO   Connect success, Close.
	//   WARN   Retried-on-ErrBadConn classification.
	//   ERROR  Fatal classification before the error returns to the
	//          database/sql caller.
	//
	// SQL text is never logged unless LogSQL is also true; parameter
	// counts are logged, parameter values never are.
	Logger *slog.Logger

	// LogSQL gates whether the driver attaches the SQL text as an
	// attribute on Stmt.Exec / Stmt.Query DEBUG lines AND the
	// db.statement OpenTelemetry span attribute. Off by default
	// because SQL text often carries customer identifiers or other
	// data callers wouldn't want flowing through their log /
	// trace pipeline. Set true when actively debugging a specific
	// query.
	LogSQL bool

	// Tracer is the OpenTelemetry trace.Tracer the driver uses to
	// emit spans on Stmt.ExecContext / Stmt.QueryContext. Nil (the
	// default) disables span emission via an internal noop-tracer
	// fallback so call sites never have to nil-check.
	//
	// The driver follows the OpenTelemetry database semantic
	// conventions. Span names are the operation kind ("EXEC",
	// "QUERY"), and attributes include `db.system.name`,
	// `db.namespace` (Library), `db.user`, `server.address`,
	// `server.port`, `db.statement.parameters.count`, and for Exec
	// `db.response.returned_rows`. SQL text rides on `db.statement`
	// when LogSQL is also true. Errors set the span status to Error
	// and record the *Db2Error sqlstate / sqlcode / message-id when
	// the underlying error is a typed Db2Error.
	//
	// Use the API package (`go.opentelemetry.io/otel/trace`), not
	// the SDK, so callers can plug in any OTel SDK / exporter.
	Tracer trace.Tracer
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
		// Package-cache defaults match JT400's JDProperties.java
		// (PACKAGE_LIBRARY="QGPL", PACKAGE_ERROR="warning",
		// PACKAGE_CRITERIA="default", PACKAGE_CCSID=13488). The
		// gating flags ExtendedDynamic + PackageCache start false
		// so an unmodified Config never touches the package wire
		// at all.
		PackageLibrary:  "QGPL",
		PackageError:    "warning",
		PackageCriteria: "default",
		PackageCCSID:    13488,
	}
}

// silentLogger is the no-op logger the driver substitutes when
// Config.Logger is nil. Built once at init so per-Conn child
// loggers don't allocate when no caller-supplied logger is set.
var silentLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))

// resolveLogger returns the caller-supplied logger or the silent
// fallback. Always non-nil so call sites can skip the nil check.
func resolveLogger(l *slog.Logger) *slog.Logger {
	if l != nil {
		return l
	}
	return silentLogger
}

// noopTracer is the no-op trace.Tracer the driver substitutes when
// Config.Tracer is nil. Spans started on it are valid but record
// nothing.
var noopTracer = noop.NewTracerProvider().Tracer("db2i")

// resolveTracer returns the caller-supplied tracer or the no-op
// fallback. Always non-nil so call sites can skip the nil check.
func resolveTracer(t trace.Tracer) trace.Tracer {
	if t != nil {
		return t
	}
	return noopTracer
}

func parseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "db2i" {
		return nil, fmt.Errorf("scheme %q not recognised (want db2i)", u.Scheme)
	}
	if u.User == nil || u.User.Username() == "" {
		return nil, fmt.Errorf("DSN missing user info -- expected db2i://USER:PWD@HOST/...")
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
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("invalid port %d (want 1..65535)", port)
		}
		cfg.DBPort = port
	}

	q := u.Query()

	// TLS knobs are parsed before the port defaults so a tls=true
	// DSN can override-then-default to 9476 / 9471 in one pass.
	if v := q.Get("tls"); v != "" {
		b, err := parseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid tls %q (want true|false): %w", v, err)
		}
		cfg.TLS = b
	}
	if v := q.Get("tls-insecure-skip-verify"); v != "" {
		b, err := parseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid tls-insecure-skip-verify %q (want true|false): %w", v, err)
		}
		cfg.TLSInsecureSkipVerify = b
	}
	if v := q.Get("tls-server-name"); v != "" {
		cfg.TLSServerName = v
	}
	// Flip default ports to the SSL host-server pair when TLS is on
	// AND the caller didn't override via the URL port / signon-port
	// query keys. This matches what JT400 does when its
	// AS400.setUseSSL(true) is set.
	if cfg.TLS {
		if u.Port() == "" {
			cfg.DBPort = 9471
		}
		if q.Get("signon-port") == "" {
			cfg.SignonPort = 9476
		}
	}

	if v := q.Get("library"); v != "" {
		cfg.Library = strings.ToUpper(v)
	}
	if v := q.Get("signon-port"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid signon-port %q: %w", v, err)
		}
		if port < 1 || port > 65535 {
			return nil, fmt.Errorf("invalid signon-port %d (want 1..65535)", port)
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
	if v := q.Get("lob"); v != "" {
		switch strings.ToLower(v) {
		case "materialise", "materialize":
			cfg.LOBStream = false
		case "stream":
			cfg.LOBStream = true
		default:
			return nil, fmt.Errorf("invalid lob %q (want materialise|stream)", v)
		}
	}
	if v := q.Get("ccsid"); v != "" {
		n, err := strconv.ParseUint(v, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid ccsid %q (want unsigned 16-bit int): %w", v, err)
		}
		cfg.CCSID = uint16(n)
	}
	if v := q.Get("lob-threshold"); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid lob-threshold %q (want unsigned 32-bit int): %w", v, err)
		}
		cfg.LOBThreshold = uint32(n)
	}
	if v := q.Get("extended-metadata"); v != "" {
		b, err := parseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid extended-metadata %q (want true|false): %w", v, err)
		}
		cfg.ExtendedMetadata = b
	}
	if v := q.Get("extended-dynamic"); v != "" {
		b, err := parseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid extended-dynamic %q (want true|false): %w", v, err)
		}
		cfg.ExtendedDynamic = b
	}
	if v := q.Get("package"); v != "" {
		// Uppercase + space->underscore at the boundary so every
		// downstream caller sees normalised bytes. Validate the
		// charset before anything in the driver tries to hand it
		// off to BuildPackageName. Max 10 chars matches the IBM i
		// object-name limit JT400 accepts -- the encoder later
		// truncates to 6 chars before the 4-char options suffix is
		// appended, byte-equal to JT400 (JDPackageManager.java:466).
		canon := canonPackageIdent(v)
		if err := validatePackageIdent(canon, 10); err != nil {
			return nil, fmt.Errorf("invalid package %q: %w", v, err)
		}
		cfg.PackageName = canon
	}
	if v := q.Get("package-library"); v != "" {
		canon := canonPackageIdent(v)
		if err := validatePackageIdent(canon, 10); err != nil {
			return nil, fmt.Errorf("invalid package-library %q: %w", v, err)
		}
		cfg.PackageLibrary = canon
	}
	if v := q.Get("package-cache"); v != "" {
		b, err := parseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid package-cache %q (want true|false): %w", v, err)
		}
		cfg.PackageCache = b
	}
	if v := q.Get("package-error"); v != "" {
		switch strings.ToLower(v) {
		case "warning", "exception", "none":
			cfg.PackageError = strings.ToLower(v)
		default:
			return nil, fmt.Errorf("invalid package-error %q (want warning|exception|none)", v)
		}
	}
	if v := q.Get("package-criteria"); v != "" {
		switch strings.ToLower(v) {
		case "default", "select":
			cfg.PackageCriteria = strings.ToLower(v)
		default:
			return nil, fmt.Errorf("invalid package-criteria %q (want default|select)", v)
		}
	}
	if v := q.Get("package-ccsid"); v != "" {
		switch strings.ToLower(v) {
		case "system":
			cfg.PackageCCSID = 0
		case "1200":
			cfg.PackageCCSID = 1200
		case "13488":
			cfg.PackageCCSID = 13488
		default:
			// Reject any other numeric value with an explicit
			// message that points at the M11+ deferral so users
			// hitting this know where to track the broader work.
			return nil, fmt.Errorf("invalid package-ccsid %q (want 13488 | 1200 | system; broader CCSID set deferred to M11+)", v)
		}
	}
	// `package-add` is a JT400 knob whose only documented value is
	// "true" (statements get added to the package). go-db2i always
	// adds. Accept it for DSN-migration friendliness; reject other
	// values so a typo doesn't silently no-op.
	if v := q.Get("package-add"); v != "" {
		b, err := parseBool(v)
		if err != nil {
			return nil, fmt.Errorf("invalid package-add %q (want true|false; go-db2i always adds when extended-dynamic is on): %w", v, err)
		}
		if !b {
			return nil, fmt.Errorf("package-add=false is not supported (go-db2i always adds when extended-dynamic is on)")
		}
	}
	// `package-clear` is another JT400 migration knob; the server now
	// manages clearing on its own. Accept the key, validate the
	// shape, slog.Warn from the driver later when the connection
	// opens. Doing nothing else for now.
	if v := q.Get("package-clear"); v != "" {
		if _, err := parseBool(v); err != nil {
			return nil, fmt.Errorf("invalid package-clear %q (want true|false; server-managed in go-db2i): %w", v, err)
		}
		// The actual warn is emitted by the connect path so it
		// rides on the Conn-scoped logger (with conn_id attrs);
		// we just stash a flag here. But we don't have anywhere
		// to stash it yet -- so for now the DSN parse just
		// validates the shape and ignores the value.
	}
	// Cross-key sanity: package-cache=true requires extended-dynamic.
	if cfg.PackageCache && !cfg.ExtendedDynamic {
		return nil, fmt.Errorf("package-cache=true requires extended-dynamic=true")
	}
	// PackageName mandatory when extended-dynamic on (otherwise the
	// driver can't put together a CP 0x3804 to send).
	if cfg.ExtendedDynamic && cfg.PackageName == "" {
		return nil, fmt.Errorf("extended-dynamic=true requires package=<name>")
	}
	return &cfg, nil
}

// canonPackageIdent normalises a package or library identifier to
// uppercase with spaces turned into underscores, matching JT400's
// boundary normalisation in JDPackageManager.java's package-name
// derivation. The result still needs validatePackageIdent to confirm
// charset + length.
func canonPackageIdent(s string) string {
	return strings.ReplaceAll(strings.ToUpper(s), " ", "_")
}

// validatePackageIdent enforces the IBM-i object-name rules JT400's
// validateName method (JDProperties.java:1690) applies: 1..max chars,
// each from the set [A-Z 0-9 _ # @ $]. Accepts the canonical form
// produced by canonPackageIdent.
func validatePackageIdent(s string, max int) error {
	if s == "" {
		return fmt.Errorf("empty")
	}
	if len(s) > max {
		return fmt.Errorf("length %d > %d", len(s), max)
	}
	for i, r := range s {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '#' || r == '@' || r == '$':
		default:
			return fmt.Errorf("char %d (%q) outside [A-Z 0-9 _ # @ $]", i, string(r))
		}
	}
	return nil
}

// parseBool accepts the same case-insensitive set Go's strconv.ParseBool
// does (1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False) plus
// the URL-friendly "yes" / "no" aliases that some users reach for.
func parseBool(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "yes", "on":
		return true, nil
	case "no", "off":
		return false, nil
	}
	return strconv.ParseBool(s)
}
