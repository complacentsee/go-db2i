package driver

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/complacentsee/go-db2i/hostserver"
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

	// Derive a per-Conn child logger tagged with driver/host so
	// every line a single conn emits is pre-attributed. The conn_id
	// attr is the connection's correlation-counter base; database/sql
	// pool churn doesn't re-key it, so multiple sequential dials by
	// the same pool entry will show as distinct logical conns when
	// the underlying socket is recycled.
	log := resolveLogger(c.cfg.Logger).With(
		slog.String("driver", "db2i"),
		slog.String("dsn_host", c.cfg.Host),
	)

	// Sign-on phase: open as-signon, perform encrypted handshake.
	signon, err := dialHostServer(c.cfg, c.cfg.SignonPort, deadline)
	if err != nil {
		return nil, fmt.Errorf("db2i: dial as-signon: %w", err)
	}
	signon.SetDeadline(deadline)
	xs, _, err := hostserver.SignOn(signon, c.cfg.User, c.cfg.Password)
	if err != nil {
		signon.Close()
		return nil, fmt.Errorf("db2i: as-signon: %w", err)
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
		return nil, fmt.Errorf("db2i: dial as-database: %w", err)
	}
	db.SetDeadline(time.Time{}) // clear; per-statement deadlines are managed by callers
	if _, _, err := hostserver.StartDatabaseService(db, c.cfg.User, c.cfg.Password); err != nil {
		db.Close()
		return nil, fmt.Errorf("db2i: as-database start: %w", err)
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
	if c.cfg.ExtendedDynamic && c.cfg.PackageName != "" {
		// JT400 emits 5 additional date/time/separator CPs in
		// SET_SQL_ATTRIBUTES when "extended dynamic=true" so the
		// server has a definite value for the package-suffix
		// derivation. Without these the V7R6M0 server refuses to
		// file PREPAREd statements into the *PGM.
		opts.ExtendedDynamic = true
	}
	if _, err := hostserver.SetSQLAttributes(db, opts); err != nil {
		db.Close()
		return nil, fmt.Errorf("db2i: set sql attributes: %w", err)
	}
	if c.cfg.Library != "" {
		// NDB ADD_LIBRARY_LIST is treated as a session-init
		// handshake by PUB400 V7R5; we send it whenever we have
		// a library to set so PREPARE doesn't get -401.
		if err := hostserver.NDBAddLibraryList(db, c.cfg.Library, 2); err != nil {
			db.Close()
			return nil, fmt.Errorf("db2i: add library list: %w", err)
		}
	}

	conn := &Conn{
		conn:      db,
		cfg:       c.cfg,
		serverVRM: serverVRM,
		log:       log,
		tracer:    resolveTracer(c.cfg.Tracer),
	}
	conn.log = log.With(slog.Uint64("server_vrm", uint64(serverVRM)))

	// Extended-dynamic + package-cache wiring (M10-3). Has to run
	// AFTER NDBAddLibraryList because the package lives in a
	// library the session might just have added to its list; the
	// CREATE_PACKAGE call wouldn't resolve it otherwise.
	if c.cfg.ExtendedDynamic && c.cfg.PackageName != "" {
		if err := conn.initPackage(ctx); err != nil {
			// Package-error mode decides whether init failure is
			// fatal. The handler emits its own slog line and may
			// return nil to soft-fail with the package disabled.
			if fatal := conn.handlePackageError(ctx, "init", err); fatal != nil {
				db.Close()
				return nil, fmt.Errorf("db2i: init package: %w", fatal)
			}
			// Non-fatal: clear pkg so PREPARE_DESCRIBE goes
			// through the plain path.
			conn.pkg = nil
		}
	}

	conn.log.LogAttrs(ctx, slog.LevelInfo, "db2i: connected",
		slog.String("user", c.cfg.User),
		slog.Int("db_port", c.cfg.DBPort),
		slog.Int("signon_port", c.cfg.SignonPort),
		slog.Bool("tls", c.cfg.TLS),
		slog.String("library", c.cfg.Library),
		slog.Bool("extended_dynamic", c.cfg.ExtendedDynamic && conn.pkg != nil),
	)
	return conn, nil
}

// initPackage runs the per-connection package setup when
// Config.ExtendedDynamic is true. Mirrors JT400's connect-time
// JDPackageManager flow:
//
//  1. Derive the 10-char on-wire package name from cfg.PackageName +
//     active session options.
//  2. CREATE_PACKAGE the resolved name in cfg.PackageLibrary. The
//     server is idempotent: a re-create of an existing *PGM
//     returns success.
//  3. If cfg.PackageCache, RETURN_PACKAGE to download the cached
//     statement list. The raw bytes are captured for follow-up
//     parsing; the cache-hit fast path itself lands in a follow-up
//     to M10-3.
//
// Returns an error on hard failure; the caller folds it through
// Config.PackageError to decide whether to fail the connect or
// soft-disable the package for this connection.
func (c *Conn) initPackage(ctx context.Context) error {
	opts := c.packageOptions()
	wireName := hostserver.BuildPackageName(c.cfg.PackageName, opts)
	ccsid := uint16(37) // CCSID 37 is what JT400 uses on the wire for
	// the package-name/library var-strings (the JOB CCSID); the
	// `package-ccsid` DSN knob is for the per-statement SQL text
	// stored inside the *PGM, not these envelope fields.

	mgr := &hostserver.PackageManager{
		Name:    wireName,
		Library: c.cfg.PackageLibrary,
		CCSID:   uint16(c.cfg.PackageCCSID),
	}
	if err := hostserver.SendCreatePackage(c.conn, wireName, c.cfg.PackageLibrary, ccsid, c.nextCorrFunc()); err != nil {
		return fmt.Errorf("create-package %s/%s: %w", c.cfg.PackageLibrary, wireName, err)
	}

	if c.cfg.PackageCache {
		cached, err := hostserver.SendReturnPackage(c.conn, wireName, c.cfg.PackageLibrary, ccsid, c.nextCorrFunc())
		if err != nil {
			return fmt.Errorf("return-package %s/%s: %w", c.cfg.PackageLibrary, wireName, err)
		}
		mgr.Cached = make(map[string]*hostserver.PackageStatement, len(cached))
		for i := range cached {
			ps := cached[i]
			mgr.Cached[ps.SQLText] = &ps
		}
		c.log.LogAttrs(ctx, slog.LevelDebug, "db2i: RETURN_PACKAGE",
			slog.String("package", wireName),
			slog.String("library", c.cfg.PackageLibrary),
			slog.Int("cached_statements", len(cached)),
		)
	}

	c.pkg = mgr
	return nil
}

// packageOptions snapshots the session options the package-name
// suffix derivation reads. Today the driver only exposes
// DateFormat and Naming-style fields indirectly through other
// Config keys; the remaining option ints stay zero. The result is
// byte-equal to the suffix JT400 computes from the same DSN -- the
// load-bearing rule lives in
// project_db2i_m10_jt400_interop.md.
func (c *Conn) packageOptions() hostserver.PackageOptions {
	opts := hostserver.PackageOptions{
		// Naming defaults to 0 (sql); go-db2i's hostserver layer
		// uses period-qualified SQL identifiers, which is JT400's
		// "naming=sql" enum value. There is no separate Config
		// knob for system naming today.
		Naming: 0,
	}
	switch c.cfg.DateFormat {
	case hostserver.DateFormatJOB:
		// JT400 default index for date-format is 1 (mdy) when no
		// session date-format is in force. We mirror that here so
		// go-db2i's GOJTPK9899 derivation matches JT400's wire
		// output (asserted by hostserver.TestSuffixFromOptions_FixtureMatch).
		opts.DateFormat = 1
	case hostserver.DateFormatISO:
		opts.DateFormat = 5
	case hostserver.DateFormatUSA:
		opts.DateFormat = 4
	case hostserver.DateFormatEUR:
		opts.DateFormat = 6
	case hostserver.DateFormatJIS:
		opts.DateFormat = 7
	case hostserver.DateFormatMDY:
		opts.DateFormat = 1
	case hostserver.DateFormatDMY:
		opts.DateFormat = 2
	case hostserver.DateFormatYMD:
		opts.DateFormat = 3
	}
	return opts
}

// handlePackageError applies the Config.PackageError mode to a
// package-related error. Returns nil for "warning" and "none"
// modes (after slog-warn for the former), and the original err for
// "exception" mode. Caller uses the return value to decide whether
// to fail the connect or soft-disable the package.
func (c *Conn) handlePackageError(ctx context.Context, op string, err error) error {
	switch c.cfg.PackageError {
	case "exception":
		c.log.LogAttrs(ctx, slog.LevelError, "db2i: package "+op+" failed",
			slog.String("err", err.Error()),
		)
		return err
	case "none":
		// Silent drop; package soft-disabled.
		return nil
	default: // "warning"
		c.log.LogAttrs(ctx, slog.LevelWarn, "db2i: package "+op+" failed; continuing without package",
			slog.String("err", err.Error()),
		)
		return nil
	}
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

	// log is the per-conn child logger. Always non-nil (silent
	// fallback when Config.Logger is nil). Tagged with
	// driver=db2i, dsn_host=<host>, server_vrm=<vrm>.
	log *slog.Logger

	// tracer is the resolved OTel trace.Tracer. Always non-nil
	// (no-op fallback when Config.Tracer is nil).
	tracer trace.Tracer

	// pkg is the per-connection SQL package manager. Non-nil iff
	// cfg.ExtendedDynamic && cfg.PackageName != "". When set:
	//  - PREPARE_DESCRIBE requests emit the CP 0x3804 marker so the
	//    server files the prepared statement into the *PGM.
	//  - The connect path issued CREATE_PACKAGE for the resolved
	//    10-char wire name (and RETURN_PACKAGE if cfg.PackageCache
	//    is true).
	pkg *hostserver.PackageManager
}

// Logger returns the per-connection slog.Logger. Always non-nil --
// when Config.Logger was nil at connect time this returns a
// no-op logger so callers can use it without nil-checking.
func (c *Conn) Logger() *slog.Logger { return c.log }

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
// selectOptions returns the hostserver.SelectOption slice the
// per-Stmt SELECT entry points should pass. Folds in connection-
// level knobs (ExtendedMetadata today; future per-conn select
// behaviours land here). Returns nil when no options are active so
// the OpenSelectStatic / OpenSelectPrepared call sites stay zero-
// allocation for the common path.
func (c *Conn) selectOptions() []hostserver.SelectOption {
	return c.selectOptionsFor("", false)
}

// selectOptionsFor is the per-statement variant of selectOptions.
// It applies the cfg.PackageCriteria filter before deciding whether
// to emit the extended-dynamic CP 0x3804 marker for THIS sql --
// statements that don't pass the criteria (e.g. a non-parameterised
// SELECT under criteria=default) get a plain PREPARE_DESCRIBE on
// the wire and stay out of the *PGM. Mirrors JT400's
// JDSQLStatement.canHaveExtendedDynamic logic.
//
// When called with sql="" / hasParams=false the criteria filter is
// bypassed -- selectOptions() takes this shortcut for the common
// case where the caller hasn't yet routed through Stmt.Prepare /
// Stmt.Exec and just wants the conn-level flags.
func (c *Conn) selectOptionsFor(sql string, hasParams bool) []hostserver.SelectOption {
	var opts []hostserver.SelectOption
	if c.cfg != nil && c.cfg.ExtendedMetadata {
		opts = append(opts, hostserver.WithExtendedMetadata(true))
	}
	if c.pkg != nil && c.packageEligibleFor(sql, hasParams) {
		// Extended-dynamic + packaged statement: emit CP 0x3804
		// carrying the full package name + prepare-option=0x01.
		// That's JT400's wire shape for the "file into *PGM"
		// path -- the empty-marker variant M10-3 used didn't
		// populate the package on live IBM Cloud V7R6M0
		// (verified 2026-05-11).
		opts = append(opts,
			hostserver.WithExtendedDynamic(true),
			hostserver.WithPackageName(c.pkg.Name, 37),
			hostserver.WithPackageLibrary(c.pkg.Library),
		)
	}
	return opts
}

// packageEligibleFor implements the per-SQL eligibility test the
// Config.PackageCriteria knob selects between. Byte-equivalent to
// JT400's JDSQLStatement.java:950-959 isPackaged_ gate so a Go
// client and a Java client running the same SQL agree on whether
// the statement gets filed into the shared *PGM:
//
//	"default" (JDSQLStatement.java:76-79 after 2011 widening):
//	  ((numberOfParameters_ > 0) && !isCurrentOf_)
//	  || (isInsert_ && isSubSelect_)        // INSERT INTO t SELECT ...
//	  || (isSelect_ && isForUpdate_)        // SELECT ... FOR UPDATE
//	  || isDeclare_                         // DECLARE CURSOR ...
//	"select" (JDSQLStatement.java:81-84):
//	  default rules || isSelect_            // any classified SELECT
//
// See /home/complacentsee/godb2/JT400-EXTENDED-DYNAMIC-FILING.md
// for the wire-shape derivation and the gate-history rationale.
//
// Empty sql (the selectOptions() shortcut) returns true so callers
// that don't have SQL context yet still see the package flag.
func (c *Conn) packageEligibleFor(sql string, hasParams bool) bool {
	if c.cfg == nil || c.pkg == nil {
		return false
	}
	if sql == "" {
		return true
	}
	// CURRENT OF cursor: an UPDATE/DELETE bound to a previously-
	// declared cursor. JT400 unconditionally rejects.
	if containsCaseInsensitive(sql, "CURRENT OF") {
		return false
	}
	verb := firstSQLVerb(sql)
	isSelect := eqIgnoreCaseDriver(verb, "SELECT")
	isInsert := eqIgnoreCaseDriver(verb, "INSERT")
	isDeclare := eqIgnoreCaseDriver(verb, "DECLARE")
	isForUpdate := isSelect && containsCaseInsensitive(sql, "FOR UPDATE")
	isSubSelect := isInsert && hasEmbeddedSelect(sql)

	// JT400 "default" criteria, post-2011 widening. The hasParams
	// arm dominates in practice -- the other three handle the
	// non-parameterised edge cases JT400's heuristic still files.
	packaged := hasParams ||
		(isInsert && isSubSelect) ||
		(isSelect && isForUpdate) ||
		isDeclare

	if c.cfg.PackageCriteria == "select" {
		// criteria=select widens to include every parameterless
		// SELECT (JT400's `|| isSelect_` arm). Note we do NOT
		// add VALUES/WITH here -- JT400 treats those as
		// non-SELECT and excludes them from filing under either
		// criterion.
		packaged = packaged || isSelect
	}
	return packaged
}

// hasEmbeddedSelect checks whether an INSERT contains an embedded
// SELECT (i.e. INSERT INTO t (...) SELECT ...) -- the JT400 isSubSelect_
// signal that flips the filing gate on for non-parameterised
// inserts. Case-insensitive substring search; SQL identifiers don't
// contain unquoted SELECT in practice, so a literal substring match
// is good enough.
func hasEmbeddedSelect(sql string) bool {
	// Skip the leading INSERT token; the SELECT must follow.
	rest := sql
	if i := indexNonSpace(rest); i >= 0 {
		rest = rest[i:]
	}
	if j := indexSpace(rest); j > 0 {
		rest = rest[j:]
	}
	return containsCaseInsensitive(rest, "SELECT")
}

func indexNonSpace(s string) int {
	for i, r := range s {
		if r != ' ' && r != '\t' && r != '\n' && r != '\r' {
			return i
		}
	}
	return -1
}

func indexSpace(s string) int {
	for i, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			return i
		}
	}
	return -1
}

// packageLookup returns the cached entry for sql when one exists
// AND its NameBytes is populated (i.e. the entry came from a
// RETURN_PACKAGE reply, not from a local LocalPrepareCount stub
// inserted by noteFilingPrepare before we knew the server-assigned
// name). Returns nil when:
//
//   - the connection has no package context (cfg.PackageCache off,
//     or initPackage soft-disabled the package via PackageError);
//   - the cache is empty;
//   - no entry's SQL text matches;
//   - an entry matches but is a count-tracking stub without a
//     server name (filing not yet observed by this conn).
//
// Byte equality matches JT400's JDPackageManager.getCachedStatementIndex
// behaviour: a whitespace or case change in the caller's SQL string
// forces a re-prepare, which is the right outcome since the server-
// side cache lookup uses the same identity.
func (c *Conn) packageLookup(sql string) *hostserver.PackageStatement {
	if c.pkg == nil || len(c.pkg.Cached) == 0 {
		return nil
	}
	ps, ok := c.pkg.Cached[sql]
	if !ok || ps == nil || len(ps.NameBytes) != 18 {
		return nil
	}
	return ps
}

// filingRefreshTriggers is the schedule of LocalPrepareCount
// values at which the conn issues a RETURN_PACKAGE refresh to
// learn the server-assigned filed name. The first trigger (3)
// matches IBM's SI30855 threshold; subsequent triggers (6, 12)
// give the server more chances if the first refresh comes back
// without a populated entry (transient server state, package
// constraint, future PTF that bumps the threshold, etc.).
// Bounded by hostserver.MaxFilingRefreshAttempts so a SQL the
// server refuses to file doesn't burn unbounded refreshes.
var filingRefreshTriggers = [hostserver.MaxFilingRefreshAttempts]uint8{3, 6, 12}

// noteFilingPrepare is called by the Exec / Query dispatch sites
// just before issuing a filing-eligible PREPARE_DESCRIBE on this
// conn. Tracks LocalPrepareCount for SQLs we haven't yet seen
// filed; returns true to signal the caller should issue a
// RETURN_PACKAGE refresh AFTER this prepare returns.
//
// Safe to call when the conn has no package context -- returns
// false immediately.
func (c *Conn) noteFilingPrepare(sql string) (shouldRefresh bool) {
	if c.pkg == nil {
		return false
	}
	if c.pkg.Cached == nil {
		c.pkg.Cached = make(map[string]*hostserver.PackageStatement)
	}
	ps, ok := c.pkg.Cached[sql]
	if !ok {
		// First time we've seen this SQL on this conn. Insert a
		// count-tracking stub; NameBytes stays empty so cache-hit
		// dispatch will not fire until a future RETURN_PACKAGE
		// refresh populates the renamed name.
		c.pkg.Cached[sql] = &hostserver.PackageStatement{
			SQLText:           sql,
			LocalPrepareCount: 1,
		}
		return false
	}
	if len(ps.NameBytes) == 18 {
		// Already cache-hit eligible -- count tracking irrelevant.
		return false
	}
	if ps.RefreshAttempts >= hostserver.MaxFilingRefreshAttempts {
		// We've already tried MaxFilingRefreshAttempts refreshes
		// for this SQL on this conn without learning a server-
		// assigned name. Assume filing isn't going to happen
		// (package full, locked, server policy, etc.) and stop
		// burning refreshes.
		return false
	}
	ps.LocalPrepareCount++
	// Does the new count match the next scheduled refresh trigger?
	next := filingRefreshTriggers[ps.RefreshAttempts]
	return ps.LocalPrepareCount == next
}

// refreshPackageCache re-issues RETURN_PACKAGE on this conn and
// rebuilds the in-memory Cached map. Called by the dispatch sites
// after a PREPARE_DESCRIBE that crossed the local filing threshold,
// so subsequent calls of the just-filed SQL hit the cache-hit fast
// path. Errors are logged at WARN and swallowed -- a refresh failure
// is non-fatal; the regular path continues to work.
//
// triggerSQL identifies the statement whose LocalPrepareCount
// crossed a filingRefreshTriggers boundary. After a successful
// refresh, RefreshAttempts on that entry is incremented regardless
// of whether the refresh populated NameBytes for it -- otherwise
// the cap in noteFilingPrepare can never be reached and a SQL the
// server refuses to file would burn unbounded RETURN_PACKAGE
// round-trips on subsequent PREPAREs. Pass "" to skip the attempt
// bookkeeping (e.g., for connect-time priming refreshes).
func (c *Conn) refreshPackageCache(ctx context.Context, triggerSQL string) {
	if c.pkg == nil || c.cfg == nil || !c.cfg.PackageCache {
		return
	}
	ccsid := uint16(c.cfg.PackageCCSID)
	if ccsid == 0 {
		ccsid = 13488
	}
	cached, err := hostserver.SendReturnPackage(c.conn, c.pkg.Name, c.pkg.Library, ccsid, c.nextCorrFunc())
	if err != nil {
		c.log.LogAttrs(ctx, slog.LevelWarn, "db2i: cache refresh failed",
			slog.String("package", c.pkg.Name),
			slog.String("library", c.pkg.Library),
			slog.String("err", err.Error()),
		)
		return
	}
	// Merge the refresh result into the existing map so any count-
	// tracking stubs we have for not-yet-filed SQLs (e.g. SQLs
	// that have only 1-2 PREPAREs on this conn) survive.
	for i := range cached {
		ps := cached[i]
		if existing, ok := c.pkg.Cached[ps.SQLText]; ok && existing != nil {
			// Preserve any LocalPrepareCount + RefreshAttempts we
			// had accumulated. The refresh's purpose is to learn
			// NameBytes; the local counters track *our* observation
			// state and outlive any single refresh.
			ps.LocalPrepareCount = existing.LocalPrepareCount
			ps.RefreshAttempts = existing.RefreshAttempts
		}
		c.pkg.Cached[ps.SQLText] = &ps
	}
	// Bookkeep the attempt against the triggering SQL whether or not
	// the refresh learned its name. If the server hasn't filed it
	// yet (or never will -- package full, locked, etc.), this is
	// what eventually flips noteFilingPrepare to stop calling us.
	if triggerSQL != "" {
		if ps, ok := c.pkg.Cached[triggerSQL]; ok && ps != nil {
			if ps.RefreshAttempts < hostserver.MaxFilingRefreshAttempts {
				ps.RefreshAttempts++
			}
		}
	}
	c.log.LogAttrs(ctx, slog.LevelDebug, "db2i: cache refresh",
		slog.String("package", c.pkg.Name),
		slog.Int("cached_statements", len(cached)),
	)
}

// firstSQLVerb returns the first whitespace-delimited token of sql
// (after leading spaces / tabs / newlines), without allocating. The
// returned slice aliases sql.
func firstSQLVerb(sql string) string {
	i := 0
	for i < len(sql) {
		c := sql[i]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			break
		}
		i++
	}
	j := i
	for j < len(sql) {
		c := sql[j]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(' {
			break
		}
		j++
	}
	return sql[i:j]
}

func eqIgnoreCaseDriver(s, want string) bool {
	if len(s) != len(want) {
		return false
	}
	for i := 0; i < len(s); i++ {
		a := s[i]
		b := want[i]
		if a >= 'a' && a <= 'z' {
			a -= 32
		}
		if a != b {
			return false
		}
	}
	return true
}

func containsCaseInsensitive(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		match := true
		for j := 0; j < len(needle); j++ {
			a := haystack[i+j]
			b := needle[j]
			if a >= 'a' && a <= 'z' {
				a -= 32
			}
			if b >= 'a' && b <= 'z' {
				b -= 32
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

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
	err := c.conn.Close()
	if c.log != nil {
		c.log.LogAttrs(context.Background(), slog.LevelInfo, "db2i: connection closed")
	}
	return err
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
		return nil, c.classifyConnErr(fmt.Errorf("db2i: autocommit off: %w", err))
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
