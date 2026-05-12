# Extended-dynamic SQL package caching

go-db2i ships two complementary mechanisms for amortising prepared-
statement cost across connections and calls:

- **Extended-dynamic SQL packages (v0.7.0)** — each PREPAREd
  statement is filed into a per-library server-side `*PGM` object so
  subsequent client connections (Go or JT400) can re-use the plan
  without re-issuing PREPARE.
- **Client-side cache-hit fast path (v0.7.1)** — at connect time
  the driver downloads the filed entries, and on subsequent
  `Stmt.Exec` / `Stmt.Query` calls it bypasses PREPARE_DESCRIBE
  entirely when the SQL text byte-equals a cached entry.

Together they cut **one wire round-trip per call** off every
parameterised statement that has been seen at least once on any
connection sharing the same package name.

This guide covers when to turn them on, the DSN composition, how to
verify the server actually filed something, and how to observe
cache-hit dispatch in your application logs and traces.

## When to use

Turn this on if **any** of the following apply:

- You run the same parameterised SQL thousands of times per minute
  (typical of REST APIs, event consumers, scheduled batch jobs).
  Saving a round-trip per call is ~5-50ms depending on network
  topology — see [`performance.md`](./performance.md).
- You operate a polyglot environment where Go and Java clients hit
  the same LPAR. The 10-char on-wire package name is byte-equal to
  JT400 for the same session options, so both runtimes share one
  `*PGM` on disk.
- You want JT400-equivalent runtime characteristics so a migration
  from a Java service to a Go service doesn't change the LPAR's
  plan cache footprint.

Leave it off when:

- Your application generates ad-hoc SQL text that rarely repeats
  byte-for-byte (e.g. ORMs that interpolate identifiers into
  strings rather than using parameter markers).
- You can't tolerate the `*PGM` object existing in the library —
  some compliance regimes treat dynamically-created executable
  objects as code that needs a review trail.
- You don't have authority to `CRTLIB` the target library.

## Mental model

```
miss (v0.7.0 + v0.7.1 cache-miss):
  CREATE_RPB ─┐
  PREPARE_DESCRIBE ─┐
  CHANGE_DESCRIPTOR  ├── 4 wire round-trips total
  EXECUTE / OPEN ────┘

hit (v0.7.1 cache-hit):
  CREATE_RPB ─┐
  CHANGE_DESCRIPTOR    ├── 3 wire round-trips
  EXECUTE / OPEN ──────┘     (PREPARE_DESCRIBE is skipped;
                              the cached 18-byte server-assigned
                              statement name is passed in
                              CP 0x3806 as a name override)
```

On a cache-hit, the EXECUTE / OPEN frame carries the cached 18-byte
server-assigned statement name (e.g. `QZAF488C43A7873001`) in code
point `0x3806`. The server looks the plan up in the `*PGM` and runs
it directly without re-preparing. JT400's
`AS400JDBCStatement.commonExecute` internally tracks the same name
in its `nameOverride_` field, but on the wire the dispatch is
package marker + RPB handle + CP 0x3806 — no reply-side rename
capture is required (verified against `prepared_package_cache_hit.trace`
and `prepared_package_filing_iud.trace`, 2026-05-11).

## Setup

### DSN composition

The minimal opt-in is two query keys; add `package-cache=true` for
the client-side fast path:

```go
import (
    "database/sql"
    _ "github.com/complacentsee/go-db2i/driver"
)

const dsn = "db2i://USER:PWD@host:8471/" +
    "?library=MYLIB" +
    "&extended-dynamic=true" +
    "&package=APP" +
    "&package-library=MYLIB" +
    "&package-cache=true"

db, err := sql.Open("db2i", dsn)
```

Programmatic equivalent via `Connector`:

```go
import (
    "database/sql"
    db2i "github.com/complacentsee/go-db2i/driver"
)

cfg := db2i.DefaultConfig()
cfg.Host = "host"
cfg.User = "USER"
cfg.Password = "PWD"
cfg.Library = "MYLIB"
cfg.ExtendedDynamic = true
cfg.PackageName = "APP"
cfg.PackageLibrary = "MYLIB"
cfg.PackageCache = true

connector, err := db2i.NewConnector(cfg)
db := sql.OpenDB(connector)
```

### Library prerequisite

`PackageLibrary` must exist before the first connect. Create it
once via any IBM i CL session:

```
CRTLIB LIB(MYLIB) TEXT('go-db2i package cache')
```

The driver does **not** create the library — if it doesn't exist
the first `CREATE_PACKAGE` returns `SQL-204 (object not found)`
and `package-error` kicks in.

### First-connect filing

On a fresh package (the `*PGM` doesn't exist yet):

1. Connect emits `CREATE_PACKAGE` (function `0x180F`) with the
   resolved 10-char name (`APP` → `APP9899` plus session-option
   suffix; identical to what JT400 would emit).
2. Every `Stmt.Prepare` / `db.Query` / `db.Exec` issues
   `PREPARE_DESCRIBE` with CP `0x3804` carrying the full
   package name. The server stores the prepared plan in the
   `*PGM`.
3. Subsequent statements file alongside the first.

### Second-connect download

On a reconnect with the same `package` name and `package-cache=true`:

1. Connect emits `RETURN_PACKAGE` (function `0x1815`) which streams
   the `*PGM` contents back in CP `0x380B`.
2. The driver parses the entries into `Conn.pkg.Cached` — one
   `PackageStatement` per filed statement carrying the
   server-assigned 18-byte name, the original SQL text, the
   result-format SQLDA, and the parameter-marker SQLDA.
3. Every subsequent `Stmt.Exec` / `Stmt.Query` calls
   `Conn.packageLookup(sql)` which performs byte-equal SQL text
   matching against the cached entries. A hit dispatches to
   `ExecutePreparedCached` / `OpenSelectPreparedCached`; a miss
   falls through to the normal PREPARE path.

## Eligibility — `package-criteria`

Not every SQL string can be cached. The driver's gate
(`driver.packageEligibleFor`) is byte-equivalent to JT400's
`JDSQLStatement.isPackaged_` (verified against IBM/JTOpen `f14abcc`,
2026-05-11) and matches IBM's own ODBC filing rule documented in
the [SQL Package Questions and Answers](https://www.ibm.com/support/pages/sql-package-questions-and-answers)
support page:

| Criteria | Files this SQL? |
|---|---|
| `default` (default) | Parameterised SELECT / INSERT / UPDATE / DELETE; `INSERT INTO t SELECT ...` (subselect, even without markers); `SELECT ... FOR UPDATE` (positioned cursor); `DECLARE PROCEDURE` / `DECLARE CURSOR`. Excludes `CURRENT OF`, unparameterised plain SELECT, `VALUES`, `WITH`, and `CALL`. |
| `select` | Same as `default` PLUS unparameterised SELECT statements. (JT400's wider gate; matches `package criteria=select` exactly. Does **not** widen to VALUES / WITH — those remain non-cached under either criterion.) |
| `extended` (v0.7.7) | Same as `default` PLUS `CALL` / `VALUES` / `WITH`. **go-db2i-original** — JT400 has no equivalent value (its [`JDProperties.java`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDProperties.java) `PACKAGE_CRITERIA_*` enumeration is exactly two values: `default` and `select`; verified against JTOpen 2026-05-12). **Does not** inherit `select`'s unparameterised-SELECT widening — each criterion is a discrete switch, not cumulative. Interop note: a Go client passing `extended` will not share a `*PGM` with a JT400 client at all — JT400 rejects the unknown criterion value during DSN parse. The byte-equality guarantee for `default` / `select` is unaffected. |

`select` previously also accepted VALUES / WITH in go-db2i
v0.7.0–v0.7.2; v0.7.3 narrowed it to match JT400's wire-equivalent
gate so Go and Java clients hash to the same `*PGM` for the same
session options.

`extended` and OUT/INOUT CALL: a CALL with `sql.Out` destinations
files into the `*PGM` but the cache-hit fast path refuses it —
`preparedParamsFromCached` rejects non-IN direction bytes, so the
cache entry is permanently unreachable for that SQL. go-db2i skips
the v0.7.4 auto-populate `RETURN_PACKAGE` refresh when any OUT
destination is present so we don't burn round-trips chasing an
unreachable cache entry. Whether OUT-CALL cache-hit could be made
to work is an open question tracked in
[`docs/plans/v0.7.8-out-param-cache-hit.md`](./plans/v0.7.8-out-param-cache-hit.md);
no JT400 reference exists for this path since JT400 doesn't file
CALL under any of its criteria.

Examples:

```sql
-- Filed under default, select, and extended:
SELECT id, name FROM mylib.things WHERE status = ?

-- Filed under default, select, and extended (positional rules):
INSERT INTO mylib.archive SELECT * FROM mylib.things WHERE archived = 1
SELECT * FROM mylib.things WHERE id = 1 FOR UPDATE

-- Filed under select only (default rejects: zero markers;
-- extended does not inherit this branch):
SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1

-- Filed under extended only:
VALUES 1
WITH t AS (SELECT 1) SELECT * FROM t
CALL mylib.p_lookup_in_only(?, ?)
-- (OUT/INOUT CALL also files under extended; cache-hit refuses it.)

-- Never filed (DDL is not cacheable):
CREATE TABLE mylib.things (id INTEGER, name VARCHAR(64))
```

### What actually files vs only crosses the gate

Passing the client-side `packageEligibleFor` gate is necessary but
not sufficient — IBM's server-side 3-PREPARE threshold (PTF
SI30855 since IBM i 6.1) also gates filing. A statement crosses
the gate but only files once it's been PREPAREd ≥ 3 times against
the same package on the same QZDASOINIT job. See the
"NUMBER_STATEMENTS stays at 0" entry below for the gory details.

End-to-end verified against IBM Cloud V7R6M0 and PUB400 V7R5M0
(2026-05-11): all 17 JDBC types — INTEGER, BIGINT, SMALLINT,
DECIMAL, NUMERIC, REAL, DOUBLE, DECFLOAT, VARCHAR, CHAR, VARCHAR
FOR BIT DATA, DATE, TIME, TIMESTAMP, BOOLEAN, BINARY, VARBINARY —
round-trip through filing + cache-hit dispatch for SELECT and
through filing for INSERT / UPDATE / DELETE.

Use `package-criteria=select` if your workload includes many
identical zero-parameter SELECTs (typical of dashboards that hit
the same metric query every refresh). Use `default` (the JT400
default) if you want to minimise the `*PGM` size.

### LOB binds + the cache-hit fast path

`INSERT INTO t (id, payload) VALUES (?, ?)` where `payload` is a
`BLOB` / `CLOB` / `DBCLOB` column DOES file into the `*PGM`
server-side (verified on V7R6M0 via `TestLOBBind_FilingProbe`).
What it does NOT do — yet — is benefit from the client-side
cache-hit fast path. The `*PGM`-stored parameter shape uses the
raw-LOB SQL types (404/405/408/409) while our cache-hit encoder
only has branches for the live-PREPARE locator types (960/961/
964/965/968/969). When the cache-hit dispatch fires for a
LOB-bind statement, it hits `ErrUnsupportedCachedParamType` and
v0.7.5 falls through to plain `PREPARE_DESCRIBE` — same wire shape
as the original cache-miss path, no data corruption, just no
round-trip saving for that call.

Practical impact: a LOB-bind INSERT loop run on a single
connection pays the regular round-trip cost on every call,
identical to the pre-v0.7.1 behaviour for the same SQL. Non-LOB
parameterised statements still get the full cache-hit speedup.
Cache-hit support for LOB binds is a v0.7.6 candidate.

## Observability

### slog DEBUG lines

Inject a `*slog.Logger` via `Config.Logger`. Every cache-hit
dispatch emits one DEBUG line:

```
time=2026-05-11T12:52:53.744Z level=DEBUG msg="db2i: query cache-hit"
  driver=db2i dsn_host=host server_vrm=460288
  op=OPEN_SELECT_PREPARED_CACHED params=1 elapsed=35.230478ms
  cached_name=QZAF488C43A7873001
```

The Exec path emits the same shape with `op=EXECUTE_PREPARED_CACHED`
and the message text `"db2i: exec cache-hit"`. The
`cached_name` attribute is the 18-byte server-assigned statement
name — useful for cross-referencing against `QSYS2.SYSPACKAGESTMT`.

A cache **miss** emits the existing DEBUG lines
(`"db2i: exec"` / `"db2i: query"` with
`op=EXECUTE_PREPARED` / `OPEN_SELECT_PREPARED`).

Note that the driver attaches a child logger at connect time
carrying `driver=db2i`, `conn_id=<corr-base>`, and
`dsn_host=<host>` attrs, so you don't need to add those yourself.

### OpenTelemetry spans

Set `Config.Tracer` to any `trace.Tracer`. On a cache-hit:

- Span name: `EXEC` or `QUERY` (same as cache-miss; the
  span hierarchy doesn't change).
- `db.operation.name` attribute: `EXECUTE_PREPARED_CACHED` or
  `OPEN_SELECT_PREPARED_CACHED`.
- Span duration reflects the one-round-trip saving.

`db.statement` is attached only when `LogSQL=true`, same as the
non-cached path.

### Verifying server-side state

Two `QSYS2` views give the authoritative picture:

```sql
-- Did the package object get created and how many statements are filed?
SELECT package_name, package_library, cached_statements, last_used_timestamp
FROM   QSYS2.SYSPACKAGE
WHERE  package_name = 'APP9899'
  AND  package_library = 'MYLIB';

-- Inspect each filed statement (text, marker count, last-execute time):
SELECT statement_name, statement_text, marker_count, last_used_timestamp
FROM   QSYS2.SYSPACKAGESTMT
WHERE  package_name = 'APP9899'
  AND  package_library = 'MYLIB';
```

A fresh first-connect run should populate `SYSPACKAGE` with
`cached_statements > 0` and one `SYSPACKAGESTMT` row per distinct
PREPAREd SQL.

## Troubleshooting

**My SQL isn't filing — `cached_statements` stays at 0.**

Check the `package-criteria` table above. Common cases:

- Unparameterised SELECT under `default` → set `package-criteria=select`.
- `CALL ...` statements (stored procedures) are never cached.
- `sql.Out` / `sql.InOut` arguments → the driver short-circuits
  the cache for that statement.
- SQL text with leading whitespace, trailing semicolons, or
  case-different keywords against a cached entry will *match*
  if byte-equal and *miss* if not. Normalise before binding.

**The package already exists from a previous deploy.**

v0.7.1 treats `SQL-601 (object exists)` from `CREATE_PACKAGE` as
success and continues. This matches JT400's
`JDPackageManager.create()` behaviour. v0.7.0 incorrectly
soft-disabled the cache in this case — upgrade if you saw
"package soft-disabled" warnings in v0.7.0.

**I want to start fresh.**

```
DLTPGM PGM(MYLIB/APP9899)
```

The driver re-creates the package on the next connect. There is
**no** way to programmatically delete from the driver right now
(`package-clear=true` accepts the key but logs a warning and
does nothing; deferred to a future release).

**Connect returns an error mentioning the package.**

`package-error` controls the policy:

| Value | Behaviour on `CREATE_PACKAGE` / `RETURN_PACKAGE` errors |
|---|---|
| `warning` (default) | slog `WARN` line + continue without cache |
| `exception` | Return the error to the `sql.Open` / first call |
| `none` | Silent + continue without cache |

Use `warning` in production (best-effort caching) and `exception`
in CI / pre-production (catch package-creation regressions
loudly).

**I see `varchar declared length 16448 exceeds column max 64`.**

That was a v0.7.1 SQLDA bug. Upgrade to v0.7.1 or later — the
package-SQLDA VARCHAR length convention was off by 2 bytes and
caused row-decode misalignment on cache-hit Query.

**`NUMBER_STATEMENTS` stays at 0 in `QSYS2.SYSPACKAGESTAT` even
though I'm using extended-dynamic.**

Two things to check, both documented by IBM but easy to miss.

### 1. Are you crossing the 3-PREPARE filing threshold?

IBM's [SQL Package Questions and Answers](https://www.ibm.com/support/pages/sql-package-questions-and-answers)
documents the server-side gate:

> *"Starting with IBM i 6.1 PTF SI30855, a statement must be
> prepared 3 times before it is added to the SQL package. This
> change was made to prevent filling the package with statements
> that aren't used frequently."*

This is per-job (QZDASOINIT), per-package. A workload that opens a
fresh connection per call and runs one PREPARE never crosses the
threshold; the statement is held in transient compiled form and
never written to the `*PGM`. The package then shows
`PACKAGE_USED_SIZE=36848` (the empty-allocation floor) and
`NUMBER_STATEMENTS=0` indefinitely.

To verify filing works on a given LPAR, run the same
parameterised SQL **at least 3 times on a single pinned
connection** before checking `SYSPACKAGESTAT`. The Go conformance
test `TestFiling_ServerSideStateVerified` does exactly this; the
JT400 fixture case `prepared_package_filing_verify.trace`
mirrors it on the Java side. Both pass against IBM Cloud V7R6M0
(`GOTCHE9899`, `NUMBER_STATEMENTS=2`, `PACKAGE_USED_SIZE=65744`)
and against PUB400 V7R5M0 (`GOJTPK9689`, `NUMBER_STATEMENTS=2`,
`PACKAGE_USED_SIZE=58880`) on the same prepare-loop pattern.

The threshold also explains why every cache-hit-related test in
`test/conformance/cache_hit_test.go` is gated behind
`DB2I_TEST_FILING=1`: an environment without authority to drop and
re-create the package between runs accumulates state, and the
threshold makes "fresh package + one prepare" non-reproducible.

### 2. Are you querying with the right package name?

The base name from the `package=` DSN property is the user input;
the on-wire name is what the driver actually files into:

| `package=` | on-wire (V7R6M0 IBM Cloud) | on-wire (PUB400 V7R5M0) |
|---|---|---|
| `GOJTPKG` (truncated to 6) | `GOJTPK9899` | `GOJTPK9689` |
| `GOTCHE` (already 6) | `GOTCHE9899` | `GOTCHE9689` |

The 4-char suffix is the session-options hash JT400 (and
`hostserver.BuildPackageName`) append per
`JDPackageManager.java:466`. It varies with the connection
attributes — date format, decimal separator, CCSID, etc. —
specifically so two clients with different session options can
share one `*PGM` library without poisoning each other's filed
plans. Two clients with **identical** session options
produce **byte-equal** package names and share one *PGM*.

Verification queries should therefore use `LIKE 'BASE%'`:

```sql
SELECT PACKAGE_SCHEMA, PACKAGE_NAME, NUMBER_STATEMENTS,
       PACKAGE_USED_SIZE, LAST_USED_TIMESTAMP
FROM   QSYS2.SYSPACKAGESTAT
WHERE  PACKAGE_NAME LIKE 'GOJTPK%'
  AND  PACKAGE_SCHEMA = '<schema>'
ORDER  BY PACKAGE_NAME;
```

…not `PACKAGE_NAME = 'GOJTPKG'`. The exact-match query will
return zero rows regardless of whether filing is working.

### Visibility delay inside the owning connection

A subtle wrinkle: a `SYSPACKAGESTAT` query issued on the same
connection that owns the package returns zero rows even after
filing has happened. The view only reflects state after the
owning connection has cycled (commit + close, or a fresh conn).
The Go conformance test pins one `*sql.Conn` for the PREPAREs
and then queries `SYSPACKAGESTAT` from a separate
non-extended-dynamic conn — verified empirically on both
V7R6M0 and V7R5M0 on 2026-05-11.

## Cross-references

- [`docs/configuration.md`](./configuration.md) — full DSN
  reference including all package-cache keys.
- [`docs/performance.md`](./performance.md) — measured round-trip
  savings and when to opt out.
- [`docs/migrating-from-jt400.md`](./migrating-from-jt400.md) —
  JT400 `package` / `extended dynamic` JDBC property mapping.
- godoc `Example_packageCache`, `Example_packageCacheObservability`,
  `Example_packageCacheCriteria` in `driver/example_test.go`.
- Test suite `test/conformance/cache_hit_test.go` exercises the
  full type matrix against a live LPAR (build tag
  `//go:build conformance`).
