# go-db2i

A pure-Go `database/sql` driver for IBM i (DB2 for i), speaking the IBM
host-server datastream protocol directly over TCP. No CGo, no Java
sidecar, no IBM client packages — just a Go binary that talks to the
as-database (8471) and as-signon (8476) services on any IBM i.

> **Scope.** go-db2i aims to be a drop-in replacement for the
> **JT400 JDBC driver** — the `com.ibm.as400.access.AS400JDBCDriver`
> half of [IBM Toolbox for Java (JTOpen)](https://github.com/IBM/JTOpen) —
> not the entire JTOpen toolbox. The dozens of non-JDBC services
> JTOpen exposes (`AS400` programmatic auth + IFS access,
> `CommandCall`, `ProgramCall`, `DataQueue`, `IFSFile`, `SystemValue`,
> print spool, FTP, BiDi, proxy server, etc.) are explicitly out of
> scope. If your Go service talks to IBM i exclusively through
> JDBC — `db.Query` / `db.Exec` / `tx.Begin` / `CallableStatement` —
> this driver replaces the JT400 jar one-for-one. If you need
> `CommandCall.run("WRKACTJOB")` or `IFSFile`, use the JTOpen jar
> via a JVM sidecar (or fork go-db2i to add the service you need;
> the host-server datastream format is the same).

```go
import (
    "database/sql"
    _ "github.com/complacentsee/go-db2i/driver"
)

db, err := sql.Open("db2i", "db2i://USER:PWD@host.example.com:8471/?library=MYLIB")
```

## Status

Wire-validated against IBM i 7.6 (V7R6M0) on IBM Cloud Power VS.
The full `database/sql` JDBC surface that JT400 exposes is
implemented end-to-end:

- Sign-on (password levels 2 / 3 SHA-1, 4 PBKDF2-HMAC-SHA-512)
- TLS sign-on / database (ports 9476 / 9471)
- Static and parameterised `SELECT` with lazy `Rows` iteration via
  continuation FETCH (streamed 86k rows of `QSYS2.SYSCOLUMNS` in
  testing without buffering)
- Static and parameterised `INSERT` / `UPDATE` / `DELETE`
- **Stored procedures** via `db.Exec("CALL ...")` and
  `db.Query("CALL ...")`: IN parameters through driver.Value, OUT
  and INOUT parameters via `sql.Out{Dest: &x, In: bool}`, multi-
  result-set procedures via `Rows.NextResultSet`
- Transactions (`db.Begin`, `tx.Commit`, `tx.Rollback`) with
  configurable commitment-control level
- LOB bind + read: BLOB / CLOB / DBCLOB. Streaming reads via
  `?lob=stream` opt-in (`*LOBReader` per row); inline materialisation
  by default. Inline-small-LOB threshold via `?lob-threshold=N`.
- RLE-compressed `RETRIEVE_LOB_DATA` chunks (5-byte whole-payload
  wrapper) end-to-end
- Typed `*hostserver.Db2Error` with `SQLState` / `SQLCode` /
  `MessageID` / `MessageTokens` + predicate helpers
  (`IsNotFound` / `IsConstraintViolation` / `IsLockTimeout` /
  `IsConnectionLost`)
- `driver.ErrBadConn` on TCP-level failures so the pool auto-recovers
- `context.Context` propagation including mid-query cancellation
- `Result.LastInsertId` via `IDENTITY_VAL_LOCAL()`
- UTF-8 string binds and decode on V7R3+ (CCSID 1208 passthrough),
  EBCDIC fallback (CCSID 37) on older servers; per-DSN CCSID
  override via `?ccsid=N`
- Type round-trip: INTEGER, BIGINT, SMALLINT, DOUBLE, REAL, DECIMAL,
  NUMERIC, DECFLOAT(16/34), CHAR, VARCHAR (and FOR BIT DATA),
  BOOLEAN, BINARY, VARBINARY, DATE, TIME, TIMESTAMP, BLOB, CLOB,
  DBCLOB
- Extended column metadata
  (`*sql.ColumnType.ScanType` / `DatabaseTypeName` / `Length` /
  `Precision` / `Scale` / `Nullable`) including schema / table /
  base-column-name / label via the V7R3+ extended-metadata reply
- `log/slog` integration via `Config.Logger`
- OpenTelemetry spans (`Config.Tracer`) following the May 2025
  semantic-conventions refresh, with `*Db2Error` attributes for
  alerting routing
- Extended-dynamic SQL package caching (`?extended-dynamic=true&package=APP`)
  — the driver issues `CREATE_PACKAGE` on connect, adds CP 0x3804
  to each `PREPARE_DESCRIBE`, and re-uses the server's `*PGM`
  cache across reconnects. The 10-char wire name is byte-equal to
  JT400 for the same session options, so a Go client and a Java
  client targeting the same LPAR share one `*PGM`. See
  [`docs/migrating-from-jt400.md`](./docs/migrating-from-jt400.md)
  for the 9-key DSN surface (`extended-dynamic`, `package`,
  `package-library`, `package-cache`, `package-error`,
  `package-criteria`, `package-ccsid`, plus migration-friendly
  `package-add` / `package-clear`).

Out of scope (use the JTOpen Java jar for these):

- Non-JDBC JTOpen services: `AS400`-class programmatic auth,
  `CommandCall`, `ProgramCall`, `DataQueue`, `IFSFile`, `JobLog`,
  `SystemValue`, print spool, FTP, BiDi reordering, proxy server.
- JDBC extras that aren't in the database/sql contract: scrollable
  cursors (forward-only across the board), client reroute /
  seamless failover, JDBC escape syntax `{call ...}`,
  named-parameter binding via `sql.Named("p", ...)` for procs
  (positional only).

## Install

```bash
go get github.com/complacentsee/go-db2i
```

Requires **Go 1.23+** (the driver uses `context.AfterFunc` from
`context`).

## DSN

```
db2i://USER:PASSWORD@HOST[:DB_PORT]/?key=value&key=value
```

| Key                          | Default | Meaning |
|------------------------------|---------|---------|
| `library`                    | (none)  | Default schema for unqualified SQL names. Required if the user's job library list doesn't already contain it. |
| `signon-port`                | 8476 (9476 if `tls=true`) | as-signon service port. |
| `date`                       | `job`   | Session date format. One of `job`, `iso`, `usa`, `eur`, `jis`, `mdy`, `dmy`, `ymd`. |
| `isolation`                  | `none`  | Session commitment level. One of `none` (`*NONE`), `cs` (`*CS`), `all` (`*ALL`), `rs` (`*RS`), `rr` (`*RR`). The default `*NONE` matches IBM i Db2's autocommit-permissive baseline. `db.Begin()` flips to `*CS` for the duration of the transaction. |
| `tls`                        | `false` | Wrap both sockets in TLS. When `true`, the default ports flip to 9476 / 9471 (IBM i SSL host server pair). Accepts `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`. Requires the IBM i target to have SSL host server configured via DCM. |
| `tls-insecure-skip-verify`   | `false` | Skip server-cert validation. IBM i certs are commonly self-signed and lack DNS SANs, in which case `crypto/tls` rejects them by default; set this to `true` to override. Use sparingly — disables MITM protection. |
| `tls-server-name`            | (host)  | Override the SNI / cert-verify hostname. Defaults to the URL host. Useful when the cert was issued for a different name than what you connect to (e.g. via tunnel). |

The `DB_PORT` segment defaults to 8471 (as-database). Library names
are upper-cased on parse — IBM i schema lookups are case-insensitive
but the wire format expects EBCDIC uppercase.

## Quick examples

```go
// SELECT
rows, err := db.Query(`SELECT id, label FROM mylib.things WHERE status = ?`, "OPEN")
for rows.Next() {
    var id int64
    var label string
    rows.Scan(&id, &label)
}
rows.Close()

// INSERT with IDENTITY round-trip
res, err := db.Exec(`INSERT INTO mylib.things (label) VALUES (?)`, "new thing")
id, _ := res.LastInsertId()

// Transactions
tx, err := db.Begin()
tx.Exec(`INSERT ...`)
tx.Exec(`UPDATE ...`)
tx.Commit()

// Stored procedure with OUT parameters
var name string
var qty int
_, err = db.Exec(`CALL mylib.p_lookup(?, ?, ?)`,
    "WIDGET",
    sql.Out{Dest: &name},
    sql.Out{Dest: &qty},
)

// Stored procedure that returns multiple result sets
rows, _ := db.Query(`CALL mylib.p_inventory(?)`, 5)
defer rows.Close()
for rows.Next() { /* first set */ }
if rows.NextResultSet() {
    for rows.Next() { /* second set */ }
}
```

## Error classification

Server-side SQL errors come back as `*hostserver.Db2Error` with full
SQLSTATE and IBM message-id metadata. Use `errors.As` plus the
predicate methods to drive retry / surfacing logic:

```go
import "github.com/complacentsee/go-db2i/hostserver"

_, err := db.Exec(`INSERT INTO mylib.things (id) VALUES (?)`, 42)
if err != nil {
    var dbErr *hostserver.Db2Error
    if errors.As(err, &dbErr) {
        switch {
        case dbErr.IsNotFound():            // SQLSTATE 02xxx
            // no rows matched
        case dbErr.IsConstraintViolation(): // SQLSTATE 23xxx
            // duplicate key, NOT NULL, FK, etc.
        case dbErr.IsLockTimeout():         // SQLCODE -911 / -913
            // safe to retry with backoff
        case dbErr.IsConnectionLost():      // SQLSTATE 08xxx
            // pool will replace the conn automatically
        default:
            // log dbErr.SQLState, dbErr.SQLCode, dbErr.MessageTokens
        }
    }
}
```

## Server compatibility

| IBM i version | Status |
|---|---|
| V7R6 (7.6) | wire-validated (PBKDF2 / SHA-1, full feature set) |
| V7R5 (7.5) | should work — same protocol level; not regularly tested |
| V7R4 (7.4) | should work; tested via PUB400 (some features auto-fallback) |
| V7R3 (7.3) | should work via password levels 2/3 (SHA-1) |
| ≤ V7R2 | DES auth path (levels 0/1) is implemented but spec-validated only — no live testing yet. PBKDF2 is unavailable on these servers anyway. |

The `IDENTITY_VAL_LOCAL()` LastInsertId path is session-scoped (matches
JT400 / JDBC behavior); see the godoc on `Result.LastInsertId` for
details.

## Why pure Go?

The IBM-supplied options for connecting Go programs to IBM i Db2 are:

1. **`go_ibm_db`** — DRDA-only over port 446. Often firewalled in
   industrial deployments where only the host-server ports are open.
2. **CGo + IBM i Access ODBC Driver** — IBM doesn't ship a `linux/arm64`
   build of this driver, blocking deployment to ARM64 industrial
   gateways and Apple Silicon dev boxes.
3. **Java + JTOpen sidecar** — works but adds a JVM and a
   process boundary to a Go service.

go-db2i takes the **same protocol as JTOpen's JDBC driver** (which
uses the host-server datastream over 8471 / 9471) and reimplements
the JDBC half natively in Go. The result is one statically-linked
binary that runs anywhere Go runs, with the same JDBC behaviour the
JT400 jar gives a Java app — minus the JVM, the classpath, and the
~10 MB jar. Non-JDBC JTOpen services (`CommandCall`, `IFSFile`, etc.)
are out of scope; the [Migrating from JT400](docs/migrating-from-jt400.md)
guide spells out the JDBC-property-to-DSN-key mapping in detail.

## Acknowledgements

Wire-format implementation builds on the open-source
[IBM Toolbox for Java (JTOpen)](https://github.com/IBM/JTOpen)
under the IBM Public License v1.0 as a protocol reference --
specifically the
`com.ibm.as400.access.AS400JDBC*` JDBC driver classes and the
`DBBaseRequestDS` / `DBReplyRequestedDS` host-server-datastream
encoders/decoders that JT400 hands to its `AS400` connection
object. go-db2i is a clean-room reimplementation: no JTOpen
source is included in this repository or copied at build time.
The fixture harness (`testdata/jtopen-fixtures/`) pulls JTOpen
from Maven Central at trace-capture time, but the recorded
`.trace` / `.golden.json` fixtures are data-only and carry no
JTOpen code.

## License

Apache-2.0. See `LICENSE`.
