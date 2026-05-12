# go-db2i

> **Current: v0.7.15 (2026-05-12)** — JT400 parity cleanup.
> `BatchExec` now falls back to per-row EXECUTE when any row in the
> batch carries a `*LOBValue` (matches JT400; caller code unchanged).
> `Conn.BeginTx(ctx, &sql.TxOptions{Isolation: ...})` honours the
> standard `sql.IsolationLevel` constants; `Isolation` translates to
> the IBM i commitment-control level on CP 0x380E, `ReadOnly`
> rejects with a clear message (no IBM i analogue). The
> previously-implemented `Result.LastInsertId` round-trip via
> `IDENTITY_VAL_LOCAL()` now has live conformance coverage. Builds
> on v0.7.14 (large user-table streaming SELECT delivers all rows)
> and v0.7.13 (cursor + race fixes).

A pure-Go `database/sql` driver for IBM i (DB2 for i), speaking the IBM
host-server datastream protocol directly over TCP. No CGo, no Java
sidecar, no IBM client packages — just a Go binary that talks to the
as-database (8471) and as-signon (8476) services on any IBM i.

> **Scope.** go-db2i is a drop-in replacement for the **JT400 JDBC
> driver** (`com.ibm.as400.access.AS400JDBCDriver`) — not the entire
> [JTOpen toolbox](https://github.com/IBM/JTOpen). Non-JDBC services
> (`AS400`-class auth, `CommandCall`, `IFSFile`, `DataQueue`, etc.)
> stay in the JT400 jar. If your Go service talks to IBM i through
> `db.Query` / `db.Exec` / `tx.Begin` / `CallableStatement`, this
> driver replaces the jar one-for-one.

```go
import (
    "database/sql"
    _ "github.com/complacentsee/go-db2i/driver"
)

db, err := sql.Open("db2i", "db2i://USER:PWD@host.example.com:8471/?library=MYLIB")
```

## Status

Wire-validated against IBM i 7.6 (V7R6M0) on IBM Cloud Power VS;
spot-validated on PUB400 V7R5M0. **61 live-conformance tests + 0
failures** in the latest run from a cold-start server state. The
full `database/sql` JDBC surface JT400 exposes is implemented;
documented JDBC-property-to-DSN-key mapping (28 keys, with
JT400-byte-equal session options for the
`extended-dynamic + package-cache` flow) lives in
[`docs/migrating-from-jt400.md`](./docs/migrating-from-jt400.md).

### Remaining gaps vs JT400

A handful of items would benefit a future release but don't block
production use:

- **`query optimize goal`** — DSN knob not plumbed; falls back to
  the job default (`*ALLIO` on most V7R5+ systems).
- **`socket timeout`** (per-op read-timeout default) — today,
  pass a `ctx` deadline per call.
- **`login timeout` per-op override** — today, the dial timeout is
  hardcoded 30 s; pass a deadline-carrying `ctx` to
  `db.Conn(ctx)` for finer control.
- **Password levels 0/1 (DES)** — implemented but spec-validated
  only (every reachable LPAR ships `QPWDLVL ≥ 3`).
- **Multi-factor auth, Kerberos / GSSAPI signon** — not plumbed;
  only password auth at the moment.

### Out of scope (use the JTOpen Java jar for these)

- Non-JDBC JTOpen services: `AS400`-class programmatic auth,
  `CommandCall`, `ProgramCall`, `DataQueue`, `IFSFile`, `JobLog`,
  `SystemValue`, print spool, FTP, BiDi reordering, proxy server.
- JDBC extras outside the `database/sql` contract: scrollable
  cursors (forward-only here), client reroute / seamless failover,
  JDBC escape syntax `{call ...}`, XA / DTC, named-parameter
  binding via `sql.Named("p", ...)` for procs (positional only).

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

## Examples

Runnable [godoc examples](https://pkg.go.dev/github.com/complacentsee/go-db2i/driver)
live in [`driver/example_test.go`](./driver/example_test.go):

| Example | What it shows |
|---|---|
| `Example` | Open + Query basics |
| `Example_largeResult` | Lazy `Rows.Next` iteration over a million-row SELECT |
| `Example_transaction` | `db.Begin` / `tx.Commit` / `tx.Rollback` |
| `Example_lastInsertId` | `Result.LastInsertId` via `IDENTITY_VAL_LOCAL()` |
| `Example_call` | Stored procedure with IN parameters |
| `Example_callWithOut` | Stored procedure with `sql.Out` |
| `Example_callMultiResultSet` | `Rows.NextResultSet` |
| `Example_db2Error` | Typed `*hostserver.Db2Error` + predicate helpers |
| `Example_blobInsert` | BLOB locator-bind |
| `Example_clobInsert` | CLOB EBCDIC bind |
| `Example_lobValueStream` | Streamed `*LOBValue` insert |
| `Example_lobReader` | Streamed `*LOBReader` read via `?lob=stream` |
| `Example_dsnKnobs` | `?lob-threshold` / `?ccsid` / `?tls=true` together |
| `Example_contextTimeout` | `context.WithTimeout` cancellation |
| **`Example_packageCache`** *(v0.7.1)* | Extended-dynamic + cache-hit fast path enable |
| **`Example_packageCacheObservability`** *(v0.7.1)* | slog probe pattern for cache-hit dispatch |
| **`Example_packageCacheCriteria`** *(v0.7.1)* | `package-criteria=select` for unparameterised SELECT |

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
| V7R6 (7.6) | wire-validated on IBM Cloud V7R6M0 (PBKDF2 / SHA-1, full feature set). Extended-dynamic filing — including SELECT, INSERT, UPDATE, DELETE — is live-validated end-to-end against `GOTCHE9899` after the 3-PREPARE threshold (PTF SI30855) is crossed. The full 17-type JDBC matrix round-trips through filing + cache-hit dispatch. See [`docs/package-caching.md`](./docs/package-caching.md) for the operator's guide and `CHANGELOG.md` for the verification matrix. |
| V7R5 (7.5) | should work — same protocol level; full feature set including package filing (PUB400 V7R5M0 baseline) |
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
