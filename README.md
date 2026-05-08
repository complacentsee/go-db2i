# goJTOpen

A pure-Go `database/sql` driver for IBM i (DB2 for i), speaking the IBM
host-server datastream protocol directly over TCP. No CGo, no Java
sidecar, no IBM client packages — just a Go binary that talks to the
as-database (8471) and as-signon (8476) services on any IBM i.

```go
import (
    "database/sql"
    _ "github.com/complacentsee/goJTOpen/driver"
)

db, err := sql.Open("gojtopen", "gojtopen://USER:PWD@host.example.com:8471/?library=MYLIB")
```

## Status

Wire-validated against IBM i 7.6 (V7R6M0) on IBM Cloud Power VS. The
core `database/sql` interfaces are implemented end-to-end:

- Sign-on (password levels 2 / 3 SHA-1, 4 PBKDF2-HMAC-SHA-512)
- Static and parameterised `SELECT` with lazy `Rows` iteration via
  continuation FETCH (streamed 86k rows of `QSYS2.SYSCOLUMNS` in
  testing without buffering)
- Static and parameterised `INSERT` / `UPDATE` / `DELETE`
- Transactions (`db.Begin`, `tx.Commit`, `tx.Rollback`) with
  configurable commitment-control level
- Typed `*hostserver.Db2Error` with `SQLState` / `SQLCode` /
  `MessageID` / `MessageTokens`
- `driver.ErrBadConn` on TCP-level failures so the pool auto-recovers
- `context.Context` propagation including mid-query cancellation
- `Result.LastInsertId` via `IDENTITY_VAL_LOCAL()`
- UTF-8 string binds and decode on V7R3+ (CCSID 1208 passthrough),
  EBCDIC fallback (CCSID 37) on older servers
- Type round-trip: INTEGER, BIGINT, SMALLINT, DOUBLE, REAL, DECIMAL,
  NUMERIC, DECFLOAT(16/34), CHAR, VARCHAR (and FOR BIT DATA), DATE,
  TIME, TIMESTAMP, BLOB, CLOB, DBCLOB

Larger items still on the roadmap (`docs/PLAN.md`):

- **M7**: BLOB/CLOB streaming via `io.Reader` (read-side currently
  materialises the full LOB at Scan time -- fine for most LOBs;
  the streaming Reader is a follow-up). LOB *bind* on parameter
  markers. Broader CCSID coverage.
- **M8**: `slog` integration, OpenTelemetry spans, fuzz tests on the
  reply parser.

## Install

```bash
go get github.com/complacentsee/goJTOpen
```

Requires **Go 1.23+** (the driver uses `context.AfterFunc` from
`context`).

## DSN

```
gojtopen://USER:PASSWORD@HOST[:DB_PORT]/?key=value&key=value
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
```

## Error classification

Server-side SQL errors come back as `*hostserver.Db2Error` with full
SQLSTATE and IBM message-id metadata. Use `errors.As` plus the
predicate methods to drive retry / surfacing logic:

```go
import "github.com/complacentsee/goJTOpen/hostserver"

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

The IBM-supplied options for connecting Go programs to IBM i are:

1. **`go_ibm_db`** — DRDA-only over port 446. Often firewalled in
   industrial deployments where only the host-server ports are open.
2. **CGo + IBM i Access ODBC Driver** — IBM doesn't ship a `linux/arm64`
   build of this driver, blocking deployment to ARM64 industrial
   gateways and Apple Silicon dev boxes.
3. **Java + JTOpen sidecar** — works but adds a JVM and a
   process boundary to a Go service.

goJTOpen takes the **same protocol as JTOpen** (which uses the
host-server datastream over 8471) but reimplements it natively in Go.
The result is one binary that runs anywhere Go runs.

## Acknowledgements

Wire-format implementation builds on the open-source IBM Toolbox for
Java (JTOpen, IBM Public License v1.0) as a protocol reference.
goJTOpen is a clean-room reimplementation; no JTOpen source is
included in this repository or copied at build time. The fixture
harness (`testdata/jtopen-fixtures/`) uses JTOpen at runtime via
Maven Central to record wire traces, but the recorded fixtures are
data-only.

## License

Apache-2.0. See `LICENSE`.
