# go-db2i

A pure-Go `database/sql` driver for **IBM i (DB2 for i)**. Speaks the
IBM host-server datastream protocol directly over TCP. No CGo, no
Java sidecar, no IBM client packages — just a Go binary that talks
to the as-database (8471) and as-signon (8476) services on any
IBM i.

```go
import (
    "database/sql"
    _ "github.com/complacentsee/go-db2i/driver"
)

db, err := sql.Open("db2i", "db2i://USER:PWD@host.example.com:8471/?library=MYLIB")
```

## Install

```bash
go get github.com/complacentsee/go-db2i
```

Requires **Go 1.23+** (the driver uses `context.AfterFunc`).

## DSN

```
db2i://USER:PASSWORD@HOST[:DB_PORT]/?key=value&key=value
```

| Key                        | Default                          | Meaning |
|----------------------------|----------------------------------|---------|
| `library`                  | (none)                           | Default schema for unqualified SQL names. Required if the user's job library list doesn't already contain it. |
| `signon-port`              | 8476 (9476 if `tls=true`)        | as-signon service port. |
| `date`                     | `job`                            | Session date format. One of `job`, `iso`, `usa`, `eur`, `jis`, `mdy`, `dmy`, `ymd`. |
| `isolation`                | `none`                           | Session commitment level. One of `none` (`*NONE`), `cs` (`*CS`), `all` (`*ALL`), `rs` (`*RS`), `rr` (`*RR`). The default `*NONE` matches IBM i Db2's autocommit-permissive baseline. `db.Begin()` flips to `*CS` for the duration of the transaction. |
| `tls`                      | `false`                          | Wrap both sockets in TLS. When `true`, the default ports flip to 9476 / 9471 (IBM i SSL host server pair). Accepts `true`, `false`, `1`, `0`, `yes`, `no`, `on`, `off`. Requires the IBM i target to have SSL host server configured via DCM. |
| `tls-insecure-skip-verify` | `false`                          | Skip server-cert validation. IBM i certs are commonly self-signed and lack DNS SANs, in which case `crypto/tls` rejects them by default; set this to `true` to override. Use sparingly — disables MITM protection. |
| `tls-server-name`          | (host)                           | Override the SNI / cert-verify hostname. Defaults to the URL host. Useful when the cert was issued for a different name than what you connect to (e.g. via tunnel). |

The `DB_PORT` segment defaults to 8471 (as-database). Library names
are upper-cased on parse — IBM i schema lookups are case-insensitive
but the wire format expects EBCDIC uppercase. The full list of DSN
keys (~28 in total, including LOB threshold, CCSID overrides,
extended-dynamic packaging, naming convention, and diagnostics
knobs) lives in [`docs/configuration.md`](./docs/configuration.md).

## Examples

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

Runnable [godoc examples](https://pkg.go.dev/github.com/complacentsee/go-db2i/driver)
covering every feature surface live in
[`driver/example_test.go`](./driver/example_test.go), including
context cancellation, BLOB / CLOB inserts, streamed LOB reads,
extended-dynamic package caching, and typed `*Db2Error` predicate
helpers.

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

## Authentication

Password authentication over the host-server signon flow. The
driver negotiates the password level the LPAR is running:

| `QPWDLVL` | Hash on the wire | Status |
|---|---|---|
| 0, 1 | DES | implemented, spec-validated only |
| 2, 3 | SHA-1 | live-validated |
| 4    | PBKDF2 / SHA-512 | live-validated |

Kerberos / GSSAPI signon and MFA are not implemented.

## Server compatibility

| IBM i version | Status |
|---|---|
| V7R6 (7.6) | wire-validated on IBM Cloud V7R6M0. Full feature set including extended-dynamic package filing (SELECT / INSERT / UPDATE / DELETE) and the 17-type prepared-statement matrix. See [`docs/package-caching.md`](./docs/package-caching.md) for the operator's guide. |
| V7R5 (7.5) | should work — same protocol level; full feature set (PUB400 V7R5M0 baseline). |
| V7R4 (7.4) | should work; tested via PUB400. |
| V7R3 (7.3) | should work via password levels 2/3 (SHA-1). |
| ≤ V7R2     | DES auth path (`QPWDLVL` 0/1) is implemented but spec-validated only. |

The `IDENTITY_VAL_LOCAL()` LastInsertId path is session-scoped; see
the godoc on `Result.LastInsertId` for details.

## Why pure Go?

Deploying Go services that need to read or write IBM i Db2 usually
runs into one of three friction points:

1. **`go_ibm_db`** speaks DRDA on port 446, which is often firewalled
   in industrial deployments where only the host-server ports are
   open.
2. **CGo + IBM i Access ODBC** isn't shipped for `linux/arm64`,
   blocking deployment to ARM64 industrial gateways and Apple
   Silicon dev boxes.
3. **A JVM sidecar** running JTOpen works but adds a process boundary
   and ~100 MB of runtime to a Go service.

go-db2i implements the host-server datastream protocol natively in
Go. The result is one statically-linked binary that runs anywhere
Go runs.

## Coming from JT400?

The driver covers the `database/sql` surface of JT400's JDBC driver
(`com.ibm.as400.access.AS400JDBCDriver`). If your application talks
to IBM i through `db.Query` / `db.Exec` / `tx.Begin` / stored-procedure
calls today, the migration is a DSN rewrite.

See [`MIGRATING.md`](./MIGRATING.md) for
the JDBC-property-to-DSN-key mapping, recipes, and a list of JT400
features that are out of scope (non-JDBC JTOpen services like
`CommandCall` / `IFSFile` / `DataQueue`, scrollable cursors, XA,
client reroute, named-parameter binding for procs).

## Acknowledgements

Built clean-room from public protocol references including the
[IBM Toolbox for Java (JTOpen)](https://github.com/IBM/JTOpen)
project under the IBM Public License v1.0. No JTOpen source is
included in this repository or copied at build time.

## License

Apache-2.0. See `LICENSE`.
