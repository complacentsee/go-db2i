# go-db2i configuration

DSN reference + the LPAR-side setup needed to make each feature work.
For day-to-day usage examples, see the package doc on
`pkg.go.dev/github.com/complacentsee/go-db2i/driver`.

## DSN

```
db2i://USER:PASSWORD@HOST[:PORT]/?key=value&key=value
```

PORT defaults to **8471** (as-database) for plaintext, **9471** when
`tls=true`. The driver opens two sockets per pooled connection: signon
(8476 / 9476) and database (8471 / 9471).

| Key                          | Default      | Notes                                                                 |
|------------------------------|--------------|-----------------------------------------------------------------------|
| `library`                    | (none)       | Default schema. Sent via `SET_SQL_ATTRIBUTES` CP `0x380F`. Upper-cased.|
| `libraries`                  | (none)       | v0.7.11. Comma- or space-separated list of libraries to add to the job's library list at connect. First entry tagged indicator `'C'`, the rest `'L'`. Composes with `library=` via the JT400 prepend-default-schema rule. Each entry must be 1-10 chars from `[A-Z 0-9 _ # @ $]`. |
| `naming`                     | `sql`        | v0.7.11. One of `sql` (period-qualified `MYLIB.TABLE`) or `system` (slash-qualified `MYLIB/TABLE`; the JT400 default). Maps to CP `0x380C` (NamingConventionParserOption). |
| `signon-port`                | 8476 / 9476  | Override the as-signon port.                                          |
| `date`                       | `job`        | One of `job`, `iso`, `usa`, `eur`, `jis`, `mdy`, `dmy`, `ymd`.        |
| `time-format`                | `job`        | v0.7.11. One of `job`, `hms`, `usa`, `iso`, `eur`, `jis`. Maps to CP `0x3809` (TimeFormatParserOption). `usa` renders TIME values as 12-hour clock with AM/PM. *Caveat:* the driver's TIME → `time.Time` auto-promotion only understands ISO; for non-ISO values, `Scan` into a `string`. |
| `date-separator`             | (job)        | v0.7.11. One of `job`, `/`, `-`, `.`, `,`, `space` (or named aliases `slash`/`dash`/`period`/`comma`/`space`). Maps to CP `0x3808` (DateSeparatorParserOption); overrides the date-format-inferred default. |
| `time-separator`             | (job)        | v0.7.11. One of `job`, `:`, `.`, `,`, `space` (or `colon`/`period`/`comma`/`space`). Maps to CP `0x380A`. |
| `decimal-separator`          | (job)        | v0.7.11. One of `job`, `.`, `,` (or `period`/`comma`). Maps to CP `0x380B`. |
| `isolation`                  | `none`       | One of `none`, `cs`, `all`, `rs`, `rr`. `db.Begin()` flips to `cs`.   |
| `lob`                        | `materialise`| Or `stream` to get `*db2i.LOBReader`.                             |
| `ccsid`                      | `0` (auto)   | Override the connection-level application-data CCSID.                 |
| `tls`                        | `false`      | Wraps both sockets in `crypto/tls`. Flips default ports to 9471/9476. |
| `tls-insecure-skip-verify`   | `false`      | Disable cert verification (self-signed certs without SANs).           |
| `tls-server-name`            | `host`       | Override the SNI / cert-verify hostname.                              |
| `extended-dynamic`           | `false`      | Enable server-side `*PGM` package caching. See [`package-caching.md`](./package-caching.md). |
| `package`                    | (none)       | Base package name (1-6 chars). Required when `extended-dynamic=true`. |
| `package-library`            | `QGPL`       | Library housing the `*PGM` object. Must already exist (`CRTLIB`).     |
| `package-cache`              | `false`      | Download the filed `*PGM` on connect and dispatch cache-hit fast path (v0.7.1). Requires `extended-dynamic=true`. |
| `package-error`              | `warning`    | One of `warning`, `exception`, `none`. Policy for `CREATE_PACKAGE` / `RETURN_PACKAGE` failures. |
| `package-criteria`           | `default`    | One of `default`, `select`, `extended`. `select` adds unparameterised SELECT statements; `extended` (v0.7.7) adds `CALL` / `VALUES` / `WITH` instead. See [`package-caching.md`](./package-caching.md#eligibility--package-criteria) for the full matrix. |
| `package-ccsid`              | `13488`      | CCSID for package-stored SQL text. Accepts `13488` (UCS-2 BE), `1200` (UTF-16 LE), or `0` (job default). |
| `package-add`                | (ignored)    | JT400-compatibility key. Accepted but no-op; the driver always adds. |
| `package-clear`              | (warn)       | JT400-compatibility key. Accepted but emits a slog `WARN` line and does nothing; programmatic clear is not implemented. |
| `block-size`                 | `32`         | v0.7.12. KiB for the continuation-FETCH BufferSize (CP `0x3834`). Range 1-512. Default (or unset) is byte-identical to pre-v0.7.12. Mirrors JT400's `block size` JDBC URL property. |

## Driver-typed methods (`sql.Conn.Raw`)

A few methods live on `*db2i.Conn` rather than the `database/sql`
interface; reach them via `sql.Conn.Raw`:

| Method | Purpose | Since |
|---|---|---|
| `BatchExec(ctx, sql, rows [][]any) (int64, error)` | Bulk INSERT / UPDATE / DELETE / MERGE via the IBM i block-insert wire shape (CP `0x381F` multi-row). One round-trip per 32k-row chunk vs N for a per-row loop. See [`performance.md`](./performance.md). | v0.7.9 (IUD); v0.7.10 (MERGE) |
| `Savepoint(ctx, name) error` | Issue `SAVEPOINT <name> ON ROLLBACK RETAIN CURSORS`. Matches JT400's `Connection.setSavepoint` wire output (plain SQL, no special CP). Name rules: 1-128 chars, leading letter, body letters/digits/underscore. | v0.7.12 |
| `ReleaseSavepoint(ctx, name) error` | Issue `RELEASE SAVEPOINT <name>`. | v0.7.12 |
| `RollbackToSavepoint(ctx, name) error` | Issue `ROLLBACK TO SAVEPOINT <name>`. | v0.7.12 |
| `SetSchema(ctx, name) error` | Issue `SET SCHEMA <name>` to change the connection's default schema mid-session. Matches JT400's `Connection.setSchema`. Library-name rules (1-10 chars `[A-Z0-9_#@$]`); canonicalised to uppercase. | v0.7.12 |
| `AddLibraries(ctx, libs []string) error` | Append libraries to the connection's *LIBL via one NDB ADD_LIBRARY_LIST round-trip. First entry tagged `'C'` (current schema), rest `'L'`. Same wire helper as the connect-time `?libraries=` knob. | v0.7.12 |
| `RemoveLibraries(ctx, libs []string) error` | Loop `CALL QSYS2.QCMDEXC('RMVLIBLE LIB(X)')` per library. JT400 doesn't expose a NDB REMOVE wire either -- mid-session shrinking goes through CL on both sides. CPF2104 / CPF9810 downgraded to slog WARN. | v0.7.12 |

Example:

```go
conn, _ := db.Conn(ctx); defer conn.Close()
var affected int64
err := conn.Raw(func(driverConn any) error {
    d := driverConn.(*db2i.Conn)
    n, err := d.BatchExec(ctx, "INSERT INTO t VALUES (?, ?)", rows)
    affected = n
    return err
})
```

## TLS

The driver wraps the as-signon and as-database sockets in `crypto/tls`
when `tls=true`. The host-server protocol above the TLS layer is
byte-identical to the plaintext flow.

```
db, _ := sql.Open("db2i",
    "db2i://USER:PWD@host/?tls=true&tls-insecure-skip-verify=true")
```

### IBM i side

There is **no `STRHOSTSVR SSL(*YES)` switch on V7R5+**. The SSL host
servers (ports 9470-9476) light up automatically once a certificate
is assigned to the application IDs, alongside the existing plaintext
ports (8470-8476).

One-time DCM (Digital Certificate Manager) setup:

1. **Reach DCM.** New DCM on V7R5+ lives at
   `http://<host>:2006/dcm` (HTTP; port 2007 only opens after Admin3
   itself has TLS enabled, which is the chicken-and-egg the bootstrap
   flow avoids). Sign on as `QSECOFR` or a user with `*SECADM`
   `*ALLOBJ`.

2. **Create / open the `*SYSTEM` cert store.** First-time setup
   prompts for a store password.

3. **Provision a server certificate.** Either:
   - Generate in DCM signed by a Local CA (cleanest path).
   - Import a PKCS#12 file via `Import` (use when DCM's create
     wizard only offers an "Internet CA" / external-CA flow).
   The SAN should list every name the driver dials by --
   `DNS:<hostname>` and `IP:<address>` both matter when the driver
   sets `tls-server-name` or verifies a SAN.

4. **Assign the cert** to the host-server application IDs:
   - `QIBM_OS400_QZBS_SVR_DATABASE` (port 9471)
   - `QIBM_OS400_QZBS_SVR_SIGNON`   (port 9476)
   - `QIBM_OS400_QZBS_SVR_CENTRAL`  (used in the signon chain)

   Inspect the assignment via SQL:

   ```sql
   SELECT APPLICATION_ID, CERTIFICATE_LABEL_COUNT, CERTIFICATE_LABELS
   FROM   TABLE(QSYS2.CERTIFICATE_USAGE_INFO())
   WHERE  APPLICATION_ID LIKE 'QIBM_OS400_QZBS_SVR%';
   ```

5. **Verify the SSL ports are listening.** From any host with LAN
   access to the LPAR:

   ```sh
   for p in 9471 9476 9470; do
     timeout 2 bash -c "(echo > /dev/tcp/<host>/$p)" \
       && echo "$p OPEN" || echo "$p closed"
   done
   ```

   If the ports stay closed after the cert is assigned, end and
   restart the host servers: `ENDHOSTSVR SERVER(*ALL)` then
   `STRHOSTSVR SERVER(*ALL)`.

### Cert verification

`crypto/tls` validates the server cert against the system trust store
plus the hostname / IP the driver dialed. Two escape hatches when a
self-signed or SAN-light cert is in use:

- `tls-insecure-skip-verify=true` — disables all cert checks. Fine
  for development; never use against an untrusted network.
- `tls-server-name=<sni>` — overrides the name the driver presents
  for SNI and cert validation. Useful when the cert's CN/SAN names
  the LPAR's primary hostname but the driver is dialing via a tunnel
  / load balancer / different DNS.

### Validation recipe

The repo ships a live conformance test gated on `DB2I_TLS_TARGET`:

```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -tags=conformance \
    -o gosqltest ./test/conformance/
DB2I_TLS_TARGET="db2i://USER:PWD@host:9471/?signon-port=9476&tls=true&tls-insecure-skip-verify=true" \
DB2I_DSN="db2i://USER:PWD@host:8471/?signon-port=8476" \
  ./gosqltest -test.run TestTLSConnectivity -test.v
```

`TestTLSConnectivity/smoketest` confirms the full sign-on +
start-database + PREPARE / EXECUTE / FETCH path runs over TLS.
`TestTLSConnectivity/multi-row` byte-diffs a 5-row result against the
plaintext counterpart (when `DB2I_DSN` is also set) to prove the
protocol above the TLS layer is unchanged. Both subtests skip when
`DB2I_TLS_TARGET` is empty, so the suite is safe to run against
any IBM i target.
