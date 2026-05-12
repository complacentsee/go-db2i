# Migrating from JTOpen (jt400.jar) to go-db2i

This guide maps every [JTOpen JDBC URL property](https://www.ibm.com/docs/en/i/7.5?topic=jdbc-properties)
to its go-db2i DSN equivalent (or to "not supported, here's why"). It
is honest about coverage gaps: JT400 ships ~109 connection properties;
go-db2i exposes roughly a dozen. The rest are either out of scope
(client-side reroute, BiDi text reordering, JDBC SQL escape extensions)
or simply haven't been wired through yet.

## DSN shape

JT400 (JDBC URL):

```
jdbc:as400://HOST/SCHEMA;prompt=false;naming=sql;date format=iso;translate binary=true
```

go-db2i (DSN):

```
db2i://USER:PASSWORD@HOST[:PORT]/?key=value&key=value
```

- Scheme is `db2i` (registered via `sql.Register("db2i", ...)` in `driver/driver.go`).
- User + password sit in the URL userinfo, not in `?user=` / `?password=` keys.
- Default schema goes in `?library=`, **not** the URL path component.
- Default ports: `8471` (as-database) / `8476` (as-signon); when `?tls=true` they flip to `9471` / `9476`.

See [`docs/configuration.md`](./configuration.md) for the complete DSN reference.

## Property mapping

Columns: **JT400 key** (URL-style spelling JT400 accepts) /
**go-db2i key** / **notes**. "—" means go-db2i has no equivalent;
the *Notes* column says why.

### Authentication & host

| JT400 | go-db2i | Notes |
|---|---|---|
| `user` | URL userinfo | `db2i://USER:...@host/` |
| `password` | URL userinfo | `db2i://USER:PWD@host/` |
| `secure` / `tls` | `tls` | `?tls=true` |
| `key ring name` / `key ring password` | — | We use the system trust store (Go's `crypto/x509.SystemCertPool`). Self-signed IBM i certs: enable `?tls-insecure-skip-verify=true` for development, or install the CA into the OS trust store for production. |
| `secure current user` | — | Plain password auth only; no Kerberos / GSSAPI yet. |
| `additional authentication factor` | — | Multi-factor at signon is not implemented. |
| `authentication verification id` | — | Verification ID isn't sent. |
| `database name` | `library` (closest semantic match) | JT400's "database name" addresses an independent ASP via RDB directory entry; go-db2i connects to the system ASP by default. To use an iASP, run `SETASPGRP` from the server-side trigger / job initialisation programs. |
| `portnumber` | URL `:PORT` | `db2i://u:p@h:8471/`. SIGNON port lives at `?signon-port=N`. |
| `proxy server` / `secondary URL` | — | No JDBC-toolbox proxy support. |

### TLS

| JT400 | go-db2i | Notes |
|---|---|---|
| `secure=true` | `tls=true` | Same effect; default ports flip. |
| `keyStoreLocation` (JVM `-D`) | — | Use OS trust store or `tls-insecure-skip-verify=true`. |
| `trustManager` / SAN tweaks | `tls-server-name` | Override the SNI / cert-verify hostname when the cert lacks a SAN for the address being dialed. |
| — | `tls-insecure-skip-verify` | Disables verification entirely (development only). |

### Session attributes

| JT400 | go-db2i | Notes |
|---|---|---|
| `date format` | `date` | Values: `job` / `iso` / `usa` / `eur` / `jis` / `mdy` / `dmy` / `ymd`. Default `job`. |
| `time format` | `time-format` | M11. Values: `job` (default) / `hms` / `usa` / `iso` / `eur` / `jis`. Maps to CP `0x3809` (TimeFormatParserOption). *Caveat:* the driver's TIME → `time.Time` auto-promotion only understands ISO; for non-ISO values, `Scan` into a `string` or cast the column to `VARCHAR` in the SQL. |
| `date separator` / `time separator` / `decimal separator` | `date-separator` / `time-separator` / `decimal-separator` | M11. Each accepts the literal separator character (`/`, `-`, `.`, `,`, `:`, ` `) or a named alias (`slash`, `dash`, `period`, `comma`, `colon`, `space`). `job` (default) leaves the CP off so the server picks. Maps to CP `0x3808` / `0x380A` / `0x380B` respectively. |
| `naming` | `naming` | M11. `sql` (default; period-qualified `MYLIB.TABLE`) or `system` (slash-qualified `MYLIB/TABLE`; the JT400 default). Set to `system` when migrating RPG/CL apps whose embedded SQL uses slash qualifiers. Maps to CP `0x380C` (NamingConventionParserOption). |
| `transaction isolation` / `commitment control` | `isolation` | Values: `none` (default, matches IBM i Db2 autocommit-permissive baseline) / `cs` / `all` / `rr` / `rs`. `db.Begin()` flips to `cs` transparently. |
| `libraries` | `libraries` | M11. Comma- or space-separated list (e.g. `?libraries=APPLIB,DATALIB,QGPL`). The first entry is tagged with EBCDIC indicator `'C'` (current SQL schema); the rest with `'L'` (back of `*LIBL`). When both `library=X` and `libraries=A,B,C` are set and X is not in the list, X is prepended (JT400 prepend-default-schema rule). |
| `qaqqinilib` | — | Custom optimizer attribute library not plumbed. |
| `prompt` | — (always off) | We never prompt; missing credentials surface as `parseDSN` errors. |

### CCSID & translation

| JT400 | go-db2i | Notes |
|---|---|---|
| `ccsid` | `ccsid` | Same semantics: overrides the application-data CCSID (`SET_SQL_ATTRIBUTES` ClientCCSID). 0 = auto (1208 / UTF-8 on V7R3+, falls back to 37 / US English EBCDIC on older). |
| `translate binary` | — | We always treat CCSID 65535 columns as raw `[]byte`; JT400's "interpret as character data" path is intentionally not mirrored. |
| `translate boolean` | — | Native BOOLEAN columns decode to Go `bool`; "Y"/"N" CHAR(1) columns stay strings. |
| `translate hex` | — | Binary columns decode as `[]byte`; callers can `hex.EncodeToString` them if they want the JT400 "interpret as hex" rendering. |
| `bidi string type` / `bidi implicit reordering` / `bidi numeric ordering` | — | BiDi reordering is JT400 toolbox-side; not applicable to a thin host-server client. |
| `sort` / `sort language` / `sort sequence` / `sort weight` / `sort table` | — | Sort-table control not plumbed; relies on the server-side job's CURSOR_SENSITIVITY / SORT_SEQUENCE defaults. |
| `package ccsid` | `package-ccsid` | M10. Accepts `13488` (default UCS-2 BE), `1200` (UTF-16 LE), and the literal `system` (job CCSID). Wider CCSID support deferred to M11+. |

### LOB handling

| JT400 | go-db2i | Notes |
|---|---|---|
| `lob threshold` | `lob-threshold` | Same wire knob (CP `0x3822`), same default (32768). Bytes below this size travel inline in row data; bytes above it go through server-allocated locators + `RETRIEVE_LOB_DATA`. |
| `large object handling` | `lob=stream` \| `lob=materialise` | JT400 always materialises; go-db2i defaults to materialise but `lob=stream` returns `*db2i.LOBReader` so callers can pull multi-GB LOBs without exhausting the heap. |
| `hold locators` | — | We don't expose `HOLD LOCATOR` semantics; cursors close their locators when the producing `Rows` is closed. |
| `xa locks held` | — | XA / DTC is not implemented. |
| `metadata source` | — | Always uses ROI flag values (the default for V7R5+). |
| `extended dynamic` | `extended-dynamic` | M10. When `true` together with `package=<NAME>`, the driver tells the server to keep PREPAREd statements in a persistent `*PGM` so co-tenant reconnects skip the PREPARE round-trip. |
| `package` | `package` | M10. 1-6 chars from the IBM-i object-name set (`A-Z 0-9 _ # @ $`). The 10-char wire name is the base + a 4-char options-derived suffix, byte-equal to JT400 (`hostserver.BuildPackageName`). |
| `package library` | `package-library` | M10. Library the `*PGM` lives in; default `QGPL`. Up to 10 chars from the same charset. |
| `package cache` | `package-cache` | M10 + v0.7.1. When `true` (requires `extended-dynamic=true`), the driver issues `RETURN_PACKAGE` on connect to download the server's cached statement entries and then bypasses `PREPARE_DESCRIBE` on a client-side cache hit (`ExecutePreparedCached` / `OpenSelectPreparedCached`). See [`package-caching.md`](./package-caching.md) for the wire flow and observability. |
| `package error` | `package-error` | M10. `warning` (default) / `exception` / `none` — controls how the driver reacts to package-related server errors. |
| `package criteria` | `package-criteria` | M10 + v0.7.7. `default` and `select` mirror JT400's two criteria from [`JDSQLStatement.java`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDSQLStatement.java) byte-for-byte. `extended` (v0.7.7) is a **go-db2i-original** opt-in that additionally files `CALL` / `VALUES` / `WITH`; JT400 has no equivalent value. |
| `package add` | `package-add` (accept-ignore) | M10. JT400's only documented value is `true`; go-db2i always adds when `extended-dynamic=true`, so `package-add=true` is accepted as a no-op for DSN-migration friendliness and `package-add=false` is rejected with a clear message. |
| `package clear` | `package-clear` (accept-warn-log) | M10. Server-managed in go-db2i. Accepted for DSN-migration friendliness; the driver slog.Warns on connect when the key is set. |

### Performance / blocking

| JT400 | go-db2i | Notes |
|---|---|---|
| `block size` | `block-size` | v0.7.12. KiB for the continuation-FETCH BufferSize (CP `0x3834`). Range 1-512. Default 32, byte-identical to pre-v0.7.12. |
| `block criteria` | — | The cursor uses JT400's "fetch/close" path (M5+); no per-query tuning. |
| `prefetch` | — (always on) | We always pre-fetch the first batch from `OPEN_DESCRIBE_FETCH`. |
| `lazy close` | — (always lazy) | `Rows.Close()` skips an explicit CLOSE when the server signalled cursor auto-close. |
| `full open` | — (always optimised) | We never request a non-prepared OPEN. |
| `data compression` | — (server-decides) | We always set the RLE-1 wholedata bit and unwrap `0x3832`. The per-CP RLE-1 path also works. |
| `variable field compression` | — | Inline-RLE-1 compression flag from JT400 isn't sent; we let the server pick. |
| `query optimize goal` | — | Falls back to the job default (`*ALLIO` on most V7R5+ systems). |
| `query storage limit` | — | Not plumbed. |
| `query timeout mechanism` | — | Use `context.WithTimeout` per call; cancel propagates via `Conn.SetDeadline`. |
| `socket timeout` | — | JT400 ties to per-read timeouts; go-db2i uses `ctx` deadline plus `Conn.SetDeadline` per-op so cancellation works the standard Go way. |
| `login timeout` | — (always 30 s) | Dial timeout default. Override via passing a deadline-carrying `ctx` to `sql.OpenDB().Conn(ctx)`. |
| `tcp no delay` / `keep alive` / `stay alive` / `receive buffer size` / `send buffer size` | — | Use Go's net.Dialer customisation if needed; not plumbed through the DSN. |
| `thread used` / `virtual threads` | — | Go runtime decides; no toolbox-side thread pool. |

### Reroute / failover

| JT400 | go-db2i | Notes |
|---|---|---|
| `enable client affinities list` | — | Use Go's `sql.DB` pool sizing instead. |
| `enable seamless failover` | — | Same. |
| `affinity failback interval` | — | — |
| `client reroute alternate server name` / `…alternate port number` | — | — |
| `max retries for client reroute` / `retry interval for client reroute` | — | — |

### Diagnostics

| JT400 | go-db2i | Notes |
|---|---|---|
| `trace` / `trace server` / `trace toolbox` | `Config.Logger` + `Config.LogSQL` | M8-3 slog integration. Pass any `*slog.Logger`; the driver emits INFO at connect/close, DEBUG per Exec / Query / LOB chunk, WARN on ErrBadConn, ERROR on statement failures. SQL text gated on `LogSQL`. |
| — | `Config.Tracer` | M8-4 OpenTelemetry spans. Set `Config.Tracer = tp.Tracer("…")` for SpanKindClient spans following the OTel database semantic conventions. |
| `metadata source` / `describe option` | `extended-metadata` | `?extended-metadata=true` enables CP `0x3811` + `0x3829=0xF1` so `Rows.ColumnTypeSchemaName` / `…TableName` / `…BaseColumnName` / `…Label` populate. |

### Behaviour overrides

| JT400 | go-db2i | Notes |
|---|---|---|
| `auto commit` | — (always on) | The driver starts every connection in autocommit mode (matches IBM i Db2 default). `db.Begin()` flips it off for the duration of the transaction. To start with autocommit off, call `hostserver.AutocommitOff` after `sql.OpenDB` (advanced). |
| `cursor hold` | — | We don't set WITH HOLD on cursors; they close when the row-set drains. |
| `cursor sensitivity` | — | Server picks based on the SELECT shape. |
| `rollback cursor hold` | — | — |
| `concurrent access resolution` | — | — |
| `data truncation` / `character truncation` | — | Returns truncated values as-is; no `*WARNING` / `*EXCEPTION` flag. |
| `decimal data errors` | — | Surfaces as `*Db2Error` from the server. |
| `decimal separator` | — | We send `*BLANK` (server defaults). |
| `decfloat rounding mode` | — | Job default. |
| `errors` | — | Always `*FULL`; no `*BASIC` mode. |
| `extended metadata` | `extended-metadata` | See above. |
| `hold statements` | — | — |
| `ignore warnings` | — | All `*Db2Error` records ship to the caller; non-fatal SQLCODEs come through as warnings via `Rows.Err()` after the result drains. |
| `maximum precision` / `maximum scale` / `minimum divide scale` | — | Server-side DECIMAL handling defaults. |
| `numeric range error` | — | — |
| `query replace truncated parameter` | — | — |
| `remarks` | — | We don't query COMMENT ON metadata; use a direct SELECT against `QSYS2.SYSCOLUMNS`. |
| `trim char fields` | — | CHAR columns return server-side padding verbatim; callers trim. |
| `true auto commit` | — | `db.Begin()` is the canonical transaction entry. |
| `use block update` | — | — |
| `use drda metadata version` | — | — |
| `autocommit exception` | — | We never raise on the autocommit transition; rollback / commit errors flow through normally. |

## Quick reference: what's there + what's not

✅ **Supported** (28 DSN keys): `library`, `libraries`, `naming`,
`signon-port`, `date`, `time-format`, `date-separator`,
`time-separator`, `decimal-separator`, `isolation`, `lob`,
`lob-threshold`, `ccsid`, `extended-metadata`, `block-size`,
`tls`, `tls-insecure-skip-verify`, `tls-server-name`,
`extended-dynamic`, `package`, `package-library`, `package-cache`,
`package-error`, `package-criteria`, `package-ccsid`,
`package-add` (accept-ignore), `package-clear` (accept-warn-log),
plus programmatic `Config.Logger` / `Config.LogSQL` /
`Config.Tracer`.

✅ **Driver-typed methods on `*db2i.Conn`** (reach via
`sql.Conn.Raw`): `BatchExec` (v0.7.9 IUD / v0.7.10 MERGE);
`Savepoint` / `ReleaseSavepoint` / `RollbackToSavepoint` (v0.7.12;
mirror `Connection.setSavepoint` etc.); `SetSchema` /
`AddLibraries` / `RemoveLibraries` (v0.7.12; mirror
`Connection.setSchema` and the Toolbox-only `setLibraries`).

✅ **Convenience** (v0.7.12): `db2iiter.ScanAll[T]` returns an
`iter.Seq2[T, error]` over `*sql.Rows` for `for v, err := range
db2iiter.ScanAll(rows, scanFn)` loops.

⏭️ **Deferred** (would benefit a future release): `query optimize goal`,
`socket timeout` (per-op default), `login timeout` (per-op
override; today the dial timeout is the only knob).

🚫 **Out of scope** (won't add): JT400-specific BiDi text reordering, JTOpen
proxy server, XA, client-reroute / seamless failover (use Go's `sql.DB`
pooling), `metadata source` (already always-on), JDBC SQL escape
expansion.

## Migration recipe

A typical jt400 URL:

```
jdbc:as400://prod.example.com;
  user=APPUSER;
  password=secret;
  naming=sql;
  date format=iso;
  libraries=APPLIB;
  translate binary=true;
  lob threshold=8192;
  trace=true;
```

translates to:

```go
dsn := "db2i://APPUSER:secret@prod.example.com/" +
    "?library=APPLIB" +
    "&date=iso" +
    "&lob-threshold=8192"
db, _ := sql.Open("db2i", dsn)
```

For the `trace=true` JT400 flag, attach a slog logger via the
programmatic `Config` path:

```go
cfg := db2i.DefaultConfig()
cfg.User, cfg.Password, cfg.Host = "APPUSER", "secret", "prod.example.com"
cfg.Library = "APPLIB"
cfg.DateFormat = hostserver.DateFormatISO
cfg.LOBThreshold = 8192
cfg.Logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
cfg.LogSQL = false // off by default to keep PII out of logs
connector, _ := db2i.NewConnector(&cfg)
db := sql.OpenDB(connector)
```

For `translate binary=true`: the equivalent behaviour is built in.
go-db2i always returns CCSID-65535 / FOR BIT DATA columns as `[]byte`;
no flag needed.

### Cache-hit fast path (v0.7.1 / v0.7.4)

`extended-dynamic=true&package=APP&package-cache=true` activates
both the server-side `*PGM` filing (JT400-equivalent) AND a
client-side cache-hit dispatch that skips `PREPARE_DESCRIBE` for
any SQL byte-equal to a filed entry. Because the 10-char on-wire
package name is byte-equal to JT400's `JDPackageManager` for the
same session options, **a Java service and a Go service hitting
the same LPAR share one `*PGM` on disk**. The Go client benefits
from plans the Java client filed (and vice versa) without any
coordination.

v0.7.4 extends filing coverage to `INSERT` / `UPDATE` / `DELETE`
(the original v0.7.0–v0.7.2 work was SELECT-only) and fixes a
cache-hit param-binding regression that silently zeroed the
bound value on filed statements. v0.7.4 also auto-populates the
cache after first-time filing on the same connection, so the
3rd / 6th / 12th call of a not-yet-filed SQL triggers a
`RETURN_PACKAGE` refresh and subsequent calls hit the fast path
without waiting for a fresh connect.

See [`package-caching.md`](./package-caching.md) for setup,
observability (slog DEBUG `"db2i: query cache-hit"` lines, OTel
`db.operation.name=OPEN_SELECT_PREPARED_CACHED`), and
`QSYS2.SYSPACKAGE` / `QSYS2.SYSPACKAGESTMT` verification.

## Cross-references

- [JT400 JDProperties.java](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDProperties.java) — the authoritative list of all 109 JT400 properties.
- [`docs/configuration.md`](./configuration.md) — the full go-db2i DSN reference.
- [`docs/package-caching.md`](./package-caching.md) — operator's guide for extended-dynamic + cache-hit dispatch.
- [`docs/performance.md`](./performance.md) — tuning notes for connection-pool sizing, LOB streaming, CCSID choice.
- [`driver/driver.go`](../driver/driver.go) — `Config` struct + `parseDSN` are the source of truth for what the driver accepts.
