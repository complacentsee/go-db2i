# Migrating from JTOpen (jt400.jar) to goJTOpen

This guide maps every [JTOpen JDBC URL property](https://www.ibm.com/docs/en/i/7.5?topic=jdbc-properties)
to its goJTOpen DSN equivalent (or to "not supported, here's why"). It
is honest about coverage gaps: JT400 ships ~109 connection properties;
goJTOpen exposes roughly a dozen. The rest are either out of scope
(client-side reroute, BiDi text reordering, JDBC SQL escape extensions)
or simply haven't been wired through yet.

## DSN shape

JT400 (JDBC URL):

```
jdbc:as400://HOST/SCHEMA;prompt=false;naming=sql;date format=iso;translate binary=true
```

goJTOpen (DSN):

```
gojtopen://USER:PASSWORD@HOST[:PORT]/?key=value&key=value
```

- Scheme is `gojtopen` (registered via `sql.Register("gojtopen", ...)` in `driver/driver.go`).
- User + password sit in the URL userinfo, not in `?user=` / `?password=` keys.
- Default schema goes in `?library=`, **not** the URL path component.
- Default ports: `8471` (as-database) / `8476` (as-signon); when `?tls=true` they flip to `9471` / `9476`.

See [`docs/configuration.md`](./configuration.md) for the complete DSN reference.

## Property mapping

Columns: **JT400 key** (URL-style spelling JT400 accepts) /
**goJTOpen key** / **notes**. "—" means goJTOpen has no equivalent;
the *Notes* column says why.

### Authentication & host

| JT400 | goJTOpen | Notes |
|---|---|---|
| `user` | URL userinfo | `gojtopen://USER:...@host/` |
| `password` | URL userinfo | `gojtopen://USER:PWD@host/` |
| `secure` / `tls` | `tls` | `?tls=true` |
| `key ring name` / `key ring password` | — | We use the system trust store (Go's `crypto/x509.SystemCertPool`). Self-signed IBM i certs: enable `?tls-insecure-skip-verify=true` for development, or install the CA into the OS trust store for production. |
| `secure current user` | — | Plain password auth only; no Kerberos / GSSAPI yet. |
| `additional authentication factor` | — | Multi-factor at signon is not implemented. |
| `authentication verification id` | — | Verification ID isn't sent. |
| `database name` | `library` (closest semantic match) | JT400's "database name" addresses an independent ASP via RDB directory entry; goJTOpen connects to the system ASP by default. To use an iASP, run `SETASPGRP` from the server-side trigger / job initialisation programs. |
| `portnumber` | URL `:PORT` | `gojtopen://u:p@h:8471/`. SIGNON port lives at `?signon-port=N`. |
| `proxy server` / `secondary URL` | — | No JDBC-toolbox proxy support. |

### TLS

| JT400 | goJTOpen | Notes |
|---|---|---|
| `secure=true` | `tls=true` | Same effect; default ports flip. |
| `keyStoreLocation` (JVM `-D`) | — | Use OS trust store or `tls-insecure-skip-verify=true`. |
| `trustManager` / SAN tweaks | `tls-server-name` | Override the SNI / cert-verify hostname when the cert lacks a SAN for the address being dialed. |
| — | `tls-insecure-skip-verify` | Disables verification entirely (development only). |

### Session attributes

| JT400 | goJTOpen | Notes |
|---|---|---|
| `date format` | `date` | Values: `job` / `iso` / `usa` / `eur` / `jis` / `mdy` / `dmy` / `ymd`. Default `job`. |
| `time format` | — | Driver currently emits `*JOB` for time format. Pin via session-init SET TIME FORMAT until wired. |
| `date separator`, `time separator`, `decimal separator` | — | We send `*BLANK` for separators (server picks based on date/time format). |
| `naming` | — (always `sql`) | goJTOpen always sends `naming=sql` (CP `0x3812 = 0`); JT400 defaults to `system` which uses `MYLIB/TABLE`-style qualification. Cross-job CL that mixes both styles must rewrite to SQL-style (`MYLIB.TABLE`) before being run via this driver. |
| `transaction isolation` / `commitment control` | `isolation` | Values: `none` (default, matches IBM i Db2 autocommit-permissive baseline) / `cs` / `all` / `rr` / `rs`. `db.Begin()` flips to `cs` transparently. |
| `libraries` | `library` (one only) | JT400 accepts a list (`/LIBA,LIBB,LIBC` or `libraries=LIBA LIBB LIBC`). goJTOpen takes one default library; add others via SQL-side `CALL QSYS2.QCMDEXC('ADDLIBLE LIBB')` after connect, or trigger them in the user profile's INLPGM. |
| `qaqqinilib` | — | Custom optimizer attribute library not plumbed. |
| `prompt` | — (always off) | We never prompt; missing credentials surface as `parseDSN` errors. |

### CCSID & translation

| JT400 | goJTOpen | Notes |
|---|---|---|
| `ccsid` | `ccsid` | Same semantics: overrides the application-data CCSID (`SET_SQL_ATTRIBUTES` ClientCCSID). 0 = auto (1208 / UTF-8 on V7R3+, falls back to 37 / US English EBCDIC on older). |
| `translate binary` | — | We always treat CCSID 65535 columns as raw `[]byte`; JT400's "interpret as character data" path is intentionally not mirrored. |
| `translate boolean` | — | Native BOOLEAN columns decode to Go `bool`; "Y"/"N" CHAR(1) columns stay strings. |
| `translate hex` | — | Binary columns decode as `[]byte`; callers can `hex.EncodeToString` them if they want the JT400 "interpret as hex" rendering. |
| `bidi string type` / `bidi implicit reordering` / `bidi numeric ordering` | — | BiDi reordering is JT400 toolbox-side; not applicable to a thin host-server client. |
| `sort` / `sort language` / `sort sequence` / `sort weight` / `sort table` | — | Sort-table control not plumbed; relies on the server-side job's CURSOR_SENSITIVITY / SORT_SEQUENCE defaults. |
| `package ccsid` | — | Extended-dynamic-package feature isn't implemented; see "Unsupported features" below. |

### LOB handling

| JT400 | goJTOpen | Notes |
|---|---|---|
| `lob threshold` | `lob-threshold` | Same wire knob (CP `0x3822`), same default (32768). Bytes below this size travel inline in row data; bytes above it go through server-allocated locators + `RETRIEVE_LOB_DATA`. |
| `large object handling` | `lob=stream` \| `lob=materialise` | JT400 always materialises; goJTOpen defaults to materialise but `lob=stream` returns `*gojtopen.LOBReader` so callers can pull multi-GB LOBs without exhausting the heap. |
| `hold locators` | — | We don't expose `HOLD LOCATOR` semantics; cursors close their locators when the producing `Rows` is closed. |
| `xa locks held` | — | XA / DTC is not implemented. |
| `metadata source` | — | Always uses ROI flag values (the default for V7R5+). |
| `extended dynamic` / `package` / `package add` / `package cache` / `package clear` / `package criteria` / `package error` / `package library` | — | The whole extended-dynamic-package family is JT400-specific (it caches statement plans in `*PGM` objects so repeated PREPAREs become locator-lookups). goJTOpen always PREPAREs fresh. For repeated-statement performance, use a `sql.DB` with a healthy connection pool — see [`docs/performance.md`](./performance.md). |

### Performance / blocking

| JT400 | goJTOpen | Notes |
|---|---|---|
| `block size` | — | Block-fetch buffer fixed at 32 KB (`hostserver.Cursor.fetchMore`). |
| `block criteria` | — | The cursor uses JT400's "fetch/close" path (M5+); no per-query tuning. |
| `prefetch` | — (always on) | We always pre-fetch the first batch from `OPEN_DESCRIBE_FETCH`. |
| `lazy close` | — (always lazy) | `Rows.Close()` skips an explicit CLOSE when the server signalled cursor auto-close. |
| `full open` | — (always optimised) | We never request a non-prepared OPEN. |
| `data compression` | — (server-decides) | We always set the RLE-1 wholedata bit and unwrap `0x3832`. The per-CP RLE-1 path also works. |
| `variable field compression` | — | Inline-RLE-1 compression flag from JT400 isn't sent; we let the server pick. |
| `query optimize goal` | — | Falls back to the job default (`*ALLIO` on most V7R5+ systems). |
| `query storage limit` | — | Not plumbed. |
| `query timeout mechanism` | — | Use `context.WithTimeout` per call; cancel propagates via `Conn.SetDeadline`. |
| `socket timeout` | — | JT400 ties to per-read timeouts; goJTOpen uses `ctx` deadline plus `Conn.SetDeadline` per-op so cancellation works the standard Go way. |
| `login timeout` | — (always 30 s) | Dial timeout default. Override via passing a deadline-carrying `ctx` to `sql.OpenDB().Conn(ctx)`. |
| `tcp no delay` / `keep alive` / `stay alive` / `receive buffer size` / `send buffer size` | — | Use Go's net.Dialer customisation if needed; not plumbed through the DSN. |
| `thread used` / `virtual threads` | — | Go runtime decides; no toolbox-side thread pool. |

### Reroute / failover

| JT400 | goJTOpen | Notes |
|---|---|---|
| `enable client affinities list` | — | Use Go's `sql.DB` pool sizing instead. |
| `enable seamless failover` | — | Same. |
| `affinity failback interval` | — | — |
| `client reroute alternate server name` / `…alternate port number` | — | — |
| `max retries for client reroute` / `retry interval for client reroute` | — | — |

### Diagnostics

| JT400 | goJTOpen | Notes |
|---|---|---|
| `trace` / `trace server` / `trace toolbox` | `Config.Logger` + `Config.LogSQL` | M8-3 slog integration. Pass any `*slog.Logger`; the driver emits INFO at connect/close, DEBUG per Exec / Query / LOB chunk, WARN on ErrBadConn, ERROR on statement failures. SQL text gated on `LogSQL`. |
| — | `Config.Tracer` | M8-4 OpenTelemetry spans. Set `Config.Tracer = tp.Tracer("…")` for SpanKindClient spans following the OTel database semantic conventions. |
| `metadata source` / `describe option` | `extended-metadata` | `?extended-metadata=true` enables CP `0x3811` + `0x3829=0xF1` so `Rows.ColumnTypeSchemaName` / `…TableName` / `…BaseColumnName` / `…Label` populate. |

### Behaviour overrides

| JT400 | goJTOpen | Notes |
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

✅ **Supported** (12 DSN keys): `library`, `signon-port`, `date`, `isolation`, `lob`,
`lob-threshold`, `ccsid`, `extended-metadata`, `tls`,
`tls-insecure-skip-verify`, `tls-server-name`, plus programmatic
`Config.Logger` / `Config.LogSQL` / `Config.Tracer`.

⏭️ **Deferred** (would benefit a future release): `libraries` (multi-
library default list), `time format`, separators, `query optimize goal`,
`socket timeout` (per-op default), `package` family for prepared-statement caching.

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
dsn := "gojtopen://APPUSER:secret@prod.example.com/" +
    "?library=APPLIB" +
    "&date=iso" +
    "&lob-threshold=8192"
db, _ := sql.Open("gojtopen", dsn)
```

For the `trace=true` JT400 flag, attach a slog logger via the
programmatic `Config` path:

```go
cfg := gojtopen.DefaultConfig()
cfg.User, cfg.Password, cfg.Host = "APPUSER", "secret", "prod.example.com"
cfg.Library = "APPLIB"
cfg.DateFormat = hostserver.DateFormatISO
cfg.LOBThreshold = 8192
cfg.Logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
cfg.LogSQL = false // off by default to keep PII out of logs
connector, _ := gojtopen.NewConnector(&cfg)
db := sql.OpenDB(connector)
```

For `translate binary=true`: the equivalent behaviour is built in.
goJTOpen always returns CCSID-65535 / FOR BIT DATA columns as `[]byte`;
no flag needed.

## Cross-references

- [JT400 JDProperties.java](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDProperties.java) — the authoritative list of all 109 JT400 properties.
- [`docs/configuration.md`](./configuration.md) — the full goJTOpen DSN reference.
- [`docs/performance.md`](./performance.md) — tuning notes for connection-pool sizing, LOB streaming, CCSID choice.
- [`driver/driver.go`](../driver/driver.go) — `Config` struct + `parseDSN` are the source of truth for what the driver accepts.
