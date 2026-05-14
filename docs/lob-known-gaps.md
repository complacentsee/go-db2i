# LOB behavior reference

What callers should know about how the driver handles BLOB, CLOB,
and DBCLOB columns — encoding choices, threshold behavior, and a
small set of pitfalls that surprise people.

The wire-protocol derivation lives in
[`../internal/docs/lob-bind-wire-protocol.md`](../internal/docs/lob-bind-wire-protocol.md);
this doc focuses on the behavior a user code path will observe.

## DBCLOB encoding (CCSID 13488 vs 1200)

DBCLOB columns can be declared in either of two graphic CCSIDs and
the driver dispatches accordingly:

| Column CCSID | Encoding         | Non-BMP runes (≥ U+10000)        |
|--------------|------------------|----------------------------------|
| 1200         | UTF-16 BE        | encoded as surrogate pairs       |
| 13488        | UCS-2 BE (strict)| substituted with `?` (U+003F)    |

CCSID 13488 disallows surrogates by spec. When a string bind to a
13488 column contains a non-BMP rune, the driver substitutes
`U+003F` and emits a one-shot `slog.Warn` so the substitution
isn't silent across a process lifetime; subsequent substitutions
stay quiet to avoid log spam.

A typed `*hostserver.NonBMPRuneError` and an opt-in
`encodeUCS2BEStrict` helper are available for callers who would
rather surface a typed error than let the substitute path quietly
transcode their data. Wiring strict mode through a DSN flag is a
future addition; today it is only reachable from package tests.

CCSID 1200 round-trips surrogate pairs end-to-end without
modification.

## Inline-LOB threshold (`?lob-threshold=N`)

The DSN key `?lob-threshold=N` (bytes) drives the server's
inline-vs-locator choice for both bind and result-data. The
threshold travels in `SET_SQL_ATTRIBUTES` CP `0x3822`.

- LOBs below the threshold ship inline as VARCHAR-shaped column
  data (SQL types 404/405 for BLOB, 408/409 for CLOB, 412/413 for
  DBCLOB).
- LOBs above the threshold use the server-allocated locator path
  (SQL types 960..969) and stream through `RETRIEVE_LOB_DATA` /
  `WRITE_LOB_DATA`.

The default threshold is 32768 bytes. The server caps at
15,728,640; values above that are accepted by the driver and the
server clamps. Set per-DSN:

```
db2i://USER:PWD@host/?lob-threshold=65536
```

The result-data decoder handles both inline and locator shapes
transparently; callers' Scan code doesn't change between them.

## RLE compression on LOB reads

`RETRIEVE_LOB_DATA` requests are sent with the
whole-datastream-RLE bit on by default. The V7R6 server wraps the
reply in CP `0x3832` whenever it judges compression worthwhile;
`hostserver.ParseDBReply` unwraps the wrapper before returning.

The bind-side request also enables per-CP RLE-1 inside CP `0x380F`
(LOB Data); `parseLOBReply` reads the header `actualLen` field and
runs the per-CP decompressor for any payload whose wire byte count
differs from `actualLen`.

Compression is opportunistic — highly repetitive content shrinks
to a fraction of a percent of its decompressed size (a 1 MiB
constant-content BLOB lands in well under 1 KiB on the wire);
mixed-entropy content pays no per-byte cost because the server
skips RLE when it wouldn't help.

## Multi-row batched LOB INSERT

There is no special multi-row block-insert wire shape for
LOB-column INSERTs. Each batch row pays its own round-trip series:

```
WRITE_LOB_DATA          upload BLOB column bytes
WRITE_LOB_DATA          upload CLOB column bytes
RETRIEVE_LOB_DATA       refresh locator handles for the next row
EXECUTE_IMMEDIATE       insert this row
```

The mid-batch `RETRIEVE_LOB_DATA` reallocates the per-marker
locator handles so the next row's `WRITE_LOB_DATA` writes to a
fresh handle (locators are per-marker, not per-row).

Callers wanting N-row LOB inserts use `db.Prepare` + N `Stmt.Exec`
calls, or multi-tuple `INSERT VALUES (?, ?), (?, ?), ...`
(exercised by `TestLOBMultiRow`).

## Pitfalls

A handful of things that read like driver bugs but are working as
designed:

- **`db.QueryRow(...).Scan(&LOBReader)` returns garbled data.**
  The producing cursor must stay open while the reader is in use,
  and `QueryRow` auto-closes on `Scan`. Use `db.Query(...).Next()` +
  `Scan(&r)` with a deferred `rows.Close()`. Documented on
  `LOBReader`'s godoc.

- **DBCLOB DDL fails on user profiles without DBCS NLSS.**
  `CREATE TABLE ... DBCLOB(64K) CCSID 1200` requires DBCS-capable
  NLSS configuration. `TestLOBDBClob` skips rather than fails on
  the create error; nothing the driver can do about it.

- **DBCLOB streamed reads/writes need 2-byte-aligned chunks.**
  The default `LOBStreamChunkSize` is 32 KiB (even). The
  chunk-loop rounds down to even bytes if a caller-supplied
  `Reader` ever returns an odd count.

- **Append-style writes aren't exposed.** All bind paths use
  truncate-on-final-frame (full replace). The streamed `LOBStream`
  path uses truncate=false on intermediate `WRITE_LOB_DATA`
  frames but truncate=true on the final, so the visible behavior
  is still full replace. An opt-out via a future
  `LOBValue.Append` field is reachable but not currently wired.
