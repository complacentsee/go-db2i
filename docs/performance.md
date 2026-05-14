# go-db2i performance tuning

Practical guidance for getting the most out of go-db2i against a real
IBM i target. Every number below comes from a reproducible test in
this repository or from a measured run against the IBM Cloud V7R6M0
LPAR captured in `AUTH.md`; pointers are inline.

## Sizing the `sql.DB` pool

The biggest single performance lever is `sql.DB.SetMaxOpenConns`. IBM i
spawns one `QZDASOINIT` job per connection, and each job costs ~1 s of
LPAR startup on a 1-CPU shared-processor partition. Once warm, the
pool is the difference between sub-millisecond and double-digit-ms
checkout.

Concrete numbers from the IBM Cloud V7R6M0 LPAR (1 CPU,
shared-processor), read-only workload mix:

| Concurrency | Throughput |
|---:|---:|
| 1   | ~400 ops/sec |
| 10  | ~3,500 ops/sec |
| 20  | ~6,200 ops/sec |
| 50  | **~8,800 ops/sec (peak)** |
| 100 | collapses (textbook saturation) |

(See `AUTH.md` "Operational notes" for the measurement context.)

**Recommendation.** On a 1-CPU LPAR, cap `SetMaxOpenConns` at 50 and
warm the pool serially before the workload starts:

```go
db, _ := sql.Open("db2i", dsn)
db.SetMaxOpenConns(50)
db.SetMaxIdleConns(50)            // hold them open
db.SetConnMaxLifetime(time.Hour)  // recycle hourly

// Pre-warm: spawn N+1 sequential pings so the prestart-job pool
// doesn't have to allocate N QZDASOINITs simultaneously when traffic
// arrives. ~1 s per new job on a 1-CPU LPAR.
for i := 0; i < 50; i++ {
    if err := db.Ping(); err != nil { ... }
}
```

On a multi-CPU LPAR or one with `QZDASOINIT` prestart-jobs already
warmed (see `AUTH.md` "Prestart-job pool tuning" for the recommended
`STRPJ` settings), the collapse point shifts higher; benchmark before
going past 50.

## `?lob=stream` vs the materialise default

The default `lob=materialise` mode reads each LOB column into a `[]byte`
(BLOB) or `string` (CLOB / DBCLOB) at `Rows.Scan` time. Simple, easy to
use, and what 90% of callers want. The cost is heap proportional to
LOB size: a 100 MB BLOB column = 100 MB allocation per row.

`lob=stream` returns a `*db2i.LOBReader` (`io.Reader` + `io.Closer`)
instead. The driver pulls 32 KB chunks per `Read` call via
`RETRIEVE_LOB_DATA`, so steady-state memory is bounded by the buffer
size regardless of LOB byte count.

**When to flip:** if any LOB column you SELECT could exceed Go heap
budget (e.g. multi-GB archives) or if you stream the bytes through
straight to disk / an HTTP response without needing them resident.

**Reference numbers** (from `TestRowsLazyMemoryBounded`,
`test/conformance/conformance_test.go:1139`):

| Path | Peak Go HeapAlloc | Notes |
|---|---:|---|
| Default (materialise) | scales linearly with cumulative LOB bytes | 50K-row × ~3 cols, no LOB → ~3.8 MiB peak (no LOB pressure) |
| `?lob=stream` | bounded by 32 KB × open readers | each `Read` pays one round trip |

**Trade-off:** `lob=stream` adds one wire round trip per chunk. For
small LOBs (< 100 KB) materialise wins on latency; for large LOBs
stream wins on memory + lets pipelined consumers start earlier.

## `?lob-threshold=N`

`?lob-threshold=N` is the byte cutoff the server uses to decide whether
to inline LOB column data in the row reply (cheaper round trip) or
allocate a server-side locator that the driver fetches via
`RETRIEVE_LOB_DATA`. Default 32768.

| Threshold | Effect | Use case |
|---|---|---|
| 0 (unset) | Default 32768 | The historical sweet spot for mixed-size LOBs. |
| Higher (e.g. 1 MiB) | Server inlines more LOB columns -> bigger row-data frames | Many small LOBs, want to skip the locator round trip. Trades wire frame size for round trips. |
| Lower (e.g. 1 KiB) | Server inlines fewer columns -> more locator round trips | Many large LOBs, want predictable inline frames. |
| 1 | Forces locator path for every LOB | Debugging the inline path vs locator path independently. The `?lob-threshold=1` setting is what the `TestCCSID1208RoundTrip/CLOB small inline` subtest uses to exercise the locator code path with a small payload. |

**Recommendation.** Leave at default unless you've measured a specific
LOB-shape mismatch. The server caps the value at 15,728,640 bytes.

## `?ccsid=N` vs the auto-pick default

By default the driver auto-picks the application-data CCSID:
- 1208 (UTF-8) on V7R3+ — the full Unicode round-trip.
- 37 (US English EBCDIC) on older releases.

Set `?ccsid=N` explicitly when you know the server's job CCSID and
want untagged CHAR / VARCHAR columns to decode without locale
ambiguity. Common choices:

| CCSID | Wire encoding | When to pin |
|---|---|---|
| 1208 | UTF-8 | V7R3+; cleanest path. Non-ASCII data round-trips byte-equal through `string` Go values. |
| 37 | US-English EBCDIC | Locked to a US-English job and you'd rather skip the V7R3 check. |
| 273 | German EBCDIC | The job is a German locale (PUB400 default); pins binds to match the job's CCSID even if your client's auto-pick would have negotiated 1208. |

**Transcoding cost.** The `ebcdic` package caches each codec at first
use; per-call decode is a memcopy + table-lookup, no per-call decoder
construction. CCSID 1208 paths are passthrough (no transcoding). For
CCSID-bound workloads the cost is essentially the wire bytes; the
codec layer disappears in the noise.

## CCSID-aware decode hot path

Per-column CCSID always wins on the read side. The cached codec map
(`ebcdic.Codecs`) gets initialised lazily on first sighting of each
CCSID; subsequent decodes go straight to the cached struct. This
matters because IBM i jobs often mix CCSIDs across columns (a
CHAR(10) tagged 37 + a CHAR(100) tagged 1208 + a DBCLOB tagged 13488
in the same row).

No tuning knob exposed — the caching is automatic. If you see
allocations show up in a `-trace` profile pointing at `ebcdicForCCSID`,
file an issue (it indicates a CCSID that's not in the static map).

## RLE compression on `RETRIEVE_LOB_DATA`

The driver always negotiates the whole-datastream RLE-1 bit
(`0x00040000` on `RETRIEVE_LOB_DATA`) and transparently unwraps CP
`0x3832` replies via `decompressDataStreamRLE`.

For high-repetition LOB content the wire savings are dramatic:

| LOB content | Uncompressed size | Compressed wire | Ratio |
|---|---:|---:|---:|
| 1 MiB constant `0xCC` BLOB | 1,048,576 bytes | **1,228 bytes** | **~854×** |
| 1 MiB random bytes | 1,048,576 bytes | ~1,049,000 bytes | ~1× (server skips compression on incompressible data) |
| 8 KiB English text CLOB | 8,192 bytes | ~6,300 bytes | ~1.3× |

(Numbers from `stress-test/rlemeasure` against IBM Cloud V7R6M0
and from the `TestDecompressDataStreamRLE` synthetic cases.)

The server-side compression decision is automatic — there's no
"force compression" or "force no-compression" knob. The driver just
reads whichever form the server picks.

## TLS overhead

Switching to the TLS host-server ports (`?tls=true`, default 9476 /
9471) costs a one-time TLS handshake on first dial. Steady-state
overhead above the host-server protocol is the per-record encryption
+ MAC, typically << 1% of total wire time for query workloads.

Measured from `TestTLSConnectivity` against IBM Cloud V7R6M0:

| Stage | Plaintext | TLS | Delta |
|---|---:|---:|---:|
| Initial dial + handshake | ~50 ms | ~150 ms | +100 ms first connect |
| Single-row SELECT | identical bytes above the host-server protocol | (no measurable per-call difference) |
| 5-row SELECT (byte-equal to plaintext) | — | byte-equal | confirmed via `TestTLSConnectivity/multi-row` |

**Recommendation.** Use TLS whenever the network path crosses a
trust boundary; the first-connect overhead is amortised by the
pool's connection lifetime, and on-the-wire metadata (CCSID, user
ID, SQL text) is otherwise transmitted in clear EBCDIC.

## Extended-dynamic SQL package caching

`?extended-dynamic=true&package=APP&package-cache=true` files every
PREPAREd statement into a server-side `*PGM` and dispatches a
cache-hit fast path that skips PREPARE_DESCRIBE for any SQL that
byte-equals a previously filed entry. The full conceptual guide
lives in [`package-caching.md`](./package-caching.md); this section
covers the perf characteristics.

### Wire round-trip saving

| Path | Frames per call |
|---|---|
| Cache miss | `CREATE_RPB` + `PREPARE_DESCRIBE` + `CHANGE_DESCRIPTOR` + `EXECUTE`/`OPEN` (4 round-trips) |
| Cache hit | `CREATE_RPB` + `CHANGE_DESCRIPTOR` + `EXECUTE`/`OPEN` with name override in CP `0x3806` (3 round-trips) |

One round-trip saved per call. The percentage saving depends on
latency to the LPAR — on a high-latency tunnel (cross-region IBM
Cloud SSH tunnel) the v0.7.1 smoke test measured warm-up at ~514 ms
and steady-state cache hits at ~104 ms (~4× speed-up). On an
LPAR-local network the saving is closer to ~10-20%; the
PREPARE_DESCRIBE frame is doing real plan-compilation work, not
just a round-trip.

### Same-session auto-populate (v0.7.4)

The cache-hit fast path needs the server-assigned 18-byte statement
name before it can dispatch. Pre-v0.7.4, that name was learned only
through `RETURN_PACKAGE` on connect — so a SQL string that the
server filed mid-session stayed off the fast path until the next
connection cycled. Cold starts paid for themselves; long-lived
sessions did not.

v0.7.4 closes that gap. The driver tracks per-SQL local
`PREPARE_DESCRIBE` counts (`Conn.noteFilingPrepare`); when the
count crosses IBM's 3-PREPARE threshold (PTF SI30855), the
post-EXECUTE handler issues a one-shot `RETURN_PACKAGE` refresh.
The next call of the same SQL dispatches via
`ExecutePreparedCached` / `OpenSelectPreparedCached` without
waiting for a reconnect. Capped at
`hostserver.MaxFilingRefreshAttempts = 3` per SQL so a statement
the server refuses to file (package full, locked, etc.) doesn't
burn unbounded refresh round-trips.

Measured on IBM Cloud V7R6M0 (CHANGELOG v0.7.4, commit `7b614b4`):
**~50% reduction in per-call latency (35 ms vs 71 ms)** after the
threshold crosses, on the same long-lived `sql.DB` pool.

v0.7.7 added a defensive `hasOutDest` gate that skipped the
refresh for OUT/INOUT dispatches, reasoning that the cache-hit
path refused non-IN direction bytes. **v0.7.8 removed that gate**
after empirically confirming the server honours OUT direction
bytes on cache-hit dispatch (probe against V7R6M0 2026-05-12;
see `docs/plans/v0.7.8-out-param-cache-hit.md`). OUT CALLs filed
under `package-criteria=extended` now auto-populate and
cache-hit-dispatch like every other eligible statement;
`ExecutePreparedCached` requests `ORSResultData` and decodes
CP `0x380E` for OUT values when any cached PMF slot carries an
OUT/INOUT byte. The same ~50% post-threshold latency reduction
applies to OUT CALLs via this path.

Reference: `docs/package-caching.md` "auto-populate" section and
`test/conformance/cache_hit_test.go` for the threshold-crossing
fixtures.

### When to opt out

- **SQL text varies every call.** ORMs that interpolate
  identifiers instead of using `?` parameter markers produce
  unique strings — every call misses, every call adds an entry,
  the `*PGM` grows without bound.
- **High deploy churn.** If you redeploy frequently and the SQL
  text shifts between releases, the stale cached entries waste
  `*PGM` storage without benefit. `DLTPGM` between deploys or
  set `package-cache=false` on the leading edge.
- **Mixed-criteria workload.** If 90% of your SQL is unparameterised
  ad-hoc queries, `package-criteria=select` will fill the `*PGM`
  with entries that miss anyway. Stay on `default` (parameterised
  only).

### Verifying cache hits in production

Inject a `*slog.Logger` at INFO+; cache-hits emit DEBUG only, so
their cost is the LevelDebug filter check (a few ns when filtered
out). To audit the cache during diagnosis, set the handler level
to DEBUG and grep for `"db2i: query cache-hit"` /
`"db2i: exec cache-hit"`. The `cached_name` attribute is the
18-byte server-assigned statement name; cross-reference against
`QSYS2.SYSPACKAGESTMT.statement_name` for the matching SQL text.

OTel users see the same signal via the span's
`db.operation.name` attribute — `EXECUTE_PREPARED_CACHED` /
`OPEN_SELECT_PREPARED_CACHED` on hit, `EXECUTE_PREPARED` /
`OPEN_SELECT_PREPARED` on miss.

Reference: `test/conformance/cache_hit_test.go` exercises 12
scenarios across the JDBC type matrix and asserts the slog probe
on each cache-hit dispatch.

## Bulk IUD + MERGE via `Conn.BatchExec` (v0.7.9, v0.7.10)

A driver-typed extension on `*db2i.Conn`. Packs N parameter-marker
sets into a single CP `0x381F` multi-row block on one EXECUTE
request — the IBM i "block insert" wire shape for non-LOB IUD +
MERGE batches. One round-trip per 32k-row chunk vs N per row for
a regular `db.Exec` loop. Auto-splits at `MaxBlockedInputRows =
32000`.

Wire savings:

| Path | Round-trips for 1000-row INSERT |
|---|---|
| `db.Exec` loop | 1000 (one PREPARE_DESCRIBE + EXECUTE per row; PREPARE_DESCRIBE is amortised via `database/sql`'s plan cache but the EXECUTE remains) |
| `Conn.BatchExec` | 1 EXECUTE for the whole batch (plus the once-per-batch CREATE_RPB + PREPARE_DESCRIBE + CHANGE_DESCRIPTOR) |

Measured on IBM Cloud V7R6M0 via VPC tunnel for 1000-row INSERT
into a small table (`TestBatch_PerfDelta` in
`test/conformance/batch_test.go`): `db.Exec` loop ~3.9 s vs
`BatchExec` ~11 ms (**~358× speed-up**). The win is the
round-trip elimination — each per-row INSERT pays one full
network round-trip; the batch collapses to one wire frame. Speed-
up scales with RTT: an LPAR-local network at ~0.1 ms RTT would
see closer to 10–50× (PREPARE_DESCRIBE's plan-compile cost on the
server dominates at low RTT), while the high-RTT tunneled path
sees the wire savings nearly unattenuated. A 50k-row batch
finishes in ~160 ms on the same tunnel (two 32k+18k chunks, two
EXECUTEs).

Verbs: INSERT, UPDATE, DELETE (v0.7.9) + MERGE (v0.7.10). MERGE
batches use the same CP `0x381F` multi-row shape on V7R1+; the
typical pattern is `MERGE INTO target USING (VALUES (?, ?))` with
each batch row supplying the source tuple.

Limitations:
- No LOB parameters. The locator-allocate flow doesn't compose
  with the multi-row CP `0x381F` shape, so `BatchExec` rejects
  `*db2i.LOBValue` rows with a pointer at the per-row path.
- No `sql.Out` destinations. IUD / MERGE have no OUT params on
  the wire.
- Every row in a single call must have the same Go types per
  column position; mismatches are caught up-front before any
  wire activity.

Access pattern documented in
[`configuration.md`](./configuration.md#driver-typed-methods-sqlconnraw).

## Lazy iteration via `Rows.Next`

`Rows.Next` pulls one row at a time from the underlying
`hostserver.Cursor`; continuation FETCH (CP `0x180B`) fires only when
the 32 KB block-fetch buffer drains. Peak heap stays bounded
regardless of result-set size.

Reference: `TestRowsLazyMemoryBounded`. Walks ~50K rows of
`QSYS2.SYSCOLUMNS` with peak `runtime.MemStats.HeapAlloc` ≤ 3.8 MiB
(budget: 16 MiB). Pre-M5 the same query would have buffered ~50K
`SelectRow` tuples (~40 MiB at our row width) before yielding the
first row.

**No tuning required** — the lazy path is the only path in M7+. If
you need batch semantics (process N rows together before the next
fetch), buffer in your loop, not in the driver.

## When DEBUG logging hurts

The slog integration emits one DEBUG line per `Stmt.Exec` /
`Stmt.Query` / `LOBReader.Read` round trip. For high-throughput
read workloads this adds 1-2 µs per call regardless of handler (the
`slog.LevelDebug` filter check fires even when the handler discards).
The discard-handler fallback (nil `Config.Logger`) elides the call
entirely.

**Recommendation.** Leave `Config.Logger = nil` in hot-path
production code; attach a logger only for the diagnosis window. If
you need always-on logs, set the handler's level to INFO or higher
so the per-call DEBUG attrs don't allocate.

## When OpenTelemetry tracing hurts

The OTel integration starts one span per `ExecContext` /
`QueryContext`. The noop-tracer fallback (nil `Config.Tracer`) costs
~50 ns per call; a real SDK-backed tracer typically lands at 2-5 µs
per span (most of which is the SDK's span allocation, not driver
code).

**Recommendation.** For workloads where per-call latency matters more
than observability, use the SDK's sampler-based gating
(`trace.WithSampler(trace.TraceIDRatioBased(0.01))` for 1%
sampling) rather than detaching the tracer entirely. The driver's
span emission is uniform across calls so dropped spans don't skew
the picture.

## Cross-references

- `AUTH.md` — environment + LPAR-specific stress-test numbers + the
  jump-host binaries used to generate them.
- `test/conformance/conformance_test.go:1125` — `TestRowsLazyMemoryBounded`
  (lazy-iteration memory cap).
- `hostserver/db_lob_rle_test.go` — RLE compression decompressor coverage.
- `docs/lob-known-gaps.md` — the LOB-side history for the compression / threshold knobs.
- `docs/configuration.md` — the full DSN reference.
- `docs/package-caching.md` — operator's guide for extended-dynamic + cache-hit dispatch.
