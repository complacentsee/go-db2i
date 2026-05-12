# Changelog

All notable changes to **go-db2i** are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).

The driver is **pre-1.0** while wire compatibility is being built up
across IBM i versions; expect the public API surface to settle at
0.5+ once LOB bind, slog observability, and OTel spans all land.

## [Unreleased]

## [0.7.14] - 2026-05-12

### Fixed: large user-table streaming SELECT delivers all rows (bug #2)

The v0.7.13 known-issue (`TestUserTableLargeScan_KnownIssue`, gated
on `DB2I_TEST_BUG_LARGE_SCAN`) is closed. A streaming
`SELECT * FROM <user-table>` over a freshly-inserted 10000-row
table on V7R6M0 used to deliver only 8625 rows -- the server
correctly shipped the closing batch with its remaining 1375 rows
AND the EC=2 RC=700 "fetch/close" signal in the same reply, but
`Cursor.Next` dropped the entire batch on the exhausted path. The
v0.7.13 bug-#1 fix had already taught `fetchMoreRows` to parse
rows before honouring `exhausted`; v0.7.14 propagates the same
invariant up one layer to `Cursor.Next` -- when a continuation
FETCH carries rows AND an end-of-data signal, the cursor buffers
them in `pending` like any other batch, and the `exhausted` flag
only prevents *future* FETCH issuance, not delivery of the current
batch. Live-verified: 10000 of 10000 rows on V7R6M0.

### Changed: continuation FETCH wire shape matches JT400

`hostserver.fetchMoreRows` now emits the JT400-canonical
continuation-FETCH parameter set: `FetchScrollOption` (CP 0x380E,
FETCH_NEXT) + `BlockingFactor` (CP 0x380C, big-endian uint32),
with `ORSBitmap = 0x84040000` (no SQLCA). Replaces the earlier
`BufferSize` + `VariableFieldCompr` + `ScrollableCursorFlag` shape
that V7R6M0 honoured for catalog scans but ambiguously coupled to
the premature "fetch/close" path on large user-table scans. New
offline regression test `TestContinuationFetchWireShapeMatchesJT400`
pins both halves of the contract:

- JT400's wire bytes from `select_large_user_table_10k.trace` (every
  continuation FETCH) carry exactly `FetchScrollOption` +
  `BlockingFactor` with ORS `0x84040000`.
- Our `fetchMoreRows` encoder emits the same CP set and ORS bits.

BlockingFactor is computed from the column descriptors
(`bufferBytes / sumOfColumnLengths`, clamped to `[64, 32767]`)
rather than asserted byte-for-byte against JT400's per-query
value -- BlockingFactor is a server-side hint, not a hard cap, so
heuristic divergence is safe and the row-delivery behaviour
asserted by `TestUserTableLargeScanReturnsAllRows` is unchanged.

`?block-size=N` continues to set the buffer-byte input to the
heuristic; existing block-size variants in `TestBlockSize` and
`TestFetchFirstNExact` still pass.

## [0.7.13] - 2026-05-12

### Fixed: fetchMoreRows dropped rows on cursor-exhausts-mid-batch (bug #1)

Continuation FETCH (`hostserver.fetchMoreRows`) used to early-return
on `outcome.exhausted` without parsing the row payload, dropping any
rows the server delivered in the same reply. The server can — and
does — emit rows AND an end-of-data signal (EC=2 RC=700/701, SQL
+100) together when:

- `FETCH FIRST N ROWS ONLY` queries hit the row-cap mid-batch
- Any cursor exhausts naturally inside a batch with rows pending
- The single-batch "fetch/close" path (JT400's `@pda perf2`)

The fix mirrors `fetchCallRows`'s existing row-then-outcome
ordering (CALL cursors had the same fix applied earlier for their
proc results). New regression test `TestFetchFirstNExact` pins the
contract: `SELECT … FETCH FIRST 100 ROWS ONLY` returns exactly 100
rows across all valid block-sizes (8/16/32/64/128/512).

### Changed: `?block-size=N` range tightened to 8..512

DSN `?block-size=N` now requires `N` in `8..512` (was `1..512` in
v0.7.12). Matches JT400's canonical `BLOCK_SIZE` values
(8/16/32/64/128/256/512). Values below 8 KiB trigger server-side
row-truncation under `FETCH FIRST N` on V7R6M0 even with the bug
#1 fix in place; rejecting them at the boundary is safer than
silently returning short results.

### Fixed: data race in deadlineRecorder test stub (bug #3)

`go test -race ./driver/` reported a data race in
`TestWithContextDeadlineCancelUnblocks` -- the test's stub
`deadlineRecorder` recorded `SetDeadline` calls without locking,
so the main test goroutine could read `lastDeadline` while the
`context.AfterFunc`-spawned cancellation goroutine wrote it. The
race was confined to test code (production `withContextDeadline`
in `driver/context.go` is correct), but the suite now passes
under `-race`. Wrapping `lastDeadline` in a `sync.Mutex` + an
accessor method.

### Known issue: streaming SELECT truncates large fresh user tables

Captured as `TestUserTableLargeScan_KnownIssue` (env-gated; set
`DB2I_TEST_BUG_LARGE_SCAN=1` to run). On V7R6M0 a streaming
`SELECT * FROM <user-table>` over a freshly-inserted 10k-row table
returns only ~8625 rows -- the server emits EC=2 RC=700
("fetch/close, all delivered") prematurely. `SELECT COUNT(*)
WHERE …` over the same table correctly reports 10000, and
`TestRowsLazyMemoryBounded`'s 49688-row `QSYS2.SYSCOLUMNS` scan
runs cleanly on the same connection -- so the issue is specific
to user-table scans, not the streaming-cursor path generically.
Deferred for protocol-level investigation; the bug #1 fix
recovers the closing-batch rows that were previously also being
dropped (raising the per-scan ceiling from ~6900 to ~8625).

## [0.7.12] - 2026-05-12

### Added: Savepoint driver-typed methods (M12-1)

Three new methods on `*db2i.Conn` reachable via `sql.Conn.Raw`:
`Savepoint(ctx, name)`, `ReleaseSavepoint(ctx, name)`, and
`RollbackToSavepoint(ctx, name)`. Each issues plain SQL through
the existing Exec path -- matches JT400's
`AS400JDBCConnection.setSavepoint` byte-for-byte (no special wire
CP for savepoints in either driver).

Name validation enforces the IBM i SQL identifier rules (1-128
chars, leading letter, body letters / digits / underscore) before
any wire activity, so injection attempts like `SP'; DROP TABLE T;`
reject up front.

### Added: Runtime schema + library-list mutation (M12-2)

Three new methods on `*db2i.Conn`:

- `SetSchema(ctx, name)` issues `SET SCHEMA <name>` via the
  existing Exec path. Matches JT400's `Connection.setSchema` wire
  output. Closes the longstanding "I want to switch schemas after
  connect" gap for multi-tenant / environment-mirroring shops.
- `AddLibraries(ctx, libs)` reuses the existing
  `hostserver.NDBAddLibraryListMulti` helper (same wire shape as
  the connect-time `libraries=` knob); first entry tagged `'C'`,
  the rest `'L'`.
- `RemoveLibraries(ctx, libs)` loops `CALL QSYS2.QCMDEXC('RMVLIBLE
  LIB(X)')` once per library. JT400 doesn't expose a NDB REMOVE
  wire either -- mid-session list shrinking is a CL operation on
  both sides. CPF2104 ("library not in list") and CPF9810
  ("library not found") are downgraded to slog WARN; other errors
  abort the loop with the partial-removal already applied.

Library names canonicalise to uppercase via the existing
`canonPackageIdent` helper and validate against the 1-10 char
`[A-Z0-9_#@$]` IBM-i object-name rules. Schema names use the same
charset (a library name is the maximally-restrictive form).

### Added: `?block-size=N` DSN knob (M12-3)

`Config.BlockSizeKiB int` (default 0 = the historical 32 KiB
shape) tunes the CP `0x3834` BufferSize parameter the driver emits
on every PREPARE_DESCRIBE OPEN / continuation FETCH. Valid range
1-512 KiB; out-of-range DSN values reject at parse time so users
don't accidentally request multi-MiB buffers through a typo.
Mirrors JT400's `block size` JDBC URL knob.

The default (unset / 0) byte-equals pre-M12 wire shape exactly --
existing `TestSentBytesMatch*` fixtures still match without
modification. New `hostserver.bufferSizeParam(kib)` helper builds
the parameter; `hostserver.WithBlockSize(kib)` SelectOption
threads through OpenSelectStatic / OpenSelectPrepared /
OpenSelectPreparedCached.

### Added: `db2iiter` sub-package (M12-4)

New module path `github.com/complacentsee/go-db2i/db2iiter`. One
exported function:

```go
func ScanAll[T any](rows *sql.Rows,
    scan func(*sql.Rows) (T, error)) iter.Seq2[T, error]
```

Lets callers replace the classic `for rows.Next() { ... }`
boilerplate with `for v, err := range db2iiter.ScanAll(rows,
scanFn)`. No dependency on the `driver` package -- works with any
`*sql.Rows` regardless of the underlying SQL driver. Caller still
owns `rows.Close()`.

## [0.7.11] - 2026-05-12

### Added: `?libraries=A,B,C` multi-library DSN (M11-2)

`hostserver.NDBAddLibraryListMulti` emits the CP `0x3813` multi-
entry list-of-libraries parameter, with EBCDIC indicator `'C'`
(0xC3) for the first library and `'L'` (0xD3) for the rest --
matching JT400's
[`JDLibraryList`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDLibraryList.java)
behaviour for a comma-separated list without an explicit `*LIBL`
token. Driver-side `Config.Libraries []string` plus
`?libraries=A,B,C` (comma or space delimited) plumb through to
the new helper. `?library=X&libraries=A,B,C` composes the two
knobs via JT400's prepend-default-schema rule.

The single-library wire shape is byte-identical to v0.7.10 --
`NDBAddLibraryList(conn, name, id)` is now a thin wrapper around
`NDBAddLibraryListMulti(conn, []string{name}, id)`, and the
existing `TestSentBytesMatchNDBAddLibraryListFixture` against
`select_dummy.trace` still passes.

Live evidence (`TestMultiLibrary`, V7R6M0, 2026-05-12): unqualified
`CALL P_INS(?, ?)` resolves to `GOSPROCS.P_INS` under
`?libraries=GOTEST,GOSPROCS`; the same call against a connection
without the knob fails (baseline subtest), proving the wire path
is the load-bearing piece.

### Added: `?naming=system` DSN value (M11-3)

`Config.Naming` (default `sql`, accepts `sql` | `system`) routes
to CP `0x380C` (NamingConventionParserOption) in
SET_SQL_ATTRIBUTES -- the single byte JT400 sends via
[`setNamingConventionParserOption`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/DBSQLAttributesDS.java).
The historical go-db2i default was hardcoded `sql`; the new
`system` value lets RPG/CL shops migrating from JT400 keep their
slash-qualified SQL (`MYLIB/TABLE`) working without rewriting
every statement. The flag also feeds the package-suffix
derivation (idx3 in
[`JDPackageManager.buildSuffix`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDPackageManager.java))
so cross-driver byte-equality on the wire `*PGM` name is preserved.

Live evidence (`TestSystemNaming`, V7R6M0): `SELECT ID, V FROM
GOTEST/GOSQL_M11_NAMING WHERE ID = 1` resolves under
`?naming=system`. Offline
`TestSetSQLAttributesRequestRoutesNamingToCP380C` pins both wire
bytes (0=sql, 1=system).

### Added: `?time-format` + 3 separator DSN passthroughs (M11-4)

Four new DSN keys map directly to SET_SQL_ATTRIBUTES CPs the
driver previously hardcoded as job-default. Each is independent
and composable:

| DSN key | CP | Values |
|---|---|---|
| `time-format`    | `0x3809` | `job` (default), `hms`, `usa`, `iso`, `eur`, `jis` |
| `date-separator` | `0x3808` | `job`, `/`, `-`, `.`, `,`, `space` (or `slash`/`dash`/`period`/`comma`/`space`) |
| `time-separator` | `0x380A` | `job`, `:`, `.`, `,`, `space` (or `colon`/`period`/`comma`/`space`) |
| `decimal-separator` | `0x380B` | `job`, `.`, `,` (or `period`/`comma`) |

`DBAttributesOptions` exposes these as int8 fields with `-1` =
omit the CP. An explicit `DateSeparator` overrides the
date-format-inferred CP `0x3808` value; setting nothing keeps
the v0.7.10 wire shape byte-equal (asserted by every existing
`TestSentBytesMatch*` fixture test).

Live evidence (`TestTimeFormatUSA`, V7R6M0): a TIME literal
renders as `13:45:00` under the default and `01:45 PM` under
`?time-format=usa` -- unambiguous proof the CP is honoured
end-to-end. *Note:* the driver's TIME-column `time.Time`
auto-promotion only understands ISO formats; callers using
non-ISO `time-format` should `Scan` into a string (or cast the
column to `VARCHAR` in the SQL). Widening the promotion is a
separate, larger work item.

### Closed: bug #15 (already shipped in v0.5.x)

Investigation that opened in the M7 residual plan ("CCSID-273
byte-mode codec asymmetry") was resolved in commit
[`d84cb3e`](https://github.com/complacentsee/go-db2i/commit/d84cb3e)
on 2026-05-10 -- the failure was a test-setup bug (CLOB column
inherited job CCSID 37 on the English-locale LPAR while the test
payload was pre-encoded as CCSID 273), not a codec asymmetry.
M11-1 verified no work remains: offline
`TestCCSID273ByteRoundTripIsBijective` confirms all 256 wire
bytes round-trip cleanly through Decode-then-Encode.

## [0.7.10] - 2026-05-12

### Added: MERGE batching in `Conn.BatchExec`

`MERGE INTO ... USING (VALUES (?, ?)) ...` batches now go through
the v0.7.9 CP `0x381F` multi-row block-insert wire shape. JT400
enables MERGE batching on V7R1+ via the same `canBeBatched_` flag
IUD uses
([`JDSQLStatement.java:644-648`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDSQLStatement.java));
the wire shape is identical to INSERT / UPDATE / DELETE, so
v0.7.10 is purely the verb-gate removal in `Conn.BatchExec`
(`driver/conn_batch.go`) — no encoder changes, no dispatch
changes. `statementTypeForSQL` already returned
`TYPE_UNDETERMINED=0` for MERGE and the server resolves the verb
from the SQL text.

Returns the server's combined affected count (matched-updates +
not-matched-inserts); MERGE's per-clause counts aren't broken
out on the wire.

Verified via `TestBatch_MergeVerified` (live conformance,
V7R6M0 + V7R5M0): seed M target rows, batch N source tuples
where the first M hit `WHEN MATCHED THEN UPDATE` and the
remaining N-M hit `WHEN NOT MATCHED THEN INSERT`. Asserts
rows-affected = N + a deterministic spot-check on both branches.

No JT400 byte-equivalence fixture captured for MERGE — the wire
shape is provably the same as IUD per the source review.

## [0.7.9] - 2026-05-12

### Added: `Conn.BatchExec` for bulk IUD via block insert

A driver-typed method on `*db2i.Conn` (reach it via `sql.Conn.Raw`)
that packs N rows of parameter values into a single CP `0x381F`
multi-row block on one EXECUTE request — the IBM i "block insert"
wire shape JT400 uses for non-LOB INSERT / UPDATE / DELETE batches.
One round-trip per 32k-row chunk vs one per row for a `db.Exec`
loop.

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

Measured **~358× speed-up** for a 1000-row INSERT against IBM
Cloud V7R6M0 via VPC tunnel (`TestBatch_PerfDelta`: 3.9 s
`db.Exec` loop vs 11 ms `BatchExec`; the high-RTT path
amplifies the round-trip savings). LPAR-local networks see
smaller multipliers (10–50×) since PREPARE_DESCRIBE plan-compile
cost dominates over RTT there. A 50k-row batch finishes in
~160 ms on the same tunnel (two 32k+18k chunks). Auto-splits at
`MaxBlockedInputRows = 32000` —
mirrors JT400's `maximumBlockedInputRows`
([`AS400JDBCPreparedStatementImpl.java:1636-1677`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/AS400JDBCPreparedStatementImpl.java)).

Limitations (v0.7.9):
- INSERT / UPDATE / DELETE only. MERGE deferred to v0.7.10 with a
  JT400 fixture capture (MERGE has trickier parameter-marker
  semantics that warrant byte-equivalence evidence).
- No LOB parameters. JT400 falls back to per-row EXECUTE for LOB
  batches (`JDSQLStatement.canBatch` returns false on
  `containsLocator_ == LOCATOR_FOUND`); v0.7.9 rejects with a
  pointer at the per-row path.
- No `sql.Out` destinations. IUD has no OUT params on the wire.
- Every row in a single call must have the same Go types per
  column position (validated up-front before any wire activity).

Implementation:
- `hostserver/db_prepared.go` — `EncodeDBExtendedData` refactored
  into a thin wrapper over the new
  `EncodeDBExtendedDataBatch(params, rows [][]any)` which
  parametrises the `rowCount` field in the CP `0x381F` header
  (was hard-coded to 1 since v0.3 / M3). Per-row encoding is the
  same byte-format as the legacy single-row path.
- `hostserver/db_exec.go` — `ExecuteBatch` adds the multi-row
  EXECUTE dispatch sequence (CREATE_RPB + PREPARE_DESCRIBE +
  CHANGE_DESCRIPTOR + EXECUTE with the multi-row payload). Skips
  the LOB-rewrite and OUT-fixup paths since neither is relevant
  to IUD batches.
- `driver/conn_batch.go` — `Conn.BatchExec` validates the verb,
  row arity, value types, and splits at 32k rows.

Unit tests (no live LPAR): byte-level golden vectors for the
multi-row encoder; verb / arity / LOB / `sql.Out` rejection
truth tables for `Conn.BatchExec`. Live conformance:
`TestBatch_InsertVerified` / `TestBatch_UpdateVerified` /
`TestBatch_DeleteVerified` / `TestBatch_AutoSplits32k` /
`TestBatch_PerfDelta` (the last logs the per-row vs batch
timing for perf-doc citation).

## [0.7.8] - 2026-05-12

### Added: OUT/INOUT CALL cache-hit dispatch (v0.7.8)

`package-criteria=extended` plus an OUT-parameter stored procedure
now dispatches through the cache-hit fast path the same way IN-only
CALLs do. The cached PMF preserves OUT (`0xF1`) and INOUT (`0xF2`)
direction bytes through `preparedParamsFromCached`;
`ExecutePreparedCached` sets `ORSResultData` when the shape carries
any non-IN slot and decodes the OUT-value row from CP `0x380E` via
the same `parseOutParameterRow` the regular `ExecutePreparedSQL`
path uses; `Stmt.Exec`'s cache-hit branch calls `writeBackOutParams`
into the bound `sql.Out` destinations.

Empirically validated against IBM Cloud V7R6M0 with
`GOSPROCS.P_LOOKUP(IN VARCHAR, OUT VARCHAR, OUT INTEGER)`:
post-threshold cache-hit dispatch returns the same OUT values the
regular path returns, with the wire savings of skipped
`PREPARE_DESCRIBE`. No JT400 reference exists (JT400 doesn't file
CALL under any of its criteria) — this is a go-db2i-original
extension built on top of v0.7.7's `criteria=extended`.

### Removed: v0.7.7 `hasOutDest` refresh-skip gate

The v0.7.7 defensive gate at `driver/stmt.go` that suppressed the
v0.7.4 auto-populate `RETURN_PACKAGE` refresh for OUT/INOUT
dispatches is gone. Its premise (the cache-hit path refused
non-IN direction bytes, so refresh would chase an unreachable
entry) was empirically wrong — the server honours OUT bytes on
cache-hit. OUT CALLs now auto-populate and cache-hit-dispatch
like every other eligible statement.

### Changed: `preparedParamsFromCached` accepts OUT direction bytes

The defensive reject of non-IN direction bytes (in place since
v0.7.1) is gone. The function now accepts `0x00 / 0xF0 / 0xF1 /
0xF2` and preserves the byte through to the wire (normalising
only the `0xF0 → 0x00` IN equivalence). Unknown direction bytes
still abort with a clear error. The probe in
`docs/plans/v0.7.8-out-param-cache-hit.md` Part A documents the
V7R6M0 evidence.

### Tests

- New `TestCacheHit_CriteriaExtended_OutCallDispatches` replaces
  v0.7.7's `TestCacheHit_CriteriaExtended_OutCallSkipsRefresh`
  (whose premise — refresh-skip — is gone in v0.7.8).
- `TestExecutePreparedCached_RejectsOutParameter` renamed to
  `TestExecutePreparedCached_RejectsUnknownDirection` and now
  asserts on `0xAB` rather than `0xF1`.
- `TestPreparedParamsFromCached` extended with OUT (`0xF1`) and
  INOUT (`0xF2`) preservation cases plus an unknown-byte abort.

## [0.7.7.1] - 2026-05-12

### Docs: correct `package-criteria=extended` attribution

v0.7.7 shipped with CHANGELOG / docs / source-comment claims that
`extended` is "JT400's third criterion." That's factually wrong:
JT400's [`JDProperties.java`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDProperties.java)
enumerates exactly two criteria (`default`, `select`), and
[`JDSQLStatement.java:950-959`](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDSQLStatement.java)'s
`isPackaged_` gate has matching arms only. `extended` is a
**go-db2i-original** opt-in. The feature itself works as designed
(CALL / VALUES / WITH filing) and remains in v0.7.7; this is a
labels-only correction across CHANGELOG, `docs/package-caching.md`,
`docs/migrating-from-jt400.md`, `driver/conn.go`'s package comment,
and the v0.7.7 test names. README's "Current" stamp bumped to
v0.7.7.

No code-behaviour change. Verified locally with the unit + cross-
LPAR conformance suites unchanged.

## [0.7.7] - 2026-05-12

### Added: `package-criteria=extended` for CALL / VALUES / WITH

A **go-db2i-original** filing criterion — JT400 has no equivalent
value. `?package-criteria=extended` files `CALL`, `VALUES`, and
`WITH` statements into the `*PGM` on top of the `default` matrix.

Interop note: a Go client passing `extended` does not share a
`*PGM` with a JT400 client — JT400 rejects the unknown criterion
value at DSN parse. The byte-equality guarantee for
`default` / `select` is unaffected.

### Fixed: DECLARE PROCEDURE files into the `*PGM`

`Stmt.Exec` previously routed no-args statements through
`ExecuteImmediate` whenever `isCall` was false. DECLARE PROCEDURE
has no driver-side args, so it took that single-frame path — which
omits the CP `0x3808` package marker. Result: the server never
filed the statement and the `*PGM` hash diverged from JT400's for
any app that uses DECLARE PROCEDURE. The fix extends the
`useImmediate` gate at `driver/stmt.go` to also exclude any SQL
that `packageEligibleFor` would file (DECLARE PROCEDURE /
DECLARE CURSOR / INSERT-subselect / SELECT-FOR-UPDATE under
`default`; plus the additions widened by `select` / `extended`).
Vanilla connections without packaging behave unchanged.

### Improved: skip auto-populate refresh on OUT/INOUT dispatches

`Stmt.Exec`'s post-EXECUTE `RETURN_PACKAGE` refresh
(v0.7.4 auto-populate) now short-circuits when `hasOutDest`
reports an `sql.Out` destination in the bind set. The cache-hit
fast path's `preparedParamsFromCached` rejects non-IN direction
bytes, so a refreshed cache entry for an OUT/INOUT CALL is
permanently unreachable — burning round-trips to refresh it is
pure waste. Intentional improvement over JT400, which always
refreshes regardless of direction. Particularly relevant under the
new `package-criteria=extended`, where OUT CALLs file but cannot
cache-hit-dispatch.

### Docs: v0.7.4 auto-populate cited in `performance.md`

The 35 ms vs 71 ms (~50%) post-threshold latency reduction shipped
in v0.7.4 was measured on IBM Cloud V7R6M0 but only documented in
the v0.7.4 CHANGELOG entry. `docs/performance.md` now carries a
"Same-session auto-populate (v0.7.4)" subsection covering the
mechanism, the IBM SI30855 threshold, the
`hostserver.MaxFilingRefreshAttempts` cap, and the v0.7.7
`hasOutDest` refinement.

### Docs: v0.7.8 plan filed (OUT-param cache-hit investigation)

`docs/plans/v0.7.8-out-param-cache-hit.md` opens the question of
whether the cache-hit fast path can be extended to dispatch
OUT/INOUT stored procedures, which would obsolete v0.7.7's
`hasOutDest` refresh-skip. Status: speculative — investigation
not yet started.

## [0.7.6] - deferred (2026-05-12)

LOB-bind cache-hit fast path. Deferred after the live test against
V7R6M0 returned `SQL-814` on the first `WRITE_LOB_DATA` frame
against a client-synthesised LOB locator handle — the server
requires `PREPARE_DESCRIBE`-allocated locators and there's no
separate allocate-locator wire request. JT400 doesn't take the
path either (every iter of `prepared_package_filing_lob_cache_hit`
has `PREPARE_DESCRIBE`). Part A fixture shipped (commit `ede0dc9`);
Part B implementation rolled back. v0.7.5's graceful encoder-gap
fallback remains in production. See
`docs/plans/v0.7.6-lob-bind-cache-hit.md` and
`memory/project_db2i_v076_lob_cache_hit_blocker.md` for the
finding.

## [0.7.5] - 2026-05-11

### Fixed: DDL cache invalidation (SQL-204 / SQL-805 fallback)

`Stmt.Exec` / `Stmt.Query` cache-hit dispatch now catches SQL-204
(object not found) and SQL-805 (package not usable) from the
EXECUTE / OPEN reply, purges the stale cache entry, and falls
through to the plain `PREPARE_DESCRIBE` path. The previous
behaviour propagated the error to the caller, leaving the
package's stale reference in the conn's `pkg.Cached` map until
the connection cycled.

Empirical finding on V7R6M0: same-shape `DROP` + `CREATE TABLE`
does NOT trigger SQL-204 -- the server transparently rebinds the
filed plan. The fallback fires only for object-genuinely-gone or
package-unusable conditions (the typical real-world scenarios
where this matters).

Tight scope: only -204 / -805 trigger the fallback. Constraint
violations, lock timeouts, permission errors, etc. propagate as
before; silently re-PREPAREing them would mask diagnostics.

Verified end-to-end via the new `TestCacheHit_DDLInvalidation`
(unconditional in the `DB2I_TEST_FILING=1` matrix) against
IBM Cloud V7R6M0: DROP table, observe SQL-204 on cache-hit, the
fallback purges, fresh PREPARE returns the new row. Unit tests
cover the `shouldRefallbackToPrepare` predicate and the symmetric
`purgeCachedStatement` delete.

### Validated: PUB400 V7R5M0 cross-LPAR

Full Filing + CacheHit conformance suite runs green on PUB400
V7R5M0 (the v0.7.4 plan promised this cross-LPAR validation but
the v0.7.4 session shipped with V7R6M0-only verification).

- DSN: `db2i://<USER>:<PWD>@pub400.com:8471/?signon-port=8476&library=<USER_LIB>`
  (PUB400 free-tier account; per-user library)
- 23 `TestFiling_*` + `TestCacheHit_*` tests pass in 294 s
  (vs ~3 s baseline on IBM Cloud V7R6M0 LAN; ~100× reflects the
  public-internet RTT).
- `TestFiling_WireEquivalenceWithJT400` captures 3 packaged
  PREPARE_DESCRIBE + 3 regular EXECUTE + 1 cache-hit EXECUTE on
  V7R5M0 -- byte-identical CP set to the V7R6M0 capture.
- Test bug fix: `TestFiling_WireEquivalenceWithJT400` previously
  hardcoded `GOTEST.GOJTWEQ` instead of using `schema()`; now
  reads the dynamic schema, so it runs portably against any
  configured target.
- New PUB400-specific environmental skips (not code changes):
  - `TestCacheHit_OutParameterFallthrough`: skips with a clear
    message when `CREATE SCHEMA` returns SQL-552 / 42502 (no
    authority on shared free-tier LPARs).
  - `TestTxQuery`: skips with a clear message when the test
    schema isn't journaled (SQL-7008 / 55019 from `tx.Exec`).
    AFTRAEGE1B is unjournaled by default on PUB400.

PUB400 environmental quirk worth noting: leftover `*PGM` state
from prior cache tests can accumulate and exceed the package's
usable threshold on free-tier LPARs, causing `fillPackageCache`
to silently miss the filing threshold on subsequent runs. Clean
wipe (`DLTOBJ OBJ(<lib>/GOTCHE*) OBJTYPE(*SQLPKG)`) between full
conformance runs avoids the flake. Documented for users running
their own conformance suites.

### Fixed: cache-hit fallback on unsupported parameter types (LOB-bind)

Empirical investigation of LOB-bind filing under extended-dynamic
(`TestLOBBind_FilingProbe` against V7R6M0) overturned the v0.7.4
CHANGELOG assertion that the server refuses to file LOB-bind
statements: the server DOES file them, and v0.7.4's auto-populate
correctly learned the renamed name on the 4th iteration. The
real gap was downstream: the cache-hit encoder
(`EncodeDBExtendedData` in `hostserver/db_prepared.go`) only has
branches for the live-PREPARE locator SQL types (960/961/964/965/
968/969), while the *PGM-stored `ParameterMarkerFormat` carries
the raw-LOB SQL types (404/405/408/409). The cache-hit dispatch
hit an "SQL type N not yet supported" error on iter 4+ of every
LOB-bind INSERT after the threshold crossed.

- New sentinel `hostserver.ErrUnsupportedCachedParamType` (wrapped
  via `%w` in `EncodeDBExtendedData`'s default branch). Drivers
  use `errors.Is` to detect the encoder gap.
- `shouldRefallbackToPrepare` extended to treat the sentinel as a
  fallback trigger, alongside the v0.7.5 SQL-204 / SQL-805 codes.
  Cache-hit dispatch purges the entry and re-routes through plain
  `PREPARE_DESCRIBE` — the regular path computes shapes from the
  live reply, gets the locator types the encoder DOES support,
  and ships the bytes via `WRITE_LOB_DATA` normally.
- `ExecutePreparedCached` cleans up the partially-built RPB on
  encoder error so the fallback `PREPARE_DESCRIBE` can
  `CREATE_RPB` cleanly (the symmetric path already exists for
  the wire-error cases). Without this, the fallback re-PREPARE
  failed with SQL-101 from the dirty RPB slot.

Verified: `TestLOBBind_FilingProbe` on V7R6M0 now completes all 4
iterations successfully (3 regular EXECUTEs + 1 cache-hit-fallback-
to-regular-EXECUTE on iter 4); full Filing + CacheHit suite green.

The fictional limitation in v0.7.4 ("LOB-bind filing continues to
fall through ... per JT400's `JDPackageManager` filter") is
removed from the Known limitations list. The actual behaviour is
now: LOB-bind statements DO file server-side, the cache-hit fast
path doesn't yet support binding their cached shape, and the
fallback to regular `PREPARE_DESCRIBE` keeps everything working
end-to-end. Cache-hit dispatch for LOB binds remains a v0.7.6
candidate (would require extending the encoder for the raw-LOB
SQL types).

## [0.7.4] - 2026-05-11

Fifth tagged release. Wraps the post-v0.7.2 work that accumulated
without a separate tag: V7R6M0 + V7R5M0 extended-dynamic filing
investigation, JT400 wire-equivalent IUD filing, the cache-hit
SQLDA Precision/Scale fix, per-conn auto-populate after first-time
filing, and the wire-equivalence harness pinning JT400's reference
shape. End-to-end live-validated against IBM Cloud V7R6M0; the
JT400 fixture trace is captured for byte-for-byte comparison.

### Fixed: INSERT / UPDATE / DELETE filing + cache-hit dispatch end-to-end

The two known driver-side gaps documented under v0.7.3 are now
closed. End-to-end verified against IBM Cloud V7R6M0:

- **`hostserver.ExecutePreparedSQL` now files DML statements.** The
  earlier empty-marker workaround at `db_exec.go:187-200` is
  replaced with the JT400 wire shape: `0x3808=01` (prepare-option
  filing flag) plus full CP `0x3804` (package name) on
  `PREPARE_DESCRIBE`, CP `0x3801` (package library) added on
  `CREATE_RPB` to satisfy the server's SQL-112 prerequisite, and
  CP `0x3804` re-emitted on the follow-up `EXECUTE`. The
  `nameOverride_` rebind the earlier comment block predicted
  turned out unnecessary -- JT400 doesn't use one either; the
  server resolves the server-renamed filed plan via the package
  marker plus the RPB handle. Verified by capturing
  `prepared_package_filing_iud.trace:273+379` and re-running the
  shape from Go: `GOIUDT9899` files three statements (INSERT,
  UPDATE, DELETE) on V7R6M0 with `NUMBER_STATEMENTS=3`.

- **`preparedParamsFromCached` no longer leaks SQLDA length bytes
  into Precision/Scale for non-decimal types.** The cached SQLDA
  encodes Precision in the high byte and Scale in the low byte of
  the per-field "length" field, which is meaningful for
  `DECIMAL` / `NUMERIC` / `DECFLOAT` but garbage for everything
  else (an INTEGER with `FieldLength=4` decoded as `Precision=0`
  `Scale=4`, enough to make the server silently zero out the
  bound value on cache-hit `EXECUTE`). The fix mirrors JT400's
  cache-hit wire bytes: non-decimal types get
  `Precision=Scale=0`, decimal types retain the SQLDA's split.
  ParamType is forced to `0x00` to match JT400's cache-hit
  byte sequence (was `0xF0`, which the server tolerated but
  diverged from JT400's reference shape).

- **`driver/stmt.go` cache-hit dispatch reaches IUD.** Replaced
  `len(outDests) == 0` (which is always false for any
  parameterised call -- the slice is preallocated to
  `len(args)`) with a new `hasOutDest()` helper that walks the
  slice for actual non-nil `*sql.Out` entries. Without this fix
  every parameterised `db.Exec` skipped the cache-hit fast path
  regardless of filing state.

- **`ExecuteImmediate` / `ExecutePreparedSQL` / `ExecutePreparedCached`
  now decode rows-affected** from the SQLCA at byte offset 104
  (SQLERRD[3] under SQL-standard 1-indexing, the same slot
  JT400's `getRowsAffected` reads). The earlier `TODO(M7)`
  placeholders returned 0 unconditionally.

### Validated

Under `DB2I_TEST_FILING=1` against IBM Cloud V7R6M0:

- **17 SQL types round-trip end-to-end** through filing +
  cache-hit dispatch -- `TestCacheHit_SelectTypeMatrix` passes
  for INTEGER, BIGINT, SMALLINT, DECIMAL(15,4), NUMERIC(10,2),
  REAL, DOUBLE, DECFLOAT(16), VARCHAR(64), CHAR(20),
  `VARCHAR(40) FOR BIT DATA`, DATE, TIME, TIMESTAMP, BOOLEAN,
  BINARY(16), VARBINARY(64). 35.7s total.
- **All four IUD-filing tests pass**: `TestFiling_InsertVerified`,
  `TestFiling_UpdateVerified`, `TestFiling_DeleteVerified`,
  `TestFiling_AllThreeInOnePackage`. `GOTCHE9899` records 3
  filed statements (one per verb).
- **`TestFiling_ParamBindingRoundTrip`** runs unconditionally
  (not requireFiling-gated) so the SQLDA-leak regression has a
  smoke test that fires on every CI pass.

### Added

- **`testdata/jtopen-fixtures/.../PackageFilingIUD`** JT400
  fixture: wipes the package, runs parameterised INSERT/UPDATE/DELETE
  4 times each on a pinned conn, queries `SYSPACKAGESTAT` +
  `SYSPACKAGESTMTSTAT`. Trace captures JT400's EXECUTE wire shape
  for filed IUDs (CP 0x3804 + CP 0x380D + CP 0x3830 + CP 0x3812
  + CP 0x381F, in that order). Run + golden file checked in
  alongside the existing `prepared_package_filing_verify` case.
- **`hostserver.DBReply.RowsAffected()`** method -- reusable
  SQLCA decoder for the SQLERRD[3] / rows-affected slot.
- **Auto-populate `Conn.pkg.Cached` after first-time filing.**
  `Conn.pkg.Cached` is now a `map[string]*PackageStatement` (was
  a linear slice). On each filing-eligible PREPARE the conn ticks
  a per-SQL `LocalPrepareCount`; once it hits an entry on the
  retry schedule (3 / 6 / 12 -- the first matches IBM's documented
  SI30855 threshold), the dispatch site issues a follow-up
  `RETURN_PACKAGE` to learn the server-renamed name, and the
  next call of the same SQL dispatches via the cache-hit fast
  path. Capped at `hostserver.MaxFilingRefreshAttempts = 3` so a
  SQL the server refuses to file (package full, locked, etc.)
  doesn't burn unbounded refreshes. Measured ~50% reduction in
  per-call latency (35 ms vs 71 ms) after the threshold crosses.
- **`hostserver.DecodeDBRequest` / `DecodeDBRequestFrame`** --
  inverse of `BuildDBRequest`, preserves param order. Plus
  `SwapWireHook` for tests that need to restore a prior hook
  on defer. Together they back the new wire-equivalence harness.
- **`hostserver.TestWireEquivalence_PackageFilingIUDFixture`** --
  unit test that decodes the JT400 trace and asserts JT400's
  CREATE_PACKAGE, PREPARE_DESCRIBE, and EXECUTE wire shapes for
  filing IUD. Pins the JT400 reference so a regenerated fixture
  (different SQL, different release) fails the test rather than
  silently shifting our reference.
- **`TestFiling_WireEquivalenceWithJT400`** -- live conformance
  test that hooks `SetWireHook`, runs a 4-iter parameterised
  INSERT through filing + cache-hit dispatch, and asserts the
  driver's PREPARE_DESCRIBE + EXECUTE CP set matches JT400's
  for the regular path. Documents the intentional cache-hit
  divergence (CP 0x3806 statement-name override in place of
  CP 0x3804 RPB+package-marker resolution) as a logged
  observation, not a failure. Captures 3 PREPARE + 3 regular
  EXECUTE + 1 cache-hit EXECUTE on V7R6M0 as expected.

### Known driver-side limitations (still tracked)

- **LOB-bind cache-hit fast path** is not yet implemented: the
  *PGM-stored parameter shape uses raw-LOB SQL types (404/405/
  etc) while the cache-hit encoder only handles the live-PREPARE
  locator types (960/961/etc). v0.7.5 added a graceful fallback
  via `ErrUnsupportedCachedParamType` -- the regular path keeps
  working end-to-end; the cache-hit round-trip saving simply
  doesn't apply to LOB binds yet. (Prior CHANGELOG text claimed
  LOB-bind statements weren't filed at all per a JT400 source
  inference; empirical testing showed the server DOES file them,
  and the limitation is in our cache-hit encoder, not in JT400's
  upstream filter or the server.)

### Investigation: extended-dynamic filing on V7R6M0 IBM Cloud + V7R5M0 PUB400

v0.7.2 documented a "filing-suppression" limitation on the IBM
Cloud V7R6M0 LPAR: `NUMBER_STATEMENTS` in
`QSYS2.SYSPACKAGESTAT` stayed at 0 after running parameterised
SQL through extended-dynamic. The diagnosis was wrong on two
counts, both surfaced 2026-05-11 by IBM's own docs and direct
ground-truth experiments:

1. **Verification SQL used the wrong package name.** Previous
   queries searched `PACKAGE_NAME = 'GOJTPKG'` (the JDBC `package=`
   property value). JT400 — and `hostserver.BuildPackageName()`
   — append a 4-char session-options hash so the on-wire name is
   `'GOJTPK9899'` (V7R6M0) or `'GOJTPK9689'` (V7R5M0). Exact-name
   matches always returned zero rows; `LIKE 'GOJTPK%'` is the
   correct verification pattern.

2. **The 3-PREPARE filing threshold wasn't being crossed.** IBM's
   [SQL Package Questions and Answers](https://www.ibm.com/support/pages/sql-package-questions-and-answers)
   documents (verbatim): "Starting with IBM i 6.1 PTF SI30855, a
   statement must be prepared 3 times before it is added to the
   SQL package. This change was made to prevent filling the
   package with statements that aren't used frequently." The
   v0.7.2 tests PREPAREd once and concluded the LPAR was broken;
   the LPAR was working as designed all along.

With both methodology bugs corrected — `LIKE 'BASE%'` + a 4-iter
prepare loop on a pinned `*sql.Conn` — filing is end-to-end
working on both V7R6M0 (`GOTCHE9899`, 2 statements, used_size
65744) and V7R5M0 (`GOJTPK9689`, 2 statements, used_size 58880).

### Changed

- **`driver.packageEligibleFor()` byte-equivalent to JT400's
  `JDSQLStatement.isPackaged()`.** Previously the default-criterion
  branch only accepted parameterised statements and the select-
  criterion branch incorrectly accepted VALUES / WITH. JT400's
  gate (verified against `IBM/JTOpen f14abcc`, also documented by
  IBM in the SQL Package Questions and Answers page: parameter
  markers, INSERT with subselect, DECLARE PROCEDURE, positioned
  UPDATE/DELETE) includes `(isInsert_ && isSubSelect_)`,
  `(isSelect_ && isForUpdate_)`, and `isDeclare_` under the
  default criterion, and explicitly *excludes* VALUES / WITH
  from the select-criterion widening. After this change a Go
  client and a Java client running the same SQL agree on whether
  the statement enters the shared `*PGM`. Pure refactor — no
  wire-format change for parameterised SQL, which remains the
  dominant filing trigger.

### Added

- **`testdata/jtopen-fixtures/.../PackageFilingVerify`** — new
  ground-truth Java fixture case that wipes any prior `GOJTPK*`
  package, then runs each parameterised SQL 4 times (crossing
  IBM's 3-PREPARE threshold) and queries
  `SYSPACKAGESTAT WHERE PACKAGE_NAME LIKE 'GOJTPK%'`. The
  captured trace proves JT400 emits `0x3808=01` on the wire
  for parameterised statements
  (`prepared_package_filing_verify.trace:273` + `:379`); the
  captured golden documents the server-side filing result.
  Validated against IBM Cloud V7R6M0 and PUB400 V7R5M0 on
  2026-05-11.

- **`TestFiling_ServerSideStateVerified`** in
  `test/conformance/cache_hit_test.go` — Go-side mirror of the
  Java fixture. Pins one `*sql.Conn`, runs two distinct
  parameterised SELECTs 4 times each, then queries
  `SYSPACKAGESTAT` from a fresh connection (the view has a
  visibility delay within the owning conn). Asserts
  `NUMBER_STATEMENTS >= 2`. Gated behind `DB2I_TEST_FILING=1`
  for environments without DLTOBJ authority on the package
  schema. Passes on V7R6M0 (`GOTCHE9899`, 2 entries,
  used_size=65744).

- **`wipePackage` + `filingPrepareCount` helpers** in
  `test/conformance/cache_hit_test.go`. `wipePackage` invokes
  `DLTOBJ` via `QSYS2.QCMDEXC` with a wildcard pattern so the
  4-char options-hash suffix `BuildPackageName` appends is
  matched. `filingPrepareCount=4` is one over IBM's documented
  3-PREPARE threshold (SI30855 since 6.1).

### Known driver-side gaps (tracked for future releases)

- **INSERT / UPDATE / DELETE filing.** `hostserver/db_exec.go:187`
  hard-codes `0x3808=0` for the EXECUTE_IMMEDIATE path because
  prepare-option=1 + full package name triggers a server-side
  rename of the RPB-bound statement (SQL-518 "STMT0001 IS NOT
  PREPARED" on the follow-up EXECUTE). JT400 handles this via a
  `nameOverride_` round-trip the driver hasn't yet implemented.
  Result: only SELECTs file through the Go driver today.

- **Cache-hit param binding.** When a filed SELECT is later
  dispatched via the v0.7.1 cache-hit fast path
  (`ExecutePreparedCached` / `OpenSelectPreparedCached`),
  parameter values are silently dropped — the server runs the
  cached plan with default-zero parameters. Filing-dependent
  cache-hit tests still SKIP without `DB2I_TEST_FILING=1` and
  this regression doesn't affect the cache-miss path.

### Documentation

- **`docs/package-caching.md`** — rewrote the "NUMBER_STATEMENTS
  stays at 0" troubleshooting entry. Cites IBM's SQL Package
  Q&A page for the 3-PREPARE threshold, documents the
  base-name-vs-wire-name table, and explains the same-conn
  visibility delay.

## [0.7.2] - 2026-05-11

Fifth tagged release. Full conformance suite green at 168 s on
IBM Cloud V7R6M0 (cache-hit tests that require filing skip via
DB2I_TEST_FILING env-var gate; rest pass). Pure documentation + observability + tests on
top of v0.7.1 — no public API additions. The cache-hit fast path
from v0.7.1 is now documented end-to-end, has three godoc examples,
and ships with a conformance test matrix that covers the JDBC type
surface, edge cases (NULL bind, no-rows result, server errors,
parallel dispatch, cross-connect preload, criteria=select), and
negative-path fallthrough (sql.Out, LOB bind). Live-validated
against IBM Cloud V7R6M0 to the limit of that environment (see
"Known limitation" below).

### Added

- **`docs/package-caching.md`** — comprehensive operator's guide
  covering when to use extended-dynamic + cache-hit, DSN setup,
  wire-flow mental model (miss vs hit), observability via slog
  (`"db2i: query cache-hit"` / `"db2i: exec cache-hit"` DEBUG
  lines) and OpenTelemetry (`db.operation.name=OPEN_SELECT_PREPARED_CACHED`
  on hit), `QSYS2.SYSPACKAGE` + `QSYS2.SYSPACKAGESTMTSTAT`
  verification queries, troubleshooting recipes, and cross-
  references to `docs/configuration.md`, `docs/performance.md`,
  and `docs/migrating-from-jt400.md`.

- **Three new godoc Examples** in `driver/example_test.go`:
  `Example_packageCache` (basic enable), `Example_packageCacheObservability`
  (`*bytes.Buffer`-backed slog handler asserting cache-hit
  dispatch), `Example_packageCacheCriteria` (`package-criteria=select`
  for unparameterised SELECT). All compile-checked by
  `go test ./driver -run Example_packageCache`.

- **`test/conformance/cache_hit_test.go`** — 12 new conformance
  tests for the cache-hit dispatch surface, sharing one
  `GOTCHE9899` package across the run:
  - `TestCacheHit_FirstUseFilesStatement` — fresh package → file
    → fresh DB → cache hit
  - `TestCacheHit_SelectTypeMatrix` — 17 type subtests covering
    INTEGER, BIGINT, SMALLINT, DECIMAL, NUMERIC, REAL, DOUBLE,
    DECFLOAT, VARCHAR, CHAR, VARCHAR FOR BIT DATA, DATE, TIME,
    TIMESTAMP, BOOLEAN, BINARY, VARBINARY
  - `TestCacheHit_MultiRowSelect` — 200-row SELECT through the
    cached OPEN with continuation FETCH
  - `TestCacheHit_ExecInsertUpdateDelete` — three I/U/D subtests
    asserting `RowsAffected` correctness + cache-hit dispatch
  - `TestCacheHit_NullBind` — `nil` bind through cached EXECUTE
    correctly signals NULL via IndicatorSize=2
  - `TestCacheHit_NoRowsResult` — `UPDATE ... WHERE 1=0`
    `RowsAffected=0` with no error (SQL +100)
  - `TestCacheHit_ServerErrorDoesntPoisonRPB` — duplicate-key
    INSERT returns `*Db2Error{SQLCode:-803}`; subsequent INSERT
    on same pool recovers cleanly
  - `TestCacheHit_OutParameterFallthrough` — `sql.Out` correctly
    falls through to non-cache path
  - `TestCacheHit_ParallelDispatch` — 4 goroutines × 25 iterations
    of cached SELECT
  - `TestCacheHit_CrossConnectPreload` — file 3 distinct SQLs,
    reopen, first call to each hits cache (validates connect-time
    download)
  - `TestCacheHit_CriteriaSelect` — `package-criteria=select`
    files unparameterised SELECT that `default` rejects
  - `TestCacheHit_LOBBindFallthrough` — BLOB locator-bind correctly
    skips cache (matches JT400's per-statement filter)

  Tests that require fresh server-side filing (most of the above)
  are gated by `DB2I_TEST_FILING=1` env var so they skip on LPARs
  with the known V7R6M0 filing-suppression issue. Negative-path
  tests (`OutParameterFallthrough`, `LOBBindFallthrough`,
  `CriteriaSelect/default_rejects`) run unconditionally and pass.

- **`docs/configuration.md`** — DSN table now lists all 9 package-
  cache keys: `extended-dynamic`, `package`, `package-library`,
  `package-cache`, `package-error`, `package-criteria`,
  `package-ccsid`, `package-add`, `package-clear`.

- **`docs/performance.md`** — new section quantifying the
  cache-hit round-trip saving (one wire RT eliminated per call;
  ~4× speedup on a high-latency tunnel measured in v0.7.1 smoke
  testing).

- **`docs/migrating-from-jt400.md`** — cross-reference subsection
  pointing to the new package-caching doc, plus updated note that
  the byte-equal package name lets Go + Java clients share one
  `*SQLPKG`.

- **`hostserver.WriteFrames`** (additive) — concatenates multiple
  DSS frames into a single `io.Writer` call. Used by
  `OpenSelectPrepared` to match JT400's TCP framing for the
  CREATE_RPB + PREPARE_DESCRIBE pair.

- **`hostserver.DBAttributesOptions.ExtendedDynamic`** (additive)
  — flag that, when true, makes SET_SQL_ATTRIBUTES emit 5 extra
  date/time/separator CPs (`0x3807-0x380B`) in JT400-matching
  order so the server has a definite value for the package-suffix
  derivation. Threaded from `driver.Conn.connect` when
  `cfg.ExtendedDynamic && cfg.PackageName != ""`.

### Updated

- **`README.md`** reorganised with: top-of-file v0.7.2 status
  badge, new `## Examples` section enumerating every Example_*
  function in `driver/example_test.go`, updated package-caching
  bullet pointing to the new operator's guide, refreshed server
  compatibility table with the V7R6M0 filing-suppression note.

### Known limitation

- **IBM Cloud V7R6M0 LPAR doesn't accumulate filed statements**
  in the `*SQLPKG` even when both go-db2i and JT400 send the
  correct wire shape (verified by capturing JT400's wire via
  the Java fixture harness on 2026-05-11). The cache-hit
  *dispatch* path is correct — the conformance suite proves it
  works when entries exist in the package (e.g. from a prior
  PUB400 V7R5M0 run). The driver's `Example_packageCache*` godoc
  examples are correct; the test gating via `DB2I_TEST_FILING=1`
  is the practical workaround for LPARs that don't file. Filing
  on V7R6M0 is an open IBM-side investigation.

## [0.7.1] - 2026-05-11

Fourth tagged release. Closes out v0.7.0's deferred client-side
cache-hit fast path AND repairs the v0.7.0 wire-shape bug that
prevented the package from actually being populated. Both
PREPARE_DESCRIBE-side filing and EXECUTE/OPEN-side cache lookup
are now live-validated against IBM Cloud V7R6M0 (8471 plaintext);
full conformance suite green (~162s).

### Fixed

- **Package SQLDA VARCHAR length convention**: the CP 0x380B per-
  statement decoder (`parsePackageSQLDADataFormat` and
  `parsePackageSQLDAParameterMarkerFormat`) was producing
  `Length=64` for a `VARCHAR(64)` column where the row decoder
  expected the super-extended convention `Length=66` (64 declared
  chars + 2-byte SL prefix). Live testing against IBM Cloud V7R6M0
  surfaced this as
  `varchar declared length 16448 exceeds column max 64` -- the
  off-by-two-per-VARCHAR slide read EBCDIC space pairs (`0x40 0x40`
  = 16448) at the next column's SL prefix offset. Fixed by adding
  a `normalizeSQLDALength` helper that returns `rawLen + 2` for
  VAR-family types so SelectColumn.Length and PreparedParam.FieldLength
  are interchangeable between cache-hit and cache-miss paths.
  Regression caught by `TestParsePackageSQLDA_VarcharLengthNormalized`.

- **M10 wire-shape bug**: v0.7.0's extended-dynamic + package-cache
  did NOT actually file prepared statements into the *PGM. The
  PREPARE_DESCRIBE wire emitted an EMPTY CP 0x3804 marker + prepare-
  option byte 0x00 -- a wire shape the server tolerates but treats
  as a no-op for package filing. Live testing against IBM Cloud
  V7R6M0 (2026-05-11) using fixture-captured JT400 wire bytes
  showed three additional details that JT400 actually emits and
  that the server enforces:
  - PREPARE_DESCRIBE's CP 0x3804 carries the FULL 10-char EBCDIC
    package name as a CCSID-tagged var-string, not an empty marker.
  - PREPARE_DESCRIBE's CP 0x3808 (prepare option) byte is 0x01 (not
    0x00) when extended-dynamic is on.
  - OPEN_DESCRIBE_FETCH also carries the same CP 0x3804 package-
    name var-string, alongside the existing buffer/cursor CPs.
  - CREATE_RPB carries the package library in CP 0x3801.
  - SendCreatePackage now treats errorClass=1 / SQLCODE -601
    ("package already exists") as success rather than soft-disable,
    mirroring JT400's JDPackageManager.create() behaviour.

  With these fixes a fresh package goes from 0 cached statements at
  connect to N cached statements after N distinct PREPARE_DESCRIBEs
  on the same library/name; subsequent connects with `package
  cache=true` then download the populated *PGM via RETURN_PACKAGE
  and the v0.7.1-A decoder lights up `c.pkg.Cached`. Verified end-
  to-end against the IBM Cloud V7R6M0 LPAR.

### Added

- **v0.7.1-Query**: `OpenSelectPreparedCached` -- cache-hit fast
  path for SELECT. CREATE_RPB + CHANGE_DESCRIPTOR + OPEN_DESCRIBE_
  FETCH with the cached server-assigned 18-byte statement name in
  CP 0x3806; PREPARE_DESCRIBE is skipped. Returns a `*Cursor` that
  supports continuation FETCH the same way the cache-miss path
  does, so multi-block result sets work without changes to the
  cursor surface. Live-validated against IBM Cloud V7R6M0 with
  `SELECT CURRENT_USER, CAST(? AS VARCHAR(64)) FROM SYSIBM.SYSDUMMY1`
  -- three back-to-back cached queries dispatch cleanly in ~100ms
  each after the initial warm-up.

- **v0.7.1-C**: driver Stmt cache-hit dispatch (both Exec and
  Query). `Stmt.Exec` and `Stmt.Query` call
  `Conn.packageLookup(sql)` after the bind step; on a byte-equal
  hit (and no `sql.Out` arg) Exec dispatches to
  `hostserver.ExecutePreparedCached` and Query dispatches to
  `hostserver.OpenSelectPreparedCached`, skipping the
  PREPARE_DESCRIBE round-trip for that call. New slog DEBUG lines
  `db2i: exec cache-hit` and `db2i: query cache-hit`
  (op=`EXECUTE_PREPARED_CACHED` / `OPEN_SELECT_PREPARED_CACHED`,
  `cached_name`=server-assigned 18-char name) so operators can see
  fast-path activity and cross-reference `QSYS2.SYSPACKAGE` entries.

- **v0.7.1-B**: client-side cache-hit fast path -- `hostserver`
  layer. `ExecutePreparedCached` and `OpenSelectPreparedCached`
  reuse the cached `*PackageStatement` shape:
  `CREATE_RPB + CHANGE_DESCRIPTOR + EXECUTE/OPEN_DESCRIBE_FETCH`
  where the EXECUTE / OPEN frame carries the cached 18-byte
  server-assigned name in CP 0x3806 (statement-name override) and
  the cached parameter shape feeds CP 0x381E. Skips
  PREPARE_DESCRIBE -- one round-trip eliminated per cache hit.
  OUT/INOUT shapes are rejected up front so callable statements
  can't accidentally lose their destinations through the fast
  path. Mirrors JT400's `AS400JDBCStatement.commonExecute`
  `nameOverride_` behaviour.

- **v0.7.1-A**: CP 0x380B (package info) per-statement decoder.
  `hostserver.ParsePackageInfo` walks JT400's 42-byte header + N x
  64-byte entry stride + per-entry SQLDA-format regions and produces
  `PackageStatement` values carrying the server-assigned 18-byte
  statement name, the original SQL text (UCS-2 BE), the data-format
  `SelectColumn` shapes, and the parameter-marker `ParameterMarkerField`
  shapes. Wired into `SendReturnPackage` and `driver.Conn.initPackage`
  so the connect-time RETURN_PACKAGE round-trip populates
  `c.pkg.Cached` with parsed entries instead of raw bytes. Foundation
  for the v0.7.1-B/C client-side cache-hit fast path.

## [0.7.0] - 2026-05-11

Third tagged release. **Renamed from goJTOpen to go-db2i.** M10
extended-dynamic-package caching shipped on top of the v0.6.0 base.
All five M10 sub-items (fixtures, wire encoder, DSN surface, connect-
time wiring, criteria filter) are live-validated against IBM Cloud
V7R6M0 on plaintext (8471); the full conformance suite is green
(~162 s on the 1-CPU LPAR). The M0-M9 wire surface is unchanged.

### BREAKING CHANGE — rename

The project moved from `goJTOpen` to `go-db2i` to reflect its actual
scope (a DB2 for i driver, not a JT400 fork). Operators upgrading
from v0.6.0 must update three things:

- Module import path: `github.com/complacentsee/goJTOpen/...` →
  `github.com/complacentsee/go-db2i/...`
- Driver registration: `sql.Open("gojtopen", ...)` →
  `sql.Open("db2i", ...)`
- DSN scheme: `gojtopen://user:pwd@host/...` →
  `db2i://user:pwd@host/...`

There is no compatibility shim — old DSNs return a clear
`scheme %q not recognised (want db2i)` error from `parseDSN`. The
Maven fixture-harness artifact (`testdata/jtopen-fixtures/`) was
renamed too: artifactId `gojtopen-fixtures` → `db2i-fixtures`,
Java package `io.github.complacentsee.gojtopen.fixtures` →
`io.github.complacentsee.db2i.fixtures`. The `testdata/jtopen-fixtures/`
*directory* keeps its name because the fixtures are captured via
JTOpen (the Java toolbox) — that source attribution stays.

### Added (M10) — public API additive vs v0.6.0

- `Config.ExtendedDynamic`, `Config.PackageName`,
  `Config.PackageLibrary`, `Config.PackageCache`,
  `Config.PackageError`, `Config.PackageCriteria`,
  `Config.PackageCCSID` fields with JT400-matching defaults
- DSN query keys: `extended-dynamic`, `package`,
  `package-library`, `package-cache`, `package-error`,
  `package-criteria`, `package-ccsid`, `package-add`
  (accept-ignore), `package-clear` (accept-warn-log)
- `hostserver.PackageOptions`, `hostserver.PackageManager`,
  `hostserver.PackageStatement` types
- `hostserver.SuffixFromOptions`, `hostserver.BuildPackageName`
  pure functions (byte-equal to JT400 for the same session options)
- `hostserver.BuildCreatePackageParams`,
  `hostserver.BuildReturnPackageParams`,
  `hostserver.SendCreatePackage`, `hostserver.SendReturnPackage`
  wire builders + senders
- `hostserver.WithExtendedDynamic` SelectOption
- New constants: `hostserver.ReqDBSQLCreatePackage` (0x180F),
  `hostserver.ReqDBSQLReturnPackage` (0x1815),
  `hostserver.ReqDBSQLDeletePackage` (0x1811),
  `hostserver.ReqDBSQLClearPackage` (0x1810)

Three new fixture cases in `testdata/jtopen-fixtures/`:
`prepared_package_first_use`, `prepared_package_cache_hit`,
`prepared_package_cache_download`. A fourth fixture
(`prepared_package_overflow`) is deferred to a follow-up because
the server-side threshold makes the trace prohibitively large.

The client-side cache-hit fast path (Stmt.Prepare lookup +
ExecutePreparedCached bypass) is deferred to a post-M10 follow-up.
The wire shape that ENABLES it is in place; the missing piece is
the CP 0x380B per-statement decoder.

### Added (M10-0 — package-cache fixtures)

- New fixture cases in `testdata/jtopen-fixtures/src/main/java/.../Cases.java`:
  - `prepared_package_first_use` — wipes `GOTEST/GOJTPK*` via
    untraced DLTOBJ, then primes the *PGM with two parameterised
    SELECTs. Verifies CP 0x3804 (package name) and CP 0x3805
    (package library) on the wire.
  - `prepared_package_cache_hit` — back-to-back prepares of the
    same SQL on one extended-dynamic connection with
    `package cache=true`. Verifies JT400's client-side fast
    path: one 0x1803 PREPARE_DESCRIBE on the wire, two EXECUTEs.
  - `prepared_package_cache_download` — fresh connection with
    `package cache=true` against the pre-warmed package.
    Verifies 0x1815 RETURN_PACKAGE on connect with a CP 0x380B
    reply carrying both seeded entries.
- Captured `.trace` + `.golden.json` for the three cases against
  IBM Cloud V7R6M0 (one-CPU LPAR via SSH-tunnelled 8471).
- `testdata/jtopen-fixtures/src/main/java/.../Capture.java`:
  setup runs with the same extra connection properties as the
  traced run so cases needing pre-traced session state (e.g. a
  case-scoped DLTOBJ via QCMDEXC under extended-dynamic) get a
  Connection wired the same way as `execute()`.

### Added (M10-1 — package-cache wire encoder)

- `hostserver/db_package.go`: function-code constants
  `ReqDBSQLCreatePackage` (0x180F), `ReqDBSQLReturnPackage`
  (0x1815), `ReqDBSQLDeletePackage` (0x1811),
  `ReqDBSQLClearPackage` (0x1810). CP constants
  `cpPackageName` (0x3804), `cpPackageLibrary` (0x3801),
  `cpPackageReturnOption` (0x3815), `cpPackageReplyInfo`
  (0x380B). Note: the wire reality is CP 0x3801 for library,
  not 0x3805 as the plan originally guessed.
- New types: `PackageOptions` (session options the suffix
  formula reads), `PackageManager` (per-conn state for M10-3),
  `PackageStatement` (cached PREPARE entry inside a package).
- `SuffixFromOptions(opts) → 4-char string`: byte-equal to
  JT400's `JDPackageManager.java:442-522`. Includes the
  COMMIT_MODE_RR overflow encoding that re-uses dateSep bits.
- `BuildPackageName(base, opts) → 10-char wire name`: 6-char
  upper/underscore/pad base + 4-char options suffix.
- `BuildCreatePackageParams` + `BuildReturnPackageParams`:
  param-list builders for CREATE_PACKAGE / RETURN_PACKAGE.
  Output is byte-equal to JT400 (asserted against the
  `prepared_package_first_use` and `prepared_package_cache_hit`
  fixture bytes).
- Table-driven tests cover the four formula slots, both
  COMMIT_MODE_RR overflow branches, IBM-i object-name charset
  encoding, and reject-bad-character validation.
- Integration into the existing PREPARE / EXECUTE flow is
  intentionally NOT in this commit; that lands in M10-3 once
  M10-2 wires the Config / DSN surface.

### Added (M10-2 — DSN + Config surface)

- 7 new `Config` fields: `ExtendedDynamic`, `PackageName`,
  `PackageLibrary`, `PackageCache`, `PackageError`,
  `PackageCriteria`, `PackageCCSID`. Defaults match JT400's
  `JDProperties.java` so an unmodified `DefaultConfig()` stays
  byte-equal to JT400's default-properties baseline.
- 9 new DSN query keys: `extended-dynamic`, `package`,
  `package-library`, `package-cache`, `package-error`,
  `package-criteria`, `package-ccsid`, plus the migration-
  friendly accept-ignore `package-add=true` and accept-warn-log
  `package-clear`. Validation is strict on every value — wrong
  enum / bad charset / out-of-range CCSID / cache-without-
  extended-dynamic all surface a typed error from `parseDSN`.
- Package identifiers (name + library) get canonicalised at the
  DSN boundary (upper-case, space→underscore) so downstream
  callers see the same bytes the wire encoder expects. The
  charset check (`A-Z 0-9 _ # @ $`) mirrors JT400's
  `JDProperties.validateName`.
- Cross-key sanity: `package-cache=true` requires
  `extended-dynamic=true`; `extended-dynamic=true` requires
  `package=<name>`. Both rejections are tested.
- Documentation: `docs/migrating-from-jt400.md` moves the 9
  package properties from "deferred" to "supported", updates
  the supported count from 12 to 21.

### Added (M10-2.5 — fuzz)

- `hostserver/FuzzSuffixFromOptions`: random 8-int input through
  the suffix formula. Asserts 4-char output, every char from
  `SUFFIX_INVARIANT_`, deterministic, no panic on out-of-range
  values (the clipSuffixIndex clamp is exercised).
- `hostserver/FuzzBuildPackageName`: random base + options.
  Asserts 10-char output, suffix tail in `SUFFIX_INVARIANT_`.
- Extended `driver/FuzzParseDSN` seed corpus with the 9 new M10
  keys plus their pinned rejection cases. Short fuzz runs
  (5s, single core) on all three fuzz targets pass clean.

### Added (M10-3 — connect-time package wiring)

- `hostserver.SendCreatePackage`: issues CREATE_PACKAGE (0x180F)
  on a database connection and consumes the matching reply.
  Idempotent on the server side — a re-create of an existing
  *PGM returns success.
- `hostserver.SendReturnPackage`: issues RETURN_PACKAGE (0x1815)
  and returns the raw CP 0x380B body bytes. The detailed
  per-statement decoder is deferred so the M10-3 wire round-trip
  ships standalone; the cache-hit fast path lights up once the
  parser lands.
- `hostserver.WithExtendedDynamic(true)` SelectOption: appends an
  empty CP 0x3804 marker to PREPARE_DESCRIBE requests. The
  marker tells the server to file the prepared statement into
  the package context the connection established at connect time.
  Wired into all three PREPARE_DESCRIBE call sites
  (`db_prepared.go`, `db_select.go`, `db_exec.go`).
- `driver.Conn.initPackage`: connect-time setup that derives the
  10-char wire name from `cfg.PackageName` + `packageOptions()`,
  CREATE_PACKAGEs the resolved name in `cfg.PackageLibrary`, and
  (if `cfg.PackageCache`) RETURN_PACKAGEs to download cached
  statement entries. Folds errors through `Config.PackageError`
  to decide between fatal-fail and slog-warn-and-soft-disable.
- `driver.Conn.packageOptions`: builds the `PackageOptions` from
  the current `Config` so the on-wire suffix is byte-equal to
  what JT400 would compute from the same DSN.
- `driver.Conn.selectOptions` now also returns
  `WithExtendedDynamic(true)` when the connection has a live
  package manager, so every PREPARE the conn issues files into
  the *PGM.
- DSN parser: `package=<name>` accepts up to 10 chars
  (matching JT400's `JDProperties.validate` charset+max); the
  encoder later truncates to 6 chars before the 4-char options
  suffix is appended, preserving byte-equality.

### Live evidence (M10-3)

- Conformance suite green against IBM Cloud V7R6M0
  (`162 s, 0 failures`) with and without the new flags.
- Smoke test (3 iter SELECT under `extended-dynamic=true`,
  optional `package-cache=true`) verifies CREATE_PACKAGE +
  RETURN_PACKAGE round-trip with the LPAR; `QSYS2.SYSPACKAGE`
  shows the resolved 10-char name (`GOTEST/GOJTPK9899`) lives
  on disk for cross-driver share with JT400.

### Added (M10-4 — error handling + package-criteria filter)

- `driver.Conn.packageEligibleFor(sql, hasParams)`: per-SQL filter
  that mirrors JT400's `JDSQLStatement.canHaveExtendedDynamic`:
  - `criteria=default` (default) — parameterised statements only;
    `CURRENT OF <cursor>` and `DECLARE …` are always excluded.
  - `criteria=select` — default rules PLUS non-parameterised
    SELECT / VALUES / WITH.
- `driver.Conn.selectOptionsFor(sql, hasParams)`: per-statement
  variant of `selectOptions()` that drops the
  `WithExtendedDynamic` flag when the criteria filter rejects the
  SQL. Wired into `Stmt.Query` and `Stmt.Exec` so non-eligible
  statements go through the plain PREPARE wire shape and stay
  out of the *PGM.
- `driver.Conn.handlePackageError` (introduced in M10-3) gets
  three table-driven tests covering the `warning` / `exception` /
  `none` modes (plus the empty-string default = warning path).
- `driver_test.go` adds `TestPackageEligibleFor_DefaultCriteria` /
  `_SelectCriteria` / `_NoPkgIsAlwaysFalse` /
  `TestPackageCriteriaPlumbing` / `TestHandlePackageError` —
  table-driven coverage for every documented branch.

### Notes

- `prepared_package_overflow` is deferred — server-side package
  threshold (~1024 entries) makes the trace prohibitively large
  for the cache-hit suite. Will land before M10 closes.
- Client-side cache-hit fast path (Stmt.Prepare lookup +
  ExecutePreparedCached bypass) is deferred to a post-M10
  follow-up. The wire shape that ENABLES it is in place
  (extended-dynamic populates the server *PGM, RETURN_PACKAGE
  downloads its contents); the missing piece is the CP 0x380B
  per-statement decoder.

## [0.6.0] - 2026-05-11

Second tagged release. M9 stored-procedure support shipped on top
of the v0.5.0 base. The four M9 sub-items are all live-validated
against IBM Cloud V7R6M0 on plaintext (8471/8476); the M1-M8
surface is unchanged and re-validated end-to-end (full plaintext
conformance suite green, ~160 s on the 1-CPU LPAR). The TLS path
(9471/9476) also passes basic connectivity.

The added public API surface is purely additive vs v0.5.0 -- no
breaking changes:

- `sql.Out{Dest: &x, In: bool}` admission via `Stmt.CheckNamedValue`
- `*Rows.NextResultSet() error` + `HasNextResultSet() bool`
- New `hostserver` constants `ORSCursorAttributes`,
  `ORSDataCompression`, `ReqDBSQLOpenDescribe`
- New `hostserver.Cursor` methods `MoreResultSets`,
  `AdvanceResultSet`, `NumberOfResults`, `CurrentResultSet`
- `hostserver.ExecResult.OutValues []any`

`hostserver.closeCursorReuse` is exposed for the multi-result-set
advance; the existing `closeCursor` is now a wrapper preserving the
M1-M8 behaviour. The 0x00040000 / 0x00008000 / 0x1804 raw hex
literals scattered through `hostserver/db_*.go` have all been
replaced by named constants.

Roadmap parked for M10+:
- Extended-dynamic-package caching (server-side prepared-statement
  cache; highest-impact wire optimization remaining).
- Batch parameter binds (multi-row CP 0x381F or `ExecBatch` API
  sugar). Multi-row `VALUES (?,?), (?,?)` SQL already works today.
- JDBC escape syntax (`{call ...}`, `{fn ...}`, `{d ...}`).
- DatabaseMetaData catalog (currently callers must `SELECT FROM
  QSYS2.SYSTABLES` directly).
- Kerberos / GSS auth.
- Client reroute / seamless failover.
- Locator-based LOB UPDATE (`PUT_LOB_DATA`).

Documented out-of-scope (use the JTOpen jar):
- Non-JDBC JTOpen services (`CommandCall`, `IFSFile`, `DataQueue`,
  etc.). See README "Scope" section.
- Scrollable cursors (`database/sql/driver.Rows` is forward-only
  by design).
- Stored procedures returning OUT params AND result sets in the
  same call (JT400 ordering: drain result sets first, then read
  OUTs; database/sql idiom is unclear).
- Return-value parameter at index 1 for CALL (JT400 fakes this;
  IBM i procs don't actually have return values).

### Added

- M9-3 Multi-result-set stored procedures via
  `Rows.NextResultSet`. Stored procedures that declare
  `DYNAMIC RESULT SETS N` cursors WITH RETURN now surface every
  result set through database/sql's optional `NextResultSet` /
  `HasNextResultSet` interface on `*Rows`: the first set drains
  via `Rows.Next` as usual, then `Rows.NextResultSet()` attaches
  to the proc's next pre-opened cursor and `Rows.Next` continues
  from there. Returns false (and `Rows.Err()` is nil) when the
  last set is drained, matching the database/sql contract.
  Wire-protocol additions:
    - `Stmt.Query` now routes any CALL through the
      `OpenSelectPrepared` path; the underlying
      `openPreparedUntilFirstBatch` dispatches on the verb:
      SELECT/VALUES/WITH still go through OPEN_DESCRIBE_FETCH
      (0x180E) one-shot, but CALL takes JT400's
      CallableStatement wire pattern instead --
      `executeCallAndAttachFirstSet` sends EXECUTE (0x1805),
      reads `SQLERRD(2)` out of the EXECUTE reply's SQLCA to
      learn the dynamic-result-set count, then OPEN_DESCRIBE
      (0x1804) + FETCH (0x180B) to attach to and prefetch the
      first set.
    - `hostserver.findResultSetCount(rep *DBReply) int` extracts
      SQLERRD(2) from a reply's SQLCA (CP 0x3807, offset 100, BE
      int32). Mirrors JT400's `firstSqlca.getErrd(2)` read at
      `AS400JDBCStatement.java:1213`. Zero for SELECT statements.
    - `Cursor` gains `numberOfResults` / `currentResultSet` /
      `isCallCursor` state plus public `MoreResultSets() bool`,
      `AdvanceResultSet() ([]SelectColumn, error)`,
      `NumberOfResults() int`, `CurrentResultSet() int`.
      `AdvanceResultSet` closes the current cursor with
      `REUSE_RESULT_SET` (0xF2, preserving the prepared statement
      so the next OPEN_DESCRIBE can attach), issues another
      OPEN_DESCRIBE on the same RPB to attach to the next
      pre-opened proc cursor, parses its column descriptors,
      and prefetches the first row batch.
    - `fetchCallRows` uses JT400's CALL-cursor FETCH param set
      (CP 0x380E `FetchScrollOption` + CP 0x380C `BlockingFactor`
      = 2048) instead of the SELECT-cursor `BufferSize` +
      `VarFieldCompr` set. The proc-opened cursor's server-side
      state doesn't carry a default block size, so the FETCH has
      to specify one explicitly. ALSO parses rows BEFORE
      honouring `interpretFetchReply`'s `exhausted` flag: CALL
      cursors routinely return EC=2 RC=701 ("end-of-data with
      rows") in a single batch, and the SELECT-side fetcher
      would have silently dropped the rows on that path.
    - `findSuperExtendedDataFormat` (CP 0x3812) now accepts a
      zero-byte payload as "no columns described yet" -- the
      shape PREPARE_DESCRIBE replies take for CALL statements
      whose result-set layout isn't known until OPEN_DESCRIBE.
    - `closeCursorReuse(conn, corr, reuse byte)` is the new
      parameterised CLOSE entry point; `closeCursor` is a
      `closeCursorReuse(... reuseNo)` wrapper preserving the
      M1-M8 behaviour. The multi-set advance and cursor.Close
      call paths thread the right indicator through.
  Driver surface:
    - `*Rows` implements
      `database/sql/driver.RowsNextResultSet`:
      `HasNextResultSet() bool` delegates to
      `Cursor.MoreResultSets`; `NextResultSet() error` delegates
      to `Cursor.AdvanceResultSet`, returning `io.EOF` when
      drained.
  Wire-bitmap cleanup (in flight with M9-3 work):
    - New constants `ORSCursorAttributes = 0x00008000` and
      `ORSDataCompression = 0x00040000` replace the bare hex
      literals scattered through `hostserver/db_*.go`. Every
      previously-inlined `0x00040000` / `0x00008000` use site
      now references the named constant; the only remaining raw
      `0x00040000` is `signon_errors.go`'s unrelated security
      error code lookup.
  Offline coverage: new
  `hostserver/db_call_test.go::TestCallMultiSetFixtureWireSequence`
  asserts the CALL connection's request sequence in
  `prepared_call_multi_set.trace` contains
  CREATE_RPB / PREPARE_DESCRIBE / CHANGE_DESCRIPTOR / EXECUTE /
  OPEN_DESCRIBE / FETCH (each present at least once) and does
  NOT contain OPEN_DESCRIBE_FETCH -- confirming our M9-3 routing
  matches JT400 byte-for-byte at the function-code layer.
  Live evidence: new `TestStoredProcedureMultiResultSet` drives
  `CALL GOSPROCS.P_INVENTORY(5)` against IBM Cloud V7R6M0,
  drains both result sets through `Rows.NextResultSet`, asserts
  the seed data lands in the expected order: set 1 =
  `[(LOW1, 2), (LOW2, 3)]`, set 2 = `[(HIGH1, 50), (HIGH2, 100)]`.
  Full M9-1+M9-2+M9-3 conformance run 9.4 s on the 1-CPU LPAR
  plaintext path.
  `Example_callMultiResultSet` documents the API.

- M9-2 OUT and INOUT stored-procedure parameters via the
  standard-library `sql.Out` wrapper. Callers pass
  `sql.Out{Dest: &x}` for OUT-only slots and `sql.Out{Dest: &x,
  In: true}` for INOUT slots; the driver populates the right
  destinations after EXECUTE returns. Go 1.21+ required for
  `sql.Out.In` (go-db2i's go.mod floor is 1.23 so this is always
  available).
  Wire-protocol additions:
    - `Stmt.CheckNamedValue` now accepts `sql.Out` (value type, the
      shape `database/sql` passes through) so Go's default parameter
      converter doesn't reject it with "unsupported type sql.Out, a
      struct".
    - `bindArgsToPreparedParams` returns a parallel
      `[]*sql.Out` for OUT-bearing slots so the post-EXECUTE
      write-back path has stable pointers, builds a placeholder
      `PreparedParam` shape (SQL type derived from the destination's
      reflect.Kind) with direction byte `0xF1` (PARAMETER_TYPE_OUTPUT)
      or `0xF2` (PARAMETER_TYPE_INPUT_OUTPUT), and either binds the
      INOUT IN-side value (deref of *Dest) or a typed zero
      placeholder.
    - `hostserver.ExecutePreparedSQL` runs an OUT-shape fixup
      between PREPARE_DESCRIBE and CHANGE_DESCRIPTOR: for each
      OUT/INOUT slot it overrides the caller's placeholder shape
      with the SQL type / length / CCSID the server declared in
      the parameter-marker format (CP 0x3813). That brings the
      descriptor byte-for-byte in line with what the server expects.
    - The EXECUTE ORS bitmap now ORs in `ORSResultData`
      (0x04000000) whenever any param is OUT/INOUT, asking the
      server to ship a synthetic single-row CP 0x380E carrying the
      OUT values (mirrors JT400's
      `AS400JDBCPreparedStatementImpl.java:723` `outputParametersExpected_`
      path). The reply parser reuses `findExtendedResultData` with
      synthetic SelectColumn entries derived from the PreparedParam
      shapes -- the OUT row's wire layout is identical to a one-row
      SELECT result.
    - `ExecResult.OutValues []any` carries the decoded row up to the
      driver, which reflect-assigns each non-nil entry into the
      matching `sql.Out.Dest`. Conversion follows database/sql Scan
      conventions for the M9-2 destination types: `*string`
      (string / []byte), `*int / *int8 / *int16 / *int32` (int32 /
      int64 with range-check), `*int64`, `*float32 / *float64`,
      `*bool` (bool / non-zero int32).
  Offline coverage: new
  `hostserver/db_call_test.go::TestCallInOutFixtureOutDecode`
  replays the EXECUTE reply from
  `prepared_call_in_out.trace`, drives the new
  `parseOutParameterRow` against post-fixup shapes mirroring
  P_LOOKUP's IN VARCHAR(10) + OUT VARCHAR(64) + OUT INTEGER
  signature, asserts the decoded OUT slots are `"Acme Widget"`
  and `100` -- byte-equivalent to the JT400 golden recorded by
  `recordOutParam` during fixture capture.
  Live evidence: new `TestStoredProcedureOUT` calls
  `CALL GOSPROCS.P_LOOKUP('WIDGET', sql.Out{&name}, sql.Out{&qty})`
  against IBM Cloud V7R6M0 and asserts the OUT scalars land in the
  caller's variables. New `TestStoredProcedureINOUT` covers the
  INOUT direction via `P_ROUNDTRIP`: seed value 5 round-trips to 6
  through one `sql.Out{Dest: &counter, In: true}` slot.
  Plaintext live green; total M9-1+M9-2 conformance run 7.0 s.
  `Example_callWithOut` documents the OUT and INOUT idioms.

- M9-1 CALL routing for stored procedures with IN-only parameters.
  `db.Exec("CALL <schema>.<proc>(?, ?)", in1, in2)` now flows through
  the same CREATE_RPB + PREPARE_DESCRIBE + EXECUTE sequence JT400
  emits for `CallableStatement`, with statement-type byte 3
  (TYPE_CALL per JT400's `JDSQLStatement` taxonomy) on both PREPARE
  and EXECUTE so the server populates `SQLERRD(2)` correctly for
  dynamic-result-set procs (a prerequisite the M9-3 Rows.NextResultSet
  path depends on). Internal additions:
    - `driver.isCall(query string) bool` mirrors `isSelect`; `Stmt.Exec`
      bypasses the no-args ExecuteImmediate shortcut for CALL so the
      server sees PREPARE first even when literal arguments are
      embedded in the SQL text. `Stmt.Query` accepts CALL alongside
      SELECT/VALUES/WITH; full multi-result-set drain via
      `Rows.NextResultSet` is M9-3 work, but Query routing is wired
      up now so the M9-1 driver surface is consistent.
    - `hostserver.statementTypeForSQL` recognises CALL → 3 (TYPE_CALL).
      The pre-existing INSERT/UPDATE/DELETE → 3/4/5 mapping is kept
      intact (JT400 collapses these to TYPE_OTHER=1; both are accepted
      by IBM i V7R6 in practice, evidenced by the M1-M8 live runs --
      a wire-format cleanup for a future release).
    - `hostserver.DBReply.findSuperExtendedParameterMarkerFormat` now
      treats a zero-byte `0x3813` CP as "no parameter markers" (the
      shape the server returns for a marker-less CALL like
      `CALL P_INS('A', 10)`; captured in
      `prepared_call_in_only.trace` recv #12).
  Offline coverage: new `hostserver/db_call_test.go` ::
  `TestCallInOnlyFixtureWireShape` replays the captured trace's
  second connection (post-VRM-detect), confirms JT400 sent the
  4-frame CREATE_RPB / PREPARE_DESCRIBE / EXECUTE / RPB_DELETE
  sequence (no EXECUTE_IMMEDIATE, no CHANGE_DESCRIPTOR), asserts
  statement-type CP 0x3812 carries value 3 on both PREPARE and
  EXECUTE, then drives `ExecutePreparedSQL` against a fakeConn
  serving the captured replies and asserts go-db2i emits the
  same 4-frame shape with statement-type 3.
  Live evidence: new `TestStoredProcedureINOnly` conformance test
  bootstraps the GOSPROCS schema (idempotent via CREATE OR REPLACE
  + DROP+CREATE for tables; the conformance suite is now
  self-bootstrapping for the M9 procs), runs
  `db.Exec("CALL GOSPROCS.P_INS(?, ?)", "M9_INONLY", 7)`, confirms
  the proc body's `INSERT` landed exactly one row in
  `GOSPROCS.INS_AUDIT`. Plaintext run against IBM Cloud V7R6M0
  green (2.6 s including schema bootstrap).
  New `Example_call` in `driver/example_test.go` documents the
  typical pattern.

- M9-0 Foundation for stored-procedure support. Five new fixtures
  under `testdata/jtopen-fixtures/fixtures/prepared_call_*.{trace,golden.json}`
  captured against IBM Cloud V7R6M0 via JT400 21.0.4 through the
  local 127.0.0.1:8471 SSH tunnel. New `GOSPROCS` library on the
  LPAR holds four SQL procedures:
    - `P_INS(IN code VARCHAR(10), IN qty INTEGER)` — IN-only,
      inserts to `INS_AUDIT`. Wire shape: PREPARE+EXECUTE with
      statement type `0x03` (TYPE_CALL per JT400's JDSQLStatement
      taxonomy); no CHANGE_DESCRIPTOR when literal args eliminate
      parameter markers.
    - `P_LOOKUP(IN code VARCHAR(10), OUT name VARCHAR(64), OUT qty INTEGER)` —
      IN + two OUT scalars via SELECT INTO from a seeded `WIDGETS`
      table. Validates JT400's OUT-value path: descriptor direction
      bytes (0xF0/0xF1/0xF2 at wire offset 30 per JT400's
      `DBExtendedDataFormat.setFieldParameterType` line 300-302),
      synthetic single-row result-data CP in the EXECUTE reply
      decoded via `parameterRow_.setServerData()`. Golden pins
      `("Acme Widget", 100)`.
    - `P_INVENTORY(IN min_qty INTEGER) DYNAMIC RESULT SETS 2` —
      opens two server-side cursors via `DECLARE CURSOR WITH
      RETURN`. Validates the SQLCA-ERRD(2) advance signal
      (`numberOfResults_ = firstSqlca.getErrd(2)` per JT400's
      `AS400JDBCStatement` line 1213) and the `FUNCTIONID_OPEN_DESCRIBE`
      function code for `getMoreResults()`.
    - `P_ROUNDTRIP(INOUT counter INTEGER)` — single INOUT scalar
      incremented by one; validates the 0xF2 direction byte and
      round-trip of bind-value + OUT-value in a single parameter
      slot. Golden pins seed `5` → out `6`.
  The Java fixture harness gained a new `PUB400_PORT` env var that
  appends `:port` to the JDBC URL, engaging JT400's `skipSignonServer_`
  codepath (`AS400JDBCConnectionImpl` line 3550-3556) so the harness
  can reach the LPAR through a local tunnel without needing port
  449 (port mapper) or 8476 (signon) exposed. The `socket timeout`
  / `thread used` / `login timeout` properties are skipped on the
  port-override path because JT400 freezes the AS400 instance
  before `prepareConnection` runs those mutators on that path.
  Live evidence: the `WithStoredProcs.setup()` method that the
  capture run executes performs the full `CREATE OR REPLACE
  PROCEDURE` sequence for all four procs end-to-end via JT400,
  which is itself the JT400-side sanity check the M9 plan calls
  for. Fixture goldens decoded with the expected values across
  all five cases:
    - `prepared_call_in_only`: `updateCount=0` (CallableStatement
      doesn't propagate INSERT row counts up through CALL).
    - `prepared_call_in_out`: OUT VARCHAR `"Acme Widget"`, OUT
      INTEGER `100`.
    - `prepared_call_result_set`: 2 rows `[("LOW1", 2), ("LOW2", 3)]`.
    - `prepared_call_multi_set`: first set as above, second set
      `[("HIGH1", 50), ("HIGH2", 100)]`.
    - `prepared_call_inout`: OUT INTEGER `6`.
  Five new `.trace` files committed (each 14-22 KB) as the offline
  regression net for M9-1 / M9-2 / M9-3 replay tests.

## [0.5.0] - 2026-05-11

First tagged release. M1-M8 complete + live-validated against IBM
Cloud V7R6M0 over both plaintext (8471/8476) and TLS (9471/9476)
host-server pairs. The public API (`driver.Config`,
`driver.Connector`, `driver.LOBValue`, `driver.LOBReader`,
`hostserver.Db2Error`, the `ebcdic` codec interfaces) is the
intended 1.0 surface plus the M8 slog + OTel additions; all
remaining 0.x releases will iterate on the same shape unless a
specific wire-protocol gap (DES password levels 0/1, M5/M6
diffrunner) drives a breaking call.



### Added

- M8-6 Performance tuning notes at
  [`docs/performance.md`](docs/performance.md). Practical guidance
  on the eight tuning levers that actually move the needle: sql.DB
  pool sizing (1-CPU LPAR ramp + collapse-point measurement),
  `?lob=stream` vs materialise, `?lob-threshold` tradeoffs, `?ccsid`
  selection, CCSID-aware codec caching (zero per-call allocation),
  RLE compression ratios on `RETRIEVE_LOB_DATA`, TLS overhead (one-
  time handshake cost), `Rows.Next` lazy iteration peak-heap
  guarantee. Includes notes on M8-3 slog + M8-4 OTel per-call
  overhead in high-throughput workloads. Every number cited is
  either backed by a reproducible test in this repo or measured
  against IBM Cloud V7R6M0 with the source captured in `AUTH.md`.

- M8-5 JTOpen DSN migration guide at
  [`docs/migrating-from-jt400.md`](docs/migrating-from-jt400.md).
  Side-by-side table covers every JT400 JDBC URL property (~70
  distinct keys cross-referenced against JT400's
  [JDProperties.java](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDProperties.java)
  enumeration of 109 attributes). For each: the go-db2i DSN
  equivalent or "deferred / out of scope" with the reason. Groups
  by topic (auth + host, TLS, session attributes, CCSID, LOB,
  performance, reroute, diagnostics, behaviour overrides). Includes
  a migration recipe converting a typical jt400 URL into a
  `db2i://...` DSN + programmatic `db2i.NewConnector`
  Config for the slog / OTel paths. Honest about the coverage gap:
  go-db2i ships 12 DSN keys plus three programmatic Config fields;
  JT400 ships ~109 properties, most of them either toolbox-side
  features (BiDi reordering, proxy server) or features go-db2i
  intentionally defers (extended-dynamic-package caching,
  client-reroute / seamless failover).

- M8-4 OpenTelemetry spans on `Stmt.ExecContext` /
  `Stmt.QueryContext`. New `driver.Config.Tracer trace.Tracer` field
  takes a `go.opentelemetry.io/otel/trace` API tracer (v1.36.0
  pinned); the driver doesn't depend on any specific OTel SDK so
  callers can plug in any exporter. Nil tracer resolves to the
  noop-tracer fallback (same idiom as Config.Logger from M8-3).
  Spans follow the OpenTelemetry database semantic conventions
  ([May 2025 refresh](https://opentelemetry.io/docs/specs/semconv/database/database-spans/)):
    - SpanKind = Client
    - Span name = the operation verb (`EXEC` for `db.Exec`,
                  `QUERY` for `db.Query`).
    - Attributes:
        - `db.system.name = "ibm_db2_for_i"` (dialect form).
        - `db.operation.name = "EXEC" | "QUERY"`.
        - `db.namespace = <Config.Library>` when set.
        - `db.user`, `server.address`, `server.port` from Config.
        - `db.statement.parameters.count` always.
        - `db.response.returned_rows` on Exec.
        - `db.statement` only when `Config.LogSQL=true` (same gate
                                                       as the slog
                                                       integration).
    - On error: span status set to Error,
                `*hostserver.Db2Error` (when present) attaches
                `db.response.status_code` (SQLSTATE),
                `db.ibm_db2_for_i.sqlcode`, and
                `db.ibm_db2_for_i.message_id` as dedicated
                attributes so alerting routes don't have to regex
                the message.
  `cmd/smoketest -trace-stdout` wires the OTel stdout exporter for
  reproducible captures. Live-validated against IBM Cloud V7R6M0:
  a 71 ms QUERY span emitted with SpanKindClient + the full
  convention-compliant attribute set. Internal offline coverage:
  `driver/tracer_test.go` (5 cases) walks the API + the Db2Error
  recording path through an in-memory exporter. Full plaintext
  conformance suite (~152 s) remains green; the noop-tracer
  fallback adds no measurable overhead under the default
  nil-Tracer configuration.

- M8-3 slog integration. New `driver.Config.Logger *slog.Logger`
  field hands the driver a caller-controlled logging sink; nil (the
  default) silences all driver-side logging via an internal
  discard-handler fallback so call sites never need a nil check.
  Per-Conn child logger carries the attrs `driver=db2i`,
  `dsn_host=<host>`, `server_vrm=<vrm>` so every line a single
  connection emits is pre-attributed. Levels:
    - INFO  on connect (with user/db_port/signon_port/tls/library
                       attrs) and Close.
    - DEBUG on each `Stmt.Exec` / `Stmt.Query` (with op /
                                              params / elapsed /
                                              rows_affected attrs)
                       and each `LOBReader.Read` ->
                       RETRIEVE_LOB_DATA round trip (with handle /
                       col / offset / requested / returned).
    - WARN  on `classifyConnErr` producing ErrBadConn (the pool
                                                     will retire
                                                     the conn).
    - ERROR on non-fatal statement-level failures so logs
                       subscribed at ERROR catch SQL syntax /
                       constraint / lock errors without parsing
                       message text.
  New `driver.Config.LogSQL bool` gates whether the SQL text is
  attached to Exec / Query DEBUG attrs (off by default; SQL text
  often carries customer identifiers). Parameter counts are always
  attached; parameter values never are. New
  `db2i.NewConnector(*Config) (*Connector, error)` constructor
  lets callers wire a programmatic Logger that can't be expressed
  in a DSN string -- pass the Connector to `sql.OpenDB` to get a
  `*sql.DB`. `cmd/smoketest -log-debug` exercises the driver path
  with a stderr text handler so the log stream is reproducible.
  Sample run against IBM Cloud V7R6M0:

  ```
  time=2026-05-11T00:14:31Z level=INFO msg="db2i: connected" \
      driver=db2i dsn_host=localhost server_vrm=460288 user=GOTEST \
      db_port=8471 signon_port=8476 tls=false library=""
  time=2026-05-11T00:14:31Z level=DEBUG msg="db2i: query" \
      driver=db2i dsn_host=localhost server_vrm=460288 \
      op=OPEN_SELECT_STATIC params=0 elapsed=71.954ms
  time=2026-05-11T00:14:31Z level=INFO msg="db2i: connection closed" \
      driver=db2i dsn_host=localhost server_vrm=460288
  ```

  Full plaintext conformance suite (`DB2I_DSN`, ~151 s)
  remains green; the silent-handler fallback path adds < 1 ns of
  overhead per logging call site under the default nil-Logger
  configuration.

- M8-2 godoc audit. Every exported symbol across `driver/`,
  `hostserver/`, `ebcdic/`, and `internal/wirelog/` now carries a
  leading-identifier doc comment that's compatible with `go doc`
  rendering. The pre-fix gaps were all interface-method
  implementations whose containing types were already documented:
  `Rows.Columns`, the five `Rows.ColumnType*` methods,
  `Result.RowsAffected`, `Stmt.Close`, `Tx.Commit` / `Tx.Rollback`,
  `Direction.String`, `NonBMPRuneError.Error`, `SignonError.Error`.
  Add a new `ExampleDsnKnobs` walking the `?lob-threshold` / `?ccsid`
  / `?tls=true` DSN knob mix in package driver's example file
  (existing examples already covered `LOBValue`, `LOBReader`,
  `Db2Error`, and the basic DSN parse pattern). A snapshot of
  `go doc -all` output for each package is committed under
  `docs/godoc-snapshot/` so future diffs surface accidental API
  drift; total ~2,584 lines across four files.

- M8-1 fuzz tests covering the wire-touching parsers most likely to
  mishandle adversarial input: `internal/wirelog.ParseJTOpenTrace`,
  `hostserver.ParseDBReply`, `hostserver.decompressRLE1`,
  `hostserver.decompressDataStreamRLE`, and `driver.parseDSN`. Each
  fuzzer seeds from the committed fixtures + unit-test payloads and
  was run for ≥60 s against the live build. Total ≥4M execs across
  the five corpora; no panics. Two fuzz-found inputs preserved as
  regression seeds under `driver/testdata/fuzz/FuzzParseDSN/` and
  `hostserver/testdata/fuzz/FuzzParseDBReply/`. The fuzzers surfaced
  three real hardening defects, all fixed in this same milestone (see
  Fixed section below).

### Fixed

- `unwrapCompressedReply` and `parseLOBReply` now reject a
  wire-declared decompressed length exceeding 64 MiB before calling
  the RLE-1 decompressor. Pre-fix, a hostile or malformed
  `RETRIEVE_LOB_DATA` / CP 0x3832-wrapped reply could declare a
  4-byte length near 2 GiB and trigger an unbounded `make([]byte,
  expectedLen)` inside the decompressor, which would panic the Go
  process on any reasonably sized host. JT400 sidesteps this by
  trusting the JVM to raise OutOfMemoryError; Go callers don't have
  that safety net. The 64 MiB cap is two orders of magnitude above
  the LOB block size (1 MiB) and well above any host-server reply
  go-db2i has captured. Pinned via
  `TestParseDBReplyCompressedLengthCapped`; surfaced by
  `FuzzParseDBReply`.
- `ParseDBReply` always returns a non-nil `DBParam.Data` slice, even
  for header-only params (LL == 6). Pre-fix,
  `append([]byte(nil), emptyData...)` returned nil for empty data,
  so callers iterating reply params had to distinguish `Data == nil`
  from "empty payload" -- a footgun the wire shape never demanded.
  Live wire replies have never been observed to ship LL == 6 params,
  but the fuzzer found that an attacker-controlled reply could; the
  fix is to always allocate a fresh, retainable backing array.
  Pinned via the regression seed at
  `hostserver/testdata/fuzz/FuzzParseDBReply/5c5762c2c1a948f8`.
- `parseDSN` rejects port 0 / port > 65535 (for both the URL `:port`
  and `?signon-port=N`) and an empty username (e.g. `db2i://@h/`)
  at parse time rather than letting the failure surface as an
  opaque "signon rejected" error from the host server later. New
  negative cases pinned in `TestParseDSNRejectsBadInputs/{empty_user,
  port_zero, port_over_65535, signon-port_zero}`; surfaced by
  `FuzzParseDSN`. Live-validated: plaintext conformance suite
  (`DB2I_DSN` against IBM Cloud V7R6M0, ~151 s, full pass) still
  green after the parser tightenings.

### Added

- LOB *bind* via parameter markers. `db.Exec("INSERT INTO t (id, b)
  VALUES (?, ?)", id, payload)` now writes BLOB / CLOB / DBCLOB
  columns through the JT400 `WRITE_LOB_DATA` (function `0x1817`)
  flow. The driver reads the server-allocated locator handles out
  of the `PREPARE_DESCRIBE` reply (CP `0x3813` Super Extended
  Parameter Marker Format), uploads the bytes in one or more
  frames, and EXECUTE carries the 4-byte locator handle in the
  SQLDA value slot. `[]byte` and `string` work directly for
  materialised binds; the new `*db2i.LOBValue` type adds an
  explicit form (`Bytes` for materialised, `Reader` + `Length` for
  streamed binds that chunk across multiple `WRITE_LOB_DATA` frames
  at advancing offsets). CLOB strings are pre-encoded via the
  column-declared CCSID's codec (`ebcdic.CCSID37`, `ebcdic.CCSID273`,
  …) so EBCDIC code-point mismatches like "!" = 0x5A on US vs 0x4F
  on German round-trip cleanly. Wire-validated end-to-end against
  PUB400 V7R5M0: 8 KiB BLOB byte-equal byte-equal, 80 KiB streamed
  BLOB across three chunks, 1 MiB streamed BLOB across 32 chunks,
  ~8 KiB CLOB EBCDIC round-trip.
- `*db2i.LOBValue` public type (`driver/lob_value.go`) for
  callers who want explicit LOB-intent at the call site or need
  the streaming form (`Reader` + `Length`).
- Wire-protocol reference doc at `docs/lob-bind-wire-protocol.md`
  pinning the bind sequence (PREPARE_DESCRIBE → CHANGE_DESCRIPTOR
  → WRITE_LOB_DATA × N → EXECUTE) plus the corrected CP map for
  any future re-derivation: LOB Data is `0x381D`, truncation is
  `0x3822`, locator handles are server-allocated (not client).
- LOB streaming via `*db2i.LOBReader`. Opt in with the DSN
  option `?lob=stream`; the default `materialise` mode (full LOB
  into `[]byte` / `string` at Scan time) stays unchanged. `LOBReader`
  satisfies `io.Reader` + `io.Closer` and pulls 32 KB chunks per
  call via `RETRIEVE_LOB_DATA`. Lets callers process multi-GB LOBs
  without exhausting Go heap, and exposes the column CCSID +
  SQLType so callers can transcode (CLOB CCSID 273 → CCSID273
  Decode, etc.). Wire-validated against PUB400 V7R5M0: 200-byte
  BLOB streamed in 32 KB chunks; 700-char CLOB streamed and
  decoded via `ebcdic.CCSID273`.

### Added

- Cross-cutting: `tx_rollback` fixture re-captured as happy-path
  against V7R6M0 / GOTEST schema. Pre-existing trace was an
  SQL-error capture (schema had no commitment control); new trace
  shows the full rollback sequence including CP 0x1808 ROLLBACK
  and a post-rollback SELECT returning zero rows. Journal
  (`GOJTJ*`) + receiver (`GOJTR*`) bring-up retries cleanly on
  subsequent fixture runs (CPF0006 "already journaling" warnings
  are expected and don't gate the capture).

- Cross-cutting: `hostserver/doc.go` package doc refreshed for
  the M2-M7 surface. New sections: wire format breakdown, top-
  level entry points by phase (connection setup / statement
  execution / LOB I/O / transaction control), CCSID handling
  conventions, LOB compression (per-CP + whole-datastream RLE
  variants), extended-metadata two-knob requirement, cursor /
  RPB lifecycle, error classification. Cross-references to the
  contributor-facing docs (lob-bind-wire-protocol.md,
  lob-known-gaps.md, configuration.md, PLAN.md) and the JT400
  source map for contributors adding new wire flows.

- M3 deferred: native BOOLEAN (V7R5+) bind + decode live-validated
  on V7R6M0. Driver binds Go `bool` as SMALLINT(1) (the standard
  server-side coercion path JT400 documents); decoder learns
  SQL types 2436/2437 with the 1-byte wire form
  `0xF0 = false, anything else = true`, mirroring JT400's
  `SQLBoolean.convertFromRawBytes`. Offline coverage:
  `TestDecodeColumnBoolean/*` (NN + nullable, 0xF0 / 0xF1 /
  out-of-spec true sentinels). Live coverage:
  `TestBooleanRoundTrip/{scalar_via_parameter_marker,
  full_pull_preserves_order}` exercises bind + multi-row decode
  against a `CREATE TABLE ... (flag BOOLEAN)`. Skips on V7R4 and
  earlier (CREATE returns a syntax error).

- M4 deferred: native BINARY (912/913) and VARBINARY (908/909)
  result-data decoders. CHAR FOR BIT DATA (452/453 + CCSID 65535)
  and VARCHAR FOR BIT DATA (449 + CCSID 65535) already handled the
  byte-data emulation; V7R3+ servers expose the standalone BINARY
  / VARBINARY types under distinct SQL type codes that hit the
  "unsupported SQL type" path pre-fix. New decoders share the
  []byte-output shape with their CHAR-with-bit-data siblings.
  Also fixes a mislabel in `isVarLengthSQLType`: 472/473 was
  commented as "VARBINARY family" but is actually LONGVARGRAPHIC
  (per JT400's SQLDataFactory mapping); VARBINARY's real native
  type is 908/909. New offline tests
  `TestDecodeColumnBINARY` / `TestDecodeColumnVARBINARY`; new
  fixture case `prepared_binary_bind` captures JT400's
  three-column wire pattern (`CHAR(8) FOR BIT DATA` + `BINARY(8)`
  + `VARBINARY(32)`). Live coverage:
  `TestBinaryTypeRoundTrip/{CHAR FOR BIT DATA, BINARY, VARBINARY,
  all three in one row}` PASS on V7R6M0.

- M4 deferred: extended column metadata (schema, table, base column
  name, label) via DSN `?extended-metadata=true`. Plumbs the
  ORS bit `0x00020000` (ORSExtendedColumnDescrs) + the per-statement
  CP `0x3829 = 0xF1` (ExtendedColumnDescriptorOption) through
  PREPARE_DESCRIBE. The CP `0x3829` knob is **required**: pre-fix
  iterations of the patch ORed only the ORS bit and saw CP `0x3811`
  come back with zero bytes — JT400 documents this as the "empty
  descriptor" warning case. With both knobs set the server fills
  CP `0x3811` with the per-column variable-info section (LL/CP
  records 0x3900 base column / 0x3901 base table / 0x3902 label
  with CCSID prefix / 0x3904 schema name). New `SelectColumn`
  fields `Schema`, `Table`, `BaseColumnName`, `Label`; driver
  surfaces them via go-db2i-specific `Rows.ColumnTypeSchemaName`
  / `ColumnTypeTableName` / `ColumnTypeBaseColumnName` /
  `ColumnTypeLabel` methods (callers reach them via the driver-
  level Rows; `database/sql.Rows` hides them). Offline coverage:
  `TestEnrichWithExtendedColumnDescriptors` walks a synthetic
  two-column payload. Live coverage:
  `TestExtendedMetadata/{flag off..., flag on:schema and table
  populate}` PASS on V7R6M0; flag-off side stays empty (no wire
  regression vs pre-flag captures).

- Bug #15 TestLOBClob CCSID 273 byte-mode mismatch resolved. The
  test was pre-encoding "Hello, IBM i! " via
  `ebcdic.CCSID273.Encode` and inserting the bytes verbatim into a
  `CLOB(1M)` column that took the *job-default* CCSID at table
  creation -- 37 on the IBM Cloud English-locale LPAR. CCSID 37
  and CCSID 273 agree on 239 of 256 wire bytes but disagree on 17;
  the `!` character is one of them (0x4F in 273 vs 0x5A in 37), so
  the pre-encoded payload's `!` bytes decoded back as `|` and every
  iteration of the test phrase mismatched. The codec itself was
  symmetric (per the existing `TestCCSID273RoundTripASCII` /
  `TestCCSID273DivergentChars` / `TestCCSID273MatchesCCSID37OnSharedBytes`
  coverage) -- the test just assumed the wrong column CCSID.
  Fixed by declaring the column `CLOB(1M) CCSID 273` so the
  pre-encoded bytes align with the column's storage CCSID
  regardless of the connecting job's locale. New offline
  `TestCCSID273ByteRoundTripIsBijective` walks every wire byte
  through Decode-then-Encode and pins the bijection (exempting
  U+001A SUBSTITUTE bytes, the one-to-many "no destination
  mapping" sentinel). Both `TestLOBClob/[]byte_pre-encoded_CCSID_273`
  and `TestLOBClob/LOBValue_Reader_pre-encoded_80KiB` now PASS on
  V7R6M0; predicted to pass on any LPAR regardless of job CCSID.

- M7-5 + bug #14 `lob threshold` inline-small-LOB end-to-end. DSN
  flag `?lob-threshold=N` plumbs through to the
  `SET_SQL_ATTRIBUTES` CP `0x3822` (LOBFieldThreshold) parameter,
  mirroring JT400's `JDProperties.LOB_THRESHOLD` ("lob threshold"
  JDBC URL knob). Zero falls back to the historical 32768 default
  so the wire shape matches pre-M7-5 captures. New
  `driver.Config.LOBThreshold` (`uint32`) field; `TestParseDSNLOBThreshold`
  covers parsing edge cases including the 15728640 server cap.
  The trace capture (new `prepared_blob_threshold` fixture case)
  settles the open question on JT400's wire behaviour: with
  `lob threshold=32768` against V7R6M0, JT400 emits **1 `WRITE_LOB_DATA`
  + 2 `RETRIEVE_LOB_DATA`** for a 32-byte BLOB + 256-byte CLOB pair,
  compared to **2 + 4** without the threshold -- the server inlines
  both LOB columns in the SELECT-back row data, and JT400 inlines one
  of the two on EXECUTE. The inline-vs-locator decision is driven
  by the `PREPARE_DESCRIBE` reply's SQL type code; go-db2i already
  followed that shape on bind so this commit only needed to add the
  CP `0x3822` plumbing on the request side.

  Read-side fix (bug #14): `decodeColumn` now handles inline LOB
  result-data shapes. SQL types `404/405` (BLOB), `408/409` (CLOB),
  and `412/413` (DBCLOB) decode through a 4-byte BE actual-length
  prefix followed by `length` bytes of payload, slot-padded to
  `col.Length`. Mirrors JT400's `SQLBlob.convertFromRawBytes` /
  `SQLClob.convertFromRawBytes` / `SQLDBClob.convertFromRawBytes`.
  BLOB returns `[]byte`; CLOB decodes through the column CCSID
  (1208 UTF-8 passthrough, 273 German EBCDIC, default CCSID 37);
  DBCLOB decodes through a new `decodeGraphicLOB` helper.

  Pre-fix reproducer (`CLOB(4K) CCSID 1208` SELECT) surfaced as
  `unsupported SQL type 409 (col len=4100, ccsid=1208)` against
  V7R6M0; post-fix the new `TestCCSID1208RoundTrip/CLOB small
  inline` subtest exercises that path and PASSes both with the
  default threshold (server inlines) and with `?lob-threshold=1`
  (server forced to use locators) -- the same content round-trips
  byte-equal through either code path. Closes
  `docs/lob-known-gaps.md` §3 + §5.

- M7-7 RETRIEVE_LOB_DATA RLE compression re-enabled end-to-end on
  V7R6M0. The request-side bit (`0x00040000`) on
  `RetrieveLOBData` now stays ON; `ParseDBReply` transparently
  unwraps the resulting whole-datastream CP `0x3832` wrapper by
  checking the `dataCompressed` marker (high bit of the 32-bit
  word at template offset 4, matching JT400's
  `DBBaseReplyDS.parse`). The wrapper uses a **different RLE
  format** than the per-CP RLE-1 M7-7 partial shipped: 5-byte
  record (escape + 2-byte pattern + 2-byte BE count, emits
  2×count bytes per iteration) vs the per-CP form's 6-byte
  record (escape + 1-byte value + 4-byte BE count, emits count
  bytes). New `decompressDataStreamRLE` in
  `hostserver/db_lob.go` mirrors JT400's
  `DataStreamCompression.decompressRLEInternal` byte-for-byte.
  Offline coverage: `TestDecompressDataStreamRLE_RoundTrip/*`
  (passthrough, escaped 0x1B, 4 KiB run, zero-pattern fast path,
  truncated, overflow) and `TestParseDBReplyUnwrapsCP3832`
  (end-to-end synthetic wrapped reply with two inner params,
  byte-matches the uncompressed equivalent). Live evidence on
  V7R6M0 via `stress-test/rlemeasure`: 1 MiB constant-content
  0xCC BLOB SELECT shrinks `rx_bytes` from ~1 MiB to **1,228
  bytes wire** (~854× upper-bound compression ratio). The 4 KiB
  0xCC `TestLOBMultiRow` regression that surfaced when the bit
  was first re-enabled (per-CP decompressor reading
  whole-datastream bytes -> "truncated run header" parse
  error) is now the regression gate. Closes
  `docs/lob-known-gaps.md` §4.

- M7-4 TLS sign-on / database live-validated end-to-end against
  IBM Cloud V7R6M0. The DSN-level scaffolding (`tls=true`,
  `tls-insecure-skip-verify`, `tls-server-name`, default-port flip to
  9476 / 9471) shipped in an earlier commit; M7-4 closes the loop by
  proving the host-server protocol runs unchanged above the TLS layer
  on a real IBM i SSL host server. A self-signed cert was issued via
  DCM (`*SYSTEM` cert store, RSA 2048 + SAN: DNS:localhost +
  IP:192.168.20.3, signed by a Local CA created at the same time and
  imported into `*SYSTEM` as a trusted CA cert) and assigned to the
  `QIBM_OS400_QZBS_SVR_DATABASE`, `_SIGNON`, and `_CENTRAL`
  application IDs. Once assigned, the host servers automatically
  begin listening on ports 9470-9476 alongside the existing
  8470-8476 plaintext pair -- no `STRHOSTSVR SSL(*YES)` switch
  exists on V7R6. New `TestTLSConnectivity` in
  `test/conformance/`, gated on `DB2I_TLS_TARGET`, exercises
  sign-on + start-database + PREPARE + EXECUTE + multi-row FETCH
  through TLS and (when `DB2I_DSN` is also set against the
  same LPAR) byte-diffs the result against the plaintext path.
  Both subtests PASS on V7R6M0:
  `TestTLSConnectivity/smoketest` (single-row CURRENT_TIMESTAMP /
  CURRENT_USER) and `TestTLSConnectivity/multi-row` (5-row
  `QSYS2.SYSTABLES` pull, TLS result byte-equal to plaintext).

- M7-7 (partial) ships the RLE-1 decompressor as the foundation
  for compressed `RETRIEVE_LOB_DATA` replies. New
  `hostserver.decompressRLE1` is a Go port of JT400's
  `JDUtilities.decompress` (`0x1B value count(4)` runs +
  `0x1B 0x1B` escaped literals), covered offline by 8 byte-level
  tests including the 1 MiB constant-content case that motivated
  the original gap. `parseLOBReply` now reads the CP `0x380F`
  `actualLen` header and routes per-CP RLE payloads through the
  decompressor when `len(payload) != actualLen`; graphic-LOB
  length doubling (CCSID 13488 / 1200) is handled. The
  request-side RLE bit (`0x00040000`) stays OFF until the
  whole-datastream RLE wrapper (CP `0x3832`) lands -- V7R6
  servers compress the entire reply via that CP rather than
  per-CP, so `ParseDBReply` needs a CP `0x3832` unwrap pass
  before the bit can re-enable safely. `docs/lob-known-gaps.md`
  §4 documents the residual work; current LOB reads continue to
  use the uncompressed path with no behaviour change.

- M7-6 captures JT400's wire pattern for batched LOB-column INSERTs.
  New fixture `testdata/jtopen-fixtures/fixtures/prepared_blob_batch.trace`
  (case `prepared_blob_batch` in `Cases.java`) records
  `ps.addBatch()` × 5 + `ps.executeBatch()` against
  `INSERT INTO t (ID, B, C) VALUES (?, ?, ?)`. Finding settles
  the long-standing question (gap §2) that the
  M7 plan flagged: **JT400 emits N single-row EXECUTE_IMMEDIATE
  frames, not a single multi-row EXECUTE with CP 0x381F
  RowCount=5.** Per row the wire shows a fixed pattern of
  `WRITE_LOB_DATA` × K (one per LOB marker) + `RETRIEVE_LOB_DATA`
  (re-allocates the per-marker locator handles) + `EXECUTE_IMMEDIATE`.
  go-db2i's existing per-`Exec` round trip is therefore wire-
  equivalent to JT400's `executeBatch` for LOB columns; no
  multi-row code path to mirror. Doc-only resolution closes
  `docs/lob-known-gaps.md` §2; full walkthrough lives in
  `docs/lob-bind-wire-protocol.md` "Multi-row batched INSERT —
  settled".

- DSN parameter `?ccsid=N` overrides the connection's
  application-data CCSID. The value is plumbed into both the
  `SET_SQL_ATTRIBUTES` ClientCCSID negotiation (CP `0x3801`, the
  CCSID the server uses when returning untagged CHAR / VARCHAR /
  CLOB column data) and the per-bind tag the driver stamps on
  string parameters. Default 0 keeps the auto-pick behaviour
  (UCS-2 BE for negotiation, UTF-8 / CCSID 37 for binds based on
  server VRM). Use `?ccsid=1208` to force UTF-8 round-trip on a
  job where the SQL service's default CCSID would otherwise pick
  an EBCDIC SBCS table. Live-validated end-to-end against
  IBM Cloud V7R6M0 with `TestCCSID1208RoundTrip` exercising
  `VARCHAR(64) CCSID 1208`, `CHAR(64) CCSID 1208`, and
  `CLOB(1M) CCSID 1208` columns through em-dashes, curly quotes,
  smart apostrophes, Latin-1 and Greek letters byte-equal.
  Per-column CCSID still wins on the read side -- explicitly
  tagged columns ignore the connection-level setting and are
  decoded via the column's CCSID regardless.

- M7-2 lazy `Rows` iteration validated against IBM Cloud V7R6M0.
  `driver.Rows.Next` now pulls one row at a time from
  `*hostserver.Cursor`, which issues continuation FETCH (`0x180B`)
  lazily when its 32 KB block-fetch buffer drains. The plumbing
  itself landed across M5 (`Cursor` abstraction) and M6 (driver
  wiring); M7-2 closes it out by pinning two regression tests
  in the conformance suite:
  - `TestRowsLazyMemoryBounded` — a ~50K-row SELECT against
    `QSYS2.SYSCOLUMNS` walked via `rows.Next` keeps peak Go
    `HeapAlloc` ~3.8 MiB (16 MiB budget); post-iteration delta is
    *negative* after `runtime.GC()`. Pre-streaming the same query
    would have buffered ~50K row tuples into memory before
    yielding the first row.
  - `TestRowsCloseIdempotent` — three sequential `rows.Close()`
    calls return the same (nil) error; a follow-up `VALUES 1`
    on the same pool conn succeeds, proving Close doesn't leak
    a half-open cursor or RPB slot.
  The buffered hostserver entry points (`SelectStaticSQL` /
  `SelectPreparedSQL`) are preserved via `Cursor.drainAll` for
  offline fixture tests and `cmd/smoketest`.

### Fixed

- CHAR / VARCHAR result decode now picks the EBCDIC codec via the
  column's CCSID instead of always using CCSID 37. Pre-fix,
  columns declared with CCSID 273 (German EBCDIC, the historical
  PUB400 default) round-tripped some punctuation incorrectly --
  the 17 code points that diverge between CCSID 37 and CCSID 273
  (e.g. `!` is 0x5A in 37 but 0x4F in 273) decoded to the wrong
  Unicode character on a German-job server. The result-data
  decoder now routes through the same `ebcdicForCCSID` helper the
  bind path and the LOB-read path already use, so the 37/273
  symmetry that landed for LOB reads earlier this milestone now
  also covers CHAR / VARCHAR. CCSID 1208 (UTF-8) and CCSID 65535
  (FOR BIT DATA) paths are unchanged. Closes M7-3 part 1; the
  CCSID-aware decode lays the groundwork for `?ccsid=N` (added
  separately above).

- DBCLOB string bind now picks the encoder by column CCSID instead
  of always emitting UTF-16 BE. Pre-fix, binding a Go string with
  any non-BMP rune (e.g. `𝄞 = U+1D11E`) to a `DBCLOB(...) CCSID
  13488` column failed server-side with SQL-330 ("character cannot
  be converted") because CCSID 13488 is strict UCS-2 BE and rejects
  surrogate pairs. Post-fix, CCSID 13488 columns route through a new
  `encodeUCS2BE` helper that substitutes non-BMP runes with `U+003F`
  (`?`), mirroring JT400's `SQLDBClobLocator.writeToServer` parity
  behaviour; CCSID 1200 (UTF-16 BE) keeps the existing
  surrogate-aware `encodeUTF16BE` path. The first substitute event
  in a process emits a one-shot `slog.Warn` so callers notice their
  data is being transcoded. A typed `*hostserver.NonBMPRuneError`
  and an `encodeUCS2BEStrict` opt-in helper are also available for
  callers who would rather surface an error than silently
  substitute (no DSN flag wires this in yet — follow-up).
  Offline-tested via `TestEncodeUCS2BE_*`; live-validated against
  the IBM Cloud Power VS V7R6M0 LPAR with
  `DB2I_TEST_CCSID13488_TABLE=GOTEST.GOSQL_DBCLOB13488`
  (BMP round-trip + non-BMP substitute both PASS). PUB400 V7R5M0
  does not expose a CCSID-13488 target so the test skips there.
  Closes `docs/lob-known-gaps.md` §1.
- LOB reads no longer set the RLE compression bit (0x00040000) in
  the RETRIEVE_LOB_DATA ORS bitmap. Pre-fix, BLOBs whose content
  RLE-compressed well (4 KiB of identical bytes, headers padded
  with 0x00, etc.) returned 0 bytes after Scan because the server
  shipped an RLE-compressed CP and our parser interpreted the
  shorter payload as truncation. Latent since the LOB SELECT
  feature landed; surfaced by the multi-row LOB conformance test
  inserting a 4 KiB 0xCC fill.
- LOB CP 0x380F payload now uses the wire byte count instead of
  the per-LOB-type "actualLen" indicator. For graphic LOBs
  (DBCLOB), actualLen is reported in characters and the prior
  code sliced off half the payload. Now matches the JT400
  `DBLobData.adjustForGraphic` semantics implicitly.
- CLOB read decode now picks the column's actual CCSID codec
  instead of always using CCSID 37. Without this, columns declared
  CCSID 273 (German EBCDIC, the PUB400 default) round-tripped some
  punctuation incorrectly — e.g. "!" wrote as 0x4F but read back as
  "|" because the encoder used CCSID 273's mapping while the
  decoder used CCSID 37's. The 17 divergent code-points are now
  symmetric.
- DBCLOB bind on the wire now reports the *character* count for
  CP 0x3819 (Requested Size) and CP 0x381A (Start Offset),
  mirroring JT400's `JDLobLocator.writeData` graphic-LOB
  convention. Pre-fix, DBCLOB binds overstated the size by 2× and
  the server allocated a buffer twice as large as needed
  (sometimes triggering SQL-302 downstream); post-fix, byte-for-
  byte round-trips on PUB400 V7R5M0.
- DBCLOB streamed read (`*db2i.LOBReader`) halves offset and
  size at the wire boundary so the per-Read RETRIEVE_LOB_DATA
  request for graphic LOBs sends character counts. Pre-fix, the
  reader bailed mid-stream with SQL-401 once the byte offset
  passed the LOB's character count threshold.
- DBCLOB read length tracking now doubles `CurrentLength` (which
  the server reports in characters for graphic LOBs) when stored
  on `LOBReader.totalLen` so the EOF math doesn't truncate to half
  the LOB.
- LOB locator column index off-by-one — the driver passed the Go
  0-based column index to `RETRIEVE_LOB_DATA` (CP `0x3828`) where
  JT400's `JDServerRow.newData` uses 1-based (`i+1`). On V7R5+
  targets this produced `SQL-818` ("consistency tokens do not
  match") because the locator landed on the wrong column server-
  side; the LOB SELECT path was therefore broken on PUB400 V7R5M0
  even though the original validation against IBM Cloud V7R6M0
  worked. Both materialise and the new stream paths now wire-
  validate clean on V7R5M0.

### Changed

- Cursor lifecycle aligned with JT400's "fetch/close" wire pattern
  (#48). Pre-refactor, every SELECT emitted four post-PREPARE
  frames -- `OPEN_DESCRIBE_FETCH`, a continuation `FETCH`, an
  explicit `CLOSE`, and `RPB DELETE` -- because the driver always
  assumed multi-batch and always closed the cursor explicitly. The
  trailing `FETCH` and `CLOSE` always came back as warnings
  (`SQL +100`, `SQL -501 / 24501`) when the server had already
  delivered the entire result in one block-fetch buffer (the
  typical case JT400 itself optimises for); the offline tests
  worked around this by feeding three synthetic replies
  (`syntheticFetchEndReply`, `syntheticCloseReply`,
  `syntheticRPBDeleteReply`) the captured `.trace` files don't
  contain. The cursor now interprets the JT400 dispatch tuple from
  the OPEN reply: `ErrorClass=2, ReturnCode=700` is JT400's
  documented "fetch/close" signal (all rows delivered + cursor
  auto-closed; see `JDServerRowCache.fetch`), `ErrorClass=1
  ReturnCode=100` and `ErrorClass=2 ReturnCode=701` are end-of-data
  variants where the cursor stays open. `Cursor.Close` skips the
  explicit `closeCursor` call when the server already closed,
  emitting only `RPB DELETE` -- byte-for-byte the same wire pattern
  JT400 produces. Continuation `FETCH` still runs when the server
  signals more rows pending, so multi-batch result sets work
  unchanged. The three synthetic test stubs are gone; offline
  tests now consume the captured PREPARE / OPEN / RPB-DELETE
  replies directly. Wire-validated against IBM Cloud Power VS
  (V7R5M0): 19/19 type round-trip, 4163-row pull from
  `QSYS2.SYSTABLES`, autocommit toggle, all the smoketest paths.

### Fixed

- DATE descriptor parser USA-format quirk (#53). DSN `?date=usa` (and
  every non-default `?date=` value) used to silently fail: the
  driver pumped the EBCDIC date format byte into CP `0x3805`, which
  is JTOpen's `TranslateIndicator`, not a date format. `DateFormatJOB`
  happened to coincide with the only valid `TranslateIndicator`
  value (`0xF0`), so the default kept working; any explicit choice
  produced an invalid `TranslateIndicator` AND left the actual date
  format at the server's job default. CP `0x3805` is now always
  `0xF0`, and the date format / separator are sent on the correct
  CPs `0x3807` (`DateFormatParserOption`) and `0x3808`
  (`DateSeparatorParserOption`) using JTOpen's integer index mapping
  (0..7). The bind path also gained format awareness via
  `PreparedParam.DateFormat`, so a USA-session prepared `DATE` bind
  emits `MM/DD/YYYY` instead of always `YYYY-MM-DD`. Legacy callers
  that leave `DateFormat` zero continue to use the length-based
  ISO/YMD inference unchanged.

### Added

- LOB SELECT support: BLOB / CLOB / DBCLOB columns arrive as
  server-side locators (SQL types 960/961, 964/965, 968/969). The
  driver auto-fetches the full LOB content via
  `hostserver.RetrieveLOBData` (function 0x1816) on `Rows.Scan`.
  BLOBs scan into `*[]byte`, CLOBs into `*string` (decoded per the
  column CCSID -- UTF-8 / EBCDIC / UCS-2 BE for DBCLOB). Streaming
  via `io.Reader` is documented as a follow-up; for now LOBs are
  fully materialised at Scan time, which fits the typical
  small-to-medium LOB case. LOB *bind* (writing large LOBs as INSERT
  parameters) is not yet implemented; INSERT inline literals
  (`X'...'`, string literals) still work.
- TLS support via `tls=true` DSN key. Wraps both as-signon and
  as-database sockets in `crypto/tls`; default ports flip to the IBM
  i SSL host-server pair (9476 / 9471). `tls-insecure-skip-verify=true`
  for the common case of self-signed certs lacking DNS SANs;
  `tls-server-name` overrides the SNI / cert-verify hostname.
  Implementation offline-tested via DSN parsing; live TLS handshake
  validation pending an IBM i target with SSL host servers enabled
  (server-side DCM config).
- `Stmt.QueryContext` / `Stmt.ExecContext` propagate context cancellation
  to in-flight host-server I/O via `net.Conn.SetDeadline`. Returns
  `context.DeadlineExceeded` / `context.Canceled` rather than the
  underlying transport timeout.
- `driver.ErrBadConn` plumbing on TCP-level failures (`io.EOF`,
  `net.OpError`, `hostserver.ErrShortFrame`, `*Db2Error` with SQLSTATE
  08xxx). Wrapped with `errors.Is` support so `database/sql`'s pool
  auto-recovers, while `errors.Unwrap` still reaches the original cause.
- Typed `*hostserver.Db2Error` from CP 0x3807 SQLCA: `SQLState`,
  `SQLCode`, `MessageID`, `MessageTokens`, `Op`, `ErrorClass`. Predicate
  helpers `IsNotFound`, `IsConstraintViolation`, `IsConnectionLost`,
  `IsLockTimeout`.
- Streaming `*hostserver.Cursor` + `OpenSelectStatic` /
  `OpenSelectPrepared`. `database/sql.Rows` now pulls one row at a time
  via continuation FETCH; large result sets stream instead of
  buffering.
- `Result.LastInsertId` via `IDENTITY_VAL_LOCAL()`. Cached after first
  call. Documented session-scope behavior matches JT400 / standard JDBC.
- CCSID 1208 (UTF-8) string binds and result decode on V7R3+ servers.
  Falls back to CCSID 37 (US English EBCDIC) on older releases. Live
  round-trip validated for ASCII, Latin-1, CJK, and emoji.
- Prepared `Stmt.Exec` for INSERT / UPDATE / DELETE with parameter
  markers. RPB cleanup on every exit path (success, +100 no-match,
  hard error).
- `Stmt.Query` parameter binding for the full `driver.Value` union:
  `int64`, `float64`, `bool`, `[]byte`, `string`, `time.Time`, `nil`.
- VARCHAR FOR BIT DATA (`[]byte` ↔ CCSID 65535) bind + result decode.
- TIMESTAMP / DATE / TIME columns surface as `time.Time` to
  `database/sql.Scan`.
- Connection lifecycle: signon (password levels 2, 3 SHA-1; level 4
  PBKDF2-HMAC-SHA-512), as-database start, SET_SQL_ATTRIBUTES
  (date format, isolation, default library), NDB ADD_LIBRARY_LIST.
- `database/sql.Driver` registration as `"db2i"` with DSN syntax
  `db2i://USER:PASSWORD@HOST[:PORT]/?key=value`.

### Server compatibility

- IBM i V7R6 (V7R6M0): wire-validated end-to-end on IBM Cloud Power VS.
- V7R3–V7R5: should work at protocol parity; not regularly tested.
- ≤ V7R2 / DES auth (password levels 0, 1): implementation present but
  spec-validated only — no live target available for testing.

### Limitations / not yet implemented

- LOB known gaps: DBCLOB on CCSID 13488 columns (surrogate
  rejection), CP 0x381F `RowCount > 1` batched LOB INSERT,
  JT400's `lob threshold` inline-small-LOB optimisation, and
  RLE-decompression on read. Each is documented with workaround
  + fix-path notes in [`docs/lob-known-gaps.md`](docs/lob-known-gaps.md);
  none affects the BLOB / CLOB CCSID 273 / single-row paths the
  conformance suite exercises live.
- TLS sign-on / database (ports 9476 / 9471) (M7).
- `slog` integration / OpenTelemetry spans (M8).
