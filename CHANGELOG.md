# Changelog

All notable changes to **goJTOpen** are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).

The driver is **pre-1.0** while wire compatibility is being built up
across IBM i versions; expect the public API surface to settle at
0.5+ once LOB bind, slog observability, and OTel spans all land.

## [Unreleased]

### Added

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
  goJTOpen has captured. Pinned via
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
  and `?signon-port=N`) and an empty username (e.g. `gojtopen://@h/`)
  at parse time rather than letting the failure surface as an
  opaque "signon rejected" error from the host server later. New
  negative cases pinned in `TestParseDSNRejectsBadInputs/{empty_user,
  port_zero, port_over_65535, signon-port_zero}`; surfaced by
  `FuzzParseDSN`. Live-validated: plaintext conformance suite
  (`GOJTOPEN_DSN` against IBM Cloud V7R6M0, ~151 s, full pass) still
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
  materialised binds; the new `*gojtopen.LOBValue` type adds an
  explicit form (`Bytes` for materialised, `Reader` + `Length` for
  streamed binds that chunk across multiple `WRITE_LOB_DATA` frames
  at advancing offsets). CLOB strings are pre-encoded via the
  column-declared CCSID's codec (`ebcdic.CCSID37`, `ebcdic.CCSID273`,
  …) so EBCDIC code-point mismatches like "!" = 0x5A on US vs 0x4F
  on German round-trip cleanly. Wire-validated end-to-end against
  PUB400 V7R5M0: 8 KiB BLOB byte-equal byte-equal, 80 KiB streamed
  BLOB across three chunks, 1 MiB streamed BLOB across 32 chunks,
  ~8 KiB CLOB EBCDIC round-trip.
- `*gojtopen.LOBValue` public type (`driver/lob_value.go`) for
  callers who want explicit LOB-intent at the call site or need
  the streaming form (`Reader` + `Length`).
- Wire-protocol reference doc at `docs/lob-bind-wire-protocol.md`
  pinning the bind sequence (PREPARE_DESCRIBE → CHANGE_DESCRIPTOR
  → WRITE_LOB_DATA × N → EXECUTE) plus the corrected CP map for
  any future re-derivation: LOB Data is `0x381D`, truncation is
  `0x3822`, locator handles are server-allocated (not client).
- LOB streaming via `*gojtopen.LOBReader`. Opt in with the DSN
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
  surfaces them via goJTOpen-specific `Rows.ColumnTypeSchemaName`
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
  by the `PREPARE_DESCRIBE` reply's SQL type code; goJTOpen already
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
  `test/conformance/`, gated on `GOJTOPEN_TLS_TARGET`, exercises
  sign-on + start-database + PREPARE + EXECUTE + multi-row FETCH
  through TLS and (when `GOJTOPEN_DSN` is also set against the
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
  goJTOpen's existing per-`Exec` round trip is therefore wire-
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
  `GOJTOPEN_TEST_CCSID13488_TABLE=GOTEST.GOSQL_DBCLOB13488`
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
- DBCLOB streamed read (`*gojtopen.LOBReader`) halves offset and
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
- `database/sql.Driver` registration as `"gojtopen"` with DSN syntax
  `gojtopen://USER:PASSWORD@HOST[:PORT]/?key=value`.

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
