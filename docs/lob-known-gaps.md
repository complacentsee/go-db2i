# LOB support — known gaps

Companion to `docs/lob-bind-wire-protocol.md`. Captures what the
LOB layer does **not** yet do. Every gap below is something a real
caller could trip over; entries that are merely "we picked a
different default than JT400" go into the wire-protocol doc, not
here.

The four gaps are ranked by how likely they are to bite a caller
in production. None is currently a release-blocker for the
labelverification-gw use case (BLOB / CLOB on CCSID 273 columns,
single-row INSERT/UPDATE, materialised reads); other consumers
should read carefully before depending on the affected paths.

## 1. DBCLOB columns declared `CCSID 13488` (CLOSED, M7-1)

**Status.** Closed by the M7-1 commit
(`hostserver: CCSID-aware DBCLOB encode for column CCSID 13488`).
Offline tests pass; live validation against the IBM Cloud Power VS
V7R6M0 LPAR confirms both the BMP round-trip and the non-BMP
substitute behaviour against a real `DBCLOB(64K) CCSID 13488`
column.

**Symptom (pre-fix).** A surrogate-pair rune (any codepoint outside
the BMP, e.g. `𝄞 = U+1D11E`) bound to a CCSID-13488 DBCLOB column
failed server-side with SQL-330 ("character cannot be converted")
on the INSERT.

**Why.** CCSID 13488 is *strict* UCS-2 BE — surrogates are
disallowed. CCSID 1200 is UTF-16 BE and accepts surrogates. The
driver encoded all DBCLOB string binds via `unicode/utf16`
unconditionally, which produced surrogate pairs for any non-BMP
rune regardless of the column's CCSID.

**Fix.** `hostserver.bindOneLOB`'s DBCLOB string branch now
dispatches on `p.CCSID`: `1200` keeps the existing
`encodeUTF16BE` (surrogate-aware) path; `13488` routes through
the new `encodeUCS2BE` helper that BMP-checks each rune and
substitutes non-BMP runes with `U+003F` (`?`), mirroring JT400's
`SQLDBClobLocator.writeToServer` behaviour. The first substitute
event in a process emits a one-shot `slog.Warn` so callers
notice when their data is being silently transcoded; subsequent
substitutions stay quiet to avoid log spam.

A typed `*hostserver.NonBMPRuneError` and an opt-in
`encodeUCS2BEStrict` helper are also available for callers who
would rather surface a typed error than let the substitute path
silently corrupt their data. Wiring strict mode into a DSN flag
is a follow-up; today it is only reachable from package tests.

**Tested column type.** PUB400 V7R5M0 with `DBCLOB(64K) CCSID 1200`
continues to surrogate-pair round-trip
(`TestLOBDBClob/surrogate-pair_round-trip`). The fix itself is
covered by offline tests in `hostserver/db_lob_ucs2_test.go`
(`TestEncodeUCS2BE_BMP_HappyPath`, `TestEncodeUCS2BE_NonBMP_Substitute`,
`TestEncodeUCS2BE_NonBMP_StrictError`, `TestEncodeUCS2BE_BoundaryRunes`).

**Live test escape hatch.** `TestLOBDBClobCCSID13488` in
`test/conformance/conformance_test.go` exercises the substitute
path against a real CCSID-13488 column when the
`GOJTOPEN_TEST_CCSID13488_TABLE` env var is set to a
fully-qualified table name. The test recreates the table on entry
(schema is fixed: `id INTEGER` + `dc DBCLOB(64K) CCSID 13488`);
PUB400 V7R5M0 does not readily expose a target with the right
NLSS, so the test skips on free-tier targets. The IBM Cloud Power
VS V7R6M0 LPAR accepts the CREATE; live PASS recorded
2026-05-10 with `GOJTOPEN_TEST_CCSID13488_TABLE=GOTEST.GOSQL_DBCLOB13488`.

## 2. Multi-row batch INSERT via CP 0x381F `RowCount > 1` (CLOSED, M7-6)

**Status.** Closed by the M7-6 trace capture. JT400 does **not**
use CP 0x381F's `RowCount > 1` for LOB-column batched INSERTs;
its `executeBatch()` emits N separate `EXECUTE_IMMEDIATE`
(`0x1805`) frames, one per row. The captured fixture is
`testdata/jtopen-fixtures/fixtures/prepared_blob_batch.trace`
(`ps.addBatch()` × 5 + `ps.executeBatch()` against
`INSERT INTO t (ID, B, C) VALUES (?, ?, ?)`).

**Trace findings.** Per row, JT400 emits:

```
WRITE_LOB_DATA (0x1817)        upload BLOB column bytes
WRITE_LOB_DATA (0x1817)        upload CLOB column bytes
RETRIEVE_LOB_DATA (0x1816)     server roundtrip — re-allocates
                               per-marker locator handles
EXECUTE_IMMEDIATE (0x1805)     insert this row
```

The mid-batch `RETRIEVE_LOB_DATA` is the trick that makes batched
LOB binds work on a single PREPARE: it refreshes the locator
handles between rows so each row's `WRITE_LOB_DATA` writes to a
fresh handle. Without it the second row's writes would overwrite
the first row's content (locators are per-marker, not per-row).

**Implications for goJTOpen.** Today's per-`Exec` flow is
wire-equivalent to `JT400.executeBatch` for LOB-column INSERTs —
there is no special CP `0x381F` multi-row path to mirror. Callers
wanting N-row LOB batches use `db.Prepare` + N `Stmt.Exec` calls,
or multi-tuple `INSERT VALUES (?, ?), (?, ?), ...` (covered by
`TestLOBMultiRow`).

**Wire-protocol ref.** `docs/lob-bind-wire-protocol.md` "Multi-row
batched INSERT — settled" has the full byte-level walkthrough.

## 3. JT400 `lob threshold` inline-small-LOB optimisation (low)

**Symptom.** Every BLOB / CLOB / DBCLOB column INSERT pays one
extra `WRITE_LOB_DATA` round trip even when the payload is a
handful of bytes. JT400 has a connection property `lob threshold`
(default 0 / 32 KB depending on version) that, when the bound
value is below the threshold, ships the bytes inline as a regular
VARCHAR FOR BIT DATA parameter and skips the locator entirely.

**Why we don't do this.** Pre-PREPARE_DESCRIBE we don't know
which markers target LOB columns vs VARCHAR-FOR-BIT-DATA; routing
decisions happen *after* the parameter marker format reply. By
that point we'd need to re-encode the value if we want to send it
inline. Not impossible, but adds complexity for a perf win that
hasn't been measured against any real workload.

**Workaround.** Callers writing LOB-typed columns with small
content who care about latency can reshape the column to
`VARCHAR(<n>) FOR BIT DATA` (for binary) or `VARCHAR(<n>) CCSID <c>`
(for character). The existing inline-bind path (covered by
`TestBlobs`) carries those payloads in one frame.

**Fix path.** Add a `Conn.LOBThreshold` config field. In the
bind dispatcher, when `p.IsLOB() && p.LOBMaxSize <= threshold &&
len(value) <= threshold`, route through the VARCHAR FOR BIT DATA
path instead. Skips one round trip per small LOB at the cost of
metadata-driven branching.

## 5. CLOB columns ≤ 32 KB with explicit CCSID return zero rows (medium)

**Symptom.** `SELECT c FROM t` (or any projection that includes the
CLOB column) against a `CLOB(N) CCSID NNNN` column where `N <= 32K`
yields zero rows even when `SELECT COUNT(*)` and `SELECT id` confirm
rows exist. Without an explicit CCSID, or with `CLOB(1M)` and
larger, the same SELECT works.

**Reproduces.** Discovered against IBM Cloud V7R6M0 LPAR:
- `CLOB(4K) CCSID 1208` → 0 rows on SELECT
- `CLOB(32K) CCSID 1208` → 0 rows on SELECT
- `CLOB(1M) CCSID 1208` → works
- Same pattern for CCSID 37 and CCSID 273.

**Cause (suspected).** Server returns small CLOB columns inline in
the result-data stream rather than via a locator handle when the
declared maximum size fits one wire frame. The driver's result-data
parser (`hostserver/db_result_data.go`) only handles the locator-
shaped LOB column descriptor; an inline-CLOB column either drops
the row or mis-decodes it. Connected to the M7-5 `lob threshold`
work, which is about the *bind* side of the same heuristic.

**Workaround.** Declare CLOB columns at `1M` or larger when an
explicit CCSID is required. `TestCCSID1208RoundTrip/CLOB round-trip`
in the conformance suite uses `CLOB(1M) CCSID 1208` for this
reason.

**Fix path.** Capture a small-CLOB SELECT trace via `wiredump` and
compare to the locator-shaped CLOB SELECT bytes. Likely needs the
result-data column-decoder dispatch to recognise the inline-CLOB
shape and decode through `decodeLOBChars`. Tracked alongside M7-5
because the bind-side re-prepare path needs the same trace.

## 4. RLE decompression on RETRIEVE_LOB_DATA (PARTIAL, M7-7)

**Status.** Decompressor + per-CP RLE plumbing landed in M7-7;
the request-side bit (`0x00040000`) stays OFF until the
whole-datastream wrapper (CP `0x3832`) is implemented. The
remaining work is a parser-layer change, not a wire-format
discovery.

**What landed (M7-7).**
- `decompressRLE1` in `hostserver/db_lob.go` — Go port of
  JT400's `JDUtilities.decompress`. RLE-1 wire format
  (`0x1B value count(4)` runs + `0x1B 0x1B` escaped literals)
  fully covered by offline tests in `db_lob_rle_test.go`,
  including the 4 KiB and 1 MiB constant-content cases that
  motivated the original gap.
- `parseLOBReply` now reads the CP 0x380F header's `actualLen`
  field and routes payloads where `len(payload) != actualLen`
  through the decompressor. This is the per-CP RLE path JT400
  reaches via `DBLobData(actualLength, physicalLength,
  oldCompressed)`. Graphic-LOB length doubling (CCSID 13488 /
  1200) is handled.

**What's deferred.** Re-enabling the request-side RLE bit on
`RetrieveLOBData` makes the V7R6 server wrap the *entire reply*
in CP `0x3832` (the `DATA_COMPRESSION_RLE_` code point per
JT400's `AS400JDBCConnection`), not the per-CP shape M7-7's
`parseLOBReply` is wired for. CP `0x3832` carries:

```
  4 bytes ll          (compressed payload length + 10)
  2 bytes CP          (0x3832)
  4 bytes decompLen   (uncompressed payload length)
  ll-10 bytes payload (RLE-1 compressed)
```

After decompression the bytes form the *original* CP-formatted
reply (CP `0x380F` plus any companion CPs). To re-enable the
bit safely, `ParseDBReply` (or a wrapper) needs to:
1. Detect `dataCompressed = (template[4] & 0x80) != 0` on the
   reply (matches JT400's `DBBaseReplyDS.parse` check).
2. When set, find the CP `0x3832` block, decompress its payload
   via `decompressRLE1`, and re-feed the decompressed bytes to
   `ParseDBReply` so downstream parameter walks see CP `0x380F`
   normally.

**Workaround.** None needed for correctness; LOB reads work
without compression. Repetitive-content workloads see a
bandwidth ceiling, not a correctness one.

**Verification once enabled.** Round-trip a 1 MiB BLOB of
identical bytes on the wire (capture with `wiredump`) — wire
bytes should drop from ~1 MiB to under 100 bytes when
compression activates. Existing offline test
`TestParseLOBReply_RLECompressed4KOfCC` already pins the
per-CP path; add a CP `0x3832` whole-datastream replay test
when the parser change lands.

---

## Non-gaps (working as designed; documented for clarity)

These read like gaps but aren't:

- **`db.QueryRow(...).Scan(&LOBReader)` returns garbled data.**
  This is documented in `LOBReader`'s godoc: the producing cursor
  must stay open while the reader is in use, and `QueryRow`
  auto-closes on `Scan`. Use `db.Query(...).Next()` + `Scan(&r)`
  and defer `rows.Close()`.
- **DBCLOB DDL fails on PUB400 user profiles without DBCS NLSS.**
  `CREATE TABLE ... DBCLOB(64K) CCSID 1200` requires DBCS-capable
  NLSS configuration. `TestLOBDBClob` skips (rather than fails)
  on the create error. Driver issue: none.
- **DBCLOB streamed reads/writes need 2-byte-aligned chunks.**
  Default `LOBStreamChunkSize = 32 KiB` is even; the chunk-loop
  rounds down to even bytes if a caller-supplied `Reader` ever
  returns an odd count. Documented in `bindOneLOB`'s LOBStream
  branch.
- **Append-style writes (`truncate=false`).** All exposed bind
  paths use truncate=true (full replace). The streamed
  `LOBStream` path uses truncate=false on intermediate
  WRITE_LOB_DATA frames but truncate=true on the final, so the
  visible behaviour is still full replace. JT400 makes the
  same default choice; an opt-out lives behind a future
  `LOBValue.Append` field if anyone asks for it.
