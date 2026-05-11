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
`DB2I_TEST_CCSID13488_TABLE` env var is set to a
fully-qualified table name. The test recreates the table on entry
(schema is fixed: `id INTEGER` + `dc DBCLOB(64K) CCSID 13488`);
PUB400 V7R5M0 does not readily expose a target with the right
NLSS, so the test skips on free-tier targets. The IBM Cloud Power
VS V7R6M0 LPAR accepts the CREATE; live PASS recorded
2026-05-10 with `DB2I_TEST_CCSID13488_TABLE=GOTEST.GOSQL_DBCLOB13488`.

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

**Implications for go-db2i.** Today's per-`Exec` flow is
wire-equivalent to `JT400.executeBatch` for LOB-column INSERTs —
there is no special CP `0x381F` multi-row path to mirror. Callers
wanting N-row LOB batches use `db.Prepare` + N `Stmt.Exec` calls,
or multi-tuple `INSERT VALUES (?, ?), (?, ?), ...` (covered by
`TestLOBMultiRow`).

**Wire-protocol ref.** `docs/lob-bind-wire-protocol.md` "Multi-row
batched INSERT — settled" has the full byte-level walkthrough.

## 3. JT400 `lob threshold` inline-small-LOB optimisation (CLOSED, M7-5)

**Status.** `?lob-threshold=N` DSN knob lands the threshold into the
SET_SQL_ATTRIBUTES CP `0x3822` parameter, mirroring JT400's
`JDProperties.LOB_THRESHOLD`. The server then drives the
inline-vs-locator decision per LOB column it ships back, and the
result-data parser handles either shape (the second half lived in
gap §5 below, closed by the same commit).

**What the trace settled.** The `prepared_blob_threshold` fixture
case captures JT400 with `lob threshold=32768` against V7R6M0
inserting a 32-byte BLOB + 256-byte CLOB into `BLOB(64K)` +
`CLOB(4K) CCSID 1208` columns. The wire pattern vs the default
`prepared_blob_insert` fixture (same payload mix, no threshold
override):

|                              | `prepared_blob_insert` | `prepared_blob_threshold` |
|------------------------------|------------------------|---------------------------|
| `WRITE_LOB_DATA` (bind side) | 2                      | **1**                     |
| `RETRIEVE_LOB_DATA` (read)   | 4                      | **2**                     |

JT400 inlined one of the two LOBs at the EXECUTE stage and the
server inlined both LOB columns in the SELECT-back result data --
halving the wire-frame count for the read side. The bind-side
inline shape is driven by the `PREPARE_DESCRIBE` reply (server
describes the LOB param as inline VARCHAR-shape, type 404/405 for
BLOB or 408/409 for CLOB, instead of the locator 960..969 types),
not by client-side size checks. go-db2i already follows the
server's chosen shape on bind, so wiring `LOBThreshold` through to
CP `0x3822` was sufficient.

**Implementation.** `driver.Config.LOBThreshold` (`uint32`) plumbed
through to `hostserver.DBAttributesOptions.LOBThreshold`. Zero
falls back to the historical 32768 default so the wire shape
matches the pre-M7-5 capture. Set via DSN:

```
db2i://USER:PWD@host/?lob-threshold=65536
```

Server caps at 15728640 per JT400's
`AS400JDBCConnectionImpl.setLOBFieldThreshold`; values above that
are accepted by the driver (the server clamps).

**Live evidence.** `TestCCSID1208RoundTrip/CLOB small inline` PASS
on V7R6M0 with the default threshold (32768 → server inlines
`CLOB(4K) CCSID 1208`). With `?lob-threshold=1` (forces locator
path even for tiny LOBs), the same subtest still PASS via the
locator code path -- both shapes round-trip the same content.

## 5. CLOB columns ≤ 32 KB with explicit CCSID return zero rows (CLOSED, M7-5 + bug #14)

**Status.** Fixed by the M7-5 result-data inline-LOB type
dispatch. The server returns small LOB columns inline as SQL
types 404/405 (BLOB), 408/409 (CLOB), and 412/413 (DBCLOB)
when the connection-level `lob threshold` covers them; the
parser now decodes those shapes alongside the locator
(960..969) shapes.

**Root cause.** The result-data column decoder
(`hostserver.decodeColumn`) only matched the locator SQL types.
Inline LOB types reached the `default` arm and surfaced as
`unsupported SQL type 409 (col len=4100, ccsid=1208)`, dropping
the row before the next column was decoded. Pre-fix reproducer:

```
CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, c CLOB(4K) CCSID 1208);
INSERT INTO t VALUES (1, 'hello-small-clob-bug14');
SELECT c FROM t WHERE id=1;   -- driver error, 0 rows
```

**Wire format.** Inline LOB columns ship as a 4-byte BE
actual-length prefix followed by `length` bytes of payload,
slot-padded to `col.Length`. Mirrors JT400's
`SQLBlob.convertFromRawBytes`, `SQLClob.convertFromRawBytes`,
and `SQLDBClob.convertFromRawBytes`. BLOB returns `[]byte`,
CLOB decodes through the column CCSID (1208 = UTF-8
passthrough, 273 = German EBCDIC, others = CCSID 37 fallback),
DBCLOB decodes through the UCS-2 BE / UTF-16 BE graphic
helper.

**Live evidence.** `TestCCSID1208RoundTrip/CLOB small inline`
on V7R6M0 (PASS, 0.10s). The full LOB conformance suite
(`TestLOBBlob/*`, `TestLOBDBClob/*`, `TestLOBSelectBind/*`,
`TestLOBUpdate/*`, `TestLOBMultiRow`, `TestLOBClob/string`)
remains green; the pre-existing `TestLOBClob/[]byte_pre-encoded_CCSID_273`
failure is tracked separately as bug #15 (CCSID 273 byte-mode
codec asymmetry).

## 4. RLE decompression on RETRIEVE_LOB_DATA (CLOSED, M7-7)

**Status.** Both halves landed.

**M7-7 partial (commit `f288b31`).**
- `decompressRLE1` in `hostserver/db_lob.go` — Go port of
  JT400's `JDUtilities.decompress`. Per-CP RLE-1 wire format
  (`0x1B value count(4)` runs + `0x1B 0x1B` escaped literals,
  6-byte record).
- `parseLOBReply` reads the CP 0x380F header's `actualLen`
  field and routes payloads where `len(payload) != actualLen`
  through the decompressor. Graphic-LOB length doubling
  (CCSID 13488 / 1200) is handled.

**M7-7 residual.** Re-enabled the request-side RLE bit
(`0x00040000`) on `RetrieveLOBData`. The V7R6 server now wraps
the entire reply in CP `0x3832` whenever it judges compression
worthwhile; `ParseDBReply` unwraps transparently.

The whole-datastream wrapper uses a **different RLE format**
than the per-CP variant we landed in M7-7 partial. JT400
sources confirm:
- Per-CP RLE-1 (`JDUtilities.decompress`): 6-byte record
  `0x1B value count(4_BE)`, emits `count` bytes.
- Whole-datastream RLE-1 (`DataStreamCompression.decompressRLEInternal`):
  5-byte record `0x1B pattern_b1 pattern_b2 count(2_BE)`,
  emits `2*count` bytes (2-byte pattern × count iterations).

New `decompressDataStreamRLE` in `hostserver/db_lob.go`
implements the whole-datastream form; `ParseDBReply` checks
`payload[4:8] & 0x80000000` (the JT400 `dataCompressed` bit)
and unwraps the CP `0x3832` parameter before walking the
parameter list. Offline coverage:
- `TestDecompressDataStreamRLE_RoundTrip/*` — passthrough,
  escaped 0x1B, 4 KiB constant-content run, zero-pattern fast
  path, truncated repeater, overflow.
- `TestParseDBReplyUnwrapsCP3832` — end-to-end synthetic
  wrapped reply with CP 0x380F + CP 0x3810 inner params; the
  decoded `DBReply.Params` byte-match the uncompressed
  equivalent.

**Live evidence (V7R6M0, 2026-05-10).** 1 MiB constant-content
0xCC BLOB round-trip via `stress-test/rlemeasure`:

| metric                          | value                |
|---------------------------------|----------------------|
| payload (decompressed)          | 1,048,576 bytes      |
| `rx_bytes` diff on eth0         | **1,228 bytes**      |
| SELECT wall time                | 18.5 ms              |
| compression ratio (upper bound) | ~854× shrink         |

The `rx_bytes` diff includes TCP/IP headers and the request
side of the round trip, so the actual compressed CP `0x3832`
payload is well under 1 KiB for a 1 MiB constant-content LOB.
`TestLOBMultiRow` (4 KiB 0xCC BLOBs × 5 rows) is the
regression gate — pre-fix it failed with a truncated-RLE
error against the per-CP decompressor; post-fix it PASSes.

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
