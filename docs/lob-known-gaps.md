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

## 1. DBCLOB columns declared `CCSID 13488` (high)

**Symptom.** A surrogate-pair rune (any codepoint outside the BMP,
e.g. `𝄞 = U+1D11E`) bound to a CCSID-13488 DBCLOB column fails
server-side, typically with SQL-330 ("character cannot be
converted") on the INSERT.

**Why.** CCSID 13488 is *strict* UCS-2 BE — surrogates are
disallowed. CCSID 1200 is UTF-16 BE and accepts surrogates. Our
driver encodes Go strings via `unicode/utf16` which produces
surrogate pairs for any non-BMP rune, then ships the bytes with no
column-CCSID check.

**Tested column type.** PUB400 V7R5M0 with `DBCLOB(64K) CCSID 1200`
— surrogate-pair round-trips work
(`TestLOBDBClob/surrogate-pair_round-trip`). CCSID 13488 is
**untested** because PUB400 doesn't readily expose a CCSID-13488
target.

**Workaround.** Callers targeting CCSID 13488 columns must filter
their input strings to BMP-only (`r <= 0xFFFF` for every rune)
before binding. We don't surface a `LOBValue.RestrictToBMP` knob
yet.

**Fix path.** Detect `p.CCSID == 13488` in
`hostserver.bindOneLOB`'s string branch and either reject
surrogates with a typed error, or fall back to UCS-2 substitute
(`U+003F`) for non-BMP runes — JT400's behaviour. Tracked as a
follow-up in `docs/PLAN.md` M7.

## 2. Multi-row batch INSERT via CP 0x381F `RowCount > 1` (medium)

**Symptom.** The driver does not currently expose a batched
LOB-INSERT path that would set CP 0x381F's `RowCount` field to
N > 1 in a single EXECUTE.

**What works today.** Multi-tuple `INSERT INTO t (id, b) VALUES
(?, ?), (?, ?), (?, ?)` works because the server allocates an
independent locator handle per `?` marker — confirmed live by
`TestLOBMultiRow`. So a caller wanting to insert N rows in one
round trip can use N×K parameter markers in a single statement.

**Why CP 0x381F batching probably doesn't work.** Per the wire-
protocol findings (`docs/lob-bind-wire-protocol.md`), locator
handles in CP 0x3813 (parameter marker format) are allocated per
*marker position*, not per row. A multi-row CP 0x381F bind would
have only one locator per LOB column to write to across all N
rows; previous WRITE_LOB_DATA frames would be overwritten by the
last one. We have not confirmed this on the wire — there might
be a handle-cycling protocol — but neither JT400 nor goJTOpen
currently exercise it.

**Workaround.** Multi-tuple VALUES (above) for a single Exec.
For N independent INSERTs, `db.Prepare` + N `Stmt.Exec` calls
gives N round trips but reuses the same prepared statement.

**Fix path.** Trace JT400's batchExecute() with LOB columns to
see what it actually emits — likely N single-row EXECUTEs rather
than one multi-row one. Document the canonical pattern in
`docs/lob-bind-wire-protocol.md`.

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

## 4. RLE decompression on RETRIEVE_LOB_DATA (low)

**Symptom.** LOB reads currently disable the RLE bit
(`0x00040000`) in the request's ORS bitmap. The server therefore
ships the raw bytes regardless of repetition. A 1 MiB BLOB of
zeros ships as 1 MiB on the wire instead of the few bytes RLE
would compress it to.

**Why disabled.** Re-enabling RLE caused the parser to misread
constant-content LOBs (a 4 KiB run of 0xCC came back as 0 bytes).
The fix is to implement RLE decompression for the CP 0x380F
payload, but that path was never exercised in the LOB-bind work,
so we conservatively drop the bit. Documented in
`hostserver/db_lob.go` next to `RetrieveLOBData`.

**Workaround.** None needed for correctness. For bandwidth-
sensitive workloads against repetitive LOB content, the user
can negotiate compression at a lower layer (TLS compression,
SSH compression) — but those have their own pitfalls and are
generally discouraged.

**Fix path.** Mirror JT400's `JDUtilities.decompress` in
`hostserver`. The wire format is documented in IBM's
"DDS for Database Files" reference — RLE-1 with a marker byte +
run-length + value triplet, repeated. Once decompression lands,
flip the ORS bit back on and verify
`TestLOBContentMatrix2`-style constant-content cases stay green.

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
