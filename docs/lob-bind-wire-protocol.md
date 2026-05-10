# LOB bind on the host-server datastream — wire protocol

This document captures what JT400 actually puts on the wire when an
application binds a BLOB / CLOB / DBCLOB column via a parameter
marker (`?`) and runs `executeUpdate()`. The findings here came out of
decoding the two captures committed to
`testdata/jtopen-fixtures/fixtures/`:

- `prepared_blob_insert.trace` — INSERT INTO ... VALUES (?, ?, ?) with
  an 8 KiB BLOB and an ~8 KiB CLOB. JT400 21.0.4 against PUB400 V7R5M0.
- `prepared_blob_insert_large.trace` — same shape but a 64 KiB BLOB.
  Captured to confirm the chunking behaviour.

Both traces were decoded with `cmd/_lobinspect`, an off-by-default
DSS+CP walker (the `_` prefix keeps it out of `go build` / `go test`).

## TL;DR

- **Locator handles are server-allocated, not client-allocated.** The
  server hands back one handle per LOB parameter marker inside the
  PREPARE_DESCRIBE reply, embedded in the Super Extended Parameter
  Marker Format (CP `0x3813`) at the per-field LOBLocator slot.
- **The bind sequence is** PREPARE_DESCRIBE → CHANGE_DESCRIPTOR
  (CP `0x381E`, declares the input SQLDA) → one WRITE_LOB_DATA
  (FUNCTIONID `0x1817`) per LOB parameter → EXECUTE (CP `0x381F`,
  carries the 4-byte locator handle in the LOB slot, not the LOB
  bytes).
- **JT400 sends each LOB in a single WRITE_LOB_DATA frame regardless
  of size.** A 64 KiB BLOB goes out as one 65 632-byte DSS frame;
  there is no automatic fragmentation. Chunking is a wire option (CP
  `0x381A` Start Offset advances frame-to-frame) but not what JT400
  default-emits — it is only needed for streamed callers where the
  driver does not have the full byte array up front.
- **Several CPs in the original implementation plan were wrong:**
  LOB Data is CP `0x381D` (not `0x381C`); LOB Truncation is CP
  `0x3822` (not `0x382B`). The corrections come straight from
  `DBSQLRequestDS.setLOBData` / `setLobTruncation` in JT400.

## Frame-by-frame walkthrough

Captured connection IDs:
- `1604011320` — sign-on (`as-signon`, server `0xE009`).
- `1008757163` — database service (`as-database`, server `0xE004`).

The LOB-relevant traffic is on the database service; everything below
refers to that connection unless noted.

### 1. SET_SQL_ATTRIBUTES (`0x1F80`)

Connection prep — CCSID, naming convention, decimal separator,
package name. Not LOB-specific. Already handled in the existing
`hostserver/db_attributes.go` codec.

### 2. CREATE_RPB (`0x1D00`)

`DBSQLRPBDS` request that creates RPB handle `1` and sets cursor name.
Not LOB-specific; required because JT400 routes a per-prepared-
statement RPB through this handle.

### 3. PREPARE_DESCRIBE (`0x1803`)

Single round trip that prepares the SQL text and asks the server to
describe both the result columns and the parameter markers.

Sent CPs of interest:
- CP `0x3831` (extended statement text in UCS-2 BE) — the SQL with
  three `?` markers.
- CP `0x3812` (statement type) = `0x0001` (`prepare`).
- CP `0x3808` (parameter marker format request flag) = `0x00`.

Reply CPs of interest:
- CP `0x3812` (Super Extended Data Format) — output column shape.
  Empty for an INSERT (no result row).
- CP `0x3813` (**Super Extended Parameter Marker Format**) — the
  shape of the input parameters, AND **the LOB locator handles**.

Layout of CP `0x3813` (per `DBSuperExtendedDataFormat`):

```
0..3    ConsistencyToken           (uint32 BE)
4..7    NumberOfFields             (uint32 BE)
8       DateFormat                 (byte)
9       TimeFormat                 (byte)
10      DateSeparator              (byte)
11      TimeSeparator              (byte)
12..15  RecordSize                 (uint32 BE; sum of bound-row data widths)
16..    fieldCount × 48-byte field records, then variable info
```

Each 48-byte field record (`REPEATED_FIXED_LENGTH_ = 48`):

```
+0..1   ?? (typically 0x0030 == 48; record self-length)
+2..3   FieldSQLType   (uint16 BE, e.g. 0x01F0 INTEGER, 0x03C1 BLOB,
                        0x03C5 CLOB, 0x03C9 DBCLOB)
+4..7   FieldLength    (uint32 BE; for LOB locator types this is 4)
+8..9   FieldScale     (uint16 BE)
+10..11 FieldPrecision (uint16 BE)
+12..13 FieldCCSID     (uint16 BE; 65535 binary, 273 PUB400 job, etc.)
+14     FieldParameterType (byte; input/output/inout)
+17..20 FieldLOBLocator (uint32 BE)  ← server-allocated handle
+26..29 FieldLOBMaxSize (uint32 BE)
+30..47 (variable info offset / reserved; field name lives in the
         trailing variable area, keyed by inner CP 0x3840)
```

Concrete example from `prepared_blob_insert.trace`, CP `0x3813`
payload (199 bytes, three fields):

| Field | SQLType | FieldLen | CCSID | LOBLocator (offset+33) |
|-------|---------|----------|-------|------------------------|
| 0: ID INTEGER     | `0x01F0` (496) | 4 | 0      | 0           |
| 1: B BLOB         | `0x03C1` (961) | 4 | 0xFFFF | **0x00000100** |
| 2: C CLOB         | `0x03C5` (965) | 4 | 0x0111 (273) | **0x00000200** |

These are the handles JT400 then puts on the wire as the WRITE_LOB_DATA
locator. The `prepared_blob_insert_large.trace` shows the same handles
(`0x100`, `0x200`) for the same parameter shape — confirming the
server allocation is deterministic per (cursor, parameter-marker
position) under PUB400 V7R5M0.

### 4. CHANGE_DESCRIPTOR (`0x1E00`)

Declares the input SQLDA the EXECUTE will reference. Carries CP
`0x381E` (Extended SQLDA descriptor, `DBExtendedDataFormat`).

CP `0x381E` layout — distinct from CP `0x3813` above:

```
0..3    ConsistencyToken
4..7    NumberOfFields
12..15  RecordSize
16..    fieldCount × 64-byte records (REPEATED_LENGTH_ = 64)
```

Each 64-byte field record (offsets relative to the record start):

```
+0..1   0x0040 (record self-length, 64)
+2..3   FieldSQLType
+4..7   FieldLength
+8..9   FieldScale
+10..11 FieldPrecision
+12..13 FieldCCSID
+14     FieldParameterType
+17..20 FieldLOBLocator    (zero in this DS — locator goes in 381F values)
+26..29 FieldLOBMaxSize
+46..47 FieldNameLength    (uint16 BE)
+48..49 FieldNameCCSID
+50..   FieldName (CCSID-encoded)
```

For our INSERT, JT400 sends three records, all with `FieldNameLength=0`
and `FieldLength=4` (including the LOB columns — the SQLDA value
for a LOB is the locator handle, not the content).

The CHANGE_DESCRIPTOR DSS template carries:
- `ParameterMarkerDescriptor` handle = 1 (matches the EXECUTE PMD).
- `RPBHandle` = 1 (the same RPB the CREATE_RPB allocated).

### 5. WRITE_LOB_DATA (`0x1817`) — one frame per LOB parameter

Sent before the EXECUTE. CP set per JT400's `JDLobLocator.writeData`:

| CP        | Type           | Meaning                                       |
|-----------|----------------|-----------------------------------------------|
| `0x3822`  | byte           | LOB Truncation Indicator. **0xF0 = truncate**, 0xF1 = don't. JT400 always sends 0xF0 for binds (`writeData(..., truncate=true)`). |
| `0x3818`  | uint32 BE      | LOB Locator Handle (the value from CP 0x3813 in the PREPARE_DESCRIBE reply). |
| `0x3819`  | uint32 BE      | Requested Size — the byte count for BLOB / character count for CLOB / character count for DBCLOB (callers feeding bytes to a DBCLOB pass `bytes/2`). |
| `0x381A`  | uint32 BE      | Start Offset (in bytes for BLOB, characters for CLOB / DBCLOB). 0 for the default single-frame send. |
| `0x381B`  | byte           | Compression Indicator. 0xF0 = no compression. |
| `0x381D`  | var            | LOB Data — `CCSID(2) + DataLength(4) + payload`. CCSID is **always 0xFFFF** on the wire even for CLOBs; the DataLength is the byte count of the payload that follows. JT400 has already pre-encoded CLOB strings into the column CCSID before placing them here. |

Important: CP `0x381D`'s payload header (`CCSID + Length`) is NOT the
same as the read-side reply CP `0x380F` even though the layout is
identical. They are separate code points.

Observed:
- 8 KiB BLOB → DSS Length 8288 bytes, CP `0x381D` len 8198
  (`6 + 8192`), CP `0x3819` = 8192.
- 8 KiB CLOB (in the `_C` slot) → DSS Length 8300, CP `0x381D` len
  8210 (`6 + 8204`), CP `0x3819` = 8204. The 8204 reflects the EBCDIC
  CCSID 273 byte count; PUB400 encodes "Hello, IBM i! " ×∞ at one byte
  per character, but the user-supplied Java string (after
  rounding to 8192+ chars) became 8204 bytes once trimmed by JT400's
  string-to-CCSID converter.
- 64 KiB BLOB → DSS Length **65 632** bytes, single frame, CP `0x381D`
  len 65 542. **JT400 does not split.**

Reply: a generic DB_REPLY (`0x2800`) with no error CPs. The OK case
carries CP `0x380E` (translation indicator) = 0 length, nothing else.

### 6. EXECUTE (`0x1805`)

Standard execute, plus CP `0x381F` (Extended SQLDA Data,
`DBExtendedData`).

Layout of CP `0x381F` (matches the existing `EncodeDBExtendedData`
encoder in `hostserver/db_prepared.go`):

```
0..3    ConsistencyToken
4..7    RowCount
8..9    ColumnCount
10..11  IndicatorSize (0 or 2)
12..15  Reserved
16..19  RowSize (sum of FieldLength across columns)
20..    Indicators: rowCount × columnCount × indicatorSize bytes
        (0 = non-null, 0xFFFF = null, per-column 16-bit)
        Then row data: rowCount × rowSize bytes.
```

For LOB-typed slots, the row-data bytes for that column are the
**4-byte server-allocated locator handle**, big-endian. Concrete
38-byte payload from `prepared_blob_insert`:

```
00 00 00 01  ConsistencyToken
00 00 00 01  RowCount = 1
00 03        ColumnCount = 3
00 02        IndicatorSize = 2
00 00 00 00  reserved
00 00 00 0c  RowSize = 12
00 00 00 00 00 00      indicators (3 cols × 2 bytes, all non-null)
00 00 00 01            row data, col 0 (ID) = 1
00 00 01 00            row data, col 1 (B BLOB) = locator 0x100
00 00 02 00            row data, col 2 (C CLOB) = locator 0x200
```

This confirms the plan's central assumption: the SQLDA value at a
LOB slot is the 4-byte locator handle, not the content.

### 7. Cleanup

JT400 closes the prepared statement with DELETE_DESCRIPTOR (`0x1E01`)
and DELETE_RPB (`0x1D02`). The locators expire automatically with the
RPB; there is no explicit FREE_LOB on the bind side.

## What this means for the Go encoder

1. **Parse the parameter marker descriptor reply.** The PREPARE_DESCRIBE
   handler must extract the per-field SQL types and LOB locator
   handles from CP `0x3813` (Super Extended Parameter Marker Format).
   Without those handles the bind path cannot work — you cannot pick
   them client-side.

2. **Add a `WriteLOBData(handle, offset, data, truncate, ...)` helper**
   in `hostserver/db_lob.go`. CP set: `0x3822` truncation,
   `0x3818` handle, `0x3819` requested size, `0x381A` offset,
   `0x381B` compression, `0x381D` data (with the 6-byte
   `CCSID(0xFFFF) + len` header).

3. **Single-frame default.** For the `[]byte`/`string` bind path,
   emit one WRITE_LOB_DATA per parameter with the full content and
   truncate=true (CP value 0xF0). Match JT400 exactly. A streaming
   path that wants chunking can emit multiple frames at advancing
   offsets with truncate=false on all but the last; the existing
   read-side already handles chunked transfer of the analogous
   `0x380F` payload, so the symmetric write side is mechanical.

4. **EncodeDBExtendedData LOB cases.** When a parameter's SQL type is
   one of `960/961/964/965/968/969`, the row-data bytes for that
   slot are the 4-byte locator handle big-endian. FieldLength in
   the descriptor must be 4. The descriptor emitted in
   CHANGE_DESCRIPTOR (CP `0x381E`) carries the LOB SQL type and
   declared CCSID (273 for the column, or whatever PREPARE_DESCRIBE
   reported); the per-field LOBLocator slot in this CP can be left
   zero (JT400 leaves it zero too — the actual handle ships in the
   value block).

5. **`*LOBValue` type for streamed bind.** Public type for the
   Reader-driven path. Length must be known in advance so the
   driver can declare the descriptor. The simple-byte path
   (`[]byte`, `string`) auto-promotes to `LOBValue{Bytes: ...}`.

6. **Truncation defaults.** JT400 always sends 0xF0 for INSERT /
   replace UPDATE binds (see `SQLBlobLocator.writeToServer` →
   `locator_.writeData(0, value_, true)`). Match that. If a future
   API ever needs append-style behaviour, expose truncate as a
   `LOBValue` field and let the caller opt out.

## Open items found while decoding

- **CLOB CCSID negotiation.** PUB400's job CCSID is 273 and the CLOB
  column's declared CCSID propagates into the parameter marker
  descriptor (CP `0x3813`, FieldCCSID = `0x0111`). JT400 pre-encodes
  the Java `String` to that CCSID before placing it in CP `0x381D`,
  and tags the data CCSID as 0xFFFF (binary). For the Go driver,
  we transcode using the existing `internal/ebcdic` table for the
  declared FieldCCSID, then send with CCSID 0xFFFF — no negotiation
  needed beyond reading FieldCCSID.

- **DBCLOB binds.** Not exercised by the captured fixtures. JDLobLocator
  divides byte-count by 2 to derive the character count for CP `0x3819`
  (the data is already UTF-16 BE). DBCLOB binds are a follow-up.

- **Multi-row batched INSERT.** CP `0x381F`'s row-count header
  supports `RowCount > 1`, but binding multiple LOB rows would need
  the server to re-allocate handles per row. Out of scope here —
  the locked-down handles in CP `0x3813` are per-prepared-statement,
  not per-row, so single-row execution is the only pattern this
  document covers.

## Plan deltas

The original plan (`~/.claude/plans/gojtopen-lob-bind.md`) listed two
CPs incorrectly and one open question that this trace closes:

| Plan said | Reality |
|---|---|
| LOB Data CP `0x381C` | LOB Data CP `0x381D`. `0x381C` is "LOB Allocate Locator Indicator" (1-byte). |
| Truncation CP `0x382B`, semantics 0xF0 false / 0xF1 true | Truncation CP `0x3822`, semantics **0xF0 truncate / 0xF1 don't**. |
| Client- vs server-allocated handle (open) | **Server-allocated**, in CP `0x3813` (PREPARE_DESCRIBE reply) at SuperExtended-relative offset 33 of each field record. |
| Client-side chunking required for >32 KB | JT400 sends one frame regardless of size. Chunking is purely optional, used only for stream-driven binds where the driver does not have the full byte array. |
