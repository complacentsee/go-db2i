<!--
Source-cited JTOpen array wire-format spec for go-db2i issue #68.

DERIVED by the `jtopen-array-wire-spec` multi-agent workflow from the JTOpen
21.0.4 source at ~/JTOpen (2026-06-07), then LIVE-CONFIRMED in Phase 0 against
PUB400 V7R5M0 (see array_param_phase0.md and the fixtures here). 34/36 atomic
claims were adversarially confirmed against source; 2 were refuted as scoping
nuances on the DESCRIBE side (Phase 1/3 territory), NOT the 0x382F/0x3901 data
layout this file's sections 2 and 5 describe:
  - indicator width "never 1 byte" holds only within the array codec
    (DBVariableData), not universally.
  - the CHANGE_DESCRIPTOR array flag is NOT simply "elementType | 0x0001";
    array-ness rides the super-extended +21 field-flags bit 30 (see the
    existing hostserver/testdata/array_param_describe_3813.bin + its test).
Treat sections 2 (0x382F) and 5 (0x3901) as live-confirmed; treat sections 3
(CHANGE_DESCRIPTOR) and 4 (0x3813 describe) as source-derived guidance to
re-confirm when Phase 1/3 land.
-->

# JTOpen ARRAY Stored-Procedure Parameter Wire Format — Authoritative Spec

Scope: DB2 for IBM i (V6R1+, ZDA host server). Arrays cross the wire **only** as stored-procedure parameters (IN/OUT/INOUT) — never as result-set columns. Single-dimension element arrays only (`rowCount_` is always 1).

**Byte order: big-endian (network order) for every multi-byte field.** Verified in `BinaryConverter.java:36-42` (shortToByteArray: `serverValue[offset]=(byte)(v>>>8)`) and `:76-83` (intToByteArray: high byte first). `short` = 2 bytes BE, `int` = 4 bytes BE throughout.

---

## 1. Codepoint selection

When the parameter row contains **any** array parameter, the IN/INOUT marker data is sent as CP **0x382F** (`DBVariableData`) instead of the scalar CP 0x381F (`DBExtendedData`). The OUT/INOUT values come back as CP **0x3901** (`DBVariableData`) instead of scalar CP 0x380E (`DBExtendedData`).

| Direction | Scalar CP | Array CP | Object |
|---|---|---|---|
| IN/INOUT request marker data | 0x381F | **0x382F** | DBVariableData |
| OUT/INOUT reply data | 0x380E | **0x3901** | DBVariableData |

- Discriminator is the row-level flag `parameterRow_.containsArray_` (set at PREPARE time, not at execute), `AS400JDBCPreparedStatementImpl.java:931-948`.
- `containsArray_` is set when any field's `(sqlType & 0xFFFE) == SQLData.NATIVE_ARRAY` (10000), `JDServerRow.java:217-234`.
- CP follows from object class via `instanceof` in `DBSQLRequestDS.java:363-368`.
- 0x382F and 0x3901 are **overloaded** numeric values elsewhere in the package (0x382F = "Statement Text Length To Return" in `DBReturnObjectInformationRequestDS`; 0x3901 = "base table name" sub-CP in `DBColumnDescriptorsDataFormat`). The array meaning applies only in `DBSQLRequestDS.setParameterMarkerData*` (request) and the `DBBaseReplyDS` top-level reply switch.

---

## 2. CP 0x382F — IN/INOUT request payload

`overlay(data, offset)` is called with `offset` = start of the 0x382F **payload** (the 6-byte CP framing — 4-byte LL + 2-byte CP — is already stripped by the caller). Within `overlay`, `offset_ = offset` and the header writer starts at `offset_ + actual382fInArrayStart_` where `actual382fInArrayStart_ = 4` (`DBVariableData.java:87, 249`).

Three contiguous regions in fixed order: **HEADER → INDICATORS → DATA**.

```
+0    Consistency token .......................... 4 bytes
+4    Count of 9911/9912 descriptors (N) ......... 2 bytes (short)   [setColumnCount, :354]
+6    Descriptor[0] .............................. 12 / 4 / 8 bytes (per shape below)
      Descriptor[1] ...
      ...Descriptor[N-1]
      ---- INDICATOR region (all indicators, contiguous) ----
      ---- DATA region (all data bytes, contiguous) ----
```

Region offsets, `DBVariableData.java:249-253`:
- header starts at `offset_ + 4`
- indicator region starts at `offset_ + 4 + headerTotalSize`
- data region starts at `indicatorRegionStart + indicatorTotalSize`

The descriptor **count N counts only INPUT parameters** (the `DBVariableData` ctor is sized with `parameterInputCount_`, not `parameterCount_`); OUT-only params are excluded from the 0x382F stream, `AS400JDBCPreparedStatementImpl.java:933-934, 789-790`.

### 2.1 Descriptor shapes (written by `setHeaderColumnInfo`, `DBVariableData.java:382-450`)

One descriptor per INPUT column, in column order. The tag discriminates array vs scalar; `indicatorValue` discriminates non-null vs null array.

**(a) Non-null ARRAY — 12 bytes** (`type==NATIVE_ARRAY && indicatorValue==0`):
```
+0  0x9911 ................. 2 bytes (tag)
+2  element SQL data type .. 2 bytes (short)   ← the REAL element type, NOT 10000
+4  element data length ..... 4 bytes (int)    ← per-element byte stride
+8  array length (cardinality) 4 bytes (int)   ← element count
```

**(b) NULL whole-array — 4 bytes** (`type==NATIVE_ARRAY && indicatorValue!=0`):
```
+0  0x9911 ................. 2 bytes (tag)
+2  array null indicator ... 2 bytes (= 0xFFFF; written from indicatorValue)
```
No type/length/cardinality, **no indicator-region entries, and no data-region bytes** for this column.

**(c) SCALAR (non-array) — 8 bytes** (`type != NATIVE_ARRAY`):
```
+0  0x9912 ................. 2 bytes (tag)
+2  SQL data type .......... 2 bytes (short)
+4  data length ............ 4 bytes (int)
```
No cardinality field.

### 2.2 INDICATOR region

- Indicator width is **always 2 bytes** (`indicatorSize_ = 2`; `setIndicatorSize` is a no-op), `DBVariableData.java:139, 471-473`.
- Indicators are grouped **per-column, then per-element**, as one contiguous block preceding the entire data region — never interleaved with data.
- A non-null array contributes **`arrayLen` indicators** (2 bytes each), one per element; written element-by-element by iterating `SQLArray.isElementNull(e)`, `AS400JDBCPreparedStatementImpl.java:1128-1139`.
- A scalar contributes **1 indicator** (2 bytes).
- A null whole-array contributes **0 indicators** (its null is carried in the header descriptor, not here), `:1052-1063`.
- Indicator values: `0` = not null, `-1` (0xFFFF) = null element.

### 2.3 DATA region — element stride is FIXED, not length-prefixed

For an array column, total data bytes = `arrayLen * elementDataLen` (a contiguous block of `arrayLen` fixed-width slots), `DBVariableData.java:202, 207`. `SQLArray.convertToRawBytes` writes each element then advances `offset += elemDataTypeLen_` **regardless of the element's actual content size** — the per-value `getActualSize()` is explicitly commented out, `SQLArray.java:106-119`.

**VARCHAR / variable-length elements use a fixed max-length stride (RESOLVED — not length-packed):**
- Each VARCHAR element writes a 2-byte **length-in-characters** prefix at `offset`, character bytes at `offset+2`, and zero-fills the remainder up to `maxLength_`, `SQLVarcharBase.java:142, 160-162, 174-178`.
- The slot width is `maxLength_ + 2` (the full field width), and `elemDataTypeLen_` equals that field length, so the array advances by the same constant for every element.
- The 2-byte prefix is **informational to the receiver** (tells the server the real data length) but is **not** used to pack/shorten the stride. SQLArray never reads the prefix to compute the next offset.
- A null element still occupies its full `elemDataTypeLen_` slot (the content template's bytes are written into it); only the per-element 2-byte indicator marks it null, `SQLArray.java:111-118`.

VLF / variable-field-insert compression is **force-disabled** for any row containing arrays; 0x382F array streams are never VFC-compressed (`isVariableFieldsCompressed()` returns false), `AS400JDBCPreparedStatementImpl.java:938-941`, `DBVariableData.java:483-486`.

### 2.4 Encoding cases summary (IN side)

| Case | Header | Indicators | Data |
|---|---|---|---|
| Whole-null array | 0x9911 + 0xFFFF (4 B) | none | none |
| Empty array (len 0) | 0x9911 + type + len + **0** (12 B) | none (0 elements) | none |
| Populated array (len N) | 0x9911 + type + len + N (12 B) | N × 2 B | N × elementDataLen |
| Array of all-nulls (len N) | 0x9911 + type + len + N (12 B) | N × 2 B (all 0xFFFF) | N × elementDataLen (slots still written) |
| Scalar | 0x9912 + type + len (8 B) | 1 × 2 B | 1 × len |

Empty array: `set()` coerces a null Java `getArray()` to `new Object[0]`; the convert loop runs 0 times, `SQLArray.java:141-146`. Empty and array-of-nulls are distinguished from whole-null by the 12-byte header (cardinality + per-element indicators), not by header shape.

---

## 3. CHANGE_DESCRIPTOR (parameter-marker format) for array params

Sent alongside / before the data stream (the parameter-marker `DBDataFormat`, typically CP 0x3813 super-extended on V5R4+).

- The marker's **SQL type** for an array is `elementNativeType | 0x0001` (low bit = array flag); scalars are `nativeType | 0x0001`. ZDA infers array-ness from this flag bit, `AS400JDBCPreparedStatementImpl.java:556-563` (comment: "arrays sent in as the element type and zda will know they are arrays").
- The marker's **FieldLength** is the **per-element** length = `parameterLengths_[i] / arrayLen` (NOT the whole-array length, NOT the cardinality), `:535-541`.
- **Cardinality is carried only in the 0x382F data-stream 9911 descriptor** (`# of data items`, the +8 int), never in the change descriptor.
- Total wire byte length for the array param = `elementSize * arrayLen` where `elementSize = parameterRow_.getArrayDataLength(i+1)`; indicator total adds `arrayLen * 2`, `:826-839`.
- Because array length can change per execute, the descriptor/size computation re-runs on **every** execute when `containsArrayParameter_`, `:804-805`.

---

## 4. CP 0x3813 describe confirmation (`DBSuperExtendedDataFormat`)

Per-field record layout. Format header `FIXED_LENGTH_ = 16` bytes; per-field stride `REPEATED_FIXED_LENGTH_ = 48` bytes. Field 0's record base = `offset_ + 16`. All offsets below are **absolute** (`offset_ + ABS + fieldIndex*48`); the relative-to-record-base value is `ABS - 16`. (Source: `DBSuperExtendedDataFormat.java:28-56, 68-69` + getters.)

| Field | Abs offset | Rel | Width | Getter / line |
|---|---|---|---|---|
| field description LL | 16 | 0 | 2 B | (record base) |
| element/field SQL type | 18 | +2 | 2 B short | getFieldSQLType :155-156 |
| field length (= **element** length for arrays) | 20 | +4 | 4 B int | getFieldLength :161-162; getArrayFieldLength :165-170 |
| field scale | 24 | +8 | 2 B | — |
| field precision | 26 | +10 | 2 B | — |
| field CCSID | 28 | +12 | 2 B **unsigned** short | getFieldCCSID :189-190 |
| parameter type (IN/OUT/INOUT byte) | 30 | +14 | 1 B signed | getFieldParameterType :195; setter :414 |
| field LOB locator | 33 | +17 | 4 B int | getFieldLOBLocator :200-201 |
| **field flags (array/XML bits)** | 37 | +21 | read as 4 B int (BE) | getArrayType :220-221; getXMLCharType :209 |
| field max cardinality of array | 38 | +22 | 4 B int (doc only) | **no runtime getter** |
| field LOB max size | 42 | +26 | 4 B int | getFieldLOBMaxSize :231-232 |

- **Array bit:** read 4-byte BE int at abs 37, then `(flag >> 30) & 1`, `:220-225`. The flags are "actually only 1 byte" (the high byte at abs 37); a byte-equal port may read `byte[37]` and test `(b>>6)&1`. XML doublebyte bit is `(flag >> 27) & 1`.
- **Element length** for an array parameter is read at **abs 20** (same slot as ordinary field length), with the source caveat `/* for now, this is in same position */`, `:165-170`.
- **Max-cardinality at abs 38 is documented in the header comment only** — **no JT400 runtime code reads abs 38.** A Go port reading cardinality at abs 38 / rel 22 follows the comment, not any JT400 runtime behavior, and **must** be validated against a live CP 0x3813 capture (see claims). The array/LOB-specific fields physically overlap within the 48-byte record; disambiguation is the array bit alone, with no union tag — both getters read their fixed offsets unconditionally.

---

## 5. CP 0x3901 — OUT/INOUT reply payload

`overlay(data, offset+6)` is called from `DBBaseReplyDS.java:944` — the 6-byte CP framing is stripped, so `offset_` = start of the 0x3901 payload.

### 5.1 CRITICAL doc-vs-code disagreement (RESOLVED — prefer the CODE reading)

The class doc-block (`DBVariableData.java:39-51`) and inline comment (`:164`) describe a **20-byte preamble** before useful data:
```
Consistency token 4 / Row Count 4 / Column Count 2 / Indicator Size 2 / RESERVED 4 / Row Size 4 / Consistency token 4   ← per DOC = 20 bytes, then count
```
But the **runtime code does NOT honor a 20-byte skip.** `actual3901OutArrayStart_ = 4` and the column count is read at `offset_ + 4` (`:165`), with the first descriptor at `offset_ + 6` (`:179`). So the code treats the payload identically to 0x382F: 4-byte consistency token, 2-byte count at +4, descriptors at +6.

**Authoritative reading (prefer code): the 0x3901 payload that `DBVariableData` parses begins with a 4-byte consistency token, then the 2-byte descriptor count at +4.** A 20-byte preamble would only be consistent with the doc if the caller passed an `offset` already advanced 16 bytes past the LL/CP-stripped start — but the caller passes exactly `offset+6` (`:944`), with no extra +16. **This is the single highest-risk item for a Go port and MUST be confirmed by live capture** (it determines whether the parser lands on the count at payload+4 or payload+20).

### 5.2 Parsed layout (code reading)

```
+0    Consistency token .......................... 4 bytes
+4    Count of 9911/9912 descriptors (M) ......... 2 bytes (short)   [:165]
+6    Descriptor[0..M-1] ......................... 12 / 4 / 8 bytes each
      ---- INDICATOR region (all indicators) ----
      ---- DATA region (all data bytes) ----
```
- `headerLength` is computed dynamically as `colDescs - offset_` after consuming all descriptors (sizes vary 12/4/8), `:232`. There is no fixed header size.
- indicator region start = `offset_ + headerLength`; data region start = that + total indicator bytes, `:238-239`.
- `rowCount_` is always 1; `getRowDataOffset` ignores rowIndex, `:166, 313-318`.

### 5.3 Descriptors parsed (`overlay` output branch, `:183-228`)

- **0x9911 non-null array (12 B):** tag(2) + datatype(2, parsed but unused for type — real type comes from describe format) + dataLen(int @ colDescs+4) + arrayLen(int @ colDescs+8). Sets `indicatorCountsFromHost_[i]=arrayLen`, `arrayDataLengthsFromHost_[i]=dataLen`, `totalDataLengthsFromHost_[i]=arrayLen*dataLen`. Advance 12.
- **0x9911 null array (4 B):** detected when the 2 bytes at colDescs+2 == 0xFFFF. Sets `indicatorCountsFromHost_[i]=0`, `totalDataLengthsFromHost_[i]=0`, `dataIsNullArrayFromHost_[i]=-1`. Advance 4. No indicators, no data for this column.
- **0x9912 scalar (8 B):** tag(2) + datatype(2) + dataLen(int @ colDescs+4). 1 indicator, dataLen data bytes. Advance 8.

The reply carries the array **element count and per-element byte length inline** (from the 9911 descriptor) — not solely from the describe format. The element/column **SQL type** comes from the describe-side server format (the reply's datatype field is read only to test 0xFFFF for whole-null), `:184-225` + `JDServerRow.java:702-708`.

### 5.4 OUT-only column count + IN-skip remap

The reply contains **only OUT/INOUT parameters** (`NOTE 1`, `:53`): 3 IN + 2 OUT/INOUT → count = 2. `JDServerRow.getVariableOutputIndex(index)` maps a JDBC 0-based field index to an OUT-only column index by counting OUT params in `1..index+1`, applied **only** when `serverData_ instanceof DBVariableData`, `JDServerRow.java:704, 761-770, 820-823`. The scalar 0x380E path uses the raw all-parameter column index with no remap.

### 5.5 Indicator read semantics (reply)

- Width always 2 bytes. Per-element indicator at `indicatorOffsetFromHost_ + indicatorOffsetsFromHost_[col] + arrayIndex*2`; column-level (arrayIndex == -1) for a scalar at `indicatorOffsetFromHost_ + indicatorOffsetsFromHost_[col]`, `DBVariableData.java:503-528`.
- Indicator values: **-1 (0xFFFF) = NULL**, **-2 (0xFFFE) = data-mapping error** (also treated as NULL). Both surface NULL via `isNull`; `-2` is additionally reported by `isDataMappingError`, `JDServerRow.java:791-796, 823-826`. Per-element -1 or -2 → `SQLArray.setElementNull(i)`, `:713-717`.
- Whole-array null is signaled at the column level (header 0xFFFF → `dataIsNullArrayFromHost_=-1`), distinct from per-element nulls.

### 5.6 Parameter-direction byte values

From the describe-format parameter-type byte (`& 0x00FF`): **0xF0 = IN-only, 0xF1 = OUT-only, 0xF2 = INOUT**. isOutput → F1|F2; isInput → F0|F2, `JDServerRow.java:969, 1002-1003`.

---

## 6. Java client API recipe (compilable sketch)

Constraints enforced by JT400:
- `setArray` is legal **only** on a procedure CALL (`sqlStatement_.isProcedureCall()`), else `EXC_PARAMETER_TYPE_INVALID`, `AS400JDBCPreparedStatementImpl.java:2352-2368`.
- `createArrayOf(typeName, elements)` — `typeName` is the **ELEMENT** type name (a JDBC built-in type-name string, e.g. "INTEGER", "VARCHAR"), resolved via `JDUtilities.getTypeCode`; unknown → `EXC_DATA_TYPE_MISMATCH`, "NULL" → `EXC_DATA_TYPE_INVALID`. It is **not** the SQL array-UDT name. `AS400JDBCConnectionImpl.java:5637-5641`, `AS400JDBCArray.java:87-88`.
- `registerOutParameter(idx, Types.ARRAY)` — typeName arg (3-arg form) is ignored on IBM i; just records the registered type. `getArray` later requires registered type ∈ {ARRAY, JAVA_OBJECT}. `AS400JDBCCallableStatement.java:2458-2459, 2490, 407-414`.
- INOUT array = call **both** `setArray(idx, v)` and `registerOutParameter(idx, Types.ARRAY)` on the same index.

```java
// Server-side DDL (issue once; host syntax — validate live on PUB400):
//   CREATE TYPE MYLIB.INTARR AS INTEGER ARRAY[100]
//   CREATE PROCEDURE MYLIB.ARRPROC
//     (IN P_IN MYLIB.INTARR, OUT P_OUT MYLIB.INTARR, INOUT P_BOTH MYLIB.INTARR)
//     LANGUAGE SQL ...

// (a) build array values — typeName is the ELEMENT type name
Integer[] elems = { 10, 20, 30 };
java.sql.Array inArr   = conn.createArrayOf("INTEGER", elems);
java.sql.Array bothArr = conn.createArrayOf("INTEGER", new Integer[]{ 1, 2, 3 });

// must be a procedure CALL
java.sql.CallableStatement cs = conn.prepareCall("CALL MYLIB.ARRPROC(?, ?, ?)");

cs.setArray(1, inArr);                                  // (b) IN array
cs.registerOutParameter(2, java.sql.Types.ARRAY);      // (c) OUT array
cs.setArray(3, bothArr);                               // INOUT: set IN side ...
cs.registerOutParameter(3, java.sql.Types.ARRAY);      // ... and register OUT side

cs.execute();   // 0x382F on request (IN/INOUT), 0x3901 on reply (OUT/INOUT)

// (d) read back
java.sql.Array out  = cs.getArray(2);                  // or (Array) cs.getObject(2)
java.sql.Array both = cs.getArray(3);
Integer[] outVals = (Integer[]) out.getArray();        // Java element array
// or iterate: java.sql.ResultSet rs = out.getResultSet();
```

`getObject(idx)` returns the same array (SQLArray.getObject delegates to getArray) and does **not** type-check against Types.ARRAY, unlike `getArray` which enforces ARRAY/JAVA_OBJECT. Element Java types are canonicalized by SQL type (SMALLINT→Integer, CHAR/VARCHAR→String, DECIMAL/NUMERIC/DECFLOAT→BigDecimal, BINARY/VARBINARY→byte[][]). Array-of-array, STRUCT, REF, TINYINT, BIT, NULL, JAVA_OBJECT, LONGVARCHAR, LONGVARBINARY, DATALINK, OTHER are rejected (`07006`). No max-cardinality validation exists client-side.
```

---

## 7. Reader-disagreement / thin-evidence flags

1. **0x3901 preamble (HIGH RISK):** doc-block (20-byte preamble) vs runtime code (4-byte skip, count at +4). **Prefer code** (verified: `overlay(data, offset+6)` at DBBaseReplyDS:944 + `actual3901OutArrayStart_=4`). MUST confirm by capture.
2. **CP 0x3813 max-cardinality at abs 38 (THIN):** documented in header comment only; no runtime getter reads it (JT400 reads element length at abs 20). A Go port cannot byte-match JT400 source here — verify against a live CP 0x3813 array describe.
3. **-2 mapping-error sentinel (MEDIUM):** consumed in `JDServerRow`, not special-cased inside `DBVariableData`; the byte value 0xFFFE is inferred from the signed-short read, no server spec line located.
4. **INOUT round-trip (MEDIUM):** supported per protocol note (`:53`) + standard setArray+registerOutParameter pattern; no JT400 sample/test demonstrates it. Confirm host echoes INOUT array on 0x3901.