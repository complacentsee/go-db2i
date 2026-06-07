# Issue #68 — Phase 0: live capture of stored-procedure ARRAY wire frames

**Status: DONE** (2026-06-07, PUB400 V7R5M0, branch `feat/issue-68-array-params-phase0`).
Capture-and-confirm only — no production encoder/decoder was written.

Phase 0 was the keystone gate: the IN-array EXECUTE frame (**CP 0x382F**) and the
OUT-array reply frame (**CP 0x3901**) were previously *inferred* from the JTOpen
source. They are now **captured live from JT400 21.0.4** against PUB400 and the
inferred layout is **confirmed in full**, with the refinements noted below.

## How it was captured

go-db2i cannot yet emit a 0x382F (no encoder) and — critically — the server's
reply CP follows the *client's declared descriptor*, not the proc signature (see
"go-db2i-side finding"), so **JT400 is the only source for an authoritative
0x382F/0x3901**. Captured via the existing JT400 trace harness:

- `Cases.java` gained an `ArrayProcs` base + seven `array_*` cases (CREATE TYPE
  `... ARRAY[10]` UDTs + IN/OUT/INOUT/mixed procedures, CALLed with
  `createArrayOf` + `setArray`/`registerOutParameter`/`getArray`).
- Run: `ONLY=array_* mvn -q -o exec:java` with `PUB400_*` env mapped from
  `.env.pub400-v7r5`. Produces `fixtures/array_*.trace` + `.golden.json`.
- Offline decode + confirm: `hostserver/array_param_wire_test.go` (parses the
  traces via `internal/wirelog`, walks codepoints with `DecodeDBRequest` /
  `ParseDBReply`, decodes the DBVariableData structure, asserts every case).
- Canonical extracted payloads: `hostserver/testdata/array_param_execute_382f.bin`
  (48 B) and `array_param_reply_3901.bin` (40 B).

### JT400 array-bind gotcha (cost one failed capture)

`createArrayOf("INTEGER", elements)` needs a **typed** array — `new Integer[]{...}`
/ `new String[]{...}`, **not** `new Object[]{...}`. `AS400JDBCArray.setArray` keys
on `inArray.getClass().getComponentType()`; an `Object[]` yields component type
`java.lang.Object` and throws client-side `07006 "Data type mismatch
(INTEGER<>java.lang.Object)"` before anything reaches the wire. `setArray` also
requires the statement to be a CALL (`isProcedureCall()`).

## Confirmed wire layout (DBVariableData, big-endian)

Both 0x382F (IN/INOUT request, replaces scalar 0x381F) and 0x3901 (OUT/INOUT
reply, replaces scalar 0x380E) share:

```
[4B consistency token][2B column count][descriptors...][indicators...][data...]
```

- **Descriptors** (one per column, in column order):
  - non-null array — 12 B: `0x9911 | elemType(2) | elemDataLen(4) | cardinality(4)`
  - whole-null array — 4 B: `0x9911 0xFFFF` (no indicators, no data slot)
  - scalar — 8 B: `0x9912 | sqlType(2) | dataLen(4)`
- **Indicators**: always 2 B each, grouped per-column-then-per-element,
  contiguous, before all data. `0x0000` = not null, `0xFFFF` = null element.
- **Data**: FIXED stride = `elemDataLen` per element. A null element still
  occupies a (zeroed) slot. **VARCHAR(n) stride = n+2** — each slot is a 2-byte
  character-length prefix + char bytes + zero-fill.

Matches `DBVariableData.java` (`overlay`/`setHeaderColumnInfo`, lines 183-228,
382-450). See `array-wire-spec.md` for the full source-cited spec.

## Per-case evidence (all live, all pass in the offline test)

| Case | CP | Captured |
|---|---|---|
| `array_in_int_basic` | 0x382F | token=1, 1 col, `9911` type=496 len=4 card=5, 5×`0000` ind, data `[10,20,30,40,50]` |
| `array_in_int_nullelem` | 0x382F | indicators `0000 0000 FFFF 0000 0000`; null elem data slot present & zeroed |
| `array_in_int_wholenull` | 0x382F | 1 col `9911 FFFF`; no indicators, no data |
| `array_out_int` | 0x3901 | token=0, `9911` type=497 len=4 card=3, data `[11,22,33]`; its 0x382F has count=0 |
| `array_inout_int` | 0x382F + 0x3901 | req `[1,2,3]` (type 496) / reply `[100,200,300]` (type 497) |
| `array_inout_vc` | 0x382F + 0x3901 | VARCHAR(20) stride=22; req `["AB","CDE",null]` / reply `["XX","YYY"]` |
| `array_mixed` | 0x382F + 0x3901 | req count=2 (`9912` scalar 7 + `9911` `[100,200,300]`); reply `9912` scalar OUT `CNT=10` |

## Findings beyond the plan

1. **PUB400 V7R5M0 accepts IN, OUT, and INOUT array params** (plan open-Q #2:
   resolved — not OUT-only). VARCHAR and mixed scalar+array procs work too.
2. **VARCHAR element stride = maxLen+2, fixed** (plan's key open question:
   resolved — fixed-stride, not length-packed).
3. **0x382F column count = INPUT params only** (OUT-only excluded): pure-OUT proc
   → 0x382F token=1, count=0; mixed (IN, IN-arr, OUT) → count=2.
4. **When any param is an array, the reply uses 0x3901 even for scalar OUT
   params** (carried via the `0x9912` scalar descriptor), not 0x380E.
5. **elemType parity asymmetry**: request (0x382F) uses the EVEN/not-null type
   code (INTEGER 496=0x01F0, VARCHAR 448=0x01C0); reply (0x3901) uses the
   ODD/nullable code (497, 449). The describe (0x3813) also reports the nullable
   code. (Per-element nullability is carried by the 2-byte indicator, not the type.)
6. **0x3901 replies carry 4 trailing zero bytes** beyond the structure JT400's
   overlay navigates (it computes offsets from the descriptor and ignores them).
7. Consistency token observed = 1 on requests, 0 on replies.

## go-db2i-side finding (sequences Phase 3 before Phase 4)

`test/conformance/array_param_test.go::TestArrayParamGoClientReplyCP` CALLs an
array-OUT proc through go-db2i with a read-hook installed. go-db2i still drops the
describe-side array flag and binds the OUT param as **scalar INTEGER**, so the
server answers go-db2i's scalar-shaped EXECUTE with the **scalar CP 0x380E**
(plus an SQL-601 warning), **not** 0x3901.

**Implication:** the reply CP follows the *client's CHANGE_DESCRIPTOR*, not the
proc signature. 0x3901 is only reachable once the client sends the array-aware
describe/encode — i.e. **Phase 3 (IN encode + array describe) gates Phase 4
(0x3901 decode)**. This is also why the authoritative 0x3901 had to come from
JT400, which sends the array descriptor.

## What downstream phases can now rely on

- Phase 1 (describe parse): unchanged — pinned by `array_param_describe_3813.bin`.
- Phase 3 (0x382F encode): the exact byte layout above, validated against
  `array_param_execute_382f.bin` + the per-case traces. Use the not-null type
  code on the request side; emit a whole-null array as `0x9911 0xFFFF`.
- Phase 4 (0x3901 decode): the exact byte layout above, validated against
  `array_param_reply_3901.bin`; tolerate the 4 trailing bytes; OUT-only column
  remap (count = OUT/INOUT params in OUT order). Reachable only after Phase 3.
- Cleanup: `CREATE TYPE` leaves a `*SQLUDT`; `test/conformance/zz_cleanup_test.go`
  now drops it via `DROP TYPE`.
