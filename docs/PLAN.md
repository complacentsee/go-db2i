# goJTOpen — implementation roadmap

This document is the post-M1 working plan for the pure-Go IBM i driver.
It supersedes the original feasibility plan (which considered a
DRDA-first path). Reality picked **host-server datastream over port
8471/8476** because that's the protocol the captured fixtures speak,
and the M1 sign-on smoke-tested clean against PUB400.

## Where we are

- ✅ **M0** — Fixture-capture harness (Java/Maven). 30 cases committed
  under `testdata/jtopen-fixtures/fixtures/`. Re-runnable against
  PUB400 with `mvn -q exec:java`.
- ✅ **M1** — Sign-on (port 8476) + as-database handshake (port 8471).
  Pure Go, live against PUB400.
- ✅ **M2** — Static SELECT round-trip. Live decodes
  `select_dummy.golden.json` byte-equal.
- ✅ **M3** — Prepared statements with bound parameters
  (INT/VARCHAR live; full type set lives in M4).
- ✅ **M4** — Type system. 19/19 round-trip suite green.
  All numeric/character/date/time/decimal/decfloat types decode
  AND bind round-trip live; CCSID 37 + 273 codecs are real.
- ✅ **M5** — ResultSet metadata, multi-row continuation FETCH
  (live: 5000 rows from PUB400), COMMIT/ROLLBACK/autocommit
  encoders, isolation-level knob.
- ⏳ **M6** — `database/sql/driver` registration. Up next.

Each closed milestone has a **Deferred** subsection below listing
the items that were pushed past it; nothing is closed silently.

## Quick-start on the remote machine

```sh
git clone https://github.com/complacentsee/goJTOpen.git
cd goJTOpen

# Build everything; run unit tests offline (no IBM i needed).
go build ./...
go test ./...                # 4 packages, ~30 tests, all offline

# Verify M1 sign-on against PUB400 from this machine.
export PUB400_USER=AFTRAEGE1
export PUB400_PWD='your-password'
go run ./cmd/smoketest

# Re-capture fixtures if you change a case in Cases.java.
cd testdata/jtopen-fixtures
mvn -q exec:java
git add fixtures/ && git commit -m 'fixtures: re-capture' && git push
```

## Repo layout (current + planned)

```
goJTOpen/
  go.mod                                # github.com/complacentsee/goJTOpen
  README.md
  docs/
    PLAN.md                             # this file
  cmd/
    smoketest/                          # M1+ live IBM i demo (current: sign-on)
    dumprecv/                           # debug tool: hex-dump trace recv frames
  hostserver/                           # IBM i host-server datastream protocol
    dss.go                              # 20-byte frame header + ReadFrame/WriteFrame
    signon.go                           # 0x7003 / 0xF003 / 0x7004 / 0xF004
    signon_flow.go                      # SignOn() orchestration
    database.go            (M1 cont.)   # 0x7001 / 0xF001 / 0x7002 / 0xF002
    db_request.go          (M2)         # DBBaseRequestDS / DBSQLRequestDS
    db_reply.go            (M2)         # DBReplyServerAttributes / row replies
  auth/                                 # password / token encryption
    password_sha1.go                    # levels 2-3 (SHA-1)
    password_des.go        (later)      # levels 0-1 (DES)
    password_pbkdf2.go     (later)      # level 4 (PBKDF2-HMAC-SHA-512)
  ebcdic/                               # CCSID conversion
    ccsid.go                            # CCSID 37 (US English)
    ccsid_273.go           (M2)         # CCSID 273 (German -- PUB400's job CCSID)
    ccsid_1141.go          (later)      # other Western European CCSIDs
    tables/                (M4+)        # generated tables for non-x/text CCSIDs
  decimal/                 (M4)         # packed/zoned BCD + DECFLOAT
    packed.go
    zoned.go
    decfloat.go
  driver/                  (M6)         # database/sql/driver interface
    driver.go
    conn.go stmt.go rows.go tx.go types.go
  internal/
    wirelog/                            # JTOpen .trace parser + frame consolidation
  testdata/
    jtopen-fixtures/                    # M0 Java harness + 30 .trace + .golden.json
```

## Remaining milestones

Each milestone has the same shape:

1. Add encoder/parser code in the relevant package.
2. Write unit tests against committed fixtures (offline; no IBM i).
3. Extend `cmd/smoketest` to exercise the new path against PUB400.
4. Commit per milestone; push.

### M1 finish — as-database handshake (~3-5 days)

The as-database service (port 8471) opens with a different sequence
than as-signon. Each case in `connect_only.trace` shows it after
recv #2.

- **`hostserver/database.go`**:
  - `XChgRandSeedRequest(serverID, clientSeed)` builds the 28-byte
    0x7001 frame (header byte 4 = 0x03 client attrs).
  - `XChgRandSeedReply` struct + `ParseXChgRandSeedReply` for 0xF001
    (RC + 8-byte server seed + optional 0x112E AAF CP).
  - `StartServerRequest(userIDEbcdic, password, ...)` builds the
    0x7002 frame -- the actual auth round trip on this service.
  - `StartServerReply` parser for 0xF002 (RC + job name).
- Cross-reference JTOpen sources:
  - `AS400XChgRandSeedDS.java` (28-byte request, byte-4 attrs)
  - `AS400XChgRandSeedReplyDS.java` (RC + server seed + AAF)
  - `AS400StrSvrDS.java` (start server / sign-on for DB service)
  - `AS400StrSvrReplyDS.java`
- Acceptance test: `TestStartDatabaseService` -- replay the relevant
  fixture frames into a `fakeConn`, drive the new orchestrator, parse
  the replies cleanly.
- Smoketest extension: connect to port 8471 after sign-on; print the
  database job name + negotiated server-job CCSID.

**Gate to M2:** smoketest opens both as-signon and as-database
connections and prints info from both.

**Status:** ✅ landed — commit `2980ce5`.

**Deferred from M1:**

- **Password level 4 (PBKDF2-HMAC-SHA-512)** — ✅ wire-validated
  2026-05-08 against IBM i 7.6 (V7R6M0) on IBM Cloud Power VS.
  The original implementation followed JT400's spec comment which
  says "Data = Unicode password value" and used UTF-16BE for the
  PBKDF2 password input. The live server returned SQL -3008
  ("password incorrect") -- exactly what the warning predicted.
  Root cause: JT400's actual Java code goes through PBEKeySpec,
  whose PBKDF2KeyImpl encodes the char[] as UTF-8 internally,
  not UTF-16BE. The IBM i server matches the implementation, not
  the spec text. Fix was one line; regression vector pinned via
  `TestEncryptPasswordPBKDF2WireValidatedVector`. Without live
  IBM i access this bug would have stayed hidden indefinitely --
  the offline RFC 6070 PBKDF2 vectors and Python cross-check both
  validated the wrong-but-self-consistent UTF-16BE path.
- **Password levels 0/1 (DES)** — ⚠️ implemented but
  **spec-validated only**. PUB400 / PUB1.de / IBM Cloud V7R6 all
  ship at `QPWDLVL >= 3` and refuse to issue level-0/1 challenges,
  so we can't reach a live target. The implementation mirrors
  JT400's `AS400ImplRemote.encryptPassword` byte-for-byte and
  emits a one-shot stderr warning on first use. Given the PBKDF2
  experience, expect a similar spec-vs-implementation gap to
  surface here too if a real legacy server is ever found.
- **`signon.go:318` TODO** — ✅ already closed (stale PLAN note).
  Typed `*hostserver.SignonError` with `Category()` / `Subcode()`
  accessors and an `Unwrap()` chain to sentinel errors
  (`ErrPasswordIncorrect`, `ErrUserIDUnknown`, `ErrPasswordExpired`,
  …) lives in `hostserver/signon_errors.go`; `wrapSignonRC(rc)` is
  the one-line callsite helper. The line-318 comment in `signon.go`
  just documents the existing type. No action needed.
- **TLS sign-on / database (ports 9476 / 9471)** — ✅ live-validated
  2026-05-10 against IBM Cloud V7R6M0. Self-signed cert issued via
  DCM (`*SYSTEM` store, signed by a Local CA generated in the same
  flow), assigned to `QIBM_OS400_QZBS_SVR_DATABASE` / `_SIGNON` /
  `_CENTRAL`. Host servers light up 9470-9476 alongside 8470-8476
  automatically once the application-ID cert assignment lands -- no
  `STRHOSTSVR SSL(*YES)` switch on V7R6. DSN scaffolding
  (`tls=true`, `tls-insecure-skip-verify`, `tls-server-name`,
  port flip) landed in commit `1e8ad69`; `TestTLSConnectivity` in
  `test/conformance/` (gated on `GOJTOPEN_TLS_TARGET`) pins the
  full sign-on → start-database → multi-row FETCH flow plus a
  byte-diff against the plaintext result.

### M2 — Static SELECT round-trip (~2-3 weeks)

Goal: run `SELECT CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_SERVER FROM SYSIBM.SYSDUMMY1`
through the Go code and decode the result row.

- **`hostserver/db_request.go`** — encoder for `DBSQLRequestDS`
  (request ID 0x180D PREPARE_AND_EXECUTE_AND_DESCRIBE_AND_FETCH or
  similar). The 20-byte DSS header is the same; the template adds
  a 17-byte SQL-request header (operation result-set bitmap, etc.).
- **`hostserver/db_reply.go`** — decoder for the multi-frame reply:
  - `DBReplyServerAttributes` (one-time at start of session)
  - `JDServerRow` -- a column descriptor list
  - `DBOriginalReplyDataStream` -- the row data
- **`hostserver/sql_session.go`** — orchestrates a single
  prepare-execute-fetch against an opened DB connection.
- Cross-reference JTOpen sources:
  - `DBBaseRequestDS.java` (header + bitmap layout)
  - `DBSQLRequestDS.java` (request flavors)
  - `DBSQLAttributesDS.java` (server attributes)
  - `JDServerRow.java` (row descriptor decode)
  - `DBReplySQLCA.java` (SQLCA -- error info)
- Acceptance fixture: `select_dummy.trace`. Three columns,
  one row. The Go side must produce the same three values as
  `select_dummy.golden.json`:
  `["2026-05-07T15:46:55.11755", "AFTRAEGE1", "PUB400"]`.
- Smoketest extension: after sign-on + DB-service start, run the
  static SELECT and print the row.

**Gate to M3:** the static SELECT row prints byte-equal values to the
fixture's golden.json (semantically equal for floats/timestamps).

**Status:** ✅ landed — commits `457a987`, `054edf9`, `b37bc08`,
`3de7fe9`. The `TestSentBytesMatchSelectDummyFixture` byte-equality
scaffold caught a CREATE_RPB template-handle bug that days of
side-by-side hex inspection missed; this pattern is the regression
net for every encoder M3+ adds.

**Deferred from M2:**

- **CP 0x3807 (SQLCA) is captured but not decoded** — we surface
  ErrorClass + ReturnCode out of the template, sufficient for the
  +100 (end-of-data) and -501 (cursor-not-open) checks the result
  loop needs. A typed `*Db2Error{SQLState, SQLCode, Message,
  Tokens}` wrapper lands in M7.
- **Stream/cursor `*Rows` API** — current `SelectStaticSQL` returns
  the full result set in memory. database/sql's `Rows` interface
  (M6) will need an iterator that pulls FETCH batches lazily; M5
  added the underlying continuation FETCH but kept the
  return-everything API for simplicity.

### M3 — Prepared statements + parameter binding (~2 weeks)

Goal: support `db.QueryContext(ctx, "SELECT CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1", 42)`.

- Extend `hostserver/db_request.go` to build PREPARE-with-parameters
  + EXECUTE-with-bound-values request streams.
- Add **input** SQLDA encoding -- bound parameter values into the
  bytes the server expects.
- Type coverage for M3: int32, int64, float64, string (CCSID 37 +
  UTF-8), bool. Decimal/timestamp/null land in M4.
- Acceptance fixtures: `prepared_int_param.trace`,
  `prepared_string_param.trace`, `prepared_decimal_param.trace`
  (decimal validates partially -- full path in M4).
- Smoketest extension: bind an integer parameter, print the result.

**Gate to M4:** prepared statements with primitive parameters
round-trip cleanly against PUB400.

**Status:** ✅ landed — commits `8420d79`, `b5c7f64`, `71bc3aa`.
Prepared SELECT with INT and VARCHAR live, plus the full bind
encoder framework (CP 0x381E shape descriptor + CP 0x381F value
carrier + 0x1E00 CHANGE_DESCRIPTOR frame). M4 widened the bind
type matrix on top of this scaffold.

**Deferred from M3:**

- **`bool` parameter binding** — ✅ closed 2026-05-10 by the M3
  deferred wrap-up. V7R5+ native `BOOLEAN` (SQL types 2436/2437)
  decodes via the 1-byte `0xF0 = false / else = true` form;
  bind via SMALLINT(1) and let the server coerce. Live-validated
  on V7R6M0 via `TestBooleanRoundTrip`. SMALLINT(1) substitute
  still works for V7R4 and earlier.
- **String CCSIDs beyond 37 / 273** — UTF-8 (1208) and UCS-2 BE
  (13488) for column data are referenced in the connection
  metadata path but not used for VARCHAR bind. Lands with the M7
  CCSID negotiation work.
- **Multi-row INSERT bind paths** — the descriptor + data CPs we
  ship handle one row of input. Batch INSERT (multiple rows in
  one frame) is a separate encoding; defer until either M5
  acceptance forces it or a real workload requires it.

### M4 — Type system (~3-4 weeks; **highest risk**)

Goal: every IBM i SQL type the user's workload uses round-trips
correctly between PUB400 and Go.

- **`decimal/`** package:
  - `packed.go` -- DB2 for i `DECIMAL(p,s)` is packed BCD. Sign
    nibbles: `0xC` = +, `0xD` = -, `0xF` = unsigned. Accept
    `0xA`/`0xE`/`0xB` on input, normalize to `0xC`/`0xD` on output.
    **This is where money columns go silently wrong if you misread
    the spec -- spend extra time here.** Reference `SQLDecimal.java`
    line-by-line.
  - `zoned.go` -- `NUMERIC(p,s)` is zoned. Each digit one byte; sign
    nibble in the upper nibble of the last byte.
  - `decfloat.go` -- IEEE 754-2008 decimal128. No Go stdlib.
    Either bring in `github.com/cockroachdb/apd` (~5K LOC dep) or
    hand-roll a string-only decoder. Recommendation: `apd` for M4.
- **`ebcdic/`** expansion: add CCSID 273 (German -- PUB400's job
  CCSID). Long term, generate tables for the remaining 184 CCSIDs
  with `cmd/gen-ccsid` from public IBM mapping files.
- **Date/time** decoders. DB2 for i sends ISO text in the wire
  format, but the wire CCSID may be EBCDIC -- convert before
  parsing.
- **NULL** indicators. SQLDA has a per-column null indicator byte
  array.
- **CCSID 65535** -- "no conversion / binary." Treat as `[]byte`
  unconditionally; do NOT attempt EBCDIC decode. Easy to miss.
- Acceptance fixtures: every `types_*.trace` file. The
  cross-driver type matrix must produce `golden.json`-equivalent
  values from the Go decoder.

**Gate to M5:** `types_*.trace` cases all decode equal values; the
`AMT DECIMAL(11,2)` column in `multi_row_fetch_1k` round-trips for
all 1000 rows; negative-decimal sign nibbles handled.

**Status:** ✅ landed — commits `3de699b`, `383d8af`, `c3a3d09`,
`50a3c25`, `757e5e7`, `4be6736`. Type round-trip suite at 19/19;
DPD codec hand-rolled (8 + 16 byte forms); CCSID 273 mapping
verified byte-for-byte against Python's stdlib `cp273`; packed
BCD sign nibbles cover both directions.

**Deferred from M4:**

- **Live INSERT/UPDATE/DELETE round-trip via prepared bind** — every
  type now has a Go encoder, and live writes work via
  `ExecuteImmediate` (0x1806) against `AFTRAEGE11.DCLVRP02` on
  PUB400 (validated 2026-05-07: INSERT a row, SELECT it back,
  DELETE it cleanly). The static-SQL write path is covered.
  What's still deferred is **prepared-statement bind for I/U/D**
  with parameter markers — the bind encoders are exercised via
  `SELECT CAST(? AS T)` loopback today; per-type bind on
  INSERT/UPDATE picks up in M6 alongside `database/sql.Stmt.Exec`
  and write fixtures.
- **DATE format negotiation for non-default formats** — landed
  via #53 fix on 2026-05-08. SET_SQL_ATTRIBUTES now routes
  `DateFormat` to the correct CPs (`0x3807` parser option +
  `0x3808` separator) instead of the wrong `0x3805`
  (TranslateIndicator); bind path honours `PreparedParam.DateFormat`
  so USA/EUR/JIS/MDY/DMY/YMD emit format-correct wire bytes.
  Legacy zero-`DateFormat` callers keep length-based ISO/YMD
  inference unchanged.
- **CCSID 65535 (binary "no conversion")** — ✅ closed 2026-05-10.
  CHAR FOR BIT DATA / VARCHAR FOR BIT DATA always routed
  through the CCSID-65535 → `[]byte` path; the M4 deferred
  wrap-up added the standalone V7R3+ types BINARY (912/913) and
  VARBINARY (908/909) to the same decoder shape. Captured
  fixture `prepared_binary_bind` + live conformance
  `TestBinaryTypeRoundTrip` pin all three byte-flavours.
- **Schema/table column metadata** — ✅ closed 2026-05-10. DSN
  `?extended-metadata=true` ORs the ORS bit `0x00020000` AND
  sends the per-statement CP `0x3829 = 0xF1` so the server
  populates CP `0x3811` with per-column schema / table / base
  column / label. Both knobs are required: ORS-only returns CP
  `0x3811` with zero bytes (JT400's "empty descriptor" case).
  Surfaced via driver-level `Rows.ColumnTypeSchemaName` /
  `Rows.ColumnTypeTableName` / `Rows.ColumnTypeBaseColumnName` /
  `Rows.ColumnTypeLabel`. Live-validated on V7R6M0 via
  `TestExtendedMetadata`. Default-off keeps the wire shape
  byte-identical to pre-flag fixtures.

### M5 — ResultSet metadata, transactions, isolation (~1-2 weeks)

- Column metadata (`getColumnName/Type/Precision/Scale/etc`) from
  the JDServerRow descriptors.
- `database/sql.Tx` support: `RDBCMM` and `RDBRLLBCK` request
  encoding, autocommit toggle wire path.
- Isolation levels: map JDBC constants (`*CHG`, `*CS`, `*ALL`,
  `*RR`) to JTOpen's URL property values.
- Acceptance: extend smoketest with a `BEGIN; INSERT; COMMIT; SELECT`
  cycle on a journaled table.

**Status:** ✅ landed — commits `f533969`, `fd52945`, `34f7696`.
Multi-row continuation FETCH live (5000-row pull from
`QSYS2.SYSTABLES`); column metadata `TypeName/DisplaySize/
Nullable/Signed` matches JTOpen for all 15 type fixtures;
`Commit` (0x1807) / `Rollback` (0x1808) / `AutocommitOff/On`
encoders match JT400 source byte-for-byte; isolation level knob
on `DBAttributesOptions` covers `*NONE` / `*CS` / `*ALL` / `*RR`
/ `*RS` (CP 0x380E).

**Deferred from M5:**

- **Live COMMIT / ROLLBACK semantics on a journaled table** —
  ✅ validated 2026-05-08 against `AFTRAEGE11.DCLVRP02` after
  enabling commitment control on the schema:
  ```
  CRTJRNRCV JRNRCV(AFTRAEGE11/JRNRCV0001)
  CRTJRN    JRN(AFTRAEGE11/QSQJRN) JRNRCV(AFTRAEGE11/JRNRCV0001)
  STRJRNPF  FILE(AFTRAEGE11/DCLVRP02) JRN(AFTRAEGE11/QSQJRN)
  ```
  Round trip: `AutocommitOff → INSERT 9999991 → COMMIT` persists
  the row; `AutocommitOff → INSERT 9999993 → ROLLBACK` cleanly
  removes it; verify on the same connection finds exactly the
  one committed row. The `tx_commit.trace` fixture has been
  re-captured as a happy-path JT400 trace (replacing the prior
  SQL7008 error capture).
  Two driver bugs were flushed out by the JT400 wire diff during
  this validation:
  1. `Commit` / `Rollback` were sending CP 0x380F (HoldIndicator)
     as 0xE8 ('Y' EBCDIC) instead of numeric 0x01 — the server
     validates the byte and silently rejects everything outside
     {0,1} with SQL -211 errorClass=0x0002.
  2. `AutocommitOff` / `AutocommitOn` were sending only CP
     0x3824. JT400 bundles three CPs in one SET_SQL_ATTRIBUTES
     (0x3824 autocommit, 0x380E commitment level, 0x3830 locator
     persistence); without the bundle the server stays in *NONE
     and COMMIT/ROLLBACK have no commitment definition to act
     on. The autocommit-on transition gets errClass=7 RC=-601 on
     some IBM i releases (server rejects locator-persistence
     change); JT400 catches that and resends without CP 0x3830,
     and `setSessionMode` now mirrors that fallback.
  Both fixes commit-pinned and unit-tested.
- **Re-capture fixtures with M5 RPB DELETE in scope** — landed
  via #48 fix on 2026-05-09. Rather than re-capturing fixtures,
  the cursor was refactored to align with JT400's actual wire
  pattern: the OPEN reply's `(ErrorClass=2, ReturnCode=700)` tuple
  is JT400's "fetch/close" signal (all rows delivered + cursor
  auto-closed by server), so the driver now skips the
  unnecessary continuation `FETCH` and explicit `CLOSE`. The
  three synthetic test stubs (`syntheticFetchEndReply`,
  `syntheticCloseReply`, `syntheticRPBDeleteReply`) are gone;
  offline tests consume the captured PREPARE/OPEN/RPB-DELETE
  replies directly. Multi-batch results still work via
  continuation `FETCH` when the server signals more rows pending.
  Wire-validated against IBM Cloud V7R5M0.
- **`cmd/diffrunner`** — mentioned in the original M5 plan as a
  nightly Java/Go cross-check. Never built; still a good idea
  during M6 conformance work.
- **Streaming `*Rows` API** — see "Deferred from M2"; same item.
  Continuation FETCH lands in M5 but the public API still buffers
  every row. M6 wraps a lazy iterator for `database/sql.Rows`.

### M6 — `database/sql` driver registration + conformance (~2 weeks)

- Implement `database/sql/driver`: `Driver`, `Connector`, `Conn`,
  `Stmt`, `Rows`, `Tx`, plus the `Context` variants.
- `OpenConnector` with DSN parsing. Mirror JTOpen URL syntax where
  reasonable: `goJTOpen://user:pwd@host:port/?library=AFTRAEGE1&naming=sql`.
- Run `bradfitz/go-sql-test` (or the `go-sql-driver/mysql` adapted
  test harness) end-to-end.

**Status:** ⏳ scaffold landed — commit `ddf7ce3`. The `driver/`
package exposes the standard `database/sql` surface and wraps our
hostserver protocol; live-validated end-to-end against IBM Cloud
IBM i 7.6 (V7R6M0): `sql.Open` + `db.Query` + `Rows.ColumnTypes`
+ `db.Begin` / `tx.Commit` / `tx.Rollback` all round-trip cleanly.
DSN syntax: `gojtopen://USER:PWD@host:8471/?library=MYLIB&date=iso&isolation=cs`.

**Parameter binding ✅ wire-validated.** `Stmt.Query` dispatches to
`hostserver.SelectPreparedSQL` when args are present, with a typed
binder that maps each `driver.Value` flavour to a `PreparedParam`
shape:

| `driver.Value` | SQL type / CCSID | Wire format |
|---|---|---|
| `int64` | BIGINT nullable (493) | 8 bytes BE |
| `float64` | DOUBLE nullable (481) | 8 bytes IEEE 754 |
| `bool` | SMALLINT nullable (501) | 2 bytes (0/1) |
| `[]byte` | VARCHAR FOR BIT DATA (449 + CCSID 65535) | 2-byte SL + bytes |
| `string` | VARCHAR (449 + CCSID 37) | 2-byte SL + EBCDIC bytes |
| `time.Time` | TIMESTAMP nullable (393) | 26 EBCDIC bytes ISO |
| `nil` | INTEGER nullable (497) + indicator | NULL via indicator |

Encoder gained CCSID 65535 (binary passthrough) and CCSID 1208
(UTF-8 passthrough) routes alongside the existing EBCDIC SBCS path
so the binary and UTF-8 columns the live server uses bind cleanly.
TIMESTAMP `Rows.Next` promotes the hostserver's ISO string to a
`time.Time` so callers can `Scan(&t)` into `*time.Time` directly.
All eight bind paths round-tripped exact values against IBM Cloud
IBM i 7.6 (`/tmp/sqldriver_paramtest.go`).

**Stmt.Exec parameter binding ✅ wire-validated.**
`hostserver.ExecutePreparedSQL` (CREATE_RPB / PREPARE_DESCRIBE /
CHANGE_DESCRIPTOR / EXECUTE 0x1805) lands the prepared INSERT /
UPDATE / DELETE flow; `Stmt.Exec` reuses the same
`bindArgsToPreparedParams` helper as `Stmt.Query`. The EXECUTE
path also runs RPB DELETE in cleanup so the next prepared call on
the same connection doesn't trip the "RPB slot still occupied"
guard. SQL +100 (no rows matched) is treated as success with
rows-affected=0.

While testing the prepared INSERT/UPDATE flow, found and fixed a
result-data parser bug: a one-row variable-length-only result's
VLF total bytes can coincide with `rowSize*rowCount` of the
non-VLF layout, making length-detection pick the wrong path and
return an empty `[]byte`. Fix is to honour the response ORS
bitmap echo bit `ORSVarFieldComp` (`0x00010000`) read from
`payload[0:4]` of the reply -- when set, use VLF regardless of
size. Length-detection remains as a fallback for callers that
don't request the echo (preserves byte-equality with existing
JT400 fixtures).

**Typed `*Db2Error` ✅ wire-validated.** SQLCA decoding (CP 0x3807)
surfaces `SQLState`, `SQLCode`, `MessageID`, `MessageTokens`, and
the operation label (`PREPARE_DESCRIBE`, `EXECUTE`, `FETCH`, etc.)
that produced the error. Engines `errors.As(err, &dbErr)` and
dispatch on:

| Predicate | Trigger |
|---|---|
| `IsNotFound()` | SQLSTATE 02xxx or SQLCODE +100 |
| `IsConstraintViolation()` | SQLSTATE 23xxx (dup key, NOT NULL, FK, check) |
| `IsConnectionLost()` | SQLSTATE 08xxx |
| `IsLockTimeout()` | SQLCODE -911 / -913 |

Validated live: SQL-204 / 42704 (table-not-found), SQL-205 / 42703
(column-not-found), SQL-104 / 42601 (syntax), SQL-302 / 22023
(numeric out of range) all surface with proper SQLSTATE and
substitution tokens (table name, column name, etc.).

**`driver.ErrBadConn` ✅ wire-validated.** Connection-level errors
(`io.EOF`, `net.OpError`, `hostserver.ErrShortFrame`, `*Db2Error`
with SQLSTATE 08xxx) get wrapped through `badConnWrap` so they
satisfy `errors.Is(err, driver.ErrBadConn)` -- triggering
`database/sql`'s auto-retry on a fresh connection -- while still
unwrapping to the original cause for diagnostic logs. Statement-
level errors flow through as typed `*Db2Error`. `Conn.IsValid` /
`Conn.ResetSession` round out the pool-eviction path. Live-tested
by force-closing the underlying `net.Conn` mid-pool: subsequent
queries on the same `*sql.DB` succeeded transparently on a
freshly-opened replacement.

**Context cancellation ✅ wire-validated.** `Stmt.QueryContext` /
`Stmt.ExecContext` plumb `ctx` through to `net.Conn.SetDeadline`,
plus a `context.AfterFunc` that fires `SetDeadline(now)` on cancel
to unblock any in-flight read/write. `resolveCtxErr` substitutes
`ctx.Err()` for the I/O timeout when the cancellation was the
real cause, so callers see `context.Canceled` /
`context.DeadlineExceeded` rather than the transport error.
Live-validated: a cross-join SELECT that takes the server several
seconds to materialise canceled in 250ms (`WithTimeout`) and
200ms (explicit `cancel()`); the pool then auto-recovered with a
fresh connection.

**Deferred from M6 (nice-to-have, not blocking labelverification-gw):**

- **Lazy Rows iteration via continuation FETCH** ✅ — landed across
  M5 (`hostserver.Cursor` + continuation FETCH on 0x180B) and M7-2
  (driver-layer wiring). `driver.Rows.Next` now pulls one row at a
  time from `*hostserver.Cursor`, which issues continuation FETCH
  lazily when its 32 KB block-fetch buffer drains. Live-validated
  against IBM Cloud V7R6M0 with `TestRowsLazyMemoryBounded`: a
  ~50K-row SELECT walks via `rows.Next` with peak HeapAlloc 3.8 MiB
  vs a 16 MiB budget, post-iteration delta is *negative* after GC.
  `TestRowsCloseIdempotent` pins idempotent `Rows.Close` and
  confirms no cursor leak on subsequent queries on the same conn.
  Buffered API (`SelectStaticSQL` / `SelectPreparedSQL`) preserved
  via `Cursor.drainAll` for the existing offline fixture tests and
  `cmd/smoketest`.
- **`LastInsertId` via IDENTITY_VAL_LOCAL()** — currently returns
  a "not supported" error. Not used by labelverification-gw
  (legacy DCLVRP02 has fixed keys, no IDENTITY columns).
- **`bradfitz/go-sql-test` conformance** — landed via commit
  `2d6cf16` and re-validated 2026-05-09 against PUB400 V7R5M0
  (post-#48 cursor refactor). Four scenarios pass: TestBlobs,
  TestManyQueryRow (1000 prepared QueryRow round-trips),
  TestTxQuery, TestPreparedStmt (10 goroutines × 10 iterations
  of shared prepared SELECT + INSERT). Build-tag-gated under
  `//go:build conformance`; opt-in via `GOJTOPEN_DSN`. Surfaced
  three real bugs during the original run (empty 0x380E CP,
  open-cursor cleanup ordering, RPB DELETE on error path) which
  shipped fixed in the same commit.
- **`cmd/diffrunner`** — never built; useful for conformance work
  but not for shipping the gateway.

### M7 — LOB streaming, CCSID negotiation, error mapping (COMPLETE, 2026-05-10)

All seven plan items closed; residual plan items (M7-4 TLS,
M7-5 lob threshold + bug #14, M7-7 RLE whole-datastream wrapper,
bug #15 CCSID 273 byte-mode) also landed. See CHANGELOG
`[Unreleased]` for per-item closure notes and live-evidence
citations.

- BLOB/CLOB handling via the host-server's chunked-data path
  (DBLobLocator-equivalent).
  - **LOB SELECT (materialise)** — landed via commit `c11e2f1`,
    fixed for V7R5+ (column-index off-by-one, SQL-818) on
    2026-05-09.
  - **LOB SELECT (streaming)** — landed 2026-05-09 via the DSN
    option `?lob=stream` and `*gojtopen.LOBReader`. Wire-validated
    on PUB400 V7R5M0 (200-byte BLOB chunked, 700-char CCSID-273
    CLOB streamed and EBCDIC-decoded).
  - **LOB bind** — INSERT / UPDATE of large LOBs via `?`
    placeholders landed 2026-05-10. `[]byte`, `string`, and the
    new `*gojtopen.LOBValue` type all route through the JT400
    `WRITE_LOB_DATA` (function `0x1817`) flow with server-allocated
    locator handles read out of `PREPARE_DESCRIBE` reply CP `0x3813`.
    Wire-validated end-to-end on PUB400 V7R5M0: 8 KiB BLOB
    byte-equal vs JT400 fixture, 1 MiB streamed BLOB across 32
    chunks, ~8 KiB CLOB EBCDIC round-trip via `ebcdic.CCSID273`.
    Wire-protocol reference at `docs/lob-bind-wire-protocol.md`.
  - **LOB inline (M7-5 + bug #14)** — DSN `?lob-threshold=N`
    drives CP `0x3822` `LOBFieldThreshold` in
    `SET_SQL_ATTRIBUTES`. Result-data decoder learnt the inline
    LOB SQL types (404/405 BLOB, 408/409 CLOB, 412/413 DBCLOB).
    The pre-fix `CLOB(4K) CCSID 1208` SELECT regression is
    pinned by `TestCCSID1208RoundTrip/CLOB small inline`.
  - **LOB read compression (M7-7)** — RLE-1 decompressor + the
    whole-datastream CP `0x3832` unwrap landed in two commits.
    1 MiB constant-content BLOB SELECT shrinks `rx_bytes` from
    ~1 MiB to **1,228 bytes wire** (~854× shrink); offline
    coverage in `TestDecompressDataStreamRLE_RoundTrip/*` and
    `TestParseDBReplyUnwrapsCP3832`.
- SQLCA → typed `*Db2Error{SQLState, SQLCode, Message, Tokens}`
  with substitution. Landed in M6 (`d0ba583`).
- Connection-level CCSID negotiation (M7-3). DSN `?ccsid=N`
  overrides the connection-default ClientCCSID and per-bind tag;
  CHAR/VARCHAR/CLOB decode is column-CCSID-aware via
  `ebcdicForCCSID`. Live-validated end-to-end on V7R6M0 via
  `TestCCSID1208RoundTrip`.
- TLS sign-on / database (M7-4). DSN `?tls=true` +
  `tls-insecure-skip-verify` + `tls-server-name`; default ports
  flip to 9476 / 9471. Live-validated on V7R6M0 with a self-
  signed cert assigned to `QIBM_OS400_QZBS_SVR_DATABASE` /
  `_SIGNON` / `_CENTRAL` via DCM. See `docs/configuration.md`
  for the LPAR turn-up.

### M8 — Hardening, observability, docs (~2-3 weeks)

Track A (foundations):

- **M8-1 Fuzz tests** ✅ 2026-05-11 — `testing.F` corpora seeded
  against `internal/wirelog.ParseJTOpenTrace`,
  `hostserver.ParseDBReply`, `hostserver.decompressRLE1`,
  `hostserver.decompressDataStreamRLE`, and `driver.parseDSN`. Five
  fuzzers, ≥60 s each. Surfaced and fixed three hardening defects:
  unbounded preallocation in `unwrapCompressedReply` /
  `parseLOBReply` (now capped at 64 MiB), `DBParam.Data` returning
  nil for header-only params, and `parseDSN` accepting port 0 plus
  empty username. Two fuzz-found corpus seeds preserved as
  regression gates (`driver/testdata/fuzz/FuzzParseDSN/...`,
  `hostserver/testdata/fuzz/FuzzParseDBReply/...`).
- **M8-2 godoc audit** ✅ 2026-05-11 — every exported symbol in
  `driver/`, `hostserver/`, `ebcdic/`, `internal/wirelog/` now has a
  leading-identifier doc comment (interface methods like Columns,
  ColumnType*, Commit, Rollback, RowsAffected, Stmt.Close,
  Direction.String, NonBMPRuneError.Error, SignonError.Error were
  the residual gaps). New `ExampleDsnKnobs` covers the
  `?lob-threshold` / `?ccsid` / `?tls=true` knob mix the M8 plan
  called out. Existing examples (`ExampleLOBValue` /
  `ExampleLOBReader` / `ExampleDb2Error` / DSN parse) already
  covered the other high-traffic types. Snapshot of `go doc -all`
  output across the four packages committed to
  `docs/godoc-snapshot/` for diffability.
- **M8-3 slog integration** ✅ 2026-05-11 — `Config.Logger *slog.Logger`
  + `Config.LogSQL bool` API surface. Nil Logger silences all
  driver-side logging via an internal discard-handler fallback so
  call sites never have to nil-check. Per-Conn child logger carries
  `driver=gojtopen`, `dsn_host=<host>`, `server_vrm=<vrm>` attrs.
  Levels: INFO on connect/close, DEBUG on each Stmt.Exec /
  Stmt.Query + RETRIEVE_LOB_DATA chunk, WARN on ErrBadConn
  classification, ERROR on non-fatal statement failures. New
  `gojtopen.NewConnector(cfg *Config) (*Connector, error)` so
  callers can programmatically set Logger + LogSQL (the DSN can't
  express either). `cmd/smoketest -log-debug` flag dumps the level-
  filtered text-handler stream to stderr; live-validated against
  IBM Cloud V7R6M0 (visible: INFO connect, DEBUG
  OPEN_SELECT_STATIC, INFO close).
- **M8-4 OpenTelemetry spans** ✅ 2026-05-11 — `Config.Tracer
  trace.Tracer` field. Nil resolves to the noop-tracer fallback so
  call sites are nil-free. Stmt.ExecContext / Stmt.QueryContext
  emit a SpanKindClient span tagged with the OTel database
  semantic-convention attributes: `db.system.name=ibm_db2_for_i`,
  `db.operation.name=EXEC|QUERY`, `db.namespace=<library>`,
  `db.user`, `server.address`, `server.port`,
  `db.statement.parameters.count`, plus `db.response.returned_rows`
  for Exec. `db.statement` rides on the span only when LogSQL is
  also true (same gate as M8-3). On *Db2Error, the span status
  flips to Error and the SQLSTATE / SQLCODE / MessageID attach as
  `db.response.status_code` / `db.ibm_db2_for_i.sqlcode` /
  `db.ibm_db2_for_i.message_id`. API uses
  `go.opentelemetry.io/otel/trace` v1.36.0 so callers can mix any
  OTel SDK. `cmd/smoketest -trace-stdout` flag wires the stdout
  exporter; live-validated against IBM Cloud V7R6M0 (span tree
  matches conventions byte-for-byte).
- **M8-5 JTOpen DSN migration guide** ✅ 2026-05-11 —
  `docs/migrating-from-jt400.md` enumerates every JTOpen JDBC URL
  property (~70 distinct keys covered, cross-referenced against
  JT400's [JDProperties.java](https://github.com/IBM/JTOpen/blob/main/src/main/java/com/ibm/as400/access/JDProperties.java)
  enumeration). For each, the doc shows the goJTOpen DSN equivalent
  or marks it "deferred" / "out of scope" with reasoning. Includes
  a side-by-side migration recipe (jt400 URL -> gojtopen DSN +
  programmatic Config code) and gotchas (naming default, extended
  dynamic, socket timeout, lob threshold).
- **M8-6 Performance tuning notes** ✅ 2026-05-11 —
  `docs/performance.md`. Covers connection-pool sizing on a 1-CPU
  LPAR (~8,800 ops/sec peak at concurrency=50, cited from AUTH.md),
  `?lob=stream` vs materialise tradeoffs (TestRowsLazyMemoryBounded
  bound), `?lob-threshold` trade-offs, `?ccsid=1208` vs default
  auto-pick, CCSID-aware codec caching (no per-call decoder
  construction), RLE compression measurements (~854× on
  constant-content 1 MiB BLOB, per the rlemeasure tool numbers),
  TLS handshake overhead (one-time first-dial cost, no steady-state
  measurable delta vs plaintext per TestTLSConnectivity). Includes
  guidance on the M8-3 slog and M8-4 OTel overhead under
  high-throughput workloads.

**M8 complete 2026-05-11.** All six items shipped + live-validated.
Public API additions (slog + OTel) are purely additive; tagged
v0.5.0 once the final conformance pass on both plaintext and TLS
DSNs landed.

### M9 — Stored procedure support (in progress, started 2026-05-11)

The post-v0.5.0 milestone. Closes the biggest remaining parity gap
vs JT400 JDBC: honest `CallableStatement` semantics via
database/sql idioms. `db.Exec("CALL p(?, ?)", in, sql.Out{Dest: &out})`
covers IN / OUT / INOUT; `rows.NextResultSet()` covers multi-
result-set procs. The full plan (wire facts cited from JT400
source, ORS bits, four sub-items M9-0 through M9-3, deferred-to-
M10+ items) lives in the goJTOpen M9 planning document under
the user's plans/ directory.

- **M9-0 Foundation** ✅ 2026-05-11 — Five fixtures captured against
  IBM Cloud V7R6M0 via JT400 21.0.4 through the local SSH tunnel
  (127.0.0.1:8471). The Java harness now honours `PUB400_PORT` to
  engage JT400's `skipSignonServer_` codepath; new `GOSPROCS`
  library on the LPAR carries `P_INS` (IN-only), `P_LOOKUP`
  (IN + 2 OUT scalars), `P_INVENTORY` (DYNAMIC RESULT SETS 2),
  `P_ROUNDTRIP` (INOUT scalar). Fixtures land under
  `testdata/jtopen-fixtures/fixtures/prepared_call_*.{trace,golden.json}`:
    - `prepared_call_in_only` — `CALL GOSPROCS.P_INS('A', 10)`;
      no params, no result sets, no OUT values.
    - `prepared_call_in_out` — `CALL GOSPROCS.P_LOOKUP(?, ?, ?)`
      with IN VARCHAR `'WIDGET'` + two OUT registrations; golden
      pins OUT values `("Acme Widget", 100)`.
    - `prepared_call_result_set` — `CALL GOSPROCS.P_INVENTORY(5)`
      first dynamic result set only; golden pins
      `[("LOW1", 2), ("LOW2", 3)]`.
    - `prepared_call_multi_set` — same proc, both result sets
      drained via `getMoreResults()`; second set golden pins
      `[("HIGH1", 50), ("HIGH2", 100)]`.
    - `prepared_call_inout` — `CALL GOSPROCS.P_ROUNDTRIP(?)` with
      INOUT INTEGER seeded at 5; golden pins OUT value 6.
  All five fixtures committed; M9-1 / M9-2 / M9-3 replays consume
  them as their offline regression net. The same `WithStoredProcs`
  setup() proves JT400 can `CREATE OR REPLACE` all four procs and
  call them end-to-end against the LPAR — that's the live
  evidence the plan asks for.

- **M9-1 CALL with IN-only parameters** — pending.
- **M9-2 OUT and INOUT via `sql.Out`** — pending.
- **M9-3 Multi-result-set via `Rows.NextResultSet`** — pending.

## Working rhythm

The pattern that worked for M1 should keep working:

1. **Look at JTOpen source** to understand the wire format.
2. **Extract bytes from a fixture** with `go run ./cmd/dumprecv
   testdata/jtopen-fixtures/fixtures/<case>.trace` to see what's on
   the wire.
3. **Encode/decode in Go** to match.
4. **Unit-test** against the fixture using `wirelog.Consolidate` +
   `bytes.Reader` -- no IBM i needed.
5. **Smoketest extension** against PUB400 to validate live behavior.
6. **Commit per logical chunk.** Big commits hide bugs; small commits
   make blame useful when something regresses.

## Cross-reference: JTOpen sources by topic

When you need to understand a wire format, look in
`../JTOpen/src/main/java/com/ibm/as400/access/`:

| Topic | JTOpen source files |
|---|---|
| DSS frame header | `ClientAccessDataStream.java` |
| Sign-on (signon service) | `SignonExchangeAttributeReq.java`, `SignonExchangeAttributeRep.java`, `SignonInfoReq.java`, `SignonInfoRep.java` |
| Database service handshake | `AS400XChgRandSeedDS.java`, `AS400XChgRandSeedReplyDS.java`, `AS400StrSvrDS.java`, `AS400StrSvrReplyDS.java` |
| DB request envelope | `DBBaseRequestDS.java`, `DBConcatenatedRequestDS.java` |
| SQL request flavors | `DBSQLRequestDS.java`, `DBSQLAttributesDS.java` |
| SQL reply / row descriptors | `DBReplyRequestedDS.java`, `JDServerRow.java`, `JDFieldMap.java` |
| SQLCA / errors | `DBReplySQLCA.java`, `JDError.java`, `JDError.properties` |
| Type encoders | `SQLDecimal.java`, `SQLNumeric.java`, `SQLTimestamp.java`, `SQLDataFactory.java` |
| Password encryption | `AS400ImplRemote.java` (search `generateShaToken`, `generatePasswordSubstitute`, `MessageDigest`) |
| Port mapping | `PortMapper.java` |
| CCSID tables | `ConvTable37.java`, `ConvTable273.java`, `ConvTableMixedMap.java`, etc. |

## Known issues / parking lot

The closed-milestone "Deferred" subsections above are the
authoritative list. Items below are cross-cutting concerns that
don't slot into a single milestone.

- **`tx_commit` + `tx_rollback` fixtures captured as happy-path** —
  ✅ both done. tx_commit re-captured 2026-05-09 after enabling
  QSQJRN on AFTRAEGE11; tx_rollback re-captured 2026-05-10 against
  the GOTEST schema on IBM Cloud V7R6M0 (journal `GOJTJ*` +
  receiver `GOJTR*` created on first run, reused on subsequent
  runs). The new tx_rollback trace shows the full happy-path
  sequence: CREATE_RPB → PREPARE_DESCRIBE → CHANGE_DESCRIPTOR →
  EXECUTE → RPB cleanup → ROLLBACK (0x1808) → re-prepared SELECT
  returning zero rows → autocommit-on COMMIT + disconnect.
- **CCSID 65535 binary handling** — ✅ closed 2026-05-10; see
  "Deferred from M4" above.
- **`hostserver/doc.go` "database (TODO)" stub** — ✅ refreshed
  2026-05-10. Package doc now covers the full M2-M7 surface
  (wire format, top-level entry points, CCSID handling, LOB
  compression, extended metadata, cursor lifecycle, error
  semantics) plus cross-references to the contributor-facing
  docs and the JT400 source map.
- **Re-capture all fixtures post-M5** — resolved via #48 (cursor
  aligned with JT400's "fetch/close" signal); see "Deferred from
  M5" above. The synthetic helpers are gone.

## Risks (still relevant from feasibility plan)

1. **Packed-decimal sign nibbles** -- highest risk. Money columns
   silently corrupt if wrong. Reference `SQLDecimal.java`
   line-by-line.
2. **Unknown CPs in newer-server replies** -- log-and-skip strategy
   in parsers; never fail closed.
3. **Block-fetch boundaries** -- the 1000-row fixture is the
   stress test; the server returns multiple frames and the Go side
   has to stitch them.
4. **Connection-level CCSID double-meaning** -- one CCSID for SQL
   statement text, another for application data. Get this wrong and
   "broken EBCDIC" errors appear.
5. **TLS quirks if/when SSL ports come up** -- ✅ closed 2026-05-10.
   Live-validated against IBM Cloud V7R6M0; see "Deferred from M1"
   above for the cert-assignment flow.

## Testing strategy

Three layers, established in M0/M1:

- **Unit (fast, offline)** — codec tests with hand-computed expected
  bytes; ~70% of coverage.
- **Wire-replay (offline, fixture-driven)** — `wirelog.Consolidate`
  + `fakeConn` style. Full protocol round trips without IBM i.
- **Smoketest (live PUB400)** — `cmd/smoketest`. Run after every
  milestone gate.

Plus: when M5 lands, build a `cmd/diffrunner` that runs a query
through both the JTOpen Java sidecar (subprocess) and the Go driver,
diffs row-by-row. Use it nightly during M4-M7.

## Definition of done (M1 → first usable driver)

After M6, an external user should be able to:

```go
import _ "github.com/complacentsee/goJTOpen/driver"

db, _ := sql.Open("gojtopen",
    "gojtopen://user:pwd@host:8471/?library=AFTRAEGE1")
rows, _ := db.QueryContext(ctx,
    "SELECT ID, NAME, AMT FROM ORDERS WHERE STATUS = ?", "OPEN")
for rows.Next() { ... }
```

without a JVM, without a sidecar, without cgo.
