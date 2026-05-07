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
- ✅ **M1 (sign-on side)** — Pure-Go sign-on against as-signon (port
  8476) works end-to-end on PUB400. Includes:
  - DSS framing (`hostserver/dss.go`)
  - Exchange-attributes + signon-info request/reply
    (`hostserver/signon.go`)
  - SHA-1 password encryption — levels 2 + 3 (`auth/password_sha1.go`)
  - CCSID 37 codec (`ebcdic/ccsid.go`)
  - `hostserver.SignOn()` orchestration
  - `cmd/smoketest` validates live against PUB400
- ⏳ **M1 (database side)** — `as-database` handshake on port 8471.
  Different (simpler) opening sequence than as-signon. Fixtures in
  `connect_only.trace` recv #3..#6 + sent frames 4..8.

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

### M5 — ResultSet metadata, transactions, isolation (~1-2 weeks)

- Column metadata (`getColumnName/Type/Precision/Scale/etc`) from
  the JDServerRow descriptors.
- `database/sql.Tx` support: `RDBCMM` and `RDBRLLBCK` request
  encoding, autocommit toggle wire path.
- Isolation levels: map JDBC constants (`*CHG`, `*CS`, `*ALL`,
  `*RR`) to JTOpen's URL property values.
- Acceptance: extend smoketest with a `BEGIN; INSERT; COMMIT; SELECT`
  cycle on a journaled table.

### M6 — `database/sql` driver registration + conformance (~2 weeks)

- Implement `database/sql/driver`: `Driver`, `Connector`, `Conn`,
  `Stmt`, `Rows`, `Tx`, plus the `Context` variants.
- `OpenConnector` with DSN parsing. Mirror JTOpen URL syntax where
  reasonable: `goJTOpen://user:pwd@host:port/?library=AFTRAEGE1&naming=sql`.
- Run `bradfitz/go-sql-test` (or the `go-sql-driver/mysql` adapted
  test harness) end-to-end.

### M7 — LOB streaming, CCSID negotiation, error mapping (~3-4 weeks)

- BLOB/CLOB handling via the host-server's chunked-data path
  (DBLobLocator-equivalent).
- SQLCA → typed `*Db2Error{SQLState, SQLCode, Message, Tokens}`
  with substitution.
- Connection-level CCSID negotiation. Distinguish the SQL-statement
  CCSID from the application-data CCSID.

### M8 — Hardening, observability, docs (~2-3 weeks)

- `slog` integration in the driver layer.
- OpenTelemetry spans on `Query`/`Exec`.
- Fuzz tests on `wirelog.ParseJTOpenTrace` and the DB reply parser.
- godoc, migration guide from JTOpen DSN, performance tuning notes.

**Total remaining: ~14-22 weeks.** The original 17-25 week M0-M8
estimate net of M0/M1 gives us roughly the same window.

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

- **`tx_commit` / `tx_rollback` fixtures** — currently SQL7008
  error-path captures because PUB400 won't journal our test table for
  this account. The SQL7008 wire path is still useful (for
  M5 error handling). Re-capture against a real IBM i with
  journaling enabled to get the happy commit/rollback wire trace.
- **Password levels 0/1 (DES) and 4 (PBKDF2-SHA-512)** —
  unimplemented. PUB400 is level 3 so this hasn't blocked anything,
  but a production IBM i may use level 4. Add when needed; algorithms
  in `AS400ImplRemote.encryptPassword` (DES) and the PBKDF2 block
  starting around line 5200.
- **Schema/table names in column metadata are blank** — JTOpen needs
  the `extended metadata=true` URL property for these to populate.
  Decide whether to enable by default or expose as a Conn option.
- **CCSID 65535 binary handling** — make sure every type-decoding
  path checks the column CCSID before attempting EBCDIC decode.
- **DECFLOAT** — currently surfaces as `Types.OTHER` (1111) in the
  fixture goldens because JDBC has no constant for it. M4 handles
  this by typename match.

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
5. **TLS quirks if/when SSL ports come up** -- IBM i certs sometimes
   have IP in CN with no DNS SAN; `crypto/tls` rejects by default.

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
