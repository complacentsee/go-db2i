# Changelog

All notable changes to **goJTOpen** are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).

The driver is **pre-1.0** while wire compatibility is being built up
across IBM i versions; expect the public API surface to settle at
0.5+ once LOB bind, slog observability, and OTel spans all land.

## [Unreleased]

### Added

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

### Fixed

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

- LOB bind on parameter markers (writing large LOBs as INSERT /
  UPDATE values via `?` placeholders). Inline literals
  (`X'...'`, string literals) still work for small LOBs.
- TLS sign-on / database (ports 9476 / 9471) (M7).
- `slog` integration / OpenTelemetry spans (M8).
