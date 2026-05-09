# Changelog

All notable changes to **goJTOpen** are documented here. Format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/).

The driver is **pre-1.0** while wire compatibility is being built up
across IBM i versions; expect the public API surface to settle at
0.5+ once `bradfitz/go-sql-test` conformance, TLS, and LOB streaming
have all landed.

## [Unreleased]

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

- BLOB/CLOB streaming (M7).
- TLS sign-on / database (ports 9476 / 9471) (M7).
- `bradfitz/go-sql-test` formal conformance run (M8).
- `slog` integration / OpenTelemetry spans (M8).
