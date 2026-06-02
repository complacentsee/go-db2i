# go-db2i roadmap

Forward-looking work for the driver. This file is intentionally *not*
a history — shipped milestones live in [`CHANGELOG.md`](./CHANGELOG.md)
and the archived milestone record in
[`internal/history/PLAN.md`](./internal/history/PLAN.md).

Items are grouped by area and tagged with a rough priority:
**planned** (scheduled), **demand-driven** (built when a workload needs
it), or **deferred** (acknowledged, not yet scheduled). Nothing here is
a commitment to a date.

## CCSID coverage

The driver round-trips 6 CCSIDs today (37, 273, 1208, 13488, 1200,
65535 — see [`docs/ccsid-support.md`](./docs/ccsid-support.md)). JT400
ships ~50 SBCS pages plus DBCS. Expansion is phased by effort and by
the quality of the source-authority data.

The `?charset-strict=true` knob that promotes the unknown-CCSID
fallback from a one-time warning to a hard error is **already shipped**
— the remaining work below is codec pages only.

### Phase 1 — SBCS European EBCDIC (planned)

Extend the real CDRA-sourced `[256]rune` table pattern already used by
`ebcdic/ccsid273.go` to the rest of the common European SBCS pages:

- **Stdlib-backed** (mapping comes from `golang.org/x/text/encoding/charmap`,
  no committed table data): 1047, 1140.
- **Hand-tabled** via a new `cmd/gen-ebcdic` generator: 277, 278, 280,
  284, 285, 297, 500, 871, 1141–1149.

New tooling: `cmd/gen-ebcdic/main.go` reads ICU UCM files committed at
`ebcdic/testdata/cdra-ucm/ibm-NNN.ucm` and emits `ebcdic/ccsidNNN.go`
files. The generator cross-checks its 037 / 1140 output against the two
stdlib oracles (`charmap.CodePage037` / `charmap.CodePage1140`) and
round-trips every byte per page; the ~17–20 divergent positions per
page are hand-verified.

### Phase 2 — remaining SBCS (demand-driven)

Cyrillic (1025), Greek (875), Turkish (1026), Hebrew (424), Arabic
(420), plus the legacy EBCDIC pages (256, 290, 423, 833, 836, 838, 870,
880, 905, 918, 920, 1097, 1112, 1122, 1123, 1153–1158, 1160, 1164,
1166). Per-page effort is identical to Phase 1; held back by demand,
not complexity.

**Bidi caveat (Hebrew 424, Arabic 420).** Byte→codepoint mapping is
straightforward, but Hebrew/Arabic codecs would ship as *logical-order
Unicode codepoints only* — visual reordering and Arabic shaping stay
the caller's responsibility. Full JT400 bidi parity (porting
`AS400BidiTransform` / `BidiOrder` / `BidiShape`) is out of scope
without explicit customer demand.

### Phase 3 — DBCS / mixed encodings (deferred)

Japanese (930, 933, 939), Korean (933, 1364), Simplified Chinese (935,
1388), Traditional Chinese (937, 1371); pure DBCS pages only if
mixed-DBCS lands. Qualitatively harder to vet than SBCS: stateful
shift-in/shift-out transitions, no `golang.org/x/text` oracle (cross-check
is against JT400 directly), CDRA mapping-revision ambiguity, and PUA /
Han-unification edge cases. Not started without a concrete deployment
that needs it.

## LOB handling

- **DBCLOB strict-UCS-2 via DSN flag** — the strict encoder
  (`encodeUCS2BEStrict` + the typed `*hostserver.NonBMPRuneError`) is
  implemented but only reachable from package tests. Planned to surface
  as `?dbclob-strict=true` so callers can promote the non-BMP
  substitution to a typed error. (deferred)
- **DBCLOB bind fixture** — the DBCLOB bind wire path ships; only the
  JT400 reference capture is still outstanding. (deferred)
- **Append-style LOB writes** — all bind paths are full-replace today;
  an opt-in `LOBValue.Append` field is reachable but unwired. (deferred)
- **Large-LOB throughput** — a server-side throughput cliff on very
  large (>16 MiB) LOBs is tracked as a separate line of work. (deferred)

## Type & format handling

- **Non-ISO TIME → `time.Time`** — the TIME auto-promotion currently
  only understands ISO; widening it to the USA/EUR/JIS `?time-format`
  values touches the row-decode path. Until then, `Scan` non-ISO TIME
  into a `string`. (deferred)
- **`LastInsertId` via `IDENTITY_VAL_LOCAL()`** — currently
  session-scoped / best-effort; a more complete implementation is a
  nice-to-have. (deferred)

## Package caching

- **Programmatic package clear** — `?package-clear` is accepted and
  shape-validated but is a no-op (the server manages clearing). A real
  programmatic `DLTPGM`-equivalent from the driver is deferred. (deferred)

## Tooling

- **`cmd/diffrunner`** — a proposed nightly JT400-vs-Go row-by-row
  cross-check harness. Proposed since M5, never built; either schedule
  it as conformance tooling or formally drop it. (deferred / decide)

## Toward v1.0

The driver is intentionally pre-1.0 while wire compatibility is built
up across IBM i versions. The public API (the `database/sql` surface
plus the driver-typed `*db2i.Conn` methods) is expected to settle at
**v1.0** once that compatibility is locked. Strict-by-default
unknown-CCSID handling is a v1.0 candidate (it changes default
behaviour, so it needs the major-version boundary).

## Out of scope (won't add)

These are deliberate non-goals, not backlog:

- Full JT400 BiDi text reordering / Arabic shaping (see Phase 2 caveat).
- ICU via cgo — the no-cgo property is a load-bearing project tenet.
- JT400 `translate boolean` / `translate hex` / `translate binary`.
- XA / distributed transactions, client reroute / seamless failover
  (use Go's `sql.DB` pooling), and the JTOpen proxy server.
- Non-JDBC JTOpen services (`CommandCall`, `IFSFile`, `DataQueue`, …)
  and scrollable cursors.
