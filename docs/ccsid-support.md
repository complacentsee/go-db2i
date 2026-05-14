# CCSID support in go-db2i

This page documents which IBM CCSIDs the driver round-trips
correctly, which it does not, and the workarounds for the gaps.
CCSID = Coded Character Set Identifier; an IBM-assigned ID for a
specific byte-to-character mapping. IBM i jobs run under a
job-level CCSID, and CHAR/VARCHAR/CLOB columns each carry their
own CCSID tag that tells the client how to interpret column bytes.

## Currently supported (encode + decode)

| CCSID | Description           | Region / use         | Backing                                                |
|-------|-----------------------|----------------------|--------------------------------------------------------|
| 37    | US English EBCDIC     | US / Canada          | `golang.org/x/text/encoding/charmap.CodePage037`       |
| 273   | German EBCDIC         | DE / AT (PARTIAL)    | CCSID 37 stand-in (see limitation below)               |
| 1208  | UTF-8                 | Unicode              | Passthrough (no transcode)                             |
| 13488 | UCS-2 BE              | Unicode (GRAPHIC)    | `unicode/utf16`                                        |
| 1200  | UTF-16 BE             | Unicode (GRAPHIC)    | `unicode/utf16`                                        |
| 65535 | FOR BIT DATA          | Binary (no encoding) | Raw passthrough as `[]byte`                            |

### Note on CCSID 273

The driver currently wraps `charmap.CodePage037` for the CCSID
273 codec slot. For the ASCII printable subset (digits, `A-Z`,
`a-z`, space, common punctuation) this produces correct bytes
because 037 and 273 agree on 239 of 256 positions. The 17
positions that diverge — `§ Ä Ö Ü ä ö ü ß` plus reshuffled
punctuation (`@ # $ ^ [ ] { } ~ \`` and a few others) — will
round-trip **incorrectly**. A real CCSID 273 mapping table is
planned (see "Planned" below). German shops that need correct
Umlaut handling today should override the session CCSID to 1208
(UTF-8) via `?ccsid=1208` if the server supports it (V7R3+ in
most configurations).

## Unsupported (silent fallback to CCSID 37)

Any CCSID not in the table above is silently decoded as CCSID 37.
This means **wrong bytes without an error** when:

- An IBM i job advertises an unsupported job CCSID and the driver
  inherits it.
- A column carries an unsupported CCSID tag in its metadata.

Common unsupported CCSIDs:

- **European SBCS** — 277 (DK / NO), 278 (FI / SE), 280 (IT),
  284 (ES), 285 (UK), 297 (FR), 500 (International), 871 (IS).
- **Euro variants of the above** — 1140 (US-euro),
  1141 (DE-euro) through 1149 (FI / SE-euro).
- **Latin-1 EBCDIC** — 1047 (USS / AIX / z/OS interop).
- **DBCS / mixed SBCS+DBCS** — 930, 933, 935, 937, 939, 1364,
  1371, 1388 (Japanese / Korean / Simplified Chinese /
  Traditional Chinese).
- **Eastern European / Greek / Cyrillic / Hebrew / Arabic** — 870,
  875, 1025, 424, 420, 1026.

### Workarounds

1. **Override the session CCSID.** If your IBM i job advertises a
   CCSID we don't decode but the *data* in your CHAR columns is
   actually stored as one we support, force the override:

   ```
   db2i://USER:PASS@host:8471/?ccsid=37&library=...
   ```

   The driver will tag client binds and decode replies as if the
   column were CCSID 37. Only safe if you know the data is
   genuinely in that CCSID, not just the job default.

2. **Cast to Unicode on the wire.** Force server-side transcode
   to UCS-2 (which the driver round-trips natively):

   ```sql
   SELECT CAST(col AS VARGRAPHIC(N) CCSID 13488) FROM YOURLIB.YOURTAB;
   ```

   Works for any source CCSID the server understands, at the cost
   of one extra server-side conversion step per row.

3. **Treat as binary.** If you control the schema, declare
   affected columns as `FOR BIT DATA` (CCSID 65535) and handle
   encoding in your Go code via the `ebcdic` subpackage or
   another library.

## Planned

Additional CCSID coverage is rolled out in three phases. The
roadmap and per-CCSID priority lives in
[`../internal/PLAN.md`](../internal/PLAN.md). Phase 1 adds the
common European SBCS pages (273 real-table upgrade, 277, 278,
280, 284, 285, 297, 500, 871, 1047, 1140-1149) and introduces an
opt-in `?charset-strict=true` DSN knob that promotes
unknown-CCSID fallback from silent to a hard error. Phase 2
extends to Cyrillic / Greek / Turkish / Hebrew / Arabic plus
legacy EBCDIC pages on a demand-driven basis; Phase 3 covers
DBCS / mixed Japanese / Korean / Chinese encodings.

If you need a specific CCSID that's not yet supported, please
open an issue with the CCSID number, sample column shape, and
the deployment context — that's what drives prioritization.

## How to verify CCSID handling for your workload

1. **Find your job CCSID** on the IBM i:

   ```sql
   VALUES CURRENT SERVER.CCSID;
   -- or
   SELECT CCSID FROM TABLE(QSYS2.JOB_INFO()) WHERE ORDINAL_POSITION = 1;
   ```

2. **Find your column CCSIDs**:

   ```sql
   SELECT COLUMN_NAME, CCSID
     FROM QSYS2.SYSCOLUMNS
     WHERE TABLE_SCHEMA = 'YOURLIB' AND TABLE_NAME = 'YOURTAB';
   ```

3. **Cross-reference both numbers against "Currently supported"
   above.** If everything appears there, you're good. If either
   number is in "Unsupported," apply a workaround.

4. **Smoke test against a known-good string.** Pull a single row
   whose contents you can recognize visually:

   ```go
   var s string
   err := db.QueryRow("SELECT col FROM YOURLIB.YOURTAB FETCH FIRST 1 ROW ONLY").Scan(&s)
   ```

   Unsupported CCSIDs surface as visibly garbled text rather than
   an error today — a hex-dump of the returned bytes against the
   IBM CDRA table for your actual CCSID is the most reliable
   diagnostic. The `?charset-strict=true` DSN knob (planned) will
   convert these silent fallbacks into typed errors.

## Related

- [`docs/configuration.md`](./configuration.md) — `?ccsid=` DSN knob.
- [`MIGRATING.md`](../MIGRATING.md) —
  JT400 ↔ go-db2i CCSID feature parity table.
- [`docs/lob-known-gaps.md`](./lob-known-gaps.md) — DBCLOB
  encoding (CCSID 13488 vs 1200) details.
- `ebcdic` subpackage godoc — the public `Codec` interface and
  per-CCSID codecs that back the driver's encode / decode paths.
