# JTOpen fixture-capture harness

This is the M0 deliverable for goJTOpen: a small Maven project that uses the
real JTOpen JDBC driver against [PUB400](https://pub400.com) to produce two
artifacts per test case:

1. **`fixtures/<case>.trace`** — JTOpen's `com.ibm.as400.access.Trace`
   datastream dump (every byte sent and received, with offsets).
2. **`fixtures/<case>.golden.json`** — the JDBC ground truth for the same
   case: column metadata, every row's typed values, and any `SQLException`
   detail (SQLSTATE, SQLCODE, message).

Both files are committed to this repo. The Go driver's wire-replay tests
read the `.trace` to drive a fake server, then assert the parsed result
matches the `.golden.json`. This means **Go tests need no network and no
IBM i** — once fixtures land in git, anyone can run `go test ./...`.

## Caveats

- JTOpen JDBC speaks the **IBM i host-server datastream on port 8471**, not
  DRDA on port 446. So these fixtures drive the host-server fallback path,
  not the DRDA path. DRDA fixtures need a separate capture (Wireshark
  against an IBM i, or Apache Derby in Docker for spec-only cases). See the
  goJTOpen plan for details.
- PUB400 schema names are user-specific. Fixtures captured from one PUB400
  account may show that account name in some metadata responses (e.g.,
  catalog calls). For the cases declared here we mostly target
  `SYSIBM.SYSDUMMY1` and use `<schema>.GOJTOPEN_T1` for the few
  table-backed cases.

## Prerequisites

- Java 8+ and Maven 3.6+
- A free PUB400 account ([sign up](https://pub400.com)). The harness uses
  TCP 8471 outbound from your machine to `pub400.com`.

## Configuration

Set environment variables before running:

| Variable        | Required | Default            | Notes                                      |
| --------------- | -------- | ------------------ | ------------------------------------------ |
| `PUB400_USER`   | yes      | —                  | Your PUB400 user ID                        |
| `PUB400_PWD`    | yes      | —                  | Your PUB400 password                       |
| `PUB400_HOST`   | no       | `pub400.com`       | Override only if running against a private IBM i |
| `PUB400_SCHEMA` | no       | `PUB400_USER` (uppercased) | Schema/library used for table-backed cases |
| `FIXTURES_DIR`  | no       | `fixtures`         | Where to write `.trace` and `.golden.json` |
| `ONLY`          | no       | (run all)          | Comma-separated list of case names to run  |

## Run

From this directory (`testdata/jtopen-fixtures/`):

```sh
# bash / zsh
PUB400_USER=YOURUSER PUB400_PWD='your-password' \
  mvn -q exec:java

# PowerShell
$env:PUB400_USER = "YOURUSER"
$env:PUB400_PWD  = "your-password"
mvn -q exec:java
```

Or build a fat-jar and run that:

```sh
mvn -q package
java -jar target/gojtopen-fixtures-0.1.0-SNAPSHOT.jar
```

Output:

```
goJTOpen fixture capture
  host:     pub400.com
  user:     YOURUSER
  schema:   YOURUSER
  fixtures: /path/to/goJTOpen/testdata/jtopen-fixtures/fixtures
  cases:    27

[ connect_only ] ok
[ select_dummy ] ok
[ types_smallint ] ok
...
Done. 27 ok, 0 failed.
```

## Run a subset

```sh
ONLY=connect_only,select_dummy mvn -q exec:java
```

## What the captured cases cover

| Case                            | What it exercises                              |
| ------------------------------- | ---------------------------------------------- |
| `connect_only`                  | Sign-on, exchange-attributes, disconnect       |
| `select_dummy`                  | Bare SELECT, server timestamp, current user    |
| `types_smallint` … `types_bigint` | Binary integer width                         |
| `types_real`, `types_double`    | IEEE 754 floats                                |
| `types_decimal_*`               | Packed decimal at common + edge precisions     |
| `types_decimal_negative_31_5`   | Negative packed decimal sign nibble            |
| `types_numeric_*`               | Zoned decimal                                  |
| `types_decfloat_16`, `_34`      | IEEE 754-2008 decimal float                    |
| `types_char_10`, `_varchar_*`   | EBCDIC strings (default CCSID 37)              |
| `types_varchar_empty`           | Empty string vs NULL distinction               |
| `types_date`, `_time`, `_timestamp` | Date/time formats                          |
| `types_null`                    | NULL of every common type                      |
| `select_multi_column`           | Multi-column row, mixed types                  |
| `prepared_int_param`, etc.      | PREPARE + EXECUTE with bound parameters        |
| `multi_row_fetch_1k`            | Block fetch / continuation across 1000 rows    |
| `tx_commit`, `tx_rollback`      | Manual transaction boundaries                  |
| `error_syntax`, `error_table_not_found` | SQLException → SQLCARD mapping         |

Add new cases by editing `src/main/java/.../Cases.java` and re-running.

## Adding a case

1. Add a new `Case` subclass (or use `SelectStatic`) inside
   `Cases.all(String schema)`. Pick a stable, lowercase, underscored name.
2. Run the harness: `ONLY=my_new_case mvn -q exec:java`.
3. Inspect `fixtures/my_new_case.golden.json` for sanity.
4. Commit both `fixtures/my_new_case.trace` and `fixtures/my_new_case.golden.json`.

## Re-capturing after JTOpen upgrades

Bump `<jt400.version>` in `pom.xml`, re-run, diff the fixtures. If they
change, the Go driver's replay tests will catch any behavioral drift on
re-test.
