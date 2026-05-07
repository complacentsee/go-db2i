# goJTOpen

A pure-Go `database/sql` driver for IBM i (DB2 for i), in development.

## Status

**Pre-alpha.** The plan is in `docs/plan.md` (link from this README once it
lands). The current milestone is **M0: fixture-capture harness** — a Java
program that uses the real JTOpen driver against PUB400 to record wire
datastreams and JDBC ground truth. Fixtures will drive the Go driver's
offline replay tests.

## Layout

```
goJTOpen/
  testdata/
    jtopen-fixtures/   Maven project; M0 deliverable. See its README.
```

The Go module structure (`drda/`, `ebcdic/`, `decimal/`, `auth/`,
`driver/`, etc.) lands as M1+ work begins.

## License

TBD. JTOpen itself is IBM Public License v1.0; this project does not
redistribute JTOpen source. The fixture harness depends on JTOpen at
runtime via Maven Central, which is permitted.
