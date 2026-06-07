# Stored procedures

go-db2i calls DB2 for i stored procedures through the standard
`database/sql` interface: a `CALL` statement, parameter markers for the
arguments, and `sql.Out` for OUT/INOUT registration. Procedures that open
cursors return their result sets through `Rows`/`Rows.NextResultSet`, and
`ARRAY` parameters are carried by the generic [`db2i.Array[T]`](#array-parameters)
type.

All of the snippets below have runnable counterparts in
[`driver/example_test.go`](../driver/example_test.go)
(`Example_call`, `Example_callWithOut`, `Example_callMultiResultSet`,
`Example_callWithNamedParameters`).

## IN parameters

An IN-only `CALL` is an ordinary `Exec`: arguments flow through
`driver.Value` like any other prepared statement.

```go
_, err := db.Exec(`CALL mylib.bump_counter(?, ?)`, "WIDGET", 1)
```

## OUT and INOUT parameters

Wrap the destination in `sql.Out`. The driver tags each marker with its
direction on the wire (`0xF1` OUT, `0xF2` INOUT), asks the server to ship
the returned values back as a single-row reply, and reflect-assigns the
decoded values into your destinations when `Exec` returns. Set
`In: true` for an INOUT parameter so the bound value also flows in.

```go
var name string
var qty int
_, err := db.Exec(`CALL mylib.p_lookup(?, ?, ?)`,
    "WIDGET",
    sql.Out{Dest: &name},
    sql.Out{Dest: &qty},
)

// INOUT: the bound value flows in via *Dest, the server-returned value
// flows back to the same destination.
counter := 5
_, err = db.Exec(`CALL mylib.p_increment(?)`, sql.Out{Dest: &counter, In: true})
```

A SQL NULL returned into a non-pointer destination becomes the zero value
of that type; use a pointer destination (`sql.Out{Dest: &p}` with `p` a
`*int`) when you must tell NULL apart from zero.

### Named binding

`sql.Named` binds by the procedure's parameter name instead of position;
the driver resolves each name to its slot via the `QSYS2.SYSPARMS`
catalog, so the Go bind order need not match the declared order.

```go
_, err := db.Exec("CALL mylib.p_lookup(?, ?, ?)",
    sql.Named("P_QTY", sql.Out{Dest: &qty}),
    sql.Named("P_CODE", "WIDGET"),
    sql.Named("P_NAME", sql.Out{Dest: &name}),
)
```

## Result-set procedures

A procedure that opens cursors with `DECLARE CURSOR ... WITH RETURN`
returns them through `Query`. Iterate the first set with `Rows.Next`, then
advance to each further set with `Rows.NextResultSet`.

```go
rows, _ := db.Query(`CALL mylib.p_inventory(?)`, 5)
defer rows.Close()
for rows.Next() {
    var code string
    var qty int
    _ = rows.Scan(&code, &qty)
}
if rows.NextResultSet() {
    for rows.Next() {
        // second result set
    }
}
```

## ARRAY parameters

On DB2 for i an `ARRAY` crosses the host-server wire **only** as a
procedure parameter (IN/OUT/INOUT) — never as a result-set column
(see [Limitations](#limitations)). The driver carries one with the
generic carrier:

```go
type Array[T any] struct {
    Elements []T
    Null     bool
}
```

Bind it like any other argument; wrap it in `sql.Out` for OUT/INOUT:

```go
// IN
db.Exec("CALL mylib.p(?)", db2i.Array[int32]{Elements: []int32{1, 2, 3}})

// OUT
var a db2i.Array[int32]
db.Exec("CALL mylib.p(?)", sql.Out{Dest: &a})

// INOUT
db.Exec("CALL mylib.p(?)", sql.Out{Dest: &a, In: true})
```

The server-side type is a SQL array UDT, e.g.:

```sql
CREATE TYPE mylib.intarr AS INTEGER ARRAY[100];
CREATE PROCEDURE mylib.p (INOUT P mylib.intarr) LANGUAGE SQL ...
```

### Element types

The element SQL type is taken from the procedure's declared `ARRAY`
element type (the server's `PREPARE_DESCRIBE`), not from `T`; `T` only has
to be assignable to/from that element type. Supported element types,
round-tripped live (`test/conformance/array_param_test.go`):

| Element SQL type | Go element type |
|---|---|
| SMALLINT / INTEGER / BIGINT | `int16` / `int32` / `int64` |
| REAL / DOUBLE | `float32` / `float64` |
| DECIMAL / NUMERIC / DECFLOAT | `string` (or a `math/big` carrier) |
| CHAR / VARCHAR | `string` |
| BINARY / VARBINARY | `[]byte` |

**Temporal array elements (DATE / TIME / TIMESTAMP) are not supported.**
The driver's `time.Time` → IBM 26-char-timestamp conversion is applied to
scalar binds, not to per-element array encoding, so a `time.Time` element
is rejected. If you need a temporal array element, bind a `string` in the
IBM `YYYY-MM-DD-HH.MM.SS.ffffff` form against an array of the matching SQL
type.

### Per-element and whole-array NULLs

For per-element NULLs use a pointer element type; a nil pointer element is
sent (and decoded back) as a SQL NULL element:

```go
db2i.Array[*int32]{Elements: []*int32{&x, nil, &z}} // element 1 is NULL
```

With a non-pointer element type a decoded NULL element becomes the zero
value of `T` (a NULL INTEGER scans as `0`, indistinguishable from a real
`0`) — the same way an OUT scalar into a non-pointer destination resolves.
Reach for a pointer element type whenever NULL must be distinguishable.

Set `Null` for a whole-array SQL NULL, which is distinct from an empty
`Elements` (an array with zero elements).

## Wire protocol

When any parameter in the row is an array, the IN/INOUT marker data is
sent as **CP 0x382F** (`DBVariableData`) instead of the scalar CP 0x381F,
and the OUT/INOUT values come back as **CP 0x3901** instead of the scalar
CP 0x380E. Each descriptor carries the element type, per-element byte
stride, and cardinality inline. The byte layout is documented, with live
JT400 fixtures, in
[`testdata/jtopen-fixtures/array-wire-spec.md`](../testdata/jtopen-fixtures/array-wire-spec.md).

## Package caching and CALL

`CALL` always takes the full PREPARE_DESCRIBE + EXECUTE path, matching
JT400's `CallableStatement` flow. The extended-dynamic package mechanism
(see [`docs/package-caching.md`](./package-caching.md)) files a `CALL` into
the server-side `*PGM` only under go-db2i's own `package-criteria=extended`
(JT400 has no equivalent criterion). Scalar OUT/INOUT CALLs (integers,
decimals, binary) do use the cache-hit fast path.

A `CALL` with any `ARRAY` parameter, however, is **never** placed on the
cache-hit path — and this is a permanent, server-enforced limitation, not a
deferral. DB2 for i cannot serialize an array parameter's SQLDA through the
package mechanism: the array `CALL` files into the `*PGM`, but a subsequent
`RETURN_PACKAGE` fails with **SQL-351 ("unsupported SQLTYPE")** and returns
the package empty — which would poison cache-hit dispatch for every
statement sharing that package. (A scalar `CALL` round-trips fine, so the
limitation is array-specific.) Array CALLs therefore always re-prepare. The
same divert applies to a `CALL` with a temporal (DATE/TIME/TIMESTAMP)
OUT/INOUT parameter, for an unrelated reason (server SQL-180).

## Error handling

Server-side SQL errors arrive as `*hostserver.Db2Error`, so you can switch
on SQLSTATE / SQLCODE without parsing the formatted message
(`Example_db2Error` in [`driver/example_test.go`](../driver/example_test.go)).

## Limitations

- **Arrays are procedure parameters only**, never result-set columns —
  DB2 for i rejects an ARRAY result column with SQL-20441 (issue #39).
- **Single dimension only**: DB2 for i arrays are one-dimensional; nested
  or multi-dimensional arrays are not supported.
- **No LOB array elements**: arrays are inline-only; a CLOB/BLOB locator
  cannot be an array element.
- **No client-side cardinality check**: the declared `ARRAY[N]` maximum is
  not validated in the driver; the server enforces it.

## See also

- [`docs/configuration.md`](./configuration.md) — full DSN reference.
- [`docs/package-caching.md`](./package-caching.md) — extended-dynamic
  package eligibility for CALL.
- [`testdata/jtopen-fixtures/array-wire-spec.md`](../testdata/jtopen-fixtures/array-wire-spec.md)
  — the authoritative ARRAY wire-format spec.
- [`driver/example_test.go`](../driver/example_test.go) — runnable CALL
  examples.
