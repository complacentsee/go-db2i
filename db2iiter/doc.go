// Package db2iiter provides range-over-func (Go 1.23
// [iter.Seq2]) adapters that turn a [*database/sql.Rows] into an
// idiomatic for-range loop.
//
// The package has no dependency on go-db2i internals; it works
// against any driver. It lives in the go-db2i module because the
// migration story from JT400 includes "show me the modern Go
// idiom for the equivalent of `while (rs.next())`" -- this is it.
//
// # Example
//
//	import (
//	    "database/sql"
//	    "log"
//	    "github.com/complacentsee/go-db2i/db2iiter"
//	)
//
//	type Row struct{ ID int64; Name string }
//
//	rows, err := db.QueryContext(ctx, "SELECT id, name FROM t")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer rows.Close()
//
//	scan := func(rs *sql.Rows) (Row, error) {
//	    var r Row
//	    return r, rs.Scan(&r.ID, &r.Name)
//	}
//
//	for row, err := range db2iiter.ScanAll(rows, scan) {
//	    if err != nil {
//	        return err
//	    }
//	    use(row)
//	}
//
// # Lifetime
//
// The caller owns the [*database/sql.Rows] and is responsible for
// `rows.Close()` -- the adapter does not close it for you, since
// closing-on-iter-exhaustion would hide late "rows.Err() failed"
// reports and complicate cancel-mid-iteration.
package db2iiter
