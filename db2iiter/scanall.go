package db2iiter

import (
	"database/sql"
	"iter"
)

// ScanAll returns an [iter.Seq2] over rows that yields one
// (T, error) pair per row, where T is whatever `scan` builds from
// the current row. The contract:
//
//   - rows.Next() drives the loop; iteration stops at end-of-data.
//   - scan is called once per row; its error is yielded alongside
//     the (zero-)value and iteration stops.
//   - After the loop, rows.Err() is checked once; if non-nil it is
//     yielded as a final (zero, err) pair.
//   - Closing rows is the caller's responsibility (use
//     `defer rows.Close()` before ranging).
//   - When the range body breaks early, the helper returns
//     immediately -- no further scan() calls and no rows.Err()
//     check (the caller's break signals they're done with the
//     result set).
//
// scan must NOT call rows.Next() itself; the helper owns the
// cursor advancement.
func ScanAll[T any](rows *sql.Rows, scan func(*sql.Rows) (T, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for rows.Next() {
			v, err := scan(rows)
			if !yield(v, err) {
				return
			}
			if err != nil {
				return
			}
		}
		if err := rows.Err(); err != nil {
			var zero T
			yield(zero, err)
		}
	}
}
