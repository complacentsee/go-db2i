package driver

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/complacentsee/goJTOpen/hostserver"
)

// Result implements driver.Result.
//
// LastInsertId issues a follow-up `VALUES IDENTITY_VAL_LOCAL()`
// against the same connection that ran the INSERT. The result is
// cached so repeated LastInsertId calls don't hit the wire more
// than once. When the table has no IDENTITY column, the server
// returns NULL and we surface ErrNoLastInsertId so callers can
// distinguish "not supported" from "no rows affected".
//
// RowsAffected currently returns the row count carried back via
// the Exec path; SQLCA decoding (CP 0x3807 SQLERRD[2]) lands with
// M7 -- today the host-server layer pre-fills 0.
type Result struct {
	rowsAffected int64

	// conn is the connection the INSERT ran on; required for
	// IDENTITY_VAL_LOCAL since the value is session-scoped. nil
	// when the Result was created outside of an Exec path
	// (e.g. tests).
	conn *Conn

	// once gates the IDENTITY_VAL_LOCAL round trip so concurrent
	// LastInsertId calls only fire one query.
	once   sync.Once
	cachedID int64
	cachedErr error
}

// ErrNoLastInsertId is returned by Result.LastInsertId when the
// underlying INSERT did not generate an IDENTITY value -- typically
// because the target table has no IDENTITY column. Distinct from a
// generic "not supported" error so callers can tell the difference
// between "the driver doesn't support this" (impossible here) and
// "this particular table doesn't use it".
var ErrNoLastInsertId = errors.New("gojtopen: INSERT did not generate an IDENTITY value (table has no IDENTITY column)")

// LastInsertId returns the IDENTITY value most recently generated
// in this session, via a `VALUES IDENTITY_VAL_LOCAL()` round trip.
// Cached after the first call on a given Result so repeated
// invocations are free.
//
// Important caveat: IBM i `IDENTITY_VAL_LOCAL()` is session-scoped,
// not statement-scoped. It returns the IDENTITY value from the most
// recent IDENTITY-generating INSERT on the session, regardless of
// which table the most recent INSERT (or this Result's INSERT) hit.
// Concretely:
//
//   1. INSERT INTO has_identity → LastInsertId() = N
//   2. INSERT INTO no_identity  → LastInsertId() STILL = N (sticky)
//
// Callers who need to know whether a specific INSERT generated an
// IDENTITY value should compare LastInsertId() before-and-after,
// or use `INSERT INTO ... SELECT IDENTITY_VAL_LOCAL() FROM
// SYSIBM.SYSDUMMY1` to scope the lookup. JDBC / JT400 has the same
// limitation.
//
// Returns ErrNoLastInsertId when no IDENTITY-generating INSERT has
// run on the session yet (IDENTITY_VAL_LOCAL() returns NULL).
// Returns a connection error when the underlying SELECT itself
// fails -- in that case the conn is also marked dead via
// classifyConnErr per the usual error path.
func (r *Result) LastInsertId() (int64, error) {
	if r.conn == nil {
		// Programmatically-constructed Result (no conn attached).
		// Fall back to the legacy error so callers can detect this
		// case via errors.Is.
		return 0, ErrNoLastInsertId
	}
	r.once.Do(func() {
		r.cachedID, r.cachedErr = r.fetchLastInsertId()
	})
	return r.cachedID, r.cachedErr
}

// fetchLastInsertId runs the IDENTITY_VAL_LOCAL() round trip. On a
// successful row with a non-NULL value, parses it as int64. NULL or
// no-rows surfaces as ErrNoLastInsertId.
func (r *Result) fetchLastInsertId() (int64, error) {
	cursor, err := hostserver.OpenSelectStatic(r.conn.conn, "VALUES IDENTITY_VAL_LOCAL()", r.conn.nextCorrFunc())
	if err != nil {
		return 0, r.conn.classifyConnErr(err)
	}
	defer cursor.Close()
	row, err := cursor.Next()
	if errors.Is(err, io.EOF) {
		return 0, ErrNoLastInsertId
	}
	if err != nil {
		return 0, r.conn.classifyConnErr(err)
	}
	if len(row) == 0 || row[0] == nil {
		return 0, ErrNoLastInsertId
	}
	switch v := row[0].(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case string:
		// IDENTITY columns returning DECIMAL(31,0) come through as a
		// decimal string; trim and parse. Strip any trailing
		// whitespace from EBCDIC space padding.
		s := strings.TrimSpace(v)
		// IDENTITY_VAL_LOCAL never returns fractional values but the
		// DECIMAL(31,0) string form may include a trailing dot.
		if i := strings.IndexByte(s, '.'); i >= 0 {
			s = s[:i]
		}
		n, perr := strconv.ParseInt(s, 10, 64)
		if perr != nil {
			return 0, fmt.Errorf("gojtopen: parse IDENTITY value %q: %w", v, perr)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("gojtopen: unexpected IDENTITY_VAL_LOCAL() result type %T (%v)", row[0], row[0])
	}
}

// RowsAffected returns the row count parsed out of the EXECUTE
// reply's SQLERRD3 slot (CP 0x3807 SQLCA bytes 100..103). Implements
// database/sql/driver.Result.RowsAffected.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
