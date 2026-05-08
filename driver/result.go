package driver

// Result implements driver.Result. We only carry rows-affected; the
// IBM i host server doesn't expose generated keys via the standard
// SQL service path, so LastInsertId returns an error per the
// database/sql contract for drivers that don't support it.
type Result struct {
	rowsAffected int64
}

func (r *Result) LastInsertId() (int64, error) {
	return 0, errLastInsertIdUnsupported
}

func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// errLastInsertIdUnsupported is returned by Result.LastInsertId.
// IBM i's IDENTITY column lookup typically uses a separate
// IDENTITY_VAL_LOCAL() round trip; we don't issue that today.
var errLastInsertIdUnsupported = lastInsertIdUnsupportedError{}

type lastInsertIdUnsupportedError struct{}

func (lastInsertIdUnsupportedError) Error() string {
	return "gojtopen: LastInsertId not supported (use IDENTITY_VAL_LOCAL() in a follow-up SELECT)"
}
