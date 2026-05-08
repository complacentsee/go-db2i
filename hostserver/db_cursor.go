package hostserver

import (
	"errors"
	"fmt"
	"io"
)

// Cursor is an open server-side SELECT cursor with rows pending. The
// driver creates one via OpenSelectStatic / OpenSelectPrepared and
// drains it row-by-row through Next; Close releases the RPB slot
// and any partially-consumed cursor on the server.
//
// Cursor is NOT safe for concurrent use. database/sql guarantees
// only one Rows iteration per Conn at a time, which is the only
// caller we currently support.
//
// Lifecycle:
//
//	c, err := OpenSelectStatic(conn, sql, nextCorr)
//	if err != nil { ... }
//	defer c.Close()
//	for {
//	    row, err := c.Next()
//	    if errors.Is(err, io.EOF) { break }
//	    if err != nil { return err }
//	    use(row)
//	}
//
// Calling Close on an already-drained cursor is a no-op. Calling
// Close before draining sends an RPB DELETE (the server uses the
// implicit cursor-on-RPB association to release the cursor along
// with the RPB slot, which is what JT400 does too).
type Cursor struct {
	conn      io.ReadWriter
	cols      []SelectColumn
	pending   []SelectRow // rows from the most recent FETCH batch not yet returned
	pendingIx int         // index into pending
	nextCorr  func() uint32
	exhausted bool // server signaled end-of-data; no more FETCHes needed
	rpbActive bool // true between OPEN and DELETE; false after Close
}

// Columns returns the result-set descriptor list. Stable for the
// lifetime of the cursor (server cannot change column shape mid-
// iteration).
func (c *Cursor) Columns() []SelectColumn { return c.cols }

// Next returns the next row. Returns io.EOF when the cursor is
// drained. Errors from continuation FETCH are surfaced verbatim
// (typically *Db2Error with the OP="FETCH" tag); the cursor remains
// open so the caller can still Close it.
func (c *Cursor) Next() (SelectRow, error) {
	if c.pendingIx < len(c.pending) {
		row := c.pending[c.pendingIx]
		c.pendingIx++
		return row, nil
	}
	if c.exhausted {
		return nil, io.EOF
	}
	// Pull another batch.
	more, done, err := fetchMoreRows(c.conn, c.cols, c.nextCorr())
	if err != nil {
		return nil, err
	}
	if done {
		c.exhausted = true
		c.pending = nil
		c.pendingIx = 0
		// PUB400 sometimes ships rows on the same FETCH that signals
		// done -- but our fetchMoreRows treats done=true as "no
		// rows", so this branch is always the empty-tail case.
		// Keep the API simple.
		return nil, io.EOF
	}
	if len(more) == 0 {
		// Empty batch with done=false shouldn't happen on PUB400 but
		// some V7R3 servers send a synthetic 0-row reply between
		// real batches. Loop once.
		return c.Next()
	}
	c.pending = more
	c.pendingIx = 1
	return more[0], nil
}

// Close releases server-side resources. When the cursor still has
// rows pending (early-close from the caller), sends an explicit
// CLOSE (0x180A) first to tell the server the cursor is no longer
// in use; otherwise the next PREPARE_DESCRIBE on the same statement
// name returns SQL-519 / SQLSTATE 24506 ("prepared statement
// identifying open cursor cannot be re-prepared"). After the cursor
// is closed (either explicitly or implicitly via end-of-data), drop
// the RPB slot.
//
// Idempotent; safe to call from a defer.
func (c *Cursor) Close() error {
	if !c.rpbActive {
		return nil
	}
	c.rpbActive = false
	// If the cursor wasn't fully drained, the server still has it
	// open. Issue a CLOSE so the next prepared statement on the
	// same RPB slot can re-prepare freely.
	if !c.exhausted {
		if err := closeCursor(c.conn, c.nextCorr()); err != nil {
			// Best-effort: log via the returned error but still
			// try the RPB DELETE -- a stuck cursor is bad enough
			// without leaving the slot occupied too.
			_ = deleteRPB(c.conn, c.nextCorr())
			return fmt.Errorf("hostserver: cursor early-close: %w", err)
		}
	}
	if err := deleteRPB(c.conn, c.nextCorr()); err != nil {
		return fmt.Errorf("hostserver: cursor close (RPB DELETE): %w", err)
	}
	return nil
}

// OpenSelectStatic opens a streaming cursor for a static (no-param)
// SELECT. Mirrors the SelectStaticSQL bytes through the initial
// PREPARE_DESCRIBE + OPEN_DESCRIBE_FETCH pair, but stops after the
// first batch is parsed -- the caller drains via Cursor.Next.
//
// nextCorr supplies fresh correlation IDs on demand; the cursor
// holds onto it so continuation FETCH (issued lazily from Next) and
// RPB DELETE (from Close) keep advancing the same counter the
// driver Conn uses.
func OpenSelectStatic(conn io.ReadWriter, sql string, nextCorr func() uint32) (*Cursor, error) {
	if nextCorr == nil {
		return nil, errors.New("hostserver: OpenSelectStatic requires a non-nil nextCorr")
	}
	cols, firstBatch, err := openStaticUntilFirstBatch(conn, sql, nextCorr)
	if err != nil {
		return nil, err
	}
	return newCursor(conn, cols, firstBatch, nextCorr), nil
}

// OpenSelectPrepared opens a streaming cursor for a parameterised
// SELECT. Same shape as OpenSelectStatic, plus the CHANGE_DESCRIPTOR
// + bound-value frames between PREPARE and OPEN.
func OpenSelectPrepared(conn io.ReadWriter, sql string, paramShapes []PreparedParam, paramValues []any, nextCorr func() uint32) (*Cursor, error) {
	if nextCorr == nil {
		return nil, errors.New("hostserver: OpenSelectPrepared requires a non-nil nextCorr")
	}
	cols, firstBatch, err := openPreparedUntilFirstBatch(conn, sql, paramShapes, paramValues, nextCorr)
	if err != nil {
		return nil, err
	}
	return newCursor(conn, cols, firstBatch, nextCorr), nil
}

func newCursor(conn io.ReadWriter, cols []SelectColumn, firstBatch []SelectRow, nextCorr func() uint32) *Cursor {
	return &Cursor{
		conn:      conn,
		cols:      cols,
		pending:   firstBatch,
		nextCorr:  nextCorr,
		rpbActive: true,
	}
}

// drainAll consumes the cursor into a buffered SelectResult. Used by
// the legacy SelectStaticSQL / SelectPreparedSQL entry points to
// preserve their original "all rows up front" contract.
//
// On any error, the cursor is closed before returning -- callers
// don't have to remember to defer Close after a drainAll failure.
func (c *Cursor) drainAll() (*SelectResult, error) {
	res := &SelectResult{Columns: c.cols}
	for {
		row, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		res.Rows = append(res.Rows, row)
	}
	if err := c.Close(); err != nil {
		return nil, err
	}
	return res, nil
}

// closureFromInt converts a starting correlation ID into a counting
// nextCorr closure. Used by the legacy entry points (which take a
// uint32 starting value) to plug into the new streaming machinery.
func closureFromInt(start uint32) func() uint32 {
	c := start
	return func() uint32 {
		v := c
		c++
		return v
	}
}
