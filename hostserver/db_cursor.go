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
//
// On the JT400 "fetch/close" fast path -- ErrorClass=2, RC=700 in
// the OPEN_DESCRIBE_FETCH reply, the typical case for SELECTs that
// fit in one block-fetch buffer -- the server delivers all rows
// AND auto-closes the cursor in one round trip. Cursor records
// that via serverClosed and skips the explicit CLOSE in Close,
// matching JT400's wire pattern (OPEN -> RPB DELETE only). This is
// what makes captured fixtures replay cleanly without synthesising
// CLOSE / continuation-FETCH replies the server never sends.
type Cursor struct {
	conn         io.ReadWriter
	cols         []SelectColumn
	pending      []SelectRow // rows from the most recent FETCH batch not yet returned
	pendingIx    int         // index into pending
	nextCorr     func() uint32
	exhausted    bool // server signaled end-of-data; no more FETCHes needed
	serverClosed bool // server auto-closed the cursor; skip explicit CLOSE
	rpbActive    bool // true between OPEN and DELETE; false after Close

	// Multi-result-set state (M9-3). numberOfResults is the count
	// the server stamped in SQLCA SQLERRD(2) on the EXECUTE reply
	// when the SQL was a stored-procedure CALL with DYNAMIC RESULT
	// SETS N declared cursors. currentResultSet is 1-indexed:
	// initialised to 1 by OpenSelectPrepared / OpenSelectStatic,
	// bumped to 2, 3, ... as AdvanceResultSet succeeds. For a plain
	// SELECT the server returns 0 in ERRD(2), so MoreResultSets()
	// is always false there (1 < 0 is also false).
	numberOfResults  int
	currentResultSet int

	// isCallCursor distinguishes a cursor that wraps a CALL-opened
	// dynamic result set from one that wraps a plain SELECT. The
	// two require different FETCH parameter sets on the wire:
	// SELECT uses BufferSize+VarFieldCompr; CALL uses
	// FetchScrollOption+BlockingFactor per JT400. Next() dispatches
	// continuation FETCHes accordingly.
	isCallCursor bool

	// blockSizeKiB is the continuation-FETCH BufferSize the cursor
	// emits on subsequent FETCHes. Set at construction from
	// selectOpts.blockSizeKiB; zero means the historical 32 KiB
	// default (preserving byte-equality with pre-M12 fixtures).
	// Only consulted by fetchMoreRows (plain SELECT path) -- CALL-
	// cursor FETCH uses BlockingFactor instead, deliberately left
	// at the JT400 default until that path gets its own knob.
	blockSizeKiB int
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
	// Pull another batch. CALL cursors need JT400's CALL-cursor
	// FETCH param set (FetchScrollOption + BlockingFactor); plain
	// SELECT cursors use the BufferSize + VarFieldCompr set.
	var (
		more    []SelectRow
		outcome fetchOutcome
		err     error
	)
	if c.isCallCursor {
		more, outcome, err = fetchCallRows(c.conn, c.cols, c.nextCorr())
	} else {
		more, outcome, err = fetchMoreRows(c.conn, c.cols, c.nextCorr(), c.blockSizeKiB)
	}
	if err != nil {
		return nil, err
	}
	if outcome.serverClosed {
		c.serverClosed = true
	}
	if outcome.exhausted {
		c.exhausted = true
		c.pending = nil
		c.pendingIx = 0
		// fetchMoreRows yields no rows on the exhausted path.
		return nil, io.EOF
	}
	if len(more) == 0 {
		// Empty batch with exhausted=false shouldn't happen on
		// PUB400 but some V7R3 servers send a synthetic 0-row reply
		// between real batches. Loop once.
		return c.Next()
	}
	c.pending = more
	c.pendingIx = 1
	return more[0], nil
}

// Close releases server-side resources. When the server already
// auto-closed the cursor (JT400's "fetch/close" path -- recorded
// via c.serverClosed when the OPEN/FETCH reply carried EC=2 RC=700
// or SQL-501 / 24501), Close skips the explicit CLOSE (0x180A) and
// emits only RPB DELETE -- matching JT400's own wire pattern. In
// that case the prepared statement (STMT0001) was dropped along
// with the cursor on the server, so no orphan SQL-519 / 24506 is
// possible on a follow-up PREPARE.
//
// When the cursor is still open server-side (no auto-close signal,
// or Close is called mid-iteration), Close emits CLOSE first with
// REUSE_NO so both the cursor AND the prepared statement go away.
// Without that CLOSE, the next PREPARE_DESCRIBE on the same RPB
// slot returns SQL-519 / SQLSTATE 24506 ("prepared statement
// identifying open cursor cannot be re-prepared") because STMT0001
// is still in use.
//
// closeCursor swallows the SQL-501 / 24501 "cursor not open"
// warning some server versions emit when the cursor was already
// auto-closed; safe to call regardless of cursor state.
//
// Idempotent; safe to call from a defer.
func (c *Cursor) Close() error {
	if !c.rpbActive {
		return nil
	}
	c.rpbActive = false
	var closeErr error
	if !c.serverClosed {
		// CLOSE first -- drops the prepared statement and the cursor
		// in one frame. Errors here still let us try RPB DELETE; the
		// slot might recover on its own and a stuck statement is
		// preferable to a stuck statement AND a stuck slot.
		closeErr = closeCursor(c.conn, c.nextCorr())
	}
	if err := deleteRPB(c.conn, c.nextCorr()); err != nil {
		if closeErr != nil {
			return fmt.Errorf("hostserver: cursor close: CLOSE %v; RPB DELETE %w", closeErr, err)
		}
		return fmt.Errorf("hostserver: cursor close (RPB DELETE): %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("hostserver: cursor close (CLOSE): %w", closeErr)
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
func OpenSelectStatic(conn io.ReadWriter, sql string, nextCorr func() uint32, opts ...SelectOption) (*Cursor, error) {
	if nextCorr == nil {
		return nil, errors.New("hostserver: OpenSelectStatic requires a non-nil nextCorr")
	}
	o := resolveSelectOpts(opts)
	cols, firstBatch, outcome, err := openStaticUntilFirstBatch(conn, sql, nextCorr, o)
	if err != nil {
		return nil, err
	}
	return newCursor(conn, cols, firstBatch, outcome, nextCorr, outcome.numberOfResults, o.blockSizeKiB), nil
}

// OpenSelectPrepared opens a streaming cursor for a parameterised
// SELECT. Same shape as OpenSelectStatic, plus the CHANGE_DESCRIPTOR
// + bound-value frames between PREPARE and OPEN.
func OpenSelectPrepared(conn io.ReadWriter, sql string, paramShapes []PreparedParam, paramValues []any, nextCorr func() uint32, opts ...SelectOption) (*Cursor, error) {
	if nextCorr == nil {
		return nil, errors.New("hostserver: OpenSelectPrepared requires a non-nil nextCorr")
	}
	o := resolveSelectOpts(opts)
	cols, firstBatch, outcome, err := openPreparedUntilFirstBatch(conn, sql, paramShapes, paramValues, nextCorr, o)
	if err != nil {
		return nil, err
	}
	return newCursor(conn, cols, firstBatch, outcome, nextCorr, outcome.numberOfResults, o.blockSizeKiB), nil
}

func newCursor(conn io.ReadWriter, cols []SelectColumn, firstBatch []SelectRow, outcome fetchOutcome, nextCorr func() uint32, numberOfResults int, blockSizeKiB int) *Cursor {
	current := 0
	isCall := false
	if numberOfResults > 0 {
		current = 1
		isCall = true
	}
	return &Cursor{
		conn:             conn,
		cols:             cols,
		pending:          firstBatch,
		nextCorr:         nextCorr,
		exhausted:        outcome.exhausted,
		serverClosed:     outcome.serverClosed,
		rpbActive:        true,
		numberOfResults:  numberOfResults,
		currentResultSet: current,
		isCallCursor:     isCall,
		blockSizeKiB:     blockSizeKiB,
	}
}

// MoreResultSets reports whether the proc that this cursor was opened
// against has at least one more dynamic result set to drain. For a
// plain SELECT (or a CALL with DYNAMIC RESULT SETS 0) the server
// returns 0 in SQLERRD(2), so MoreResultSets always reports false.
//
// Implements the conceptual sibling of database/sql/driver.RowsNextResultSet's
// HasNextResultSet (the driver wrapper in driver/rows.go calls this).
// JT400 mirrors this state via numberOfResults_ + getMoreResults()
// in AS400JDBCStatement.java:3406.
func (c *Cursor) MoreResultSets() bool {
	return c.currentResultSet > 0 && c.currentResultSet < c.numberOfResults
}

// AdvanceResultSet closes the current cursor with REUSE_RESULT_SET
// (preserving the prepared statement on the RPB), issues a fresh
// OPEN_DESCRIBE (0x1804) on the same statement to attach to the
// next dynamic result set the proc declared, parses the new column
// descriptors out of the reply, and prefetches the first row batch
// via continuation FETCH (0x180B). Mirrors JT400's getMoreResults
// flow at AS400JDBCStatement.java:3406-3470.
//
// Returns the new column descriptors so the driver-level Rows can
// refresh its ColumnTypes / Columns view. The cursor's internal
// pending row buffer is replaced; subsequent Next() calls drain the
// next set.
//
// Caller must check MoreResultSets() before calling. AdvanceResultSet
// on a cursor with no remaining sets returns io.EOF -- safer than a
// silent no-op since the caller's intent was to advance.
func (c *Cursor) AdvanceResultSet() ([]SelectColumn, error) {
	if !c.MoreResultSets() {
		return nil, io.EOF
	}

	// Close the current cursor server-side but preserve the
	// prepared statement (REUSE_RESULT_SET = 0xF2) so the
	// subsequent OPEN_DESCRIBE can attach to the next set on the
	// same RPB. Idempotent against server-already-closed (SQL-501)
	// per closeCursor's existing handling.
	if !c.serverClosed {
		if err := closeCursorReuse(c.conn, c.nextCorr(), reuseResultSet); err != nil {
			return nil, fmt.Errorf("hostserver: AdvanceResultSet close: %w", err)
		}
	}
	c.serverClosed = false

	// OPEN_DESCRIBE (0x1804) -- ask the server to attach our client
	// cursor to the proc's next pre-opened dynamic result set and
	// describe its columns. Per JT400 V6R1+ in
	// AS400JDBCStatement.java:3433, ORS bits are RETURN_DATA + SQLCA
	// + DATA_FORMAT + CURSOR_ATTRIBUTES (0x00008000).
	openCorr := c.nextCorr()
	{
		// Match JT400's getMoreResults wire shape: ORSBitmap
		// 0x8A040000 = RETURN_DATA + DATA_FORMAT + SQLCA + RLE.
		// No CursorAttributes (0x00008000) -- mirrors the second-
		// and-later OPEN_DESCRIBE in the multi-set fixture.
		tpl := DBRequestTemplate{
			ORSBitmap:                 ORSReturnData | ORSDataFormat | ORSSQLCA | ORSDataCompression,
			ReturnORSHandle:           1,
			FillORSHandle:             1,
			BasedOnORSHandle:          0,
			RPBHandle:                 1,
			ParameterMarkerDescriptor: 0,
		}
		params := []DBParam{
			DBParamByte(cpDBOpenAttributes, 0x80),
			DBParamShort(cpDBScrollableCursorFlag, 0x0000),
		}
		hdr, payload, err := BuildDBRequest(ReqDBSQLOpenDescribe, tpl, params)
		if err != nil {
			return nil, fmt.Errorf("hostserver: build OPEN_DESCRIBE: %w", err)
		}
		hdr.CorrelationID = openCorr
		if err := WriteFrame(c.conn, hdr, payload); err != nil {
			return nil, fmt.Errorf("hostserver: send OPEN_DESCRIBE: %w", err)
		}
	}
	repHdr, repPayload, err := ReadDBReplyMatching(c.conn, openCorr, 8)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read OPEN_DESCRIBE reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: OPEN_DESCRIBE reply ReqRepID 0x%04X (want 0x%04X)", repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse OPEN_DESCRIBE reply: %w", err)
	}
	if dbErr := makeDb2Error(rep, "OPEN_DESCRIBE"); dbErr != nil {
		return nil, dbErr
	}
	newCols, err := rep.findSuperExtendedDataFormat()
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse next-set column descriptors: %w", err)
	}

	// Prefetch the first row batch via FETCH (0x180B). CALL-opened
	// cursors expect the JT400-style FetchScrollOption +
	// BlockingFactor params; SELECT cursors don't end up here
	// (numberOfResults stays 0 for non-CALL).
	firstBatch, outcome, err := fetchCallRows(c.conn, newCols, c.nextCorr())
	if err != nil {
		return nil, fmt.Errorf("hostserver: FETCH after OPEN_DESCRIBE: %w", err)
	}

	c.cols = newCols
	c.pending = firstBatch
	c.pendingIx = 0
	c.exhausted = outcome.exhausted
	c.serverClosed = outcome.serverClosed
	c.currentResultSet++

	return newCols, nil
}

// NumberOfResults exposes the total result-set count for callers
// (the driver wrapper) that want to surface metadata before
// iteration finishes. Returns 0 for plain SELECT.
func (c *Cursor) NumberOfResults() int { return c.numberOfResults }

// CurrentResultSet reports which result set (1-indexed) the cursor
// is currently positioned on. Zero when numberOfResults is 0.
func (c *Cursor) CurrentResultSet() int { return c.currentResultSet }

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
