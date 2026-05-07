package hostserver

import (
	"fmt"
	"io"
)

// Transaction control function IDs in the SQL service. Per JT400's
// DBSQLRequestDS.FUNCTIONID_COMMIT / FUNCTIONID_ROLLBACK; both are
// fire-no-params frames whose semantics depend on whether the
// connection currently has autocommit on or off.
const (
	ReqDBSQLCommit   uint16 = 0x1807
	ReqDBSQLRollback uint16 = 0x1808
)

// Commit sends a SQL service COMMIT (0x1807) on conn. The
// connection must have already opted out of autocommit (via
// AutocommitOff or DBAttributesOptions.AutoCommit = false at
// connection-attribute time); otherwise PUB400 returns SQL -7008
// ("not valid for operation") because there's nothing to commit.
//
// nextCorrelation is the request correlation ID; caller advances
// its own counter.
func Commit(conn io.ReadWriter, nextCorrelation uint32) error {
	return txEnd(conn, ReqDBSQLCommit, nextCorrelation, "COMMIT")
}

// Rollback sends a SQL service ROLLBACK (0x1808) on conn. Same
// autocommit-off prerequisite as Commit.
func Rollback(conn io.ReadWriter, nextCorrelation uint32) error {
	return txEnd(conn, ReqDBSQLRollback, nextCorrelation, "ROLLBACK")
}

// txEnd is the shared body for COMMIT and ROLLBACK. Per JT400's
// JDTransactionManager, both frames:
//   - leave all template handles at zero (no RPB attached);
//   - set CP 0x380F (HoldIndicator, 1 byte) so the server knows
//     whether to keep cursors open across the boundary -- we
//     default to 'Y' (hold) which is the JDBC default;
// and they differ only in ORS bitmap (COMMIT asks for SQLCA,
// ROLLBACK doesn't).
func txEnd(conn io.ReadWriter, reqRepID uint16, corr uint32, label string) error {
	ors := ORSReturnData
	if reqRepID == ReqDBSQLCommit {
		ors |= ORSSQLCA
	}
	tpl := DBRequestTemplate{
		ORSBitmap: ors,
		// All handles 0 -- COMMIT/ROLLBACK don't reference the
		// SQL service's RPB slot.
	}
	hdr, payload, err := BuildDBRequest(reqRepID, tpl, []DBParam{
		DBParamByte(0x380F, 0xE8), // HoldIndicator = 'Y' (preserve cursors)
	})
	if err != nil {
		return fmt.Errorf("hostserver: build %s: %w", label, err)
	}
	hdr.CorrelationID = corr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send %s: %w", label, err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, corr, 4)
	if err != nil {
		return fmt.Errorf("hostserver: read %s reply: %w", label, err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return fmt.Errorf("hostserver: %s reply ReqRepID 0x%04X (want 0x%04X)", label, repHdr.ReqRepID, RepDBReply)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return fmt.Errorf("hostserver: parse %s reply: %w", label, err)
	}
	if rep.ErrorClass != 0 || (rep.ReturnCode != 0 && !isSQLWarning(rep.ReturnCode)) {
		return fmt.Errorf("hostserver: %s RC=%d errorClass=0x%04X",
			label, int32(rep.ReturnCode), rep.ErrorClass)
	}
	return nil
}

// AutocommitOff turns autocommit OFF for conn via a small
// SET_SQL_ATTRIBUTES (0x1F80) request that sets only CP 0x3824
// (autoCommit = 0xD5 = EBCDIC 'N' = "no"). Lets a caller pair
// Commit/Rollback with explicit transaction boundaries without
// rebuilding the full attributes frame.
//
// CP 0x3824 values (per JT400 setAutoCommit): 0xE8 = EBCDIC 'Y' =
// autocommit on (default), 0xD5 = EBCDIC 'N' = autocommit off.
func AutocommitOff(conn io.ReadWriter, corr uint32) error {
	return setAutocommit(conn, corr, 0xD5)
}

// AutocommitOn turns autocommit back on. Useful after an explicit
// transaction completes, so subsequent simple SELECTs / INSERTs
// don't accumulate uncommitted state.
func AutocommitOn(conn io.ReadWriter, corr uint32) error {
	return setAutocommit(conn, corr, 0xE8)
}

func setAutocommit(conn io.ReadWriter, corr uint32, ebcdicYN byte) error {
	tpl := DBRequestTemplate{
		ORSBitmap: ORSReturnData | ORSServerAttributes | 0x00040000,
	}
	hdr, payload, err := BuildDBRequest(ReqDBSetSQLAttributes, tpl, []DBParam{
		DBParamByte(0x3824, ebcdicYN),
	})
	if err != nil {
		return fmt.Errorf("hostserver: build set-autocommit: %w", err)
	}
	hdr.CorrelationID = corr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return fmt.Errorf("hostserver: send set-autocommit: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, corr, 4)
	if err != nil {
		return fmt.Errorf("hostserver: read set-autocommit reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return fmt.Errorf("hostserver: set-autocommit reply ReqRepID 0x%04X", repHdr.ReqRepID)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return fmt.Errorf("hostserver: parse set-autocommit reply: %w", err)
	}
	if rep.ErrorClass != 0 || (rep.ReturnCode != 0 && !isSQLWarning(rep.ReturnCode)) {
		return fmt.Errorf("hostserver: set-autocommit RC=%d errorClass=0x%04X",
			int32(rep.ReturnCode), rep.ErrorClass)
	}
	return nil
}
