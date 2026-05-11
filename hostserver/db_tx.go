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
//     whether to keep cursors open across the boundary. The
//     server expects a NUMERIC byte (0 = close cursors,
//     1 = hold cursors), NOT an EBCDIC 'N'/'Y'. We default to
//     1 (hold) which matches JT400's "cursor hold = true"
//     default; sending 0xE8 ('Y') silently produces SQL -211
//     errorClass=0x0002 because the server treats anything other
//     than 0/1 as an invalid hold indicator and rejects the
//     transaction boundary.
//
// COMMIT and ROLLBACK differ only in ORS bitmap (COMMIT asks for
// SQLCA, ROLLBACK doesn't).
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
		DBParamByte(0x380F, 0x01), // HoldIndicator: numeric 1 = hold cursors
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
	if dbErr := makeDb2Error(rep, label); dbErr != nil {
		return dbErr
	}
	return nil
}

// AutocommitOff turns autocommit OFF for conn via a SET_SQL_ATTRIBUTES
// (0x1F80) request that bundles three CPs the same way JT400 does
// when JDBC's setAutoCommit(false) is invoked:
//
//	CP 0x3824 = 0xD5  -- autocommit OFF (EBCDIC 'N')
//	CP 0x380E = *CS   -- start a commitment definition at cursor
//	                     stability (wire value 2 per JT400's
//	                     COMMIT_MODE_CS_); without this the server
//	                     stays in *NONE and COMMIT/ROLLBACK return
//	                     SQL -211 errorClass=0x0002 even with a real
//	                     INSERT pending.
//	CP 0x3830 = 1     -- LocatorPersistence "scoped to transaction"
//	                     (matches JT400 default and is required for
//	                     the file's commitment definition to actually
//	                     get started).
//
// Validated live against AFTRAEGE11.DCLVRP02 on PUB400 (journaled).
// JT400's tx_commit fixture sent #8 has the exact byte layout this
// builds.
func AutocommitOff(conn io.ReadWriter, corr uint32) error {
	return setSessionMode(conn, corr, 0xD5, 2 /*CS*/, 1 /*scope=tx*/)
}

// AutocommitOn turns autocommit back on. Mirrors AutocommitOff:
// flips the same three CPs back to JT400's "autocommit, no
// commitment, no locator persistence" baseline. Used after an
// explicit transaction completes so subsequent simple SELECT/INSERT
// runs don't keep a stale commitment definition open.
func AutocommitOn(conn io.ReadWriter, corr uint32) error {
	return setSessionMode(conn, corr, 0xE8, 0 /*NONE*/, 0 /*no scope*/)
}

func setSessionMode(conn io.ReadWriter, corr uint32, autoCommitYN byte, isolationLevel int16, locatorPersistence int16) error {
	rep, err := sendSessionModeFrame(conn, corr, autoCommitYN, isolationLevel, &locatorPersistence)
	if err != nil {
		return err
	}
	// Some IBM i releases reject the locator-persistence change on
	// the autocommit-on transition with errClass=7 RC=-601. JT400
	// catches this and resends without CP 0x3830; we mirror that
	// fallback so the round-trip stays clean. Don't cache the
	// "doesn't support" flag here -- the caller's setSessionMode
	// is per-toggle, and the off→on direction is the only one we've
	// observed reject the CP.
	if rep.ErrorClass == 7 && int32(rep.ReturnCode) == -601 {
		rep, err = sendSessionModeFrame(conn, corr+1, autoCommitYN, isolationLevel, nil)
		if err != nil {
			return err
		}
	}
	if dbErr := makeDb2Error(rep, "SET_SESSION_MODE"); dbErr != nil {
		return dbErr
	}
	return nil
}

// sendSessionModeFrame sends one SET_SQL_ATTRIBUTES frame with up to
// three CPs. When locatorPersistence is nil, CP 0x3830 is omitted --
// used by setSessionMode's fallback path when the server rejects the
// locator-persistence change with errClass=7 RC=-601.
func sendSessionModeFrame(conn io.ReadWriter, corr uint32, autoCommitYN byte, isolationLevel int16, locatorPersistence *int16) (*DBReply, error) {
	tpl := DBRequestTemplate{
		ORSBitmap: ORSReturnData | ORSServerAttributes | ORSDataCompression,
	}
	params := []DBParam{
		DBParamByte(0x3824, autoCommitYN),
		DBParamShort(0x380E, isolationLevel),
	}
	if locatorPersistence != nil {
		params = append(params, DBParamShort(0x3830, *locatorPersistence))
	}
	hdr, payload, err := BuildDBRequest(ReqDBSetSQLAttributes, tpl, params)
	if err != nil {
		return nil, fmt.Errorf("hostserver: build set-session-mode: %w", err)
	}
	hdr.CorrelationID = corr
	if err := WriteFrame(conn, hdr, payload); err != nil {
		return nil, fmt.Errorf("hostserver: send set-session-mode: %w", err)
	}
	repHdr, repPayload, err := ReadDBReplyMatching(conn, corr, 4)
	if err != nil {
		return nil, fmt.Errorf("hostserver: read set-session-mode reply: %w", err)
	}
	if repHdr.ReqRepID != RepDBReply {
		return nil, fmt.Errorf("hostserver: set-session-mode reply ReqRepID 0x%04X", repHdr.ReqRepID)
	}
	rep, err := ParseDBReply(repPayload)
	if err != nil {
		return nil, fmt.Errorf("hostserver: parse set-session-mode reply: %w", err)
	}
	return rep, nil
}
