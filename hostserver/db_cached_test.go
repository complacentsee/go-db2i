package hostserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// TestExecutePreparedCached_WireShape constructs a synthetic
// PackageStatement and exercises the cached-EXECUTE frame builder,
// then re-parses the written bytes to confirm:
//
//   - The function code is 0x1805 (EXECUTE), not 0x1803
//     (PREPARE_DESCRIBE). This is the "fast path" assertion: no
//     prepare round-trip slips through.
//   - CP 0x3806 (statement-name override) is present with the
//     verbatim 18-byte EBCDIC name from the cached package entry.
//     Byte-equality here is what lets the server match the cached
//     statement; any normalisation would break it silently.
//   - CP 0x381E (extended data format) and CP 0x381F (extended data)
//     are both present, carrying the parameter shape + bound values.
//   - CP 0x3804 (package-name marker) is NOT present -- it's only
//     used to ASK the server to file a new statement into the
//     package; on a cache hit we're using an existing one.
func TestExecutePreparedCached_WireShape(t *testing.T) {
	cached := syntheticCachedSelectInt()

	hdr, payload, err := buildCachedExecuteFrame(ReqDBSQLExecute, cached,
		mustPreparedParams(t, cached.ParameterMarkerFormat),
		[]any{int64(42)},
	)
	if err != nil {
		t.Fatalf("buildCachedExecuteFrame: %v", err)
	}
	if hdr.ReqRepID != ReqDBSQLExecute {
		t.Errorf("ReqRepID = 0x%04X, want 0x%04X (EXECUTE)", hdr.ReqRepID, ReqDBSQLExecute)
	}

	saw := scanRequestParams(t, payload)
	if !saw.has(cpDBPrepareStatementName) {
		t.Errorf("EXECUTE missing CP 0x%04X (cached name)", cpDBPrepareStatementName)
	}
	if !saw.has(cpDBExtendedDataFormat) {
		t.Errorf("EXECUTE missing CP 0x%04X (cached PMF descriptor)", cpDBExtendedDataFormat)
	}
	if !saw.has(cpDBExtendedData) {
		t.Errorf("EXECUTE missing CP 0x%04X (bound values)", cpDBExtendedData)
	}
	if saw.has(cpPackageName) {
		t.Errorf("EXECUTE unexpectedly carries CP 0x%04X marker -- the cache-hit path should NOT ask the server to file a new statement", cpPackageName)
	}

	// The statement-name CP payload is CCSID(2) + SL(2) + 18 EBCDIC
	// bytes -- byte-equal to the cached.NameBytes field.
	nameWire := saw.dataFor(cpDBPrepareStatementName)
	if len(nameWire) < 22 {
		t.Fatalf("statement-name CP payload too short: %d bytes", len(nameWire))
	}
	if got, want := nameWire[4:22], cached.NameBytes; !bytes.Equal(got, want) {
		t.Errorf("statement-name bytes = %x, want %x (cached EBCDIC)", got, want)
	}
}

// TestExecutePreparedCached_RejectsOutParameter is the defense-in-
// depth assertion: a cached PMF with a non-input direction byte must
// abort the fast path rather than silently lose the OUT slot.
// Callable statements are excluded from the package by the criteria
// filter, but a future custom-criteria flag might let them through;
// the host-server layer must still refuse.
func TestExecutePreparedCached_RejectsOutParameter(t *testing.T) {
	cached := syntheticCachedSelectInt()
	cached.ParameterMarkerFormat[0].ParamType = 0xF1 // OUT

	conn := newFakeConn() // no replies; the call should error before Write
	_, err := ExecutePreparedCached(conn, cached, []any{int64(42)}, closureFromInt(3))
	if err == nil {
		t.Fatalf("expected error for OUT-direction param")
	}
	if conn.written.Len() != 0 {
		t.Errorf("written bytes leaked on rejected fast-path call: %d", conn.written.Len())
	}
}

// TestExecutePreparedCached_ParamCountMismatch surfaces the slice-
// length sanity check up front, before we encode any wire bytes.
func TestExecutePreparedCached_ParamCountMismatch(t *testing.T) {
	cached := syntheticCachedSelectInt()
	conn := newFakeConn()
	_, err := ExecutePreparedCached(conn, cached, nil, closureFromInt(3))
	if err == nil {
		t.Fatalf("expected error for value-count mismatch")
	}
	if conn.written.Len() != 0 {
		t.Errorf("written bytes leaked on mismatched-arg call: %d", conn.written.Len())
	}
}

// TestExecutePreparedCached_RejectsNilCached covers the "package
// disabled or lookup miss got past the gate" defense.
func TestExecutePreparedCached_RejectsNilCached(t *testing.T) {
	conn := newFakeConn()
	_, err := ExecutePreparedCached(conn, nil, []any{int64(1)}, closureFromInt(3))
	if err == nil {
		t.Fatalf("expected error for nil cached statement")
	}
}

// TestOpenSelectPreparedCached_WireShape mirrors the EXECUTE test on
// the Query path. Function code must be 0x180E
// (OPEN_DESCRIBE_FETCH), CP 0x3806 must carry the cached name, and
// no 0x1803 (PREPARE_DESCRIBE) bytes can appear.
func TestOpenSelectPreparedCached_WireShape(t *testing.T) {
	cached := syntheticCachedSelectInt()

	hdr, payload, err := buildCachedExecuteFrame(ReqDBSQLOpenDescribeFetch, cached,
		mustPreparedParams(t, cached.ParameterMarkerFormat),
		[]any{int64(42)},
	)
	if err != nil {
		t.Fatalf("buildCachedExecuteFrame: %v", err)
	}
	if hdr.ReqRepID != ReqDBSQLOpenDescribeFetch {
		t.Errorf("ReqRepID = 0x%04X, want 0x%04X (OPEN_DESCRIBE_FETCH)", hdr.ReqRepID, ReqDBSQLOpenDescribeFetch)
	}
	saw := scanRequestParams(t, payload)
	if !saw.has(cpDBPrepareStatementName) {
		t.Errorf("OPEN missing CP 0x%04X (cached name)", cpDBPrepareStatementName)
	}
}

// syntheticCachedSelectInt builds a PackageStatement that mirrors the
// shape captured in prepared_package_cache_download.trace -- a
// SELECT with one INTEGER input marker and two result columns --
// without going through the wire-byte fixture. Used by the fast-
// path unit tests so a regression in the PackageStatement struct
// surfaces independently from the fixture parser.
func syntheticCachedSelectInt() *PackageStatement {
	return &PackageStatement{
		Name: "QZAF481815E802E001",
		NameBytes: []byte{
			0xd8, 0xe9, 0xc1, 0xc6, 0xf4, 0xf8, 0xf1, 0xf8, 0xf1,
			0xf5, 0xc5, 0xf8, 0xf0, 0xf2, 0xc5, 0xf0, 0xf0, 0xf1,
		},
		StatementType: 2, // SELECT
		SQLText:       "SELECT CURRENT_TIMESTAMP, CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1",
		DataFormat: []SelectColumn{
			{SQLType: SQLTypeTimestampNN, Length: 26},
			{SQLType: 497, Length: 4},
		},
		ParameterMarkerFormat: []ParameterMarkerField{
			{SQLType: 497, FieldLength: 4, ParamType: 0xF0},
		},
	}
}

// mustPreparedParams converts a cached PMF list into the
// PreparedParam shapes the encoder consumes, failing the test on a
// rejection (which only happens for non-input direction bytes).
func mustPreparedParams(t *testing.T, pmf []ParameterMarkerField) []PreparedParam {
	t.Helper()
	shapes, err := preparedParamsFromCached(pmf)
	if err != nil {
		t.Fatalf("preparedParamsFromCached: %v", err)
	}
	return shapes
}

// scannedParams collects the CPs observed in a request payload's
// parameter section, with quick lookup helpers for the assertions
// above.
type scannedParams struct {
	cps  []uint16
	data map[uint16][]byte
}

func (s *scannedParams) has(cp uint16) bool { _, ok := s.data[cp]; return ok }
func (s *scannedParams) dataFor(cp uint16) []byte {
	if b, ok := s.data[cp]; ok {
		return b
	}
	return nil
}

// scanRequestParams walks the LL/CP records past the 20-byte
// DBRequestTemplate header and returns every CP + data block. Fails
// the test on a malformed record so a fixture-induced bug surfaces
// immediately.
func scanRequestParams(t *testing.T, payload []byte) *scannedParams {
	t.Helper()
	if len(payload) < 20 {
		t.Fatalf("request payload too short: %d bytes", len(payload))
	}
	out := &scannedParams{data: map[uint16][]byte{}}
	off := 20 // skip template
	be := binary.BigEndian
	for off+6 <= len(payload) {
		ll := be.Uint32(payload[off : off+4])
		if ll < 6 || int(ll) > len(payload)-off {
			t.Fatalf("malformed LL=%d at off=%d (payload=%d)", ll, off, len(payload))
		}
		cp := be.Uint16(payload[off+4 : off+6])
		out.cps = append(out.cps, cp)
		data := payload[off+6 : off+int(ll)]
		clone := make([]byte, len(data))
		copy(clone, data)
		out.data[cp] = clone
		off += int(ll)
	}
	return out
}

// closureFromInt and minimal hex helpers are reused across tests.

// fmtCPs is a debug helper for failing assertions; we don't use it
// in the happy path but keep it around so a future test can print
// the observed CP list when it diverges.
func fmtCPs(cps []uint16) string {
	var b []byte
	for i, cp := range cps {
		if i > 0 {
			b = append(b, ',', ' ')
		}
		b = append(b, []byte(fmt.Sprintf("0x%04X", cp))...)
	}
	return string(b)
}
