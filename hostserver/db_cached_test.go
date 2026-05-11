package hostserver

import (
	"encoding/binary"
	"testing"
)

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
	_, err := ExecutePreparedCached(conn, cached, []any{int64(42)}, closureFromInt(3), "GOJTPK9899", "GOTEST", 37)
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
	_, err := ExecutePreparedCached(conn, cached, nil, closureFromInt(3), "GOJTPK9899", "GOTEST", 37)
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
	_, err := ExecutePreparedCached(conn, nil, []any{int64(1)}, closureFromInt(3), "GOJTPK9899", "GOTEST", 37)
	if err == nil {
		t.Fatalf("expected error for nil cached statement")
	}
}

// TestPreparedParamsFromCached exercises the SQLDA -> PreparedParam
// shape conversion that drives both Exec and Query cache-hit paths.
// Non-input direction bytes must abort; input-only round-trips the
// SQL type / length / CCSID. Precision and Scale are zeroed for
// non-decimal SQLTypes (the cached SQLDA's high/low-byte split is
// redundant with FieldLength for non-numeric types and produces
// values the server interprets incorrectly on EXECUTE -- see
// preparedParamsFromCached doc). ParamType is forced to 0x00 to
// match JT400's cache-hit wire bytes.
func TestPreparedParamsFromCached(t *testing.T) {
	in := []ParameterMarkerField{
		// INTEGER with a "scale=4" leak from the SQLDA's high/low
		// byte overlap (Length=4 stored as 0x0004 reads as
		// Precision=0, Scale=4 through the package decoder).
		{SQLType: 497, FieldLength: 4, Precision: 0, Scale: 4, ParamType: 0x00},
		{SQLType: 449, FieldLength: 16, CCSID: 1208, ParamType: 0xF0},
		// DECIMAL(5,2) -- the precision/scale survive the SQLDA
		// because the high/low-byte split is semantically meaningful
		// for decimal-family types.
		{SQLType: 484, FieldLength: 3, Precision: 5, Scale: 2, ParamType: 0x00},
	}
	out, err := preparedParamsFromCached(in)
	if err != nil {
		t.Fatalf("preparedParamsFromCached: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("got %d shapes, want 3", len(out))
	}
	if out[0].SQLType != 497 || out[0].FieldLength != 4 ||
		out[0].Precision != 0 || out[0].Scale != 0 || out[0].ParamType != 0x00 {
		t.Errorf("shape[0] INTEGER mismatch: %+v "+
			"(want SQLType=497 FieldLength=4 Precision=0 Scale=0 ParamType=0x00)", out[0])
	}
	if out[1].CCSID != 1208 || out[1].ParamType != 0x00 {
		t.Errorf("shape[1] VARCHAR mismatch: %+v "+
			"(want CCSID=1208 ParamType=0x00)", out[1])
	}
	if out[2].Precision != 5 || out[2].Scale != 2 || out[2].ParamType != 0x00 {
		t.Errorf("shape[2] DECIMAL(5,2) precision/scale stripped: %+v "+
			"(want Precision=5 Scale=2 ParamType=0x00)", out[2])
	}

	// OUT direction must abort.
	_, err = preparedParamsFromCached([]ParameterMarkerField{
		{SQLType: 497, FieldLength: 4, ParamType: 0xF1},
	})
	if err == nil {
		t.Fatalf("expected error for OUT direction")
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

// scannedParams collects the CPs observed in a request payload's
// parameter section.
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
// DBRequestTemplate header and returns every CP + data block.
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
