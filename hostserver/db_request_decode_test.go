package hostserver

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/complacentsee/go-db2i/internal/wirelog"
)

// TestDecodeDBRequestRoundTrip asserts BuildDBRequest + DecodeDBRequest
// are bijective for the param shapes the driver emits (mix of byte,
// short, variable-length CCSID-tagged strings, raw bytes).
func TestDecodeDBRequestRoundTrip(t *testing.T) {
	tpl := DBRequestTemplate{
		ORSBitmap:                 0x80040000,
		RPBHandle:                 1,
		ParameterMarkerDescriptor: 2,
	}
	params := []DBParam{
		DBParamByte(0x3808, 0x01),
		DBParamShort(0x3812, 0x0001),
		DBParamVarString(0x3804, 37, []byte("GOJTPK9899")),
		{CodePoint: 0x381F, Data: []byte{0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD}},
	}
	_, payload, err := BuildDBRequest(ReqDBSQLExecute, tpl, params)
	if err != nil {
		t.Fatalf("BuildDBRequest: %v", err)
	}
	gotTpl, gotParams, err := DecodeDBRequest(payload)
	if err != nil {
		t.Fatalf("DecodeDBRequest: %v", err)
	}
	if gotTpl != tpl {
		t.Errorf("template mismatch: got %+v, want %+v", gotTpl, tpl)
	}
	if len(gotParams) != len(params) {
		t.Fatalf("param count mismatch: got %d, want %d", len(gotParams), len(params))
	}
	for i, p := range params {
		g := gotParams[i]
		if g.CodePoint != p.CodePoint {
			t.Errorf("param[%d] CP: got 0x%04X, want 0x%04X", i, g.CodePoint, p.CodePoint)
		}
		if !bytes.Equal(g.Data, p.Data) {
			t.Errorf("param[%d] data: got % X, want % X", i, g.Data, p.Data)
		}
	}
}

// TestDecodeDBRequestRejectsBadLL guards against confusing
// callers that get a partial frame -- an LL that runs past the
// buffer must return an error rather than panic.
func TestDecodeDBRequestRejectsBadLL(t *testing.T) {
	buf := make([]byte, 30)
	be := binary.BigEndian
	be.PutUint16(buf[18:20], 1) // 1 param declared
	be.PutUint32(buf[20:24], 0xFFFF)
	be.PutUint16(buf[24:26], 0x3808)
	if _, _, err := DecodeDBRequest(buf); err == nil {
		t.Fatal("expected error for over-long LL")
	}
}

// TestWireEquivalence_PackageFilingIUDFixture walks the JT400
// fixture trace for the filing IUD flow (4-iter INSERT/UPDATE/DELETE
// against a freshly created *SQLPKG) and asserts the wire-shape
// invariants v0.7.4 depends on:
//
//   - CREATE_PACKAGE (ReqDBSQLCreatePackage=0x180F) carries CP 0x3804
//     (package name) + CP 0x3801 (package library). The connect-time
//     handshake our initPackage emits matches.
//   - PREPARE_DESCRIBE (ReqDBSQLPrepareDescribe=0x1803) for a filing-
//     eligible statement carries CP 0x3808 (prepare option) and
//     CP 0x3804 (package name). The packageEligibleFor gate emits
//     these via WithExtendedDynamic + WithPackageName.
//   - EXECUTE (ReqDBSQLExecute=0x1805) carries CP 0x3804 (package
//     name, 14 bytes) AND CP 0x3812 (statement type), and DOES NOT
//     carry CP 0x3806 (statement-name override). This is the
//     resolved-by-package-marker pattern: the server uses the RPB
//     handle + CP 0x3804 to dispatch to the filed plan; no client-
//     side renamed-name reuse is required.
//
// The asserts pin the JT400 fixture's wire pattern so a regenerated
// fixture (different SQL, different release, different naming
// convention) fails the test rather than silently shifting our
// reference. Future work: capture our driver's bytes via
// SetWireHook against the same SQL flow, decode with
// DecodeDBRequestFrame, and assert byte-equivalent CP order.
func TestWireEquivalence_PackageFilingIUDFixture(t *testing.T) {
	path := filepath.Join(fixturesDir, "prepared_package_filing_iud.trace")
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	defer f.Close()
	frames, err := wirelog.ParseJTOpenTrace(f)
	if err != nil {
		t.Fatalf("parse trace: %v", err)
	}

	sawCreatePackage := false
	sawPreparePackaged := 0
	sawExecutePackaged := 0

	// Consolidate adjacent same-direction segments so a write that
	// was split across two trace events parses as one continuous
	// stream. Each consolidated Sent buffer may carry multiple
	// concatenated DSS frames (the SQL package-filing path bundles
	// CREATE_RPB + PREPARE_DESCRIBE in a single TCP segment).
	for _, frame := range wirelog.Sents(wirelog.Consolidate(frames)) {
		stream := frame.Bytes
		for len(stream) >= HeaderLength {
			ln := binary.BigEndian.Uint32(stream[0:4])
			if ln < HeaderLength || int(ln) > len(stream) {
				break // truncated; stop walking this buffer
			}
			frameBytes := stream[:ln]
			stream = stream[ln:]
			var hdr Header
			if err := hdr.UnmarshalBinary(frameBytes[:HeaderLength]); err != nil {
				continue // signon/non-DB frames; skip
			}
			if hdr.ServerID != ServerDatabase || hdr.TemplateLength != 20 {
				// Skip signon (0x7001 XChgRandSeed, 0x7002
				// StartServer) and any other non-SQL frames whose
				// payload doesn't follow the DBRequestTemplate
				// shape.
				continue
			}
			_, params, err := DecodeDBRequest(frameBytes[HeaderLength:])
			if err != nil {
				t.Errorf("decode req 0x%04X corr %d: %v", hdr.ReqRepID, hdr.CorrelationID, err)
				continue
			}

			switch hdr.ReqRepID {
			case ReqDBSQLCreatePackage:
				if hasCP(params, 0x3804) && hasCP(params, 0x3801) {
					sawCreatePackage = true
				}
			case ReqDBSQLPrepareDescribe:
				// Filing-eligible PREPAREs carry a non-empty CP
				// 0x3804 (14-byte var-string: CCSID(2) + SL(2) +
				// 10-byte EBCDIC name). The first prepare in a
				// per-conn run may carry CP 0x3804 with an empty
				// 0-byte payload before the server has filed --
				// only count the populated ones.
				if pkg, ok := cpData(params, 0x3804); ok && len(pkg) > 4 {
					if !hasCP(params, 0x3808) {
						t.Errorf("packaged PREPARE corr %d missing CP 0x3808 (prepare option)", hdr.CorrelationID)
					}
					sawPreparePackaged++
				}
			case ReqDBSQLExecute:
				pkg, ok := cpData(params, 0x3804)
				if !ok || len(pkg) <= 4 {
					continue // not a packaged EXECUTE
				}
				if !hasCP(params, 0x3812) {
					t.Errorf("packaged EXECUTE corr %d missing CP 0x3812 (statement type)", hdr.CorrelationID)
				}
				if hasCP(params, 0x3806) {
					t.Errorf("packaged EXECUTE corr %d unexpectedly carries CP 0x3806 (statement-name override); JT400 relies on RPB+package-name resolution", hdr.CorrelationID)
				}
				sawExecutePackaged++
			}
		}
	}

	if !sawCreatePackage {
		t.Errorf("fixture has no CREATE_PACKAGE with CP 0x3804+0x3801 -- v0.7.4 init handshake not reflected")
	}
	if sawPreparePackaged < 3 {
		t.Errorf("fixture has %d packaged PREPARE_DESCRIBE frames; expected >=3 (INSERT/UPDATE/DELETE)", sawPreparePackaged)
	}
	if sawExecutePackaged < 3 {
		t.Errorf("fixture has %d packaged EXECUTE frames; expected >=3 (INSERT/UPDATE/DELETE)", sawExecutePackaged)
	}
}

func cpData(params []DBParam, cp uint16) ([]byte, bool) {
	for _, p := range params {
		if p.CodePoint == cp {
			return p.Data, true
		}
	}
	return nil, false
}

func hasCP(params []DBParam, cp uint16) bool {
	for _, p := range params {
		if p.CodePoint == cp {
			return true
		}
	}
	return false
}
