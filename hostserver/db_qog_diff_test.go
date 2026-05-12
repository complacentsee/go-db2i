package hostserver

import (
	"encoding/binary"
	"testing"
)

// TestQueryOptimizeGoalWireShape asserts the v0.7.17 fix at the
// wire level: SetSQLAttributesRequest emits CP 0x3833 with EBCDIC
// 'F' (0xC6) when QueryOptimizeGoal=QueryOptimizeFirstIO is set,
// EBCDIC 'A' (0xC1) when QueryOptimizeAllIO, and omits the CP
// entirely when the field stays at zero. Pinned against the live-
// captured JT400 fixtures select_dummy_qog_*.trace -- if a future
// change broke the wire shape (e.g. inserted the CP into the
// wrong byte position, or changed value encoding), this test fails
// offline without an LPAR.
func TestQueryOptimizeGoalWireShape(t *testing.T) {
	cases := []struct {
		name    string
		fixture string
		opts    func(o *DBAttributesOptions)
		wantCP  byte // 0 = expect the CP to be absent
	}{
		{
			name:    "unset omits CP 0x3833",
			fixture: "select_dummy.trace",
			opts:    func(o *DBAttributesOptions) {},
			wantCP:  0,
		},
		{
			name:    "firstio emits 0xC6",
			fixture: "select_dummy_qog_firstio.trace",
			opts:    func(o *DBAttributesOptions) { o.QueryOptimizeGoal = QueryOptimizeFirstIO },
			wantCP:  0xC6,
		},
		{
			name:    "allio emits 0xC1",
			fixture: "select_dummy_qog_allio.trace",
			opts:    func(o *DBAttributesOptions) { o.QueryOptimizeGoal = QueryOptimizeAllIO },
			wantCP:  0xC1,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// 1. Confirm the JT400 fixture matches our expectation.
			all := allSentsByServerID(t, tc.fixture, ServerDatabase)
			gotJT400 := byte(0)
			for _, b := range all {
				if len(b) < 20 || binary.BigEndian.Uint16(b[18:20]) != ReqDBSetSQLAttributes {
					continue
				}
				_, _, params, err := DecodeDBRequestFrame(b)
				if err != nil {
					t.Fatalf("decode JT400 fixture: %v", err)
				}
				for _, p := range params {
					if p.CodePoint == cpDBQueryOptimizeGoal && len(p.Data) == 1 {
						gotJT400 = p.Data[0]
					}
				}
			}
			if gotJT400 != tc.wantCP {
				t.Errorf("JT400 fixture %s: CP 0x3833 = 0x%02X, want 0x%02X", tc.fixture, gotJT400, tc.wantCP)
			}

			// 2. Confirm our encoder emits the same CP byte.
			o := DefaultDBAttributesOptions()
			tc.opts(&o)
			_, payload, err := SetSQLAttributesRequest(o)
			if err != nil {
				t.Fatalf("SetSQLAttributesRequest: %v", err)
			}
			gotOurs := byte(0)
			tpl, params, err := DecodeDBRequest(payload)
			if err != nil {
				t.Fatalf("DecodeDBRequest: %v", err)
			}
			_ = tpl
			for _, p := range params {
				if p.CodePoint == cpDBQueryOptimizeGoal && len(p.Data) == 1 {
					gotOurs = p.Data[0]
				}
			}
			if gotOurs != tc.wantCP {
				t.Errorf("our encoder: CP 0x3833 = 0x%02X, want 0x%02X", gotOurs, tc.wantCP)
			}
		})
	}
}
