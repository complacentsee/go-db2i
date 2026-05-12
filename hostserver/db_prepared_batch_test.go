package hostserver

import (
	"bytes"
	"strings"
	"testing"
)

// TestEncodeDBExtendedDataBatch_TwoIntRows pins the byte layout for
// the v0.7.9 block-insert wire shape: two rows of two INTEGER
// columns packed into a single CP 0x381F payload with rowCount=2.
//
// Header (20 bytes) + indicator block (2 rows * 2 cols * 2 bytes =
// 8 bytes) + data block (2 rows * 8 bytes/row = 16 bytes). Per row
// the indicator pair and data pair appear in declaration order.
func TestEncodeDBExtendedDataBatch_TwoIntRows(t *testing.T) {
	params := []PreparedParam{
		{SQLType: 496, FieldLength: 4}, // INTEGER NN
		{SQLType: 496, FieldLength: 4}, // INTEGER NN
	}
	rows := [][]any{
		{int32(1), int32(2)},
		{int32(0x10), int32(0x20)},
	}
	got, err := EncodeDBExtendedDataBatch(params, rows)
	if err != nil {
		t.Fatalf("EncodeDBExtendedDataBatch: %v", err)
	}

	wantHeader := []byte{
		0x00, 0x00, 0x00, 0x01, // consistency token = 1
		0x00, 0x00, 0x00, 0x02, // row count = 2 ← the v0.7.9 change
		0x00, 0x02, // column count = 2
		0x00, 0x02, // indicator size = 2
		0x00, 0x00, 0x00, 0x00, // reserved
		0x00, 0x00, 0x00, 0x08, // row size = 4 + 4
	}
	wantIndicators := []byte{
		0x00, 0x00, 0x00, 0x00, // row 0: not-null, not-null
		0x00, 0x00, 0x00, 0x00, // row 1: not-null, not-null
	}
	wantData := []byte{
		0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, // row 0
		0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x20, // row 1
	}
	want := bytes.Join([][]byte{wantHeader, wantIndicators, wantData}, nil)
	if !bytes.Equal(got, want) {
		t.Fatalf("byte mismatch:\n got=% X\nwant=% X", got, want)
	}
}

// TestEncodeDBExtendedDataBatch_NullsAcrossRows confirms the NULL
// indicator handling works per-row -- one NULL in row 0, a
// different NULL in row 1.
func TestEncodeDBExtendedDataBatch_NullsAcrossRows(t *testing.T) {
	params := []PreparedParam{
		{SQLType: 497, FieldLength: 4}, // INTEGER nullable
		{SQLType: 497, FieldLength: 4}, // INTEGER nullable
	}
	rows := [][]any{
		{int32(7), nil},    // row 0: col 1 is NULL
		{nil, int32(0x42)}, // row 1: col 0 is NULL
	}
	got, err := EncodeDBExtendedDataBatch(params, rows)
	if err != nil {
		t.Fatalf("EncodeDBExtendedDataBatch: %v", err)
	}
	// Indicator block at bytes 20..27: row 0 (0,1), row 1 (0,1)
	wantInd := []byte{
		0x00, 0x00, 0xFF, 0xFF, // row 0: not-null, NULL
		0xFF, 0xFF, 0x00, 0x00, // row 1: NULL, not-null
	}
	if !bytes.Equal(got[20:28], wantInd) {
		t.Errorf("indicator block mismatch:\n got=% X\nwant=% X", got[20:28], wantInd)
	}
	// Data block at bytes 28..43: 2 rows * 8 bytes/row. Null slots
	// advance the offset without writing (stays zero from buf init).
	wantData := []byte{
		0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, // row 0: 7, NULL (zeros)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42, // row 1: NULL (zeros), 0x42
	}
	if !bytes.Equal(got[28:44], wantData) {
		t.Errorf("data block mismatch:\n got=% X\nwant=% X", got[28:44], wantData)
	}
}

// TestEncodeDBExtendedDataBatch_RowLengthMismatch verifies the
// per-row arity check surfaces the offending row index, distinct
// from the single-row error format.
func TestEncodeDBExtendedDataBatch_RowLengthMismatch(t *testing.T) {
	params := []PreparedParam{
		{SQLType: 496, FieldLength: 4},
		{SQLType: 496, FieldLength: 4},
	}
	rows := [][]any{
		{int32(1), int32(2)},
		{int32(3)}, // wrong width
	}
	_, err := EncodeDBExtendedDataBatch(params, rows)
	if err == nil {
		t.Fatal("expected error for mismatched row width")
	}
	if !strings.Contains(err.Error(), "row 1") {
		t.Errorf("error should name the offending row index: %v", err)
	}
}

// TestEncodeDBExtendedDataBatch_ZeroRows rejects an empty rows
// slice up front so the server never sees a header-only payload.
func TestEncodeDBExtendedDataBatch_ZeroRows(t *testing.T) {
	params := []PreparedParam{{SQLType: 496, FieldLength: 4}}
	_, err := EncodeDBExtendedDataBatch(params, nil)
	if err == nil {
		t.Fatal("expected error for zero rows")
	}
	if !strings.Contains(err.Error(), "at least one row") {
		t.Errorf("unexpected error wording: %v", err)
	}
}

// TestEncodeDBExtendedDataBatch_SingleRowMatchesSingleAPI guards
// against drift between the legacy EncodeDBExtendedData
// (single-row callers: ExecutePreparedSQL, OpenSelectPrepared,
// ExecutePreparedCached, wire-equivalence tests) and the new batch
// API. Both must produce byte-identical output at N=1.
func TestEncodeDBExtendedDataBatch_SingleRowMatchesSingleAPI(t *testing.T) {
	params := []PreparedParam{
		{SQLType: 497, FieldLength: 4}, // INTEGER nullable
		{SQLType: 449, FieldLength: 8, Precision: 6, CCSID: 1208},
	}
	values := []any{int32(42), "café"}

	single, err := EncodeDBExtendedData(params, values)
	if err != nil {
		t.Fatalf("EncodeDBExtendedData: %v", err)
	}
	batch, err := EncodeDBExtendedDataBatch(params, [][]any{values})
	if err != nil {
		t.Fatalf("EncodeDBExtendedDataBatch: %v", err)
	}
	if !bytes.Equal(single, batch) {
		t.Fatalf("single-row drift:\n single=% X\n  batch=% X", single, batch)
	}
}

// TestEncodeDBExtendedDataBatch_BatchErrorPrefixesRow confirms the
// rowPrefix flows into per-value errors so callers can pinpoint the
// failing row in a multi-row batch.
func TestEncodeDBExtendedDataBatch_BatchErrorPrefixesRow(t *testing.T) {
	params := []PreparedParam{
		{SQLType: 500, FieldLength: 2}, // SMALLINT NN
	}
	rows := [][]any{
		{int32(1)},        // ok
		{int32(100_000)},  // overflow on row 1
	}
	_, err := EncodeDBExtendedDataBatch(params, rows)
	if err == nil {
		t.Fatal("expected smallint overflow on row 1")
	}
	if !strings.Contains(err.Error(), "row 1 ") {
		t.Errorf("expected error to include 'row 1 ' prefix, got: %v", err)
	}
	if !strings.Contains(err.Error(), "overflows int16") {
		t.Errorf("expected original error text preserved, got: %v", err)
	}
}
