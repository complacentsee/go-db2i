package hostserver

import (
	"encoding/binary"
	"strings"
	"testing"
)

// These tests pin the bounds-checks added for issue #27: the CP 0x380E
// result-data decoder must reject hostile/corrupt/truncated payloads
// with a typed error instead of panicking or fatal-OOMing the process.
// Each case below panics (slice-bounds-out-of-range) or OOMs the
// process on the pre-#27 parser; after the fix they return an error.

// resultHeader writes the fixed 20-byte CP 0x380E header into a buffer
// of the given total size and returns it. Callers fill in the trailing
// indicator/row bytes themselves.
func resultHeader(total, rowCount, colCount, indicatorSize, rowSize int) []byte {
	be := binary.BigEndian
	data := make([]byte, total)
	be.PutUint32(data[4:8], uint32(rowCount))
	be.PutUint16(data[8:10], uint16(colCount))
	be.PutUint16(data[10:12], uint16(indicatorSize))
	be.PutUint32(data[16:20], uint32(rowSize))
	return data
}

func TestParseExtendedResultDataRejectsRowOffOOB(t *testing.T) {
	// parser-vlf-rowoff-oob-panic: a VLF row-info-array entry points
	// past the end of the payload. Pre-fix data[rowOff:] panics.
	be := binary.BigEndian
	// 1 row, 1 col, indicatorSize 0 so the row-info header sits right
	// after the 20-byte fixed header.
	data := resultHeader(32, 1, 1, 0, 0)
	rowInfoHeaderStart := 20
	be.PutUint32(data[rowInfoHeaderStart:rowInfoHeaderStart+4], 8)   // rowInfoArrayOffset
	be.PutUint32(data[rowInfoHeaderStart+4:rowInfoHeaderStart+8], 1) // rows fetched
	// row info array entry 0 (at offset 20+8=28): claim the row starts
	// 0x7FFFFFFF bytes past the header -- way past len(data).
	be.PutUint32(data[28:32], 0x7FFFFFFF)

	cols := []SelectColumn{{SQLType: 449, Length: 10, CCSID: 65535, Name: "C"}}
	rows, err := parseExtendedResultData(data, cols, true)
	if err == nil {
		t.Fatalf("expected error for out-of-range row offset, got %d rows", len(rows))
	}
	if !strings.Contains(err.Error(), "out of range") {
		t.Errorf("error = %q, want it to mention out of range", err)
	}
}

func TestParseExtendedResultDataRejectsHugeRowCount(t *testing.T) {
	// parser-rowcount-unbounded-alloc-oom: a ~4 billion rowCount drives
	// make([]SelectRow, rowCount) into a fatal OOM. The cap rejects it.
	data := resultHeader(20, 0x7FFFFFFF, 1, 0, 0)
	cols := []SelectColumn{{SQLType: 449, Length: 10, CCSID: 65535, Name: "C"}}
	rows, err := parseExtendedResultData(data, cols, true)
	if err == nil {
		t.Fatalf("expected error for implausible row count, got %d rows", len(rows))
	}
	if !strings.Contains(err.Error(), "row count") {
		t.Errorf("error = %q, want it to mention row count", err)
	}
}

func TestParseExtendedResultDataRejectsIndicatorOverflow(t *testing.T) {
	// parser-indicatorbytes-int-overflow-panic: rowCount * colCount *
	// indicatorSize must not wrap into a small/negative int that
	// defeats the past-end-of-data check. With rowCount and colCount
	// near 2^16 each and indicatorSize 2, the 32-bit product wraps but
	// the int64 math catches the real (enormous) indicator span.
	data := resultHeader(20, 0xFFFF, 0xFFFF, 2, 0)
	cols := make([]SelectColumn, 0xFFFF)
	for i := range cols {
		cols[i] = SelectColumn{SQLType: 449, Length: 10, CCSID: 65535}
	}
	rows, err := parseExtendedResultData(data, cols, true)
	if err == nil {
		t.Fatalf("expected error for overflowing indicator span, got %d rows", len(rows))
	}
}

func TestParseExtendedResultDataRejectsOddIndicatorSize(t *testing.T) {
	// parser-odd-indicatorsize-panic: indicatorSize=1 means decodeRow
	// would read a 2-byte be.Uint16 out of a 1-byte indicator slot and
	// panic. Reject any indicatorSize other than 0 or 2 up front.
	for _, sz := range []int{1, 3, 4, 0xFFFF} {
		data := resultHeader(64, 1, 1, sz, 0)
		cols := []SelectColumn{{SQLType: 449, Length: 10, CCSID: 65535, Name: "C"}}
		rows, err := parseExtendedResultData(data, cols, true)
		if err == nil {
			t.Fatalf("indicatorSize=%d: expected error, got %d rows", sz, len(rows))
		}
		if !strings.Contains(err.Error(), "indicator size") {
			t.Errorf("indicatorSize=%d: error = %q, want it to mention indicator size", sz, err)
		}
	}
}

// FuzzParseExtendedResultData drives the CP 0x380E inner decoder
// directly with adversarial payloads against a small fixed column
// set, in both the VLF and non-VLF layout selectors. The parser must
// return a row slice or a typed error -- never panic, never OOM. The
// seeds include the four malformed cases above plus the well-formed
// VLF fixtures so the corpus starts from valid shapes the mutator can
// corrupt.
func FuzzParseExtendedResultData(f *testing.F) {
	for _, seed := range extendedResultDataFuzzSeeds() {
		f.Add(seed)
	}

	// A modest, mixed column set: one fixed CHAR, one VARCHAR, one
	// INTEGER. colCount mismatch is itself a rejected error, so most
	// mutated inputs bail early -- that's fine, we're hunting panics.
	cols := []SelectColumn{
		{SQLType: 452, Length: 4, CCSID: 37, Name: "A"},     // CHAR(4)
		{SQLType: 449, Length: 12, CCSID: 65535, Name: "B"}, // VARCHAR(10) FOR BIT DATA
		{SQLType: 496, Length: 4, CCSID: 0, Name: "C"},      // INTEGER
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Exercise both layout selectors; neither may panic or OOM.
		_, _ = parseExtendedResultData(data, cols, true)
		_, _ = parseExtendedResultData(data, cols, false)
	})
}

func extendedResultDataFuzzSeeds() [][]byte {
	be := binary.BigEndian
	var seeds [][]byte

	// 1. Empty + sub-header lengths (caught by the length guard).
	seeds = append(seeds, nil)
	seeds = append(seeds, make([]byte, 10))
	seeds = append(seeds, make([]byte, 20))

	// 2. The four malformed cases, sized for the 3-col fuzz set.
	{ // out-of-range VLF row offset
		data := resultHeader(40, 1, 3, 0, 0)
		be.PutUint32(data[20:24], 8)
		be.PutUint32(data[24:28], 1)
		be.PutUint32(data[28:32], 0x7FFFFFFF)
		seeds = append(seeds, data)
	}
	{ // huge row count
		seeds = append(seeds, resultHeader(20, 0x7FFFFFFF, 3, 0, 0))
	}
	{ // odd indicator size
		seeds = append(seeds, resultHeader(64, 1, 3, 1, 0))
	}
	{ // indicator span past end of data
		seeds = append(seeds, resultHeader(20, 0xFFFF, 3, 2, 0))
	}

	// 3. A well-formed VLF 3-col row so the mutator has a valid shape
	//    to corrupt: CHAR(4)="AB  ", VARCHAR bit-data 2 bytes, INT.
	{
		// header(20) + indicators(0) + rowInfoHeader(8) + row + array(4)
		// Row payload: CHAR 4 bytes | VARCHAR(SL=2 + 2 bytes) | INT 4
		row := []byte{
			0xC1, 0xC2, 0x40, 0x40, // CHAR(4) "AB" padded
			0x00, 0x02, 0xDE, 0xAD, // VARCHAR SL=2 + 2 bytes
			0x00, 0x00, 0x00, 0x07, // INTEGER 7
		}
		total := 20 + 8 + len(row) + 4
		data := resultHeader(total, 1, 3, 0, len(row))
		be.PutUint32(data[20:24], uint32(8+len(row))) // rowInfoArrayOffset
		be.PutUint32(data[24:28], 1)                  // rows fetched
		copy(data[28:28+len(row)], row)
		be.PutUint32(data[28+len(row):], 8) // row 0 offset, relative to header start
		seeds = append(seeds, data)
	}

	return seeds
}
