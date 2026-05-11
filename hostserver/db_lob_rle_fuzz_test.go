package hostserver

import (
	"bytes"
	"testing"
)

// FuzzDecompressRLE1 fuzzes the per-CP RLE-1 decompressor JT400's
// JDUtilities.decompress uses for compressed RETRIEVE_LOB_DATA
// payloads. The expectedLen parameter is the caller-declared upper
// bound on output bytes; the function must either return a slice of
// length <= expectedLen or a typed error. No panics, no unbounded
// allocations.
func FuzzDecompressRLE1(f *testing.F) {
	for _, seed := range rle1FuzzSeeds() {
		f.Add(seed.src, seed.expected)
	}

	f.Fuzz(func(t *testing.T, src []byte, expected int32) {
		// Clamp expected to a reasonable cap (256 KiB) -- the
		// decompressor pre-allocates this much so unbounded values
		// would just OOM the fuzzer.
		if expected < 0 || expected > 256*1024 {
			t.Skip()
		}
		out, err := decompressRLE1(src, int(expected))
		if err != nil {
			return
		}
		if out == nil {
			t.Fatalf("decompressRLE1: nil slice without error (src=%x expected=%d)", src, expected)
		}
		if len(out) > int(expected) {
			t.Fatalf("decompressRLE1: output %d > expected %d", len(out), expected)
		}
	})
}

// FuzzDecompressDataStreamRLE fuzzes the whole-datastream RLE-1
// wrapper CP 0x3832 carries. Distinct from the per-CP RLE-1: 5-byte
// repeater record emits 2 bytes per iteration, 16-bit BE count.
func FuzzDecompressDataStreamRLE(f *testing.F) {
	for _, seed := range dataStreamRLEFuzzSeeds() {
		f.Add(seed.src, seed.expected)
	}

	f.Fuzz(func(t *testing.T, src []byte, expected int32) {
		if expected < 0 || expected > 256*1024 {
			t.Skip()
		}
		out, err := decompressDataStreamRLE(src, int(expected))
		if err != nil {
			return
		}
		if out == nil {
			t.Fatalf("decompressDataStreamRLE: nil slice without error (src=%x expected=%d)", src, expected)
		}
		if len(out) > int(expected) {
			t.Fatalf("decompressDataStreamRLE: output %d > expected %d", len(out), expected)
		}
	})
}

type rleSeed struct {
	src      []byte
	expected int32
}

func rle1FuzzSeeds() []rleSeed {
	return []rleSeed{
		{src: []byte("hello world"), expected: 11},
		{src: []byte{0x1B, 0x1B, 0x41}, expected: 2},                                  // escaped 0x1B + literal A
		{src: []byte{0x1B, 0xCC, 0x00, 0x00, 0x00, 0x08}, expected: 8},                // run of 8 0xCC
		{src: []byte{0x1B, 0x00, 0x00, 0x00, 0x10, 0x00}, expected: 4096},             // zero-pattern run
		{src: []byte{0x1B}, expected: 4},                                              // truncated escape
		{src: []byte{0x1B, 0xCC, 0x00, 0x00}, expected: 4},                            // truncated run header
		{src: []byte{0x1B, 0xCC, 0xFF, 0xFF, 0xFF, 0xFF}, expected: 4},                // negative count
		{src: []byte{0x1B, 0xCC, 0x00, 0x00, 0x10, 0x00}, expected: 16},               // run > expected
		{src: bytes.Repeat([]byte{0x41}, 1024), expected: 1024},                       // literal block
		{src: []byte{}, expected: 0},                                                  // empty
	}
}

func dataStreamRLEFuzzSeeds() []rleSeed {
	return []rleSeed{
		{src: []byte("hello world"), expected: 11},
		{src: []byte{0x1B, 0x1B, 0x41, 0x1B, 0x1B}, expected: 3},                     // two escaped 0x1B + literal
		{src: []byte{0x1B, 0xCC, 0xCC, 0x08, 0x00}, expected: 4096},                  // 4 KiB run
		{src: []byte{0x1B, 0x00, 0x00, 0x00, 0x10}, expected: 32},                    // 32-byte zero fill
		{src: []byte{0x1B, 0xAA, 0xBB, 0x00}, expected: 16},                          // truncated repeater header
		{src: []byte{0x1B, 0xAA, 0xBB, 0xFF, 0xFF}, expected: 4},                     // repeater count overflows expected
		{src: bytes.Repeat([]byte{0x41}, 1024), expected: 1024},                      // literal block
		{src: []byte{0x1B}, expected: 4},                                             // truncated escape
		{src: []byte{}, expected: 0},                                                 // empty
	}
}
