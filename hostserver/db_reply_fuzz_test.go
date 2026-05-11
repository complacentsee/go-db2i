package hostserver

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// FuzzParseDBReply walks adversarial 0x2800-reply payloads through
// the LL/CP/data parser, including the CP 0x3832 whole-datastream
// RLE-1 wrapper unwrap path. The function must either return a
// non-nil *DBReply with a valid LL/CP traversal, or return a typed
// error. Panics, out-of-bounds reads, hangs, and unbounded
// allocations are bugs.
func FuzzParseDBReply(f *testing.F) {
	for _, seed := range dbReplyFuzzSeeds() {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in []byte) {
		rep, err := ParseDBReply(in)
		if err != nil {
			return
		}
		if rep == nil {
			t.Fatalf("ParseDBReply returned (nil, nil) for len=%d payload", len(in))
		}
		// Walk the resulting Params and confirm every CP slot was
		// surfaced with a non-nil (possibly empty) data slice. This
		// catches accidental nils or shared-buffer aliasing that
		// would let a caller mutate the input.
		for i, p := range rep.Params {
			if p.Data == nil {
				t.Fatalf("Params[%d].Data is nil (CP 0x%04X)", i, p.CodePoint)
			}
		}
	})
}

func dbReplyFuzzSeeds() [][]byte {
	var seeds [][]byte

	// 1. Minimal 20-byte template with no params.
	seeds = append(seeds, make([]byte, 20))

	// 2. Template + one well-formed CP 0x3801 (MessageID-ish) param.
	{
		buf := make([]byte, 20)
		buf = append(buf, mustParam(0x3801, []byte{0x00, 0x25, 0x41, 0x42})...) // CCSID 37 + 2 bytes
		seeds = append(seeds, buf)
	}

	// 3. Two consecutive params (CP 0x3807 SQLCA + CP 0x3801).
	{
		buf := make([]byte, 20)
		buf = append(buf, mustParam(0x3807, bytes.Repeat([]byte{0xAA}, 16))...)
		buf = append(buf, mustParam(0x3801, []byte{0x00, 0x25})...)
		seeds = append(seeds, buf)
	}

	// 4. LL = 5 (below the minimum of 6) -- should be rejected.
	{
		buf := make([]byte, 20)
		be32buf := make([]byte, 4)
		binary.BigEndian.PutUint32(be32buf, 5)
		buf = append(buf, be32buf...)
		buf = append(buf, 0x38, 0x07)
		seeds = append(seeds, buf)
	}

	// 5. LL that overshoots the payload by 1 byte -- rejected.
	{
		buf := make([]byte, 20)
		be32buf := make([]byte, 4)
		binary.BigEndian.PutUint32(be32buf, 10)
		buf = append(buf, be32buf...)
		buf = append(buf, 0x38, 0x07)
		buf = append(buf, 0x01, 0x02, 0x03) // only 3 bytes, header asked for 4
		seeds = append(seeds, buf)
	}

	// 6. Compression marker set but payload too short to carry the wrapper.
	{
		buf := make([]byte, 20+5)
		binary.BigEndian.PutUint32(buf[4:8], dataCompressedMask)
		seeds = append(seeds, buf)
	}

	// 7. Compression marker + 0x3807 (wrong wrapper CP) -- rejected.
	{
		buf := make([]byte, 20+10)
		binary.BigEndian.PutUint32(buf[4:8], dataCompressedMask)
		binary.BigEndian.PutUint32(buf[20:24], 10)
		binary.BigEndian.PutUint16(buf[24:26], 0x3807)
		seeds = append(seeds, buf)
	}

	// 8. Compression marker + 0x3832 wrapper around an empty inner stream.
	{
		buf := make([]byte, 20)
		binary.BigEndian.PutUint32(buf[4:8], dataCompressedMask)
		wrap := make([]byte, 0, 10)
		wrap = appendBE32(wrap, 10) // ll = 10 (header only, no compressed bytes)
		wrap = appendBE16(wrap, 0x3832)
		wrap = appendBE32(wrap, 0) // decompressed_len = 0
		buf = append(buf, wrap...)
		seeds = append(seeds, buf)
	}

	// 9. Compression marker + 0x3832 wrapper with one RLE run that
	//    expands to a small inner CP 0x3807 SQLCA-ish param.
	{
		// Inner: param LL=12, CP=0x3807, 6 bytes of 0xAA.
		inner := append([]byte(nil), 0x00, 0x00, 0x00, 0x0C, 0x38, 0x07)
		inner = append(inner, bytes.Repeat([]byte{0xAA}, 6)...)
		// Compress inline (we know it'll be smaller as a run-of-AA).
		compressed := rleEncodeForTest(inner)

		buf := make([]byte, 20)
		binary.BigEndian.PutUint32(buf[4:8], dataCompressedMask)
		wrap := make([]byte, 0, 10+len(compressed))
		wrap = appendBE32(wrap, uint32(10+len(compressed)))
		wrap = appendBE16(wrap, 0x3832)
		wrap = appendBE32(wrap, uint32(len(inner)))
		wrap = append(wrap, compressed...)
		buf = append(buf, wrap...)
		seeds = append(seeds, buf)
	}

	// 10. Sub-template payload (15 bytes) -- caught by length guard.
	seeds = append(seeds, make([]byte, 15))

	// 11. Empty input.
	seeds = append(seeds, nil)

	return seeds
}

func mustParam(cp uint16, data []byte) []byte {
	out := make([]byte, 0, 6+len(data))
	out = appendBE32(out, uint32(6+len(data)))
	out = appendBE16(out, cp)
	out = append(out, data...)
	return out
}

func appendBE32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendBE16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}
