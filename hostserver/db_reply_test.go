package hostserver

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// TestParseDBReplyUnwrapsCP3832 confirms ParseDBReply transparently
// inflates a payload that arrived under the whole-datastream RLE-1
// wrapper. The test composes a wrapped payload by hand:
//   - 20-byte template with the high bit of payload[4:8] set (the
//     dataCompressed marker JT400 reads at full-frame offset 24).
//   - A single CP 0x3832 parameter whose data is a 4-byte
//     decompressed-length header followed by an RLE-1 encoding of a
//     two-parameter "uncompressed" payload (CP 0x380F LOB data + CP
//     0x3810 CurrentLOBLength).
//
// After ParseDBReply, the returned DBReply must look identical to the
// equivalent uncompressed reply: same ORSBitmap (sans the
// compression marker, which lives in the *next* word), same
// ErrorClass / ReturnCode, and the same two parameters in order
// with byte-equal data.
func TestParseDBReplyUnwrapsCP3832(t *testing.T) {
	be := binary.BigEndian

	// Build the inner (uncompressed) parameter stream that the
	// wrapper should expand back to. Two synthetic CPs:
	//
	//   CP 0x380F (LOB data) — 6-byte header (CCSID(2)+actualLen(4))
	//   plus 1000 0xCC bytes of payload. RLE-compresses very well.
	//
	//   CP 0x3810 (current LOB length) — SL(2) + 4-byte length.
	innerParam1 := buildParam(0x380F, append(
		buildHeader(0, 1000), bytes.Repeat([]byte{0xCC}, 1000)...))
	innerParam2 := buildParam(0x3810, append(
		buildHeader16(4), be32(1000)...))
	innerParams := append(innerParam1, innerParam2...)

	// Compress with a minimal RLE-1 encoder mirroring the inverse of
	// decompressRLE1. Encodes runs of >=5 identical bytes as
	// 0x1B value count(4); escapes literal 0x1B; passes everything
	// else through unchanged.
	compressed := rleEncodeForTest(innerParams)

	// Now compose the wrapped reply payload.
	template := make([]byte, 20)
	// payload[0:4] ORSBitmap echo -- pick something distinctive so the
	// post-unwrap DBReply.ORSBitmap can verify it survived.
	be.PutUint32(template[0:4], 0xA1B2C3D4)
	// payload[4:8] compression marker (high bit). Other bits zero so
	// we can detect any accidental rewrites.
	be.PutUint32(template[4:8], dataCompressedMask)
	// payload[14:16] ErrorClass, payload[16:20] ReturnCode.
	be.PutUint16(template[14:16], 0x1234)
	be.PutUint32(template[16:20], 0xDEADBEEF)

	// Wrapper parameter: LL(4) + CP 0x3832 + decompressed_len(4) + compressed bytes.
	wrap := append([]byte(nil), be32(uint32(10+len(compressed)))...)
	wrap = append(wrap, be16(0x3832)...)
	wrap = append(wrap, be32(uint32(len(innerParams)))...)
	wrap = append(wrap, compressed...)

	wrapped := append(append([]byte(nil), template...), wrap...)

	rep, err := ParseDBReply(wrapped)
	if err != nil {
		t.Fatalf("ParseDBReply(wrapped): %v", err)
	}

	if rep.ORSBitmap != 0xA1B2C3D4 {
		t.Errorf("ORSBitmap = 0x%08X, want 0xA1B2C3D4", rep.ORSBitmap)
	}
	if rep.ErrorClass != 0x1234 {
		t.Errorf("ErrorClass = 0x%04X, want 0x1234", rep.ErrorClass)
	}
	if rep.ReturnCode != 0xDEADBEEF {
		t.Errorf("ReturnCode = 0x%08X, want 0xDEADBEEF", rep.ReturnCode)
	}

	if len(rep.Params) != 2 {
		t.Fatalf("len(Params) = %d, want 2", len(rep.Params))
	}
	if rep.Params[0].CodePoint != 0x380F {
		t.Errorf("Params[0].CodePoint = 0x%04X, want 0x380F", rep.Params[0].CodePoint)
	}
	wantP0 := append(buildHeader(0, 1000), bytes.Repeat([]byte{0xCC}, 1000)...)
	if !bytes.Equal(rep.Params[0].Data, wantP0) {
		t.Errorf("Params[0].Data byte-diverged (len got=%d want=%d)", len(rep.Params[0].Data), len(wantP0))
	}
	if rep.Params[1].CodePoint != 0x3810 {
		t.Errorf("Params[1].CodePoint = 0x%04X, want 0x3810", rep.Params[1].CodePoint)
	}
	wantP1 := append(buildHeader16(4), be32(1000)...)
	if !bytes.Equal(rep.Params[1].Data, wantP1) {
		t.Errorf("Params[1].Data byte-diverged: got=%x want=%x", rep.Params[1].Data, wantP1)
	}

	// Sanity: a 1000-byte run compresses to ~6 bytes (0x1B + value +
	// 4-byte BE count). Confirm the wrapper actually saved bytes --
	// regressions where the encoder bypasses runs would surface here.
	if len(compressed) >= len(innerParams) {
		t.Errorf("compressed=%d not smaller than inner=%d (encoder broken?)",
			len(compressed), len(innerParams))
	}
}

// TestParseDBReplyCompressedMarkerWithoutWrapper rejects a payload
// whose template marks the data as compressed but whose first
// parameter isn't CP 0x3832. JT400 throws IOException in the same
// case (DBBaseReplyDS.parse "compressionSchemeCP != DATA_COMPRESSION_RLE_").
func TestParseDBReplyCompressedMarkerWithoutWrapper(t *testing.T) {
	be := binary.BigEndian
	payload := make([]byte, 20+10)
	be.PutUint32(payload[4:8], dataCompressedMask) // mark compressed
	be.PutUint32(payload[20:24], 10)               // LL = 10 (no data)
	be.PutUint16(payload[24:26], 0x3807)           // wrong CP -- SQLCA
	if _, err := ParseDBReply(payload); err == nil {
		t.Fatal("expected error on compressed-marker / wrong-CP payload, got nil")
	}
}

// TestParseDBReplyCompressedMarkerShort rejects a payload whose
// template marks compression but is too short to carry the wrapper
// header (10 bytes minimum after the template).
func TestParseDBReplyCompressedMarkerShort(t *testing.T) {
	be := binary.BigEndian
	payload := make([]byte, 20+5) // 5 bytes < 10-byte wrap header
	be.PutUint32(payload[4:8], dataCompressedMask)
	if _, err := ParseDBReply(payload); err == nil {
		t.Fatal("expected error on too-short compressed payload, got nil")
	}
}

// TestParseDBReplyCompressedLengthCapped rejects a payload whose
// CP 0x3832 wrapper declares a decompressed length larger than the
// 64 MiB cap. Without the cap, the underlying decompressor would
// call make([]byte, expectedLen) and OOM-kill the process on any
// box with less than ~4 GiB free. JT400 leaves the equivalent
// allocation unchecked because the JVM raises OutOfMemoryError
// rather than aborting; Go's make() panics, so we cap explicitly.
func TestParseDBReplyCompressedLengthCapped(t *testing.T) {
	be := binary.BigEndian
	payload := make([]byte, 20+10) // template + bare wrap header
	be.PutUint32(payload[4:8], dataCompressedMask)
	be.PutUint32(payload[20:24], 10)              // ll = 10 (wrap header only)
	be.PutUint16(payload[24:26], 0x3832)          // correct compression CP
	be.PutUint32(payload[26:30], 1<<30)           // 1 GiB declared decompressed length
	_, err := ParseDBReply(payload)
	if err == nil {
		t.Fatal("expected error on oversized declared decompressed length, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("expected cap-mention error, got: %v", err)
	}
}

// --- helpers ---

func be16(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func be32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// buildParam returns the LL/CP/data bytes for a single parameter:
// 4-byte LL covering the full param (LL+CP+data), 2-byte CP,
// then the data.
func buildParam(cp uint16, data []byte) []byte {
	out := be32(uint32(6 + len(data)))
	out = append(out, be16(cp)...)
	out = append(out, data...)
	return out
}

// buildHeader returns the 6-byte LOB-data header (CCSID(2)+actualLen(4)).
func buildHeader(ccsid uint16, actualLen uint32) []byte {
	out := be16(ccsid)
	out = append(out, be32(actualLen)...)
	return out
}

// buildHeader16 returns the 2-byte SL prefix used by CP 0x3810
// CurrentLOBLength and similar variable-length scalar CPs.
func buildHeader16(sl uint16) []byte {
	return be16(sl)
}

// rleEncodeForTest is a minimal whole-datastream RLE-1 encoder
// mirroring the inverse of decompressDataStreamRLE. The wire format
// is the 5-byte repeater (0x1B + 2-byte pattern + 2-byte BE count)
// the server uses inside CP 0x3832, NOT the 6-byte per-CP RLE-1
// format that wraps CP 0x380F payloads.
//
// Compresses runs of >=3 identical 2-byte pairs (~6 source bytes
// -> 5 wire bytes); shorter runs and odd-length tails pass through
// as literals. Literal 0x1B escapes to 0x1B 0x1B. Output round-trips
// through decompressDataStreamRLE to the original bytes.
//
// The 3-pair threshold is the conservative break-even (a 3-pair run
// = 6 source bytes encodes to 5 wire bytes; 2-pair runs would
// inflate). Real server encoders are more sophisticated but
// the test only needs correctness, not max compression.
func rleEncodeForTest(src []byte) []byte {
	var out []byte
	i := 0
	for i < len(src) {
		// Try to find a repeating 2-byte-pair pattern at i, but only
		// when the pattern byte isn't 0x1B (the escape byte must be
		// emitted via the 2-byte escape form).
		if i+1 < len(src) && src[i] != rleEscapeByte {
			b1, b2 := src[i], src[i+1]
			count := 1
			for i+2*(count+1) <= len(src) && src[i+2*count] == b1 && src[i+2*count+1] == b2 {
				count++
				if count == 0xFFFF { // 2-byte count cap
					break
				}
			}
			if count >= 3 {
				out = append(out, rleEscapeByte, b1, b2)
				out = append(out, be16(uint16(count))...)
				i += 2 * count
				continue
			}
		}
		// Not a run -- emit a single literal.
		if src[i] == rleEscapeByte {
			out = append(out, rleEscapeByte, rleEscapeByte)
		} else {
			out = append(out, src[i])
		}
		i++
	}
	return out
}
