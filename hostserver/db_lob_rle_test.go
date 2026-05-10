package hostserver

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// makeRLEStream builds a synthetic RLE-1 stream from a sequence of
// segments. Each segment is either a "literal" run of bytes (passed
// through verbatim, with 0x1B doubled) or an "escape" run of (value,
// count) that compresses to 6 bytes on the wire. Used to drive the
// decompressor offline without needing a real IBM i.
type rleSeg struct {
	literal []byte // when set, copied verbatim with 0x1B doubled
	value   byte   // when literal == nil and count > 0, the run value
	count   uint32 // when > 0, build a 6-byte 0x1B value count run
}

func encodeRLEStream(segs ...rleSeg) []byte {
	var b bytes.Buffer
	for _, s := range segs {
		if s.literal != nil {
			for _, by := range s.literal {
				if by == rleEscapeByte {
					b.WriteByte(rleEscapeByte)
					b.WriteByte(rleEscapeByte)
				} else {
					b.WriteByte(by)
				}
			}
			continue
		}
		if s.count == 0 {
			continue
		}
		b.WriteByte(rleEscapeByte)
		b.WriteByte(s.value)
		var cnt [4]byte
		binary.BigEndian.PutUint32(cnt[:], s.count)
		b.Write(cnt[:])
	}
	return b.Bytes()
}

// TestDecompressRLE1_PassThrough confirms a stream with no escape
// bytes round-trips verbatim. Mirrors how the server would behave
// for incompressible data that it doesn't bother encoding.
func TestDecompressRLE1_PassThrough(t *testing.T) {
	src := []byte("Hello, IBM i!")
	got, err := decompressRLE1(src, len(src))
	if err != nil {
		t.Fatalf("decompressRLE1: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Errorf("pass-through: got % x, want % x", got, src)
	}
}

// TestDecompressRLE1_EscapedLiteral confirms 0x1B 0x1B emits one
// literal 0x1B byte. The encoder produces this for any literal 0x1B
// in the source; without the doubling, a literal 0x1B in raw data
// would be misread as the start of a run header.
func TestDecompressRLE1_EscapedLiteral(t *testing.T) {
	src := []byte{rleEscapeByte, rleEscapeByte}
	got, err := decompressRLE1(src, 1)
	if err != nil {
		t.Fatalf("decompressRLE1: %v", err)
	}
	want := []byte{rleEscapeByte}
	if !bytes.Equal(got, want) {
		t.Errorf("0x1B 0x1B = %x, want %x", got, want)
	}

	// A literal 0x1B mixed with normal bytes round-trips through a
	// re-encode + decode cycle.
	original := []byte{'A', rleEscapeByte, 'B'}
	encoded := encodeRLEStream(rleSeg{literal: original})
	got, err = decompressRLE1(encoded, len(original))
	if err != nil {
		t.Fatalf("decompressRLE1: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("A 0x1B B round-trip: got % x, want % x", got, original)
	}
}

// TestDecompressRLE1_Run4KZeroes is the canonical "constant content"
// case: 4 KiB of identical bytes compress into 6 wire bytes. This
// is the exact pattern that previously returned 0 bytes through the
// driver because the parser didn't decompress; the test pins the
// pre-fix regression.
func TestDecompressRLE1_Run4KZeroes(t *testing.T) {
	const total = 4 * 1024
	src := encodeRLEStream(rleSeg{value: 0x00, count: total})
	if len(src) != 6 {
		t.Fatalf("encoded run is %d bytes, want 6", len(src))
	}
	got, err := decompressRLE1(src, total)
	if err != nil {
		t.Fatalf("decompressRLE1: %v", err)
	}
	if len(got) != total {
		t.Fatalf("decompressed length = %d, want %d", len(got), total)
	}
	for i, b := range got {
		if b != 0x00 {
			t.Fatalf("byte %d = 0x%02X, want 0x00", i, b)
			break
		}
	}
}

// TestDecompressRLE1_Run1MOfCC is the constant-content case the M7
// plan called out specifically: 1 MiB of identical 0xCC bytes
// compress into 6 wire bytes. Confirms the decompressor scales to
// LOB-sized payloads without per-byte overhead.
func TestDecompressRLE1_Run1MOfCC(t *testing.T) {
	const total = 1 * 1024 * 1024
	src := encodeRLEStream(rleSeg{value: 0xCC, count: total})
	got, err := decompressRLE1(src, total)
	if err != nil {
		t.Fatalf("decompressRLE1: %v", err)
	}
	if len(got) != total {
		t.Fatalf("decompressed length = %d, want %d", len(got), total)
	}
	want := bytes.Repeat([]byte{0xCC}, total)
	if !bytes.Equal(got, want) {
		t.Errorf("1 MiB run of 0xCC mismatch")
	}
}

// TestDecompressRLE1_MixedLiteralRunLiteral confirms the parser
// switches between literal and run modes correctly when interleaved.
// The destination index advances independently of the source index;
// any drift between them would surface here.
func TestDecompressRLE1_MixedLiteralRunLiteral(t *testing.T) {
	want := append([]byte("hello"), bytes.Repeat([]byte{0xAB}, 100)...)
	want = append(want, []byte("world")...)

	src := encodeRLEStream(
		rleSeg{literal: []byte("hello")},
		rleSeg{value: 0xAB, count: 100},
		rleSeg{literal: []byte("world")},
	)
	got, err := decompressRLE1(src, len(want))
	if err != nil {
		t.Fatalf("decompressRLE1: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("mixed literal+run+literal mismatch:\n got: % x\nwant: % x", got, want)
	}
}

// TestDecompressRLE1_TruncatedEscape covers the malformed-input
// guard: a trailing 0x1B with no payload byte.
func TestDecompressRLE1_TruncatedEscape(t *testing.T) {
	src := []byte{'A', rleEscapeByte}
	_, err := decompressRLE1(src, 2)
	if err == nil {
		t.Error("expected error for truncated escape")
	}
}

// TestDecompressRLE1_TruncatedRunHeader covers a 0x1B + value with
// fewer than 4 count bytes following.
func TestDecompressRLE1_TruncatedRunHeader(t *testing.T) {
	src := []byte{rleEscapeByte, 0xAA, 0x00, 0x00, 0x01} // missing 1 of the 4 count bytes
	_, err := decompressRLE1(src, 1)
	if err == nil {
		t.Error("expected error for truncated run header")
	}
}

// TestDecompressRLE1_RunOverflow confirms a run that would write
// past expectedLen returns an error rather than silently growing
// the output.
func TestDecompressRLE1_RunOverflow(t *testing.T) {
	src := encodeRLEStream(rleSeg{value: 0xAA, count: 1000})
	_, err := decompressRLE1(src, 100)
	if err == nil {
		t.Error("expected error for run overflow (1000 into 100-byte buffer)")
	}
}

// TestParseLOBReply_NoCompressionPassThrough confirms the per-CP
// dispatcher keeps its raw-passthrough behaviour for replies whose
// actualLen matches the wire payload size. Critical for backward
// compatibility with any server / connection that doesn't compress
// the LOB even when we set the request bit.
func TestParseLOBReply_NoCompressionPassThrough(t *testing.T) {
	// Build a CP 0x380F with CCSID 65535 (BLOB / FOR BIT DATA),
	// actualLen = 16, payload = 16 bytes of literal data.
	payload := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	cpData := make([]byte, 0, 6+len(payload))
	cpData = append(cpData, 0xFF, 0xFF) // CCSID 65535
	cpData = append(cpData, 0x00, 0x00, 0x00, byte(len(payload)))
	cpData = append(cpData, payload...)
	rep := &DBReply{
		Params: []DBParam{
			{CodePoint: cpDBLOBData, Data: cpData},
		},
	}
	out, err := parseLOBReply(rep)
	if err != nil {
		t.Fatalf("parseLOBReply: %v", err)
	}
	if !bytes.Equal(out.Bytes, payload) {
		t.Errorf("raw payload mismatch:\n got: % x\nwant: % x", out.Bytes, payload)
	}
	if out.CCSID != 65535 {
		t.Errorf("CCSID = %d, want 65535", out.CCSID)
	}
}

// TestParseLOBReply_RLECompressed4KOfCC drives the integrated path:
// a CP 0x380F whose payload is 6-byte RLE-1 encoded 4096 × 0xCC
// must decompress through parseLOBReply into 4 KiB of 0xCC.
func TestParseLOBReply_RLECompressed4KOfCC(t *testing.T) {
	const total = 4 * 1024
	wirePayload := encodeRLEStream(rleSeg{value: 0xCC, count: total})
	if len(wirePayload) != 6 {
		t.Fatalf("wire payload size = %d, want 6", len(wirePayload))
	}
	cpData := make([]byte, 0, 6+len(wirePayload))
	cpData = append(cpData, 0xFF, 0xFF) // CCSID 65535
	var actLen [4]byte
	binary.BigEndian.PutUint32(actLen[:], uint32(total))
	cpData = append(cpData, actLen[:]...) // actualLen = 4096
	cpData = append(cpData, wirePayload...)

	rep := &DBReply{
		Params: []DBParam{
			{CodePoint: cpDBLOBData, Data: cpData},
		},
	}
	out, err := parseLOBReply(rep)
	if err != nil {
		t.Fatalf("parseLOBReply: %v", err)
	}
	if len(out.Bytes) != total {
		t.Fatalf("decompressed length = %d, want %d", len(out.Bytes), total)
	}
	want := bytes.Repeat([]byte{0xCC}, total)
	if !bytes.Equal(out.Bytes, want) {
		t.Errorf("4 KiB 0xCC RLE round-trip mismatch")
	}
}

// TestParseLOBReply_GraphicLOBCharCount confirms the graphic-LOB
// (CCSID 13488 / 1200) path uses 2*actualLen for the byte-count
// reconciliation. A raw graphic payload of 1000 chars (2000 bytes)
// must NOT trigger decompression even though len(payload) > actualLen.
func TestParseLOBReply_GraphicLOBCharCount(t *testing.T) {
	const charCount = 1000
	const byteCount = charCount * 2 // graphic LOB: 2 bytes per char
	payload := make([]byte, byteCount)
	for i := 0; i < byteCount; i += 2 {
		payload[i] = 0x00
		payload[i+1] = byte('A' + (i/2)%26)
	}
	cpData := make([]byte, 0, 6+byteCount)
	cpData = append(cpData, 0x34, 0xB0) // CCSID 13488
	var actLen [4]byte
	binary.BigEndian.PutUint32(actLen[:], uint32(charCount))
	cpData = append(cpData, actLen[:]...) // actualLen = 1000 chars
	cpData = append(cpData, payload...)

	rep := &DBReply{Params: []DBParam{{CodePoint: cpDBLOBData, Data: cpData}}}
	out, err := parseLOBReply(rep)
	if err != nil {
		t.Fatalf("parseLOBReply: %v", err)
	}
	if !bytes.Equal(out.Bytes, payload) {
		t.Errorf("graphic raw passthrough mismatch (got %d bytes, want %d)", len(out.Bytes), len(payload))
	}
}

// TestDecompressDataStreamRLE_RoundTrip covers the whole-datastream
// RLE-1 wire format used inside CP 0x3832: 1-byte escape + 2-byte
// pattern + 2-byte BE count = 5-byte record, emits 2*count bytes
// per repeater. Distinct from the per-CP RLE-1 (6-byte record,
// 1-byte value, 4-byte BE count) in decompressRLE1.
func TestDecompressDataStreamRLE_RoundTrip(t *testing.T) {
	t.Run("passthrough literals", func(t *testing.T) {
		src := []byte("hello world, IBM i!")
		got, err := decompressDataStreamRLE(src, len(src))
		if err != nil {
			t.Fatalf("decompressDataStreamRLE: %v", err)
		}
		if !bytes.Equal(got, src) {
			t.Errorf("got %q, want %q", got, src)
		}
	})

	t.Run("escaped 0x1B", func(t *testing.T) {
		// 0x1B 0x1B -> one literal 0x1B
		src := []byte{0x1B, 0x1B, 0x41, 0x1B, 0x1B}
		want := []byte{0x1B, 0x41, 0x1B}
		got, err := decompressDataStreamRLE(src, len(want))
		if err != nil {
			t.Fatalf("decompressDataStreamRLE: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got %x, want %x", got, want)
		}
	})

	t.Run("repeater of 0xCC 0xCC × 2048 -> 4096 bytes", func(t *testing.T) {
		// 4 KiB of 0xCC encoded as one repeater (b1=0xCC b2=0xCC,
		// count=2048): emits 2*2048 = 4096 bytes total.
		src := []byte{rleEscapeByte, 0xCC, 0xCC, 0x08, 0x00} // count = 0x0800 = 2048
		want := bytes.Repeat([]byte{0xCC}, 4096)
		got, err := decompressDataStreamRLE(src, 4096)
		if err != nil {
			t.Fatalf("decompressDataStreamRLE: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("4 KiB run mismatch: got %d bytes (first %x...), want %d bytes",
				len(got), got[:8], len(want))
		}
		// Compression ratio sanity: 5 wire bytes -> 4096 decompressed.
		if len(src) > 8 {
			t.Errorf("encoded src grew to %d bytes (want ~5)", len(src))
		}
	})

	t.Run("zero-pattern fast path", func(t *testing.T) {
		// JT400 fast-paths zero-byte runs with the same record form.
		src := []byte{rleEscapeByte, 0x00, 0x00, 0x00, 0x10} // count = 16
		want := make([]byte, 32) // 16 × 2 = 32 zero bytes
		got, err := decompressDataStreamRLE(src, 32)
		if err != nil {
			t.Fatalf("decompressDataStreamRLE: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("zero-fill mismatch: got %x, want %x", got, want)
		}
	})

	t.Run("truncated repeater header", func(t *testing.T) {
		// 0x1B followed by only 3 bytes -- need 5 total.
		src := []byte{rleEscapeByte, 0xAA, 0xBB, 0x00}
		if _, err := decompressDataStreamRLE(src, 100); err == nil {
			t.Fatal("expected error on truncated repeater, got nil")
		}
	})

	t.Run("overflow output", func(t *testing.T) {
		src := []byte{rleEscapeByte, 0xAA, 0xBB, 0x00, 0x05} // count=5 -> 10 bytes
		if _, err := decompressDataStreamRLE(src, 4); err == nil {
			t.Fatal("expected overflow error, got nil")
		}
	})
}
