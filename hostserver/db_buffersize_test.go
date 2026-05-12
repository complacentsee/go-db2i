package hostserver

import (
	"bytes"
	"testing"
)

func TestBufferSizeParamDefault(t *testing.T) {
	t.Parallel()
	// Default (kib=0) MUST emit the historical 32 KiB shape so the
	// pre-M12 byte-equality fixtures (TestSentBytesMatch*) still
	// match without modification.
	got := bufferSizeParam(0)
	if got.CodePoint != cpDBBufferSize {
		t.Fatalf("CodePoint: got 0x%04X want 0x%04X", got.CodePoint, cpDBBufferSize)
	}
	want := []byte{0x00, 0x00, 0x80, 0x00} // 32 * 1024 = 0x8000
	if !bytes.Equal(got.Data, want) {
		t.Fatalf("default Data: got % X want % X", got.Data, want)
	}
}

func TestBufferSizeParamExplicitValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		kib  int
		want []byte
	}{
		{1, []byte{0x00, 0x00, 0x04, 0x00}},   // 1*1024 = 0x0400
		{32, []byte{0x00, 0x00, 0x80, 0x00}},  // 32*1024 = 0x8000
		{64, []byte{0x00, 0x01, 0x00, 0x00}},  // 64*1024 = 0x10000
		{128, []byte{0x00, 0x02, 0x00, 0x00}}, // 128*1024 = 0x20000
		{512, []byte{0x00, 0x08, 0x00, 0x00}}, // 512*1024 = 0x80000
	}
	for _, tc := range cases {
		got := bufferSizeParam(tc.kib)
		if !bytes.Equal(got.Data, tc.want) {
			t.Fatalf("kib=%d Data: got % X want % X", tc.kib, got.Data, tc.want)
		}
	}
}

func TestWithBlockSizeRange(t *testing.T) {
	t.Parallel()
	// Out-of-range values silently fall back to zero (the helper at
	// the SelectOption boundary doesn't panic; the driver/Config
	// layer rejects bad DSN values upstream).
	cases := []struct {
		input int
		want  int // expected o.blockSizeKiB
	}{
		{8, 8},
		{32, 32},
		{512, 512},
		// Out-of-range (sub-8 or over-512) silently fall back to zero.
		{0, 0},
		{1, 0},
		{7, 0},
		{-1, 0},
		{513, 0},
		{99999, 0},
	}
	for _, tc := range cases {
		var o selectOpts
		WithBlockSize(tc.input)(&o)
		if o.blockSizeKiB != tc.want {
			t.Fatalf("WithBlockSize(%d): got %d want %d", tc.input, o.blockSizeKiB, tc.want)
		}
	}
}
