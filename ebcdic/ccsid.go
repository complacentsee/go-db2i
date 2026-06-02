package ebcdic

import (
	"fmt"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

// Codec converts a Unicode string to bytes in one specific CCSID and back.
type Codec interface {
	// CCSID returns the IBM-assigned identifier for this encoding.
	CCSID() uint32

	// Encode converts s to its byte representation in this CCSID.
	// Characters that don't exist in the target CCSID are replaced
	// with the substitute character (0x3F in EBCDIC).
	Encode(s string) ([]byte, error)

	// Decode converts CCSID-encoded bytes to a Unicode string.
	Decode(b []byte) (string, error)
}

// charmapCodec wraps any [encoding.Encoding] from
// [golang.org/x/text/encoding] under the [Codec] interface.
type charmapCodec struct {
	name  string
	ccsid uint32
	enc   encoding.Encoding
}

func (c charmapCodec) CCSID() uint32 { return c.ccsid }

func (c charmapCodec) Encode(s string) ([]byte, error) {
	out, err := c.enc.NewEncoder().Bytes([]byte(s))
	if err != nil {
		return nil, fmt.Errorf("ebcdic: encode %q to %s: %w", s, c.name, err)
	}
	return out, nil
}

func (c charmapCodec) Decode(b []byte) (string, error) {
	out, err := c.enc.NewDecoder().Bytes(b)
	if err != nil {
		return "", fmt.Errorf("ebcdic: decode from %s: %w", c.name, err)
	}
	return string(out), nil
}

// CCSID37 is the codec for CCSID 37 (US English EBCDIC) -- the IBM i
// default encoding.
var CCSID37 Codec = charmapCodec{
	name:  "CCSID 37",
	ccsid: 37,
	enc:   charmap.CodePage037,
}

// CCSID273 is the codec for CCSID 273 (German EBCDIC) -- the default
// CCSID PUB400 advertises in its server-attributes reply.
//
// This package-level declaration is a placeholder that init() in
// ccsid273.go overrides with the real, table-backed codec
// (ccsid273Codec, a hand-verified CDRA-sourced [256]rune table). The
// real table differs from CCSID 37 in 22 of 256 byte positions --
// the German-specific characters (e.g. '@' at 0xB5 in 273 vs 0x7C in
// 37, plus the Umlaut and ß runs) -- and round-trips all of them
// correctly. The charmap.CodePage037 backing below is never observed
// at runtime; it only ensures this var is a valid Codec before
// init() runs.
var CCSID273 Codec = charmapCodec{
	name:  "CCSID 273 (placeholder -- overridden by ccsid273Codec in init)",
	ccsid: 273,
	enc:   charmap.CodePage037,
}
