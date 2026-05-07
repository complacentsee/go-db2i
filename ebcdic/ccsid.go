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
