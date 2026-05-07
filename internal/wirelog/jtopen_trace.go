package wirelog

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Direction marks whether a frame was sent by the client to the IBM i
// server, or received in reply.
type Direction int

const (
	Sent Direction = iota
	Received
)

func (d Direction) String() string {
	switch d {
	case Sent:
		return "sent"
	case Received:
		return "received"
	}
	return fmt.Sprintf("Direction(%d)", int(d))
}

// Frame is one host-server datastream extracted from a JTOpen trace,
// tagged with which side of the connection produced it and which JTOpen
// connID it was reported under.
//
// JTOpen opens multiple sub-connections per JDBC connection (one per
// host-server service: signon, database, etc.); ConnID lets a replay
// harness keep them separate when more than one appears in a single
// trace file.
type Frame struct {
	Direction Direction
	ConnID    uint32
	Bytes     []byte
}

var headerRE = regexp.MustCompile(`Data stream (sent|data received) \(connID=(\d+)\)`)

// ParseJTOpenTrace reads a JTOpen .trace file produced by
// [com.ibm.as400.access.Trace] with the DATASTREAM category enabled and
// returns the in-order sequence of wire frames it contains.
//
// Diagnostic narrative lines (timestamps, "Connecting service: 4",
// stack traces, etc.) are silently ignored; only the hex byte rows that
// follow a frame header are decoded. Hex tokens must be exactly two
// characters; anything else terminates the current frame.
//
// The function is tolerant of the trace categories that goJTOpen's
// fixture harness leaves enabled (DATASTREAM + ERROR + WARNING) and the
// fuller diagnostic dumps from running with DIAGNOSTIC enabled.
func ParseJTOpenTrace(r io.Reader) ([]Frame, error) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, 64*1024), 1<<20)

	var frames []Frame
	var cur *Frame
	flush := func() {
		if cur != nil {
			frames = append(frames, *cur)
			cur = nil
		}
	}

	for s.Scan() {
		text := s.Text()
		if m := headerRE.FindStringSubmatch(text); m != nil {
			flush()
			id, err := strconv.ParseUint(m[2], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("wirelog: bad connID %q: %w", m[2], err)
			}
			dir := Sent
			if m[1] == "data received" {
				dir = Received
			}
			cur = &Frame{Direction: dir, ConnID: uint32(id)}
			continue
		}
		if cur == nil {
			continue
		}
		bytes, ok := tryParseHexLine(text)
		if !ok {
			flush()
			continue
		}
		cur.Bytes = append(cur.Bytes, bytes...)
	}
	flush()
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("wirelog: scan: %w", err)
	}
	return frames, nil
}

// tryParseHexLine returns the bytes of a JTOpen hex dump row
// ("XX XX XX XX ...") if the line is a valid hex row, otherwise (nil, false).
func tryParseHexLine(text string) ([]byte, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, false
	}
	fields := strings.Fields(text)
	out := make([]byte, 0, len(fields))
	for _, f := range fields {
		if len(f) != 2 {
			return nil, false
		}
		b, err := hex.DecodeString(f)
		if err != nil {
			return nil, false
		}
		out = append(out, b[0])
	}
	return out, true
}

// Sent returns only the frames the client wrote.
func Sents(frames []Frame) []Frame {
	out := make([]Frame, 0, len(frames))
	for _, f := range frames {
		if f.Direction == Sent {
			out = append(out, f)
		}
	}
	return out
}

// Receiveds returns only the frames the server wrote back.
func Receiveds(frames []Frame) []Frame {
	out := make([]Frame, 0, len(frames))
	for _, f := range frames {
		if f.Direction == Received {
			out = append(out, f)
		}
	}
	return out
}
