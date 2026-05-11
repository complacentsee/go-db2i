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

// String returns the lowercase tag JTOpen prints in trace headers
// ("sent" or "received"), or the unknown-direction fallback
// "Direction(N)".
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

// Consolidate merges adjacent frames that share a (Direction, ConnID)
// pair, returning the byte stream as the caller will see it on the
// socket rather than the chunking JTOpen happened to log.
//
// JTOpen's [com.ibm.as400.access.Trace] emits one "Data stream sent"
// or "Data stream data received" event per socket I/O. A logical
// host-server frame is often delivered across two reads (a 20-byte
// header followed by the rest of the payload), producing two adjacent
// Received frames in the trace. Merging them lets a replay harness or
// frame parser treat the trace as a continuous wire stream.
//
// Frames are merged only when they are immediate neighbors and agree
// on direction + connID. A direction change, a connID change, or a
// gap (any non-Frame text in between is already filtered by the
// scanner, so "gap" only matters at the call boundary) terminates the
// current run.
func Consolidate(frames []Frame) []Frame {
	if len(frames) == 0 {
		return nil
	}
	out := make([]Frame, 0, len(frames))
	out = append(out, Frame{
		Direction: frames[0].Direction,
		ConnID:    frames[0].ConnID,
		Bytes:     append([]byte(nil), frames[0].Bytes...),
	})
	for _, f := range frames[1:] {
		last := &out[len(out)-1]
		if last.Direction == f.Direction && last.ConnID == f.ConnID {
			last.Bytes = append(last.Bytes, f.Bytes...)
			continue
		}
		out = append(out, Frame{
			Direction: f.Direction,
			ConnID:    f.ConnID,
			Bytes:     append([]byte(nil), f.Bytes...),
		})
	}
	return out
}

// Sents returns only the frames the client wrote, in order.
func Sents(frames []Frame) []Frame {
	out := make([]Frame, 0, len(frames))
	for _, f := range frames {
		if f.Direction == Sent {
			out = append(out, f)
		}
	}
	return out
}

// Receiveds returns only the frames the server wrote back, in order.
func Receiveds(frames []Frame) []Frame {
	out := make([]Frame, 0, len(frames))
	for _, f := range frames {
		if f.Direction == Received {
			out = append(out, f)
		}
	}
	return out
}
