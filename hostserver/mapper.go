package hostserver

import (
	"encoding/binary"
	"fmt"
	"io"
)

// serverMapSuccess is the status byte (ASCII '+') the IBM i server
// mapper returns in byte 0 of its 5-byte reply on a successful lookup.
// Any other value (the daemon conventionally uses ASCII '-' 0x2D)
// signals failure; JTOpen tests only for success and rejects everything
// else, so we do the same.
const serverMapSuccess = 0x2B

// ServerMapName returns the as-svrmap lookup string for service,
// appending the "-s" secure suffix when secure is true. This is the
// raw ASCII string the client writes to the mapper, e.g.
//
//	ServerDatabase, false -> "as-database"
//	ServerDatabase, true  -> "as-database-s"
//
// The name is reused verbatim from [ServerID.String], which already
// matches JTOpen's AS400.getServerName mapping.
func ServerMapName(service ServerID, secure bool) string {
	name := service.String()
	if secure {
		name += "-s"
	}
	return name
}

// ServerMapPort runs the IBM i server-mapper (as-svrmap, job
// QZSOSMAPD, TCP port 449) lookup on conn and returns the TCP port the
// named host server is currently listening on. It mirrors JTOpen's
// PortMapper exchange byte-for-byte:
//
//  1. Write the ASCII service name ([ServerMapName]) -- nothing else.
//     No length prefix, no terminator, no padding. The name is ASCII
//     even though IBM i is EBCDIC elsewhere (JTOpen casts each char to
//     a byte; we send the same bytes).
//  2. Read exactly 5 reply bytes: byte 0 is the status (0x2B '+' =
//     success, any other value = failure), bytes 1..4 are the port as
//     a big-endian uint32 -- a binary integer, NOT an ASCII string.
//
// conn is any io.ReadWriter -- typically a freshly dialled *net.TCPConn
// the caller owns. The mapper socket is always plaintext, even when the
// data connections that follow are TLS; for a TLS service pass
// secure=true so the "-s" name resolves to the SSL port. ServerMapPort
// does not close conn.
//
// This is a standalone exchange that predates the DSS datastream, so it
// deliberately does not use [WriteFrame] / [ReadFrame].
func ServerMapPort(conn io.ReadWriter, service ServerID, secure bool) (int, error) {
	name := ServerMapName(service, secure)
	if _, err := io.WriteString(conn, name); err != nil {
		return 0, fmt.Errorf("hostserver: server-map %q: write request: %w", name, err)
	}

	var reply [5]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		return 0, fmt.Errorf("hostserver: server-map %q: read reply: %w", name, err)
	}
	if reply[0] != serverMapSuccess {
		return 0, fmt.Errorf("hostserver: server-map %q: server returned status 0x%02X (want 0x2B '+')", name, reply[0])
	}

	port := int(binary.BigEndian.Uint32(reply[1:5]))
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("hostserver: server-map %q: resolved port %d out of range (1..65535)", name, port)
	}
	return port, nil
}
