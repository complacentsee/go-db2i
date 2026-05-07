// Package hostserver implements the IBM i host-server datastream
// protocol that JTOpen speaks on TCP ports 8470-8476 (and SSL variants
// 9470-9476).
//
// Every frame is a 20-byte [Header] followed by an optional template
// and a sequence of length-prefixed parameter blobs (LL CP DATA). The
// header identifies the service (database, signon, etc.) and a
// request/reply ID; the parameters carry the operation's payload.
//
// Subpackages and sibling packages handle the individual services:
//   - signon: authentication / capability exchange
//   - database (TODO): SQL request/reply
package hostserver
