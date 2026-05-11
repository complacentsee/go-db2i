// Package ebcdic converts between Unicode strings and IBM CCSID byte
// encodings.
//
// IBM i values nearly all character data under a Coded Character Set
// Identifier (CCSID); the wire protocol is full of EBCDIC strings the
// go-db2i driver has to read and write. JTOpen carries a per-CCSID
// lookup table for each (~186 of them); this package starts smaller --
// it leans on [golang.org/x/text/encoding/charmap] for the codepages
// that ship with the standard text/encoding library and adds the rest
// as the driver needs them.
//
// CCSID 37 (US English) is the IBM i default and is implemented now;
// it covers user IDs, the SQL CURRENT_USER value, error message
// substitution variables, and most other strings the driver will
// encounter on a default-locale system.
package ebcdic
