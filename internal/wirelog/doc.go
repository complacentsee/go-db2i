// Package wirelog parses captured wire-protocol logs into a sequence of
// direction-tagged frames the rest of go-db2i can replay.
//
// JTOpen's [com.ibm.as400.access.Trace] DATASTREAM category emits one
// header line per frame --
//
//	<ts> Data stream sent (connID=<N>) ...
//	<ts> Data stream data received (connID=<N>) ...
//
// followed by space-separated hex byte rows until the next non-hex line.
// [ParseJTOpenTrace] turns that text format into a slice of [Frame]s.
package wirelog
