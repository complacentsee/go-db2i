// Package hostserver implements the IBM i host-server datastream
// protocol that JT400 speaks on TCP ports 8470-8476 (TLS variants
// 9470-9476).
//
// This package is the wire-format layer underneath the database/sql
// driver in github.com/complacentsee/goJTOpen/driver. Most application
// code should import the driver package and use the standard
// database/sql APIs instead of calling into hostserver directly. The
// types and functions exposed here are public so advanced callers can
// build connection-pooling shims, custom request flows, or wire-level
// debug tools without forking the driver.
//
// # Wire format
//
// Every frame on the database / sign-on socket is a 20-byte [Header]
// (DSS header) followed by an operation-specific template (typically
// 20 bytes for the database service) and a sequence of LL/CP/DATA
// parameter blobs:
//
//	0..3    Length (total frame including header)
//	4..5    HeaderID (varies; high bit signals whole-datastream RLE)
//	6..7    ServerID (high byte 0xE0 sanity marker)
//	8..11   CSInstance
//	12..15  CorrelationID
//	16..17  TemplateLength
//	18..19  ReqRepID (request / reply identifier)
//	20..   Template + parameters
//
// Parameters are LL(4)+CP(2)+DATA, where LL covers the full block.
// CP codepoints in the 0x3800..0x38FF range are SQL-level (statement
// text, descriptors, SQLCA); 0x1100..0x11FF are signon-service CPs.
// [ParseDBReply] walks a database reply and surfaces the template
// fields (ORS bitmap echo, ErrorClass, ReturnCode) plus the
// parameter list. RLE-compressed replies (template[4:8] high bit
// set) are unwrapped transparently via the whole-datastream
// decompressor [decompressDataStreamRLE]; the per-CP RLE variant
// inside CP 0x380F LOB Data is handled by [parseLOBReply] +
// [decompressRLE1].
//
// # Top-level entry points
//
// Connection setup:
//
//   - [SignOn]              -- exchange attributes + sign on (port 8476)
//   - [StartDatabaseService] -- exchange seeds + start service (port 8471)
//   - [SetSQLAttributes]    -- date format, isolation, default library,
//     client CCSID (CP 0x3801), LOB threshold (CP 0x3822), and the
//     extended-metadata enable in one round trip
//
// Statement execution:
//
//   - [SelectStaticSQL]     -- buffered SELECT (drains all rows up front)
//   - [SelectPreparedSQL]   -- buffered SELECT with parameter binding
//   - [OpenSelectStatic]    -- streaming SELECT, returns *Cursor;
//     accepts [SelectOption] variadics (e.g. [WithExtendedMetadata])
//   - [OpenSelectPrepared]  -- streaming SELECT with parameter binding +
//     [SelectOption] variadics
//   - [ExecuteImmediate]    -- single-frame INSERT / UPDATE / DELETE / DDL
//   - [ExecutePreparedSQL]  -- prepared INSERT / UPDATE / DELETE with binds
//
// LOB I/O:
//
//   - [RetrieveLOBData]     -- server-side locator -> client bytes;
//     request-side RLE bit on by default, [ParseDBReply] unwraps
//   - [WriteLOBData]        -- chunked LOB upload for prepared binds
//
// Transaction control:
//
//   - [Commit] / [Rollback] / [AutocommitOff] / [AutocommitOn]
//
// # CCSID handling
//
// Untagged CHAR / VARCHAR / CLOB columns decode through the per-column
// CCSID via [ebcdicForCCSID]. CCSID 65535 (FOR BIT DATA) is the
// raw-bytes sentinel -- columns at that CCSID return []byte without
// EBCDIC decode. CCSID 1208 (UTF-8) is a no-op passthrough on V7R3+
// servers. The driver layer offers a `?ccsid=N` DSN knob (plumbed
// through to [DBAttributesOptions.ClientCCSID]) for callers that
// need to override the connection-default for untagged columns;
// tagged columns always win on the read side.
//
// # LOB compression
//
// V7R6 servers respond to RetrieveLOBData with a whole-datastream
// RLE-wrapped reply when the request asks for it (the bit is on by
// default). [ParseDBReply] detects the compression marker (high bit
// of the 32-bit word at template offset 4, matching JT400's
// DBBaseReplyDS.parse) and inflates the CP 0x3832 wrapper via
// [decompressDataStreamRLE] before returning. Highly compressible
// LOBs (constant-content BLOBs, JSON/XML with repeating structure)
// shrink to a handful of wire bytes for an arbitrary-sized payload;
// mixed-entropy LOBs pay no per-byte cost because the server skips
// RLE when it wouldn't help.
//
// The per-CP RLE-1 variant (used inside CP 0x380F LOB Data) is a
// different wire format (6-byte record: escape + 1-byte value +
// 4-byte BE count, vs the whole-datastream 5-byte record). Both
// decoders ship; [parseLOBReply] picks the per-CP variant
// automatically when CP 0x380F's declared `actualLen` differs from
// the wire byte count.
//
// # Extended metadata
//
// [WithExtendedMetadata] on OpenSelectStatic / OpenSelectPrepared
// asks the server to populate CP 0x3811 with per-column schema,
// base table, base column, and label. Requires two distinct knobs
// on PREPARE_DESCRIBE: the ORS bit ORSExtendedColumnDescrs
// (0x00020000) AND the per-statement CP 0x3829 = 0xF1
// (ExtendedColumnDescriptorOption). With only the ORS bit the
// server ships CP 0x3811 with zero data (JT400 source calls this
// out as "Received empty extended column descriptor"); both knobs
// are needed for the payload to actually arrive. The fields land
// on [SelectColumn] as Schema, Table, BaseColumnName, Label.
//
// # Cursor lifecycle
//
// [OpenSelectStatic] / [OpenSelectPrepared] return a [*Cursor] that
// owns one server-side cursor (CRSR0001) and one Request Parameter
// Block (RPB at slot 1). Initial open issues
// CREATE_RPB -> PREPARE_DESCRIBE -> OPEN_DESCRIBE_FETCH; subsequent
// row pulls (via Cursor.Next from the driver layer) issue
// continuation FETCH on demand, returning a 32 KB block-buffer's
// worth at a time. Cursor.Close issues CLOSE (idempotent) + RPB
// DELETE so the next statement on the same Conn doesn't trip on
// a half-open slot.
//
// # Errors
//
// Server-side SQL errors come back as [*Db2Error] with SQLSTATE,
// SQLCODE, the IBM message id, and the substitution token list
// pulled from the SQLCA (CP 0x3807). Predicate methods
// (IsNotFound / IsConstraintViolation / IsConnectionLost /
// IsLockTimeout) classify the common cases.
//
// I/O-level errors (TCP drops, short frames, deadline exceeded)
// stay as their underlying types -- the driver package wraps them
// with driver.ErrBadConn semantics for the database/sql pool.
//
// # Cross-references
//
// Wire-format references for contributors:
//
//   - docs/lob-bind-wire-protocol.md      LOB binds (CP 0x381D, 0x381E,
//                                          0x381F, 0x3813, WRITE_LOB_DATA)
//   - docs/lob-known-gaps.md              LOB SELECT quirks + closure log
//   - docs/configuration.md               Driver DSN reference
//   - docs/PLAN.md                        Milestone history + risks
//
// JT400 sources to cross-reference when adding new wire flows live
// in /home/complacentsee/JTOpen/src/main/java/com/ibm/as400/access/
// on a typical dev box; the canonical map is in docs/PLAN.md
// "Cross-reference: JTOpen sources by topic".
package hostserver
