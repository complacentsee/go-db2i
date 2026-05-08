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
// Every frame is a 20-byte [Header] followed by an optional template
// and a sequence of length-prefixed parameter blobs (LL CP DATA). The
// header identifies the service (database, signon, etc.) and a
// request/reply ID; the parameters carry the operation's payload.
//
// # Top-level entry points
//
// Connection setup:
//
//   - [SignOn]              -- exchange attributes + sign on (port 8476)
//   - [StartDatabaseService] -- exchange seeds + start service (port 8471)
//   - [SetSQLAttributes]    -- date format / commitment / library list
//
// Statement execution:
//
//   - [SelectStaticSQL]     -- buffered SELECT (drains all rows up front)
//   - [SelectPreparedSQL]   -- buffered SELECT with parameter binding
//   - [OpenSelectStatic]    -- streaming SELECT, returns *Cursor
//   - [OpenSelectPrepared]  -- streaming SELECT with parameter binding
//   - [ExecuteImmediate]    -- single-frame INSERT / UPDATE / DELETE / DDL
//   - [ExecutePreparedSQL]  -- prepared INSERT / UPDATE / DELETE with binds
//
// Transaction control:
//
//   - [Commit] / [Rollback] / [AutocommitOff] / [AutocommitOn]
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
package hostserver
