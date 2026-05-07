// Command smoketest exercises whatever the current goJTOpen milestone
// has wired up against a real IBM i. M1 (signon side): connect to
// the as-signon host server (port 8476), do the exchange-attributes
// + signon-info handshake, print server VRM / CCSID / sign-on dates.
// M1 (database side): then open a second connection to the
// as-database host server (port 8471), do the xchg-rand-seed +
// start-server handshake, print the database prestart-job name.
//
// Configuration (env vars):
//
//	PUB400_HOST  (default: pub400.com)
//	PUB400_PORT  (default: 8476 -- the as-signon port)
//	PUB400_DBPORT (default: 8471 -- the as-database port)
//	PUB400_USER  (required)
//	PUB400_PWD   (required)
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/complacentsee/goJTOpen/ebcdic"
	"github.com/complacentsee/goJTOpen/hostserver"
)

func main() {
	host := envOr("PUB400_HOST", "pub400.com")
	signonPort := envOr("PUB400_PORT", "8476")
	dbPort := envOr("PUB400_DBPORT", "8471")
	user, ok := requireEnv("PUB400_USER")
	if !ok {
		os.Exit(2)
	}
	pwd, ok := requireEnv("PUB400_PWD")
	if !ok {
		os.Exit(2)
	}

	signonAddr := net.JoinHostPort(host, signonPort)
	fmt.Fprintf(os.Stderr, "goJTOpen smoketest -> %s as %s\n", signonAddr, user)

	// --- Step 1: as-signon. ---
	conn := dialOrDie(signonAddr, "as-signon")
	defer conn.Close()

	xa, si, err := hostserver.SignOn(conn, user, pwd)
	if err != nil {
		fail("sign-on: %v", err)
	}

	fmt.Printf("server VRM:      0x%08X (V%dR%dM%d)\n",
		xa.ServerVersion,
		(xa.ServerVersion>>16)&0xFF,
		(xa.ServerVersion>>8)&0xFF,
		xa.ServerVersion&0xFF)
	fmt.Printf("ds level:        %d\n", xa.ServerLevel)
	fmt.Printf("password level:  %d\n", xa.PasswordLevel)
	jobName, _ := ebcdic.CCSID37.Decode(xa.JobName)
	fmt.Printf("signon job:      %s\n", jobName)

	fmt.Printf("RC:              %d\n", si.ReturnCode)
	fmt.Printf("server CCSID:    %d\n", si.ServerCCSID)
	fmt.Printf("current signon:  %s\n", si.CurrentSignonDate.Format(time.RFC3339))
	fmt.Printf("last signon:     %s\n", si.LastSignonDate.Format(time.RFC3339))
	fmt.Printf("password expiry: %s (warn %d days)\n",
		si.ExpirationDate.Format("2006-01-02"),
		si.PWDExpirationWarningDays)

	// --- Step 2: as-database. ---
	dbAddr := net.JoinHostPort(host, dbPort)
	fmt.Fprintf(os.Stderr, "goJTOpen smoketest -> %s (as-database)\n", dbAddr)
	dbConn := dialOrDie(dbAddr, "as-database")
	defer dbConn.Close()

	xs, ss, err := hostserver.StartDatabaseService(dbConn, user, pwd)
	if err != nil {
		fail("start db service: %v", err)
	}

	fmt.Printf("db pwd level:    %d\n", xs.PasswordLevel)
	fmt.Printf("db RC:           %d\n", ss.ReturnCode)
	dbJobName, _ := ebcdic.CCSID37.Decode(ss.JobName)
	fmt.Printf("db job:          %s\n", dbJobName)

	// --- Step 3: SET_SQL_ATTRIBUTES -- exchange capabilities with
	// the SQL service, learn the server's CCSID + default schema
	// + functional level. This is the entry to M2 (static SELECT).
	attrs, err := hostserver.SetSQLAttributes(dbConn, hostserver.DefaultDBAttributesOptions())
	if err != nil {
		fail("set-sql-attributes: %v", err)
	}
	fmt.Printf("sql server CCSID:  %d\n", attrs.ServerCCSID)
	level, _ := ebcdic.CCSID37.Decode(attrs.ServerFunctionalLevel)
	fmt.Printf("sql functional lvl:%s (VRM=0x%08X)\n", level, attrs.VRM())
	rdb, _ := ebcdic.CCSID37.Decode(attrs.RelationalDBName)
	fmt.Printf("sql RDB name:      %s\n", strings.TrimSpace(rdb))
	defLib, _ := ebcdic.CCSID37.Decode(attrs.DefaultSQLLibraryName)
	fmt.Printf("sql default lib:   %s\n", strings.TrimSpace(defLib))
	defSchema, _ := ebcdic.CCSID37.Decode(attrs.DefaultSQLSchemaName)
	fmt.Printf("sql default schema:%s\n", defSchema)
	dbJob, _ := ebcdic.CCSID37.Decode(attrs.ServerJobIdentifier)
	fmt.Printf("sql server job:    %s\n", strings.TrimSpace(dbJob))

	// --- Step 4: NDB ADD_LIBRARY_LIST. ---
	// JTOpen sends this as a session-init handshake between
	// SET_SQL_ATTRIBUTES and the first PREPARE; we mirror the
	// flow so the SQL service has fully-initialised session
	// state before any PREPARE_DESCRIBE.
	if err := hostserver.NDBAddLibraryList(dbConn, "AFTRAEGE11", 2); err != nil {
		fmt.Fprintf(os.Stderr, "warn: NDB ADD_LIBRARY_LIST failed: %v\n", err)
	}

	// --- Step 5: M3 prepared SELECT with int parameter. ---
	// Skip the M2 static SELECT step here so the prepared call
	// gets a clean RPB slot 1 -- we don't yet emit RPB DELETE
	// between SELECTs, so chaining two on one connection trips
	// the server. SelectStaticSQL is exercised by its own test;
	// the smoketest's job is end-to-end M3 validation now.
	preparedSQL := "SELECT CAST(? AS INTEGER) AS V FROM SYSIBM.SYSDUMMY1"
	shapes := []hostserver.PreparedParam{{
		SQLType:     497, // INTEGER nullable
		FieldLength: 4,
		Precision:   10,
		Scale:       0,
		CCSID:       0,
	}}
	pres, err := hostserver.SelectPreparedSQL(dbConn, preparedSQL, shapes, []any{int32(42)}, 3)
	if err != nil {
		fail("M3 prepared select (int): %v", err)
	}
	fmt.Printf("\nprepared select: %s [42]\n", preparedSQL)
	fmt.Printf("columns:           %d\n", len(pres.Columns))
	for i, c := range pres.Columns {
		fmt.Printf("  col %d: name=%q sql_type=%d length=%d\n",
			i, c.Name, c.SQLType, c.Length)
	}
	fmt.Printf("rows:              %d\n", len(pres.Rows))
	for i, r := range pres.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 7: M3 prepared SELECT with VARCHAR parameter. ---
	// Each prepared call needs its own connection until RPB
	// DELETE between statements lands; reopen for the second.
	dbConn.Close()
	dbConn2 := dialOrDie(dbAddr, "as-database (varchar bind)")
	defer dbConn2.Close()
	if _, _, err := hostserver.StartDatabaseService(dbConn2, user, pwd); err != nil {
		fail("re-open db for varchar bind: %v", err)
	}
	if _, err := hostserver.SetSQLAttributes(dbConn2, hostserver.DefaultDBAttributesOptions()); err != nil {
		fail("set-sql-attributes (varchar): %v", err)
	}
	if err := hostserver.NDBAddLibraryList(dbConn2, "AFTRAEGE11", 2); err != nil {
		fmt.Fprintf(os.Stderr, "warn: NDB add-library-list (varchar): %v\n", err)
	}
	varcharSQL := "SELECT CAST(? AS VARCHAR(50)) FROM SYSIBM.SYSDUMMY1"
	varcharShapes := []hostserver.PreparedParam{{
		SQLType:     449, // VARCHAR nullable
		FieldLength: 52,
		Precision:   50,
		Scale:       0,
		CCSID:       273,
	}}
	vres, err := hostserver.SelectPreparedSQL(dbConn2, varcharSQL, varcharShapes, []any{"hello, IBM i"}, 3)
	if err != nil {
		fail("M3 prepared select (varchar): %v", err)
	}
	fmt.Printf("\nprepared select: %s [\"hello, IBM i\"]\n", varcharSQL)
	fmt.Printf("columns:           %d\n", len(vres.Columns))
	for i, c := range vres.Columns {
		fmt.Printf("  col %d: name=%q sql_type=%d length=%d ccsid=%d\n",
			i, c.Name, c.SQLType, c.Length, c.CCSID)
	}
	fmt.Printf("rows:              %d\n", len(vres.Rows))
	for i, r := range vres.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 8: M4 BIGINT bind. ---
	bigintRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS BIGINT) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     493, // BIGINT nullable
			FieldLength: 8,
			Precision:   19,
			Scale:       0,
		}},
		[]any{int64(9223372036854775807)},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS BIGINT) ... [9223372036854775807]\n")
	fmt.Printf("rows:              %d\n", len(bigintRes.Rows))
	for i, r := range bigintRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 9: M4 DOUBLE bind. ---
	doubleRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS DOUBLE) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     481, // DOUBLE nullable
			FieldLength: 8,
			Precision:   53,
			Scale:       0,
		}},
		[]any{2.718281828459045},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS DOUBLE) ... [2.718281828459045]\n")
	fmt.Printf("rows:              %d\n", len(doubleRes.Rows))
	for i, r := range doubleRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 10: M4 REAL bind. ---
	realRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS REAL) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     481, // REAL nullable (480 NN); same code, FieldLength=4 picks REAL
			FieldLength: 4,
			Precision:   24,
			Scale:       0,
		}},
		[]any{float32(3.14)},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS REAL) ... [3.14]\n")
	fmt.Printf("rows:              %d\n", len(realRes.Rows))
	for i, r := range realRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.5: M4 static DECIMAL(p,s) read. ---
	// Decimal isn't bound here (M4 binding lands later); we
	// confirm the row decoder works live by SELECTing a
	// DECIMAL(31,5) literal.
	decConn := dialOrDie(dbAddr, "as-database (decimal read)")
	defer decConn.Close()
	if _, _, err := hostserver.StartDatabaseService(decConn, user, pwd); err != nil {
		fail("decimal db open: %v", err)
	}
	if _, err := hostserver.SetSQLAttributes(decConn, hostserver.DefaultDBAttributesOptions()); err != nil {
		fail("decimal set-sql-attributes: %v", err)
	}
	if err := hostserver.NDBAddLibraryList(decConn, "AFTRAEGE11", 2); err != nil {
		fmt.Fprintf(os.Stderr, "warn: decimal NDB add-library-list: %v\n", err)
	}
	dres, err := hostserver.SelectStaticSQL(decConn,
		"VALUES CAST(-99999999999999999999999999.12345 AS DECIMAL(31,5))",
		3,
	)
	if err != nil {
		fail("decimal static select: %v", err)
	}
	fmt.Printf("\nstatic select: VALUES CAST(... AS DECIMAL(31,5))\n")
	for i, r := range dres.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11: M4 SMALLINT bind. ---
	smallintRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS SMALLINT) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     501, // SMALLINT nullable
			FieldLength: 2,
			Precision:   5,
			Scale:       0,
		}},
		[]any{int32(-12345)},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS SMALLINT) ... [-12345]\n")
	fmt.Printf("rows:              %d\n", len(smallintRes.Rows))
	for i, r := range smallintRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	fmt.Fprintln(os.Stderr, "ok")
}

// freshPreparedSelect opens a clean db connection, runs the prepared
// SELECT, and returns the result. Dedicated connection per call works
// around the missing RPB DELETE between prepared statements
// (deferred follow-up).
func freshPreparedSelect(addr, user, pwd, sql string, shapes []hostserver.PreparedParam, vals []any) *hostserver.SelectResult {
	c := dialOrDie(addr, "as-database "+sql)
	defer c.Close()
	if _, _, err := hostserver.StartDatabaseService(c, user, pwd); err != nil {
		fail("re-open db: %v", err)
	}
	if _, err := hostserver.SetSQLAttributes(c, hostserver.DefaultDBAttributesOptions()); err != nil {
		fail("set-sql-attributes: %v", err)
	}
	if err := hostserver.NDBAddLibraryList(c, "AFTRAEGE11", 2); err != nil {
		fmt.Fprintf(os.Stderr, "warn: NDB add-library-list: %v\n", err)
	}
	r, err := hostserver.SelectPreparedSQL(c, sql, shapes, vals, 3)
	if err != nil {
		fail("prepared select %q: %v", sql, err)
	}
	return r
}

// dialOrDie connects to addr and applies a 30s deadline; on any
// error it calls fail(). label appears in error messages for clarity
// when both signon + database connections are in flight.
func dialOrDie(addr, label string) net.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := net.Dialer{Timeout: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		fail("dial %s (%s): %v", label, addr, err)
	}
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		conn.Close()
		fail("set deadline (%s): %v", label, err)
	}
	return conn
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func requireEnv(key string) (string, bool) {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "missing required env var %s\n", key)
		return "", false
	}
	return v, true
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: ")
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

