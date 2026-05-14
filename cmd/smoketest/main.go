// Command smoketest exercises whatever the current go-db2i milestone
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
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	db2i "github.com/complacentsee/go-db2i/driver"
	"github.com/complacentsee/go-db2i/ebcdic"
	"github.com/complacentsee/go-db2i/hostserver"
)

// runTraceStdout exercises a single round-trip Query through the
// database/sql driver layer with an OTel stdout exporter attached.
// Used by the -trace-stdout flag to demonstrate the OTel plumbing
// -- the resulting span JSON lands on stderr and shows
// the db.system.name / db.namespace / db.operation.name / etc.
// attributes the convention specifies. Reads the same env vars
// as runLogDebug.
func runTraceStdout() {
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

	exporter, err := stdouttrace.New(
		stdouttrace.WithWriter(os.Stderr),
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		fail("stdouttrace.New: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	cfg := db2i.DefaultConfig()
	cfg.User = user
	cfg.Password = pwd
	cfg.Host = host
	fmt.Sscanf(dbPort, "%d", &cfg.DBPort)
	fmt.Sscanf(signonPort, "%d", &cfg.SignonPort)
	cfg.Library = envOr("PUB400_LIB", "QSYS2")
	cfg.LogSQL = true // include db.statement on the span
	cfg.Tracer = tp.Tracer("db2i-smoketest")

	connector, err := db2i.NewConnector(&cfg)
	if err != nil {
		fail("new connector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT CURRENT_TIMESTAMP, CURRENT_USER FROM SYSIBM.SYSDUMMY1")
	if err != nil {
		fail("query: %v", err)
	}
	for rows.Next() {
		var ts time.Time
		var u string
		if err := rows.Scan(&ts, &u); err != nil {
			fail("scan: %v", err)
		}
		fmt.Printf("ts=%s user=%s\n", ts.Format(time.RFC3339), u)
	}
	if err := rows.Err(); err != nil {
		fail("rows: %v", err)
	}
	rows.Close()
}

// runLogDebug exercises a single round-trip Query through the
// database/sql driver layer with a slog text handler attached.
// Used by the -log-debug flag to demonstrate the slog logger
// plumbing without disturbing the rest of the smoketest's
// host-server-level harness. Reads the same env vars: PUB400_HOST,
// PUB400_PORT (signon), PUB400_DBPORT, PUB400_USER, PUB400_PWD.
func runLogDebug() {
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

	cfg := db2i.DefaultConfig()
	cfg.User = user
	cfg.Password = pwd
	cfg.Host = host
	fmt.Sscanf(dbPort, "%d", &cfg.DBPort)
	fmt.Sscanf(signonPort, "%d", &cfg.SignonPort)
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	connector, err := db2i.NewConnector(&cfg)
	if err != nil {
		fail("new connector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "SELECT CURRENT_TIMESTAMP, CURRENT_USER FROM SYSIBM.SYSDUMMY1")
	if err != nil {
		fail("query: %v", err)
	}
	for rows.Next() {
		var ts time.Time
		var u string
		if err := rows.Scan(&ts, &u); err != nil {
			fail("scan: %v", err)
		}
		fmt.Printf("ts=%s user=%s\n", ts.Format(time.RFC3339), u)
	}
	if err := rows.Err(); err != nil {
		fail("rows: %v", err)
	}
	rows.Close()
}

func main() {
	logDebug := flag.Bool("log-debug", false, "exercise the database/sql driver with slog DEBUG attached to stderr; skips the host-server smoketest")
	traceStdout := flag.Bool("trace-stdout", false, "exercise the database/sql driver with the OTel stdout span exporter; skips the host-server smoketest")
	flag.Parse()
	if *traceStdout {
		runTraceStdout()
		return
	}
	if *logDebug {
		runLogDebug()
		return
	}
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
	fmt.Fprintf(os.Stderr, "go-db2i smoketest -> %s as %s\n", signonAddr, user)

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
	fmt.Fprintf(os.Stderr, "go-db2i smoketest -> %s (as-database)\n", dbAddr)
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

	// --- Step 5a: M2 static SELECT (chained -- M4 added RPB DELETE). ---
	staticSQL := envOr("PUB400_SQL", "SELECT CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")
	staticRes, err := hostserver.SelectStaticSQL(dbConn, staticSQL, 3)
	if err != nil {
		fail("M2 static select: %v", err)
	}
	fmt.Printf("\nstatic select (chained): %s\n", staticSQL)
	for i, r := range staticRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 5b: M3 prepared SELECT with int parameter (still on same connection). ---
	preparedSQL := "SELECT CAST(? AS INTEGER) AS V FROM SYSIBM.SYSDUMMY1"
	shapes := []hostserver.PreparedParam{{
		SQLType:     497, // INTEGER nullable
		FieldLength: 4,
		Precision:   10,
		Scale:       0,
		CCSID:       0,
	}}
	// Static SELECT consumed correlations 3..6 (CREATE_RPB,
	// PREPARE_DESCRIBE, OPEN_DESCRIBE_FETCH, RPB DELETE). Start
	// the prepared SELECT at correlation 7 so they don't collide.
	pres, err := hostserver.SelectPreparedSQL(dbConn, preparedSQL, shapes, []any{int32(42)}, 7)
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

	// --- Step 11.55: M4 DECIMAL(5,2) bind. ---
	decBindRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS DECIMAL(5,2)) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     485, // DECIMAL nullable
			FieldLength: 3,   // ceil((5+1)/2) = 3 bytes
			Precision:   5,
			Scale:       2,
		}},
		[]any{"-123.45"},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS DECIMAL(5,2)) ... [\"-123.45\"]\n")
	for i, r := range decBindRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.6: M4 DECFLOAT(16) bind (decimal64). ---
	df16Res := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS DECFLOAT(16)) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     997,
			FieldLength: 8,
			Precision:   16,
		}},
		[]any{"123456.7890123456"},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS DECFLOAT(16)) ... [\"123456.7890123456\"]\n")
	for i, r := range df16Res.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.61: M4 DECFLOAT(34) bind (decimal128). ---
	df34Res := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS DECFLOAT(34)) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     997,
			FieldLength: 16,
			Precision:   34,
		}},
		[]any{"1.234567890123456789012345678901234E+100"},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS DECFLOAT(34)) ... [scientific]\n")
	for i, r := range df34Res.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.65: M4 NUMERIC(5,2) bind. ---
	numBindRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS NUMERIC(5,2)) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     489, // NUMERIC nullable
			FieldLength: 5,   // one byte per digit
			Precision:   5,
			Scale:       2,
		}},
		[]any{"-123.45"},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS NUMERIC(5,2)) ... [\"-123.45\"]\n")
	for i, r := range numBindRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.7: M4 DATE bind. ---
	dateRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS DATE) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     385, // DATE nullable
			FieldLength: 10,  // ISO format
			Precision:   10,
		}},
		[]any{"2026-05-08"},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS DATE) ... [\"2026-05-08\"]\n")
	for i, r := range dateRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.8: M4 TIME bind. ---
	timeRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS TIME) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     389, // TIME nullable
			FieldLength: 8,
			Precision:   8,
		}},
		[]any{"13:45:09"},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS TIME) ... [\"13:45:09\"]\n")
	for i, r := range timeRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.9: M4 TIMESTAMP bind. ---
	tsRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS TIMESTAMP) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     393, // TIMESTAMP nullable
			FieldLength: 26,
			Precision:   26,
			Scale:       6,
		}},
		[]any{"2026-05-08T13:45:09.123456"},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS TIMESTAMP) ... [\"2026-05-08T13:45:09.123456\"]\n")
	for i, r := range tsRes.Rows {
		fmt.Printf("  row %d: %v\n", i, r)
	}

	// --- Step 11.6: M4 NULL bind. ---
	nullRes := freshPreparedSelect(dbAddr, user, pwd,
		"SELECT CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1",
		[]hostserver.PreparedParam{{
			SQLType:     497,
			FieldLength: 4,
			Precision:   10,
		}},
		[]any{nil},
	)
	fmt.Printf("\nprepared select: SELECT CAST(? AS INTEGER) ... [NULL]\n")
	for i, r := range nullRes.Rows {
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

	// --- Step 12: M5 multi-row continuation FETCH. ---
	// SYSIBM.SYSTABLES has hundreds of system tables on PUB400 --
	// enough to push the result set past the 32 KB OPEN buffer
	// and force at least one continuation FETCH. Confirms the
	// loop terminates on SQLCODE +100 and rows are stitched
	// together correctly.
	multiConn := dialOrDie(dbAddr, "as-database (multi-row FETCH)")
	defer multiConn.Close()
	if _, _, err := hostserver.StartDatabaseService(multiConn, user, pwd); err != nil {
		fail("multi-row db open: %v", err)
	}
	if _, err := hostserver.SetSQLAttributes(multiConn, hostserver.DefaultDBAttributesOptions()); err != nil {
		fail("multi-row set-sql-attributes: %v", err)
	}
	if err := hostserver.NDBAddLibraryList(multiConn, "AFTRAEGE11", 2); err != nil {
		fmt.Fprintf(os.Stderr, "warn: multi-row NDB add-library-list: %v\n", err)
	}
	mres, err := hostserver.SelectStaticSQL(multiConn,
		"SELECT TABLE_SCHEMA, TABLE_NAME FROM QSYS2.SYSTABLES ORDER BY TABLE_SCHEMA, TABLE_NAME FETCH FIRST 5000 ROWS ONLY",
		3,
	)
	if err != nil {
		fail("M5 multi-row select: %v", err)
	}
	fmt.Printf("\nmulti-row select: QSYS2.SYSTABLES (FETCH FIRST 5000)\n")
	fmt.Printf("  total rows: %d\n", len(mres.Rows))
	if len(mres.Rows) > 0 {
		fmt.Printf("  first: %v\n", mres.Rows[0])
		fmt.Printf("  last:  %v\n", mres.Rows[len(mres.Rows)-1])
	}

	// --- Step 13: M5 autocommit toggle. ---
	// AutocommitOff/On send small SET_SQL_ATTRIBUTES frames; the
	// COMMIT (0x1807) and ROLLBACK (0x1808) frames themselves are
	// covered by TestCommitFrameShape / TestRollbackFrameShape but
	// can't be live-verified here without write privileges (PUB400
	// rejects committing a read-only transaction with SQL -211 /
	// -7008). M6 acceptance picks them up via database/sql.Tx
	// once we wire the driver.
	txConn := dialOrDie(dbAddr, "as-database (tx)")
	defer txConn.Close()
	if _, _, err := hostserver.StartDatabaseService(txConn, user, pwd); err != nil {
		fail("tx db open: %v", err)
	}
	if _, err := hostserver.SetSQLAttributes(txConn, hostserver.DefaultDBAttributesOptions()); err != nil {
		fail("tx set-sql-attributes: %v", err)
	}
	if err := hostserver.NDBAddLibraryList(txConn, "AFTRAEGE11", 2); err != nil {
		fmt.Fprintf(os.Stderr, "warn: tx NDB add-library-list: %v\n", err)
	}
	if err := hostserver.AutocommitOff(txConn, 3); err != nil {
		fail("AutocommitOff: %v", err)
	}
	if err := hostserver.AutocommitOn(txConn, 4); err != nil {
		fail("AutocommitOn: %v", err)
	}
	fmt.Printf("\nautocommit OFF/ON toggle: ok\n")

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

