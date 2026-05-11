package io.github.complacentsee.db2i.fixtures;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Declares every fixture case the harness will capture.
 *
 * Cases that need only system tables target {@code SYSIBM.SYSDUMMY1} so the
 * fixtures are reproducible across PUB400 accounts. Cases that need a real
 * table create {@code <schema>.GODB2I_T1} in setup and drop it in teardown.
 *
 * Add cases here as the Go driver progresses. Re-run the harness against
 * PUB400, commit the new {@code .trace} + {@code .golden.json} pair.
 */
final class Cases {

    static List<Case> all(String schema) {
        List<Case> cases = new ArrayList<>();

        cases.add(new ConnectOnly());
        cases.add(new SelectDummy());

        // Integer types via CAST so we don't need a schema.
        cases.add(typeFromDummy("types_smallint", "CAST(-12345 AS SMALLINT)"));
        cases.add(typeFromDummy("types_integer", "CAST(-2147483648 AS INTEGER)"));
        cases.add(typeFromDummy("types_bigint", "CAST(9223372036854775807 AS BIGINT)"));
        cases.add(typeFromDummy("types_real", "CAST(3.1415927 AS REAL)"));
        cases.add(typeFromDummy("types_double", "CAST(2.718281828459045 AS DOUBLE)"));

        // Packed decimal: DECIMAL(p,s) is packed in DB2 for i.
        cases.add(typeFromDummy("types_decimal_5_2", "CAST(-123.45 AS DECIMAL(5,2))"));
        cases.add(typeFromDummy("types_decimal_31_5",
                "CAST(99999999999999999999999999.12345 AS DECIMAL(31,5))"));
        cases.add(typeFromDummy("types_decimal_negative_31_5",
                "CAST(-99999999999999999999999999.12345 AS DECIMAL(31,5))"));

        // Zoned decimal: NUMERIC(p,s) is zoned in DB2 for i.
        cases.add(typeFromDummy("types_numeric_5_2", "CAST(-123.45 AS NUMERIC(5,2))"));
        cases.add(typeFromDummy("types_numeric_31_5",
                "CAST(12345678901234567890123456.78901 AS NUMERIC(31,5))"));

        // DECFLOAT: IEEE 754-2008 decimal floating point.
        cases.add(typeFromDummy("types_decfloat_16", "CAST(1.234567890123456E+5 AS DECFLOAT(16))"));
        cases.add(typeFromDummy("types_decfloat_34",
                "CAST(1.234567890123456789012345678901234E+100 AS DECFLOAT(34))"));

        // Strings: default CCSID (37 on PUB400 normally).
        cases.add(typeFromDummy("types_char_10", "CAST('hello' AS CHAR(10))"));
        cases.add(typeFromDummy("types_varchar_100", "CAST('hello, world' AS VARCHAR(100))"));
        cases.add(typeFromDummy("types_varchar_empty", "CAST('' AS VARCHAR(100))"));

        // Dates/times.
        cases.add(typeFromDummy("types_date", "CAST('2026-05-07' AS DATE)"));
        cases.add(typeFromDummy("types_time", "CAST('13:45:09' AS TIME)"));
        cases.add(typeFromDummy("types_timestamp", "CAST('2026-05-07 13:45:09.123456' AS TIMESTAMP)"));

        // NULL of various types in one row.
        cases.add(new SelectStatic("types_null",
                "SELECT CAST(NULL AS INTEGER), CAST(NULL AS DECIMAL(5,2)), "
                        + "CAST(NULL AS VARCHAR(10)), CAST(NULL AS TIMESTAMP) "
                        + "FROM SYSIBM.SYSDUMMY1"));

        // Multi-column row.
        cases.add(new SelectStatic("select_multi_column",
                "SELECT 1 AS A, CAST('two' AS VARCHAR(10)) AS B, "
                        + "CAST(3.14 AS DECIMAL(5,2)) AS C, CURRENT_DATE AS D "
                        + "FROM SYSIBM.SYSDUMMY1"));

        // Prepared statements with parameters.
        cases.add(new PreparedInt());
        cases.add(new PreparedString());
        cases.add(new PreparedDecimal());

        // LOB bind via parameter markers — exercises locator allocation,
        // WRITE_LOB_DATA, and the EXECUTE SQLDA carrying the 4-byte
        // locator handle in the LOB slot. Two sizes: one below the
        // single-frame WRITE_LOB_DATA limit, one above it (forces
        // chunked upload).
        cases.add(new PreparedBlobInsert(schema));
        cases.add(new PreparedBlobInsertLarge(schema));
        cases.add(new PreparedBlobBatch(schema));
        cases.add(new PreparedBlobThreshold(schema));
        cases.add(new PreparedBinaryBind(schema));

        // Block fetch — needs a real table.
        cases.add(new MultiRowFetch(schema));

        // Transactions.
        cases.add(new TxCommit(schema));
        cases.add(new TxRollback(schema));

        // Stored procedures (M9). Each case targets a dedicated GOSPROCS
        // library under the test user; WithStoredProcs.setup() bootstraps
        // the schema + procedures idempotently, so the very first capture
        // run also doubles as live evidence that JT400 can create + call
        // the procs against the LPAR.
        cases.add(new CallInOnly());
        cases.add(new CallInOut());
        cases.add(new CallResultSet());
        cases.add(new CallMultiSet());
        cases.add(new CallInout());

        // Extended-dynamic-package caching (M10). Three captures cover
        // the cache lifecycle: first PREPARE writing to the *PGM,
        // in-session cache hit skipping PREPARE, and a fresh-connection
        // RETURN_PACKAGE download of a pre-populated package. Each case
        // clears the package in setUp via DLTPGM so re-captures are
        // deterministic.
        cases.add(new PackageFirstUse(schema));
        cases.add(new PackageCacheHit(schema));
        cases.add(new PackageCacheDownload(schema));

        // Negative paths — SQLException to SQLCARD parsing.
        cases.add(new ErrorSyntax());
        cases.add(new ErrorTableNotFound());

        return cases;
    }

    // ---- Helpers ----

    private static Case typeFromDummy(String name, String expr) {
        return new SelectStatic(name, "SELECT " + expr + " FROM SYSIBM.SYSDUMMY1");
    }

    // ---- Cases ----

    private static final class ConnectOnly extends Case {
        ConnectOnly() { super("connect_only"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            // Just touch the connection so we get a guaranteed handshake-only fixture.
            golden.recordNote("autoCommit", String.valueOf(conn.getAutoCommit()));
            golden.recordNote("catalog", String.valueOf(conn.getCatalog()));
            golden.recordNote("readOnly", String.valueOf(conn.isReadOnly()));
        }
    }

    private static final class SelectDummy extends Case {
        SelectDummy() { super("select_dummy"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")) {
                golden.recordResultSet(rs);
            }
        }
    }

    private static class SelectStatic extends Case {
        private final String sql;
        SelectStatic(String name, String sql) {
            super(name);
            this.sql = sql;
        }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                golden.recordResultSet(rs);
            }
        }
    }

    private static final class PreparedInt extends Case {
        PreparedInt() { super("prepared_int_param"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT CAST(? AS INTEGER) AS V FROM SYSIBM.SYSDUMMY1")) {
                ps.setInt(1, 42);
                try (ResultSet rs = ps.executeQuery()) {
                    golden.recordResultSet(rs);
                }
            }
        }
    }

    private static final class PreparedString extends Case {
        PreparedString() { super("prepared_string_param"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT CAST(? AS VARCHAR(50)) AS V FROM SYSIBM.SYSDUMMY1")) {
                ps.setString(1, "hello, IBM i");
                try (ResultSet rs = ps.executeQuery()) {
                    golden.recordResultSet(rs);
                }
            }
        }
    }

    private static final class PreparedDecimal extends Case {
        PreparedDecimal() { super("prepared_decimal_param"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT CAST(? AS DECIMAL(7,3)) AS V FROM SYSIBM.SYSDUMMY1")) {
                ps.setBigDecimal(1, new java.math.BigDecimal("-1234.567"));
                try (ResultSet rs = ps.executeQuery()) {
                    golden.recordResultSet(rs);
                }
            }
        }
    }

    /**
     * Shared lifecycle for LOB-bind cases. The table has one INTEGER
     * primary-key column plus a BLOB and CLOB column sized large
     * enough for both the small (~8 KB) and large (≥64 KB) fixture
     * cases. setup() drops any prior incarnation and recreates so
     * each run produces a deterministic trace from a clean table.
     */
    private static abstract class WithLobTable extends Case {
        // 8-char SQL/system name; no truncation issue.
        private static final String TABLE_SHORT = "GOJT_LOB";

        protected final String schema;
        protected final String table;

        WithLobTable(String name, String schema) {
            super(name);
            this.schema = schema;
            this.table = schema + "." + TABLE_SHORT;
        }

        @Override public void setup(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
                st.execute("CREATE TABLE " + table + " ("
                        + "ID INTEGER NOT NULL PRIMARY KEY, "
                        + "B BLOB(1M), "
                        + "C CLOB(1M))");
            }
        }

        @Override public void teardown(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
            }
        }
    }

    private static final class PreparedBlobInsert extends WithLobTable {
        PreparedBlobInsert(String schema) { super("prepared_blob_insert", schema); }

        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            // 8 KB BLOB: deterministic byte ramp so byte-equality is
            // easy to assert from the SELECT-back side.
            byte[] blob = new byte[8 * 1024];
            for (int i = 0; i < blob.length; i++) {
                blob[i] = (byte) (i & 0xFF);
            }
            // ~8 KB CLOB so we cover the EBCDIC/CCSID transcoding path
            // without crossing the chunk boundary the large case
            // exercises.
            StringBuilder sb = new StringBuilder(8 * 1024);
            String unit = "Hello, IBM i! ";
            while (sb.length() < 8 * 1024) sb.append(unit);
            String clob = sb.toString();

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (ID, B, C) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, blob);
                ps.setString(3, clob);
                int n = ps.executeUpdate();
                golden.recordUpdateCount(n);
            }
            // Round-trip read: confirms the row landed and pins the
            // SELECT-side bytes for the golden.
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT B, C FROM " + table + " WHERE ID = 1");
                 ResultSet rs = ps.executeQuery()) {
                golden.recordResultSet(rs);
            }
        }
    }

    private static final class PreparedBlobInsertLarge extends WithLobTable {
        PreparedBlobInsertLarge(String schema) { super("prepared_blob_insert_large", schema); }

        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            // 64 KB BLOB: above JT400's single-frame WRITE_LOB_DATA
            // limit (~32 KB), so the trace must show multiple
            // WRITE_LOB_DATA frames at advancing offsets — that's the
            // chunking encoding we need to mirror in the Go encoder.
            byte[] blob = new byte[64 * 1024];
            for (int i = 0; i < blob.length; i++) {
                blob[i] = (byte) ((i * 31) & 0xFF);
            }
            // CLOB stays small here so the BLOB chunking is the only
            // long write in the trace; CLOB chunking can be a follow-up
            // case if the encoder needs separate validation.
            String clob = "small clob";

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (ID, B, C) VALUES (?, ?, ?)")) {
                ps.setInt(1, 2);
                ps.setBytes(2, blob);
                ps.setString(3, clob);
                int n = ps.executeUpdate();
                golden.recordUpdateCount(n);
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT B, C FROM " + table + " WHERE ID = 2");
                 ResultSet rs = ps.executeQuery()) {
                golden.recordResultSet(rs);
            }
        }
    }

    // PreparedBlobBatch captures JT400's wire pattern for batched
    // LOB-column INSERTs: addBatch() five times then executeBatch().
    // The open question (docs/lob-known-gaps.md §2) is whether JT400
    // emits one multi-row EXECUTE with CP 0x381F RowCount=5 or
    // five single-row EXECUTEs in sequence. The trace settles it.
    private static final class PreparedBlobBatch extends WithLobTable {
        PreparedBlobBatch(String schema) { super("prepared_blob_batch", schema); }

        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (ID, B, C) VALUES (?, ?, ?)")) {
                for (int row = 1; row <= 5; row++) {
                    // Each row gets a small distinct BLOB and CLOB so
                    // a multi-row EXECUTE (if that's what JT400 emits)
                    // can be distinguished from N single-row EXECUTEs
                    // by the wire bytes.
                    byte[] blob = new byte[16];
                    for (int i = 0; i < blob.length; i++) blob[i] = (byte) ((row * 17 + i) & 0xFF);
                    String clob = "row=" + row + " clob";
                    ps.setInt(1, row);
                    ps.setBytes(2, blob);
                    ps.setString(3, clob);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                int total = 0;
                for (int c : counts) total += (c >= 0 ? c : 1); // SUCCESS_NO_INFO -> count 1
                golden.recordUpdateCount(total);
            }
            // Confirm the rows are queryable so the trace also captures
            // the SELECT-back path used by goJTOpen tests.
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT ID, B, C FROM " + table + " ORDER BY ID");
                 ResultSet rs = ps.executeQuery()) {
                golden.recordResultSet(rs);
            }
        }
    }

    // PreparedBlobThreshold captures the wire pattern when JT400's
    // "lob threshold" connection property is set and the bound LOB
    // payload is *below* that threshold. The open question
    // (docs/lob-known-gaps.md §3): does JT400 skip locator allocation
    // entirely (no WRITE_LOB_DATA frames) and inline the LOB bytes in
    // the EXECUTE SQLDA as VARCHAR FOR BIT DATA? If so, the goJTOpen
    // bind side can mirror it to avoid the ~2-RTT locator overhead on
    // small LOB inserts.
    //
    // Pairs with the read-side regression bug #14: a CLOB <= 32 KB
    // declared with an explicit CCSID (e.g. CLOB(4K) CCSID 1208)
    // returns zero rows from SELECT because the server ships the
    // small payload inline as VARCHAR-shaped data and goJTOpen's
    // result-data parser only recognises the locator shape. The
    // trace from this case settles both ends of the wire heuristic.
    private static final class PreparedBlobThreshold extends Case {
        private static final String TABLE_SHORT = "GOJT_LOBT";
        private final String schema;
        private final String table;

        PreparedBlobThreshold(String schema) {
            super("prepared_blob_threshold");
            this.schema = schema;
            this.table = schema + "." + TABLE_SHORT;
        }

        @Override public java.util.Map<String, String> extraConnectionProperties() {
            // "lob threshold" is JT400's per-connection inline-LOB
            // knob; payloads at or below this byte count get inlined
            // into the EXECUTE SQLDA instead of going through the
            // locator + WRITE_LOB_DATA round trip.
            return java.util.Collections.singletonMap("lob threshold", "32768");
        }

        @Override public void setup(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
                // BLOB(64K) for the bind-side (M7-5) path; CLOB(4K)
                // CCSID 1208 for the read-side small-CLOB-inline path
                // that's currently regressing (bug #14). Both are
                // below 32 KB so the inline-threshold heuristic
                // applies symmetrically on bind and read.
                st.execute("CREATE TABLE " + table + " ("
                        + "ID INTEGER NOT NULL PRIMARY KEY, "
                        + "B BLOB(64K), "
                        + "C CLOB(4K) CCSID 1208)");
            }
        }

        @Override public void teardown(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
            }
        }

        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            // 32-byte BLOB: deeply below the 32768 threshold so JT400
            // is guaranteed to take the inline path if it has one.
            byte[] blob = new byte[32];
            for (int i = 0; i < blob.length; i++) {
                blob[i] = (byte) (i & 0xFF);
            }
            // ~256-byte CLOB so the small-CLOB read regression is in
            // play on the SELECT-back side. ASCII content keeps the
            // CCSID 1208 encode trivially comparable.
            String clob = "hello-lob-threshold-CLOB CCSID 1208 inline-return case " +
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" +
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" +
                    "0123456789abcdef0123456789abcdef";

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (ID, B, C) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, blob);
                ps.setString(3, clob);
                int n = ps.executeUpdate();
                golden.recordUpdateCount(n);
            }
            // Round-trip read pins the SELECT-side wire for bug #14.
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT B, C FROM " + table + " WHERE ID = 1");
                 ResultSet rs = ps.executeQuery()) {
                golden.recordResultSet(rs);
            }
        }
    }

    // PreparedBinaryBind captures JT400's wire pattern for the three
    // binary-flavoured CHAR/VARCHAR variants on V7R3+: CHAR FOR BIT
    // DATA (SQL types 452/453 + CCSID 65535), the native BINARY type
    // (912/913), and the native VARBINARY type (908/909). Used to
    // pin the type-dispatch byte-equality on the read side and
    // document JT400's parameter-marker shape for binary binds.
    //
    // Settles the M4 "deferred: CCSID 65535 binary" gap from
    // docs/PLAN.md -- the decoder already routes CCSID 65535 to
    // []byte but had no captured fixture exercising the path.
    private static final class PreparedBinaryBind extends Case {
        private static final String TABLE_SHORT = "GOJT_BIN";
        private final String schema;
        private final String table;

        PreparedBinaryBind(String schema) {
            super("prepared_binary_bind");
            this.schema = schema;
            this.table = schema + "." + TABLE_SHORT;
        }

        @Override public void setup(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
                st.execute("CREATE TABLE " + table + " ("
                        + "ID INTEGER NOT NULL PRIMARY KEY, "
                        + "C CHAR(8) FOR BIT DATA, "
                        + "B BINARY(8), "
                        + "V VARBINARY(32))");
            }
        }

        @Override public void teardown(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
            }
        }

        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            // Deterministic content so the read-back golden is
            // byte-stable: ascending bytes for CHAR FOR BIT DATA, a
            // recognisable hex fill for BINARY, and a "DEADBEEF
            // CAFEBABE" pattern for VARBINARY.
            byte[] charBin = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
            byte[] binFixed = {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD,
                    (byte) 0xEE, (byte) 0xFF, 0x00, 0x11};
            byte[] varBin = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF,
                    (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (ID, C, B, V) VALUES (?, ?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setBytes(2, charBin);
                ps.setBytes(3, binFixed);
                ps.setBytes(4, varBin);
                int n = ps.executeUpdate();
                golden.recordUpdateCount(n);
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT C, B, V FROM " + table + " WHERE ID = 1");
                 ResultSet rs = ps.executeQuery()) {
                golden.recordResultSet(rs);
            }
        }
    }

    private static abstract class WithTable extends Case {
        // 10-char SQL name == 10-char IBM i system name, so we avoid the
        // suffix-mangling that turned GODB2I_T1 into GOJTO00001 and made
        // the system commands below fragile.
        private static final String TABLE_SHORT = "GOJT_T1";

        // Per-command timeout for the QCMDEXC journal calls. Without
        // this, a CPF9803-style "object locked" condition would hang the
        // JDBC socket indefinitely (we saw 10+ minute stalls on the
        // first diagnostic run). The connection-level "socket timeout"
        // is the actual hard backstop (it covers PREPARE too, which
        // setQueryTimeout doesn't), but we keep this as a polite extra
        // bound for execute-phase hangs.
        private static final int CMD_TIMEOUT_SEC = 15;

        protected final String schema;
        protected final String table;

        // Whether this case needs commitment control. Subclasses that do
        // (TxCommit / TxRollback) opt in by returning true, which
        // enables the journal-bring-up path in setup.
        protected boolean needsJournaling() { return false; }

        WithTable(String name, String schema) {
            super(name);
            this.schema = schema;
            this.table = schema + "." + TABLE_SHORT;
        }
        @Override public void setup(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
                st.execute("CREATE TABLE " + table + " ("
                        + "ID INTEGER NOT NULL PRIMARY KEY, "
                        + "NAME VARCHAR(40) NOT NULL, "
                        + "AMT DECIMAL(11,2) NOT NULL"
                        + ")");
                if (needsJournaling()) {
                    bringUpJournal(st);
                }
                seed(conn);
            }
        }

        // Stand up a fresh per-case journal + receiver. We do NOT try
        // to delete prior artifacts: PUB400 has been observed to keep
        // GOJTRCV* receivers in CPF9803 ("Cannot allocate") for hours
        // after they're orphaned, and DLTJRNRCV against a stuck object
        // hangs CRTJRN behind it. By picking a fresh suffix each run
        // we always operate on a clean slate, at the cost of leaving
        // an orphaned receiver behind in the user's library each time.
        // Periodic cleanup of GOJTR* objects is the operator's job.
        //
        // The receiver name is bounded to 10 chars (system limit):
        // "GOJTR" prefix (5) + 5-char hex from nanoTime gives 10.
        private void bringUpJournal(Statement st) {
            String suffix = String.format("%05X", System.nanoTime() & 0xFFFFF);
            String jrnRcv = "GOJTR" + suffix;
            String jrn = "GOJTJ" + suffix;
            runOrLog(st, "CRTJRNRCV", "CALL QSYS2.QCMDEXC('CRTJRNRCV JRNRCV("
                    + schema + "/" + jrnRcv + ")')");
            runOrLog(st, "CRTJRN", "CALL QSYS2.QCMDEXC('CRTJRN JRN("
                    + schema + "/" + jrn + ") JRNRCV("
                    + schema + "/" + jrnRcv + ")')");
            runOrLog(st, "STRJRNPF", "CALL QSYS2.QCMDEXC('STRJRNPF FILE("
                    + schema + "/" + TABLE_SHORT + ") JRN("
                    + schema + "/" + jrn + ") IMAGES(*BOTH))')");
        }

        @Override public void teardown(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                // Best-effort: ENDJRNPF before DROP only for the cases
                // that started journaling -- otherwise we'd log a
                // spurious CPF0006 in run.log on every non-journaled
                // run. ENDJRN/DLTJRN/DLTJRNRCV are skipped for the
                // same reason setup() doesn't try to clean prior
                // runs -- PUB400 holds these objects open and we'd
                // just hang.
                if (needsJournaling()) {
                    runOrLog(st, "ENDJRNPF", "CALL QSYS2.QCMDEXC('ENDJRNPF FILE("
                            + schema + "/" + TABLE_SHORT + "))')");
                }
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
            }
        }
        protected void seed(Connection conn) throws SQLException { }

        private void runOrLog(Statement st, String label, String sql) {
            try {
                st.setQueryTimeout(CMD_TIMEOUT_SEC);
                st.execute(sql);
            } catch (java.sql.SQLTimeoutException e) {
                System.err.println("    [" + name + "] " + label
                        + ": TIMEOUT after " + CMD_TIMEOUT_SEC + "s ("
                        + e.getMessage() + ")");
            } catch (SQLException e) {
                System.err.println("    [" + name + "] " + label + ": " + e.getMessage());
            }
        }
    }

    private static final class MultiRowFetch extends WithTable {
        MultiRowFetch(String schema) { super("multi_row_fetch_1k", schema); }
        @Override protected void seed(Connection conn) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + table + " (ID, NAME, AMT) VALUES (?, ?, ?)")) {
                for (int i = 1; i <= 1000; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "row-" + i);
                    ps.setBigDecimal(3, new java.math.BigDecimal(i + ".23"));
                    ps.addBatch();
                    if (i % 250 == 0) ps.executeBatch();
                }
                ps.executeBatch();
            }
        }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT ID, NAME, AMT FROM " + table + " ORDER BY ID")) {
                golden.recordResultSet(rs);
            }
        }
    }

    private static final class TxCommit extends WithTable {
        TxCommit(String schema) { super("tx_commit", schema); }
        @Override protected boolean needsJournaling() { return true; }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + table + " (ID, NAME, AMT) VALUES (?, ?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setString(2, "committed");
                    ps.setBigDecimal(3, new java.math.BigDecimal("1.00"));
                    int n = ps.executeUpdate();
                    golden.recordUpdateCount(n);
                }
                conn.commit();
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT ID, NAME, AMT FROM " + table)) {
                    golden.recordResultSet(rs);
                }
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private static final class TxRollback extends WithTable {
        TxRollback(String schema) { super("tx_rollback", schema); }
        @Override protected boolean needsJournaling() { return true; }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + table + " (ID, NAME, AMT) VALUES (?, ?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setString(2, "rolled-back");
                    ps.setBigDecimal(3, new java.math.BigDecimal("1.00"));
                    int n = ps.executeUpdate();
                    golden.recordUpdateCount(n);
                }
                conn.rollback();
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT ID, NAME, AMT FROM " + table)) {
                    golden.recordResultSet(rs);
                }
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * Bootstraps the {@code GOSPROCS} library, supporting tables, and the
     * four stored procedures exercised by the M9 fixtures. Idempotent:
     * tables are dropped + recreated for deterministic seed data; procedures
     * use {@code CREATE OR REPLACE} (V7R2+) so re-runs reset the bodies.
     *
     * The procs:
     * <ul>
     *   <li>{@code P_INS(IN code VARCHAR(10), IN qty INTEGER)} — IN-only,
     *       inserts to {@code INS_AUDIT}. No result sets.</li>
     *   <li>{@code P_LOOKUP(IN code VARCHAR(10), OUT name VARCHAR(64), OUT qty INTEGER)} —
     *       IN + two OUT scalars, SELECT INTO from {@code WIDGETS}.</li>
     *   <li>{@code P_INVENTORY(IN min_qty INTEGER)} — DYNAMIC RESULT SETS 2;
     *       opens two cursors against {@code INVENTORY} (below / at-or-above
     *       the threshold).</li>
     *   <li>{@code P_ROUNDTRIP(INOUT counter INTEGER)} — INOUT scalar
     *       incremented by one.</li>
     * </ul>
     */
    private static abstract class WithStoredProcs extends Case {
        protected static final String LIBRARY = "GOSPROCS";

        WithStoredProcs(String name) { super(name); }

        @Override public void setup(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                // CREATE SCHEMA — ignore SQLSTATE 42710 (schema already
                // exists). DB2 for i has no CREATE SCHEMA IF NOT EXISTS.
                try {
                    st.execute("CREATE SCHEMA " + LIBRARY);
                } catch (SQLException e) {
                    if (!"42710".equals(e.getSQLState())) {
                        throw e;
                    }
                }

                // Supporting tables: drop + recreate every run for a
                // deterministic seed.
                for (String tbl : new String[]{"INS_AUDIT", "WIDGETS", "INVENTORY"}) {
                    try {
                        st.execute("DROP TABLE " + LIBRARY + "." + tbl);
                    } catch (SQLException ignored) { }
                }
                st.execute("CREATE TABLE " + LIBRARY + ".INS_AUDIT ("
                        + "CODE VARCHAR(10), QTY INTEGER, "
                        + "INSERTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
                st.execute("CREATE TABLE " + LIBRARY + ".WIDGETS ("
                        + "CODE VARCHAR(10) NOT NULL PRIMARY KEY, "
                        + "NAME VARCHAR(64), QTY INTEGER)");
                st.execute("INSERT INTO " + LIBRARY + ".WIDGETS "
                        + "VALUES ('WIDGET', 'Acme Widget', 100)");
                st.execute("INSERT INTO " + LIBRARY + ".WIDGETS "
                        + "VALUES ('GADGET', 'Acme Gadget', 5)");
                st.execute("CREATE TABLE " + LIBRARY + ".INVENTORY ("
                        + "CODE VARCHAR(10), QTY INTEGER, LOCATION VARCHAR(20))");
                st.execute("INSERT INTO " + LIBRARY + ".INVENTORY "
                        + "VALUES ('LOW1', 2, 'A1')");
                st.execute("INSERT INTO " + LIBRARY + ".INVENTORY "
                        + "VALUES ('LOW2', 3, 'A2')");
                st.execute("INSERT INTO " + LIBRARY + ".INVENTORY "
                        + "VALUES ('HIGH1', 50, 'B1')");
                st.execute("INSERT INTO " + LIBRARY + ".INVENTORY "
                        + "VALUES ('HIGH2', 100, 'B2')");

                // Procedures via CREATE OR REPLACE — re-runnable, no
                // overload disambiguation needed since each name is unique.
                st.execute("CREATE OR REPLACE PROCEDURE " + LIBRARY + ".P_INS "
                        + "(IN P_CODE VARCHAR(10), IN P_QTY INTEGER) "
                        + "LANGUAGE SQL "
                        + "BEGIN "
                        + "INSERT INTO " + LIBRARY + ".INS_AUDIT (CODE, QTY) "
                        + "VALUES (P_CODE, P_QTY); "
                        + "END");
                st.execute("CREATE OR REPLACE PROCEDURE " + LIBRARY + ".P_LOOKUP "
                        + "(IN P_CODE VARCHAR(10), OUT P_NAME VARCHAR(64), "
                        + " OUT P_QTY INTEGER) "
                        + "LANGUAGE SQL "
                        + "BEGIN "
                        + "SELECT NAME, QTY INTO P_NAME, P_QTY "
                        + "FROM " + LIBRARY + ".WIDGETS WHERE CODE = P_CODE; "
                        + "END");
                st.execute("CREATE OR REPLACE PROCEDURE " + LIBRARY + ".P_INVENTORY "
                        + "(IN P_MIN_QTY INTEGER) "
                        + "DYNAMIC RESULT SETS 2 "
                        + "LANGUAGE SQL "
                        + "BEGIN "
                        + "DECLARE C1 CURSOR WITH RETURN FOR "
                        + "SELECT CODE, QTY FROM " + LIBRARY + ".INVENTORY "
                        + "WHERE QTY < P_MIN_QTY ORDER BY CODE; "
                        + "DECLARE C2 CURSOR WITH RETURN FOR "
                        + "SELECT CODE, QTY FROM " + LIBRARY + ".INVENTORY "
                        + "WHERE QTY >= P_MIN_QTY ORDER BY CODE; "
                        + "OPEN C1; "
                        + "OPEN C2; "
                        + "END");
                st.execute("CREATE OR REPLACE PROCEDURE " + LIBRARY + ".P_ROUNDTRIP "
                        + "(INOUT P_COUNTER INTEGER) "
                        + "LANGUAGE SQL "
                        + "BEGIN "
                        + "SET P_COUNTER = P_COUNTER + 1; "
                        + "END");
            }
        }

        // Teardown leaves the GOSPROCS library in place across cases;
        // each setup() resets state idempotently. This keeps subsequent
        // captures fast (no library recreate) while still being
        // re-runnable from a clean LPAR.
    }

    /**
     * {@code prepared_call_in_only.trace} — {@code CALL GOSPROCS.P_INS('A', 10)}
     * with literal arguments. Exercises JT400's TYPE_CALL routing
     * (statement type 3) without parameter markers, so the captured wire
     * shape covers PREPARE+EXECUTE with no CHANGE_DESCRIPTOR.
     */
    private static final class CallInOnly extends WithStoredProcs {
        CallInOnly() { super("prepared_call_in_only"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (CallableStatement cs = conn.prepareCall(
                    "CALL " + LIBRARY + ".P_INS('A', 10)")) {
                int n = cs.executeUpdate();
                golden.recordUpdateCount(n);
            }
        }
    }

    /**
     * {@code prepared_call_in_out.trace} — {@code CALL GOSPROCS.P_LOOKUP(?, ?, ?)}
     * with one IN string and two OUT registrations. The EXECUTE reply
     * carries a synthetic single-row result-data CP whose row matches the
     * parameter-marker descriptor; JT400 decodes it via
     * {@code parameterRow_.setServerData()} (AS400JDBCPreparedStatementImpl.java:722-729).
     */
    private static final class CallInOut extends WithStoredProcs {
        CallInOut() { super("prepared_call_in_out"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (CallableStatement cs = conn.prepareCall(
                    "CALL " + LIBRARY + ".P_LOOKUP(?, ?, ?)")) {
                cs.setString(1, "WIDGET");
                cs.registerOutParameter(2, Types.VARCHAR);
                cs.registerOutParameter(3, Types.INTEGER);
                cs.execute();
                golden.recordOutParam(2, Types.VARCHAR, cs.getString(2));
                golden.recordOutParam(3, Types.INTEGER, Integer.valueOf(cs.getInt(3)));
            }
        }
    }

    /**
     * {@code prepared_call_result_set.trace} — {@code CALL GOSPROCS.P_INVENTORY(5)}
     * draining only the first dynamic result set. Used to pin the
     * single-result-set CALL path (M9-1 / M9-3 path A) before M9-3 adds
     * NextResultSet.
     */
    private static final class CallResultSet extends WithStoredProcs {
        CallResultSet() { super("prepared_call_result_set"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (CallableStatement cs = conn.prepareCall(
                    "CALL " + LIBRARY + ".P_INVENTORY(?)")) {
                cs.setInt(1, 5);
                cs.execute();
                try (ResultSet rs = cs.getResultSet()) {
                    if (rs != null) golden.recordResultSet(rs);
                }
            }
        }
    }

    /**
     * {@code prepared_call_multi_set.trace} — same proc, both dynamic
     * result sets drained via {@code getMoreResults()}. Captures the
     * advance path: JT400 closes the prior cursor and issues a fresh
     * OPEN_DESCRIBE (function id 0x180E) on the same statement
     * (AS400JDBCStatement.java:3406-3470). {@code numberOfResults_} is
     * sourced from {@code firstSqlca.getErrd(2)} on the original PREPARE
     * reply.
     */
    private static final class CallMultiSet extends WithStoredProcs {
        CallMultiSet() { super("prepared_call_multi_set"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (CallableStatement cs = conn.prepareCall(
                    "CALL " + LIBRARY + ".P_INVENTORY(?)")) {
                cs.setInt(1, 5);
                cs.execute();
                try (ResultSet rs = cs.getResultSet()) {
                    if (rs != null) golden.recordResultSet(rs);
                }
                if (cs.getMoreResults()) {
                    try (ResultSet rs = cs.getResultSet()) {
                        if (rs != null) golden.recordResultSet(rs);
                    }
                }
            }
        }
    }

    /**
     * {@code prepared_call_inout.trace} — {@code CALL GOSPROCS.P_ROUNDTRIP(?)}
     * with one INOUT INTEGER. Direction byte 0xF2 lands at descriptor
     * offset+30 (DBExtendedDataFormat.java:300-302); the parameter ships
     * BOTH a bind value (the IN side, 5) AND comes back with the OUT
     * value (6) in the EXECUTE reply's result-data CP.
     */
    private static final class CallInout extends WithStoredProcs {
        CallInout() { super("prepared_call_inout"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (CallableStatement cs = conn.prepareCall(
                    "CALL " + LIBRARY + ".P_ROUNDTRIP(?)")) {
                cs.setInt(1, 5);
                cs.registerOutParameter(1, Types.INTEGER);
                cs.execute();
                golden.recordOutParam(1, Types.INTEGER, Integer.valueOf(cs.getInt(1)));
            }
        }
    }

    /**
     * Base class for M10 extended-dynamic-package fixtures. Every case
     * shares the same package name + library + base SQL; per-case
     * {@link #extraConnectionProperties()} overrides toggle whether the
     * traced connection downloads the package on connect
     * ({@code package cache=true}).
     *
     * The plan calls for the package to persist across cases: first_use
     * starts fresh and populates {@code GOTEST.GOJTPK<suffix>}, and the
     * later cache_hit / cache_download cases observe the pre-warmed
     * package without running their own DDL. So setup() here is a
     * no-op by default; only {@link PackageFirstUse} overrides to wipe
     * any prior package state.
     */
    private static abstract class WithPackage extends Case {
        protected static final String PACKAGE_NAME = "GOJTPKG";
        // Parameterised SELECT — qualifies for {@code package
        // criteria=default} which only caches statements that have
        // parameter markers. The marker is cast so DB2 has a defined
        // type to project (otherwise SQL0418 — "Use of parameter
        // marker, NULL, or UNKNOWN not valid" — fires during commonPrepare
        // on extended-dynamic connections).
        protected static final String SAMPLE_SQL =
                "SELECT CURRENT_TIMESTAMP, CAST(? AS INTEGER) FROM SYSIBM.SYSDUMMY1";

        protected final String schema;

        WithPackage(String name, String schema) {
            super(name);
            this.schema = schema;
        }

        @Override public Map<String, String> extraConnectionProperties() {
            Map<String, String> p = new HashMap<>();
            p.put("extended dynamic", "true");
            p.put("package", PACKAGE_NAME);
            p.put("package library", schema);
            // Subclasses overriding this should call super and then put
            // their own keys on top.
            return p;
        }
    }

    /**
     * {@code prepared_package_first_use.trace} — wipe any prior package
     * via untraced DLTOBJ, then open a fresh connection that does the
     * extended-dynamic CREATE_PACKAGE + PREPARE_DESCRIBE on the wire.
     * The traced frames MUST carry CP 0x3804 (package name) and CP
     * 0x3805 (package library); the reply MUST eventually carry CP
     * 0x380B with the new entry. After this case the {@code *PGM}
     * exists on the LPAR, populated with {@link #SAMPLE_SQL} and the
     * second SQL used by {@link PackageCacheDownload}, so subsequent
     * cases can observe a pre-warmed package.
     */
    private static final class PackageFirstUse extends WithPackage {
        // Second SQL primed so PackageCacheDownload has at least two
        // entries to assert on. We bake the second prepare into the
        // first_use trace rather than its own setup() so the persisted
        // state across cases is "everything first_use traced", with no
        // hidden untraced side effects.
        private static final String SECOND_SQL =
                "SELECT CURRENT_USER, CAST(? AS VARCHAR(64)) FROM SYSIBM.SYSDUMMY1";

        PackageFirstUse(String schema) { super("prepared_package_first_use", schema); }

        @Override public void setup(Connection conn) throws SQLException {
            // setupConn here has the same {@code extended dynamic=true}
            // extras the traced connection will use. JT400 issues
            // CREATE_PACKAGE on sign-on, so we DLTOBJ AFTER sign-on has
            // completed (any package the sign-on just minted is the
            // one we want to drop). Wildcard pattern wipes every
            // suffix variant in case session options drifted between
            // captures.
            try (Statement st = conn.createStatement()) {
                String cmd = "DLTOBJ OBJ(" + schema + "/" + PACKAGE_NAME + "*) "
                        + "OBJTYPE(*SQLPKG)";
                try {
                    st.execute("CALL QSYS2.QCMDEXC('" + cmd + "')");
                } catch (SQLException ignored) {
                    // CPF2105 (object not found) is the expected first-run
                    // outcome; QCMDEXC wraps it as a SQL error.
                }
            }
        }

        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(SAMPLE_SQL)) {
                ps.setInt(1, 42);
                try (ResultSet rs = ps.executeQuery()) {
                    golden.recordResultSet(rs);
                }
            }
            // Prime the second SQL in the same traced connection so the
            // *PGM has two entries by the time cache_download opens.
            try (PreparedStatement ps = conn.prepareStatement(SECOND_SQL)) {
                ps.setString(1, "hello");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) { /* drain — golden was recorded for SAMPLE_SQL only */ }
                }
            }
        }
    }

    /**
     * {@code prepared_package_cache_hit.trace} — same connection, two
     * back-to-back PREPAREs of the SAME SQL. The first PREPARE warms
     * the client-side statement cache (and round-trips PREPARE_DESCRIBE
     * + CP 0x3804/0x3805 to the server); the second PREPARE MUST hit
     * the cache and NOT emit a 0x1803 frame. Verifies JT400's
     * client-side fast path. Runs after {@link PackageFirstUse} so the
     * server-side package already exists -- the first PREPARE here
     * therefore goes through the EXISTING package, not CREATE_PACKAGE.
     */
    private static final class PackageCacheHit extends WithPackage {
        PackageCacheHit(String schema) { super("prepared_package_cache_hit", schema); }
        @Override public Map<String, String> extraConnectionProperties() {
            Map<String, String> p = super.extraConnectionProperties();
            p.put("package cache", "true");
            return p;
        }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            // First prepare: round-trips to server, primes client cache.
            try (PreparedStatement ps = conn.prepareStatement(SAMPLE_SQL)) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) { /* drain */ }
                }
            }
            // Second prepare of the SAME SQL: must hit the cache. The
            // verifier asserts no 0x1803 PREPARE_DESCRIBE appears in the
            // wire bytes AFTER this point in the trace.
            try (PreparedStatement ps = conn.prepareStatement(SAMPLE_SQL)) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    golden.recordResultSet(rs);
                }
            }
        }
    }

    /**
     * {@code prepared_package_cache_download.trace} — fresh connection
     * with {@code package cache=true} against a package that
     * {@link PackageFirstUse} primed with two entries. Asserts the
     * RETURN_PACKAGE (0x1815) request fires on connect and the reply
     * carries CP 0x380B with TWO statement entries. The execute()
     * phase runs one of the two cached statements so the golden has
     * a baseline result.
     */
    private static final class PackageCacheDownload extends WithPackage {
        private static final String SECOND_SQL =
                "SELECT CURRENT_USER, CAST(? AS VARCHAR(64)) FROM SYSIBM.SYSDUMMY1";

        PackageCacheDownload(String schema) {
            super("prepared_package_cache_download", schema);
        }
        @Override public Map<String, String> extraConnectionProperties() {
            Map<String, String> p = super.extraConnectionProperties();
            p.put("package cache", "true");
            return p;
        }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (PreparedStatement ps = conn.prepareStatement(SECOND_SQL)) {
                ps.setString(1, "hello");
                try (ResultSet rs = ps.executeQuery()) {
                    golden.recordResultSet(rs);
                }
            }
        }
    }

    private static final class ErrorSyntax extends Case {
        ErrorSyntax() { super("error_syntax"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (Statement st = conn.createStatement()) {
                st.executeQuery("SELEKT 1 FROM SYSIBM.SYSDUMMY1"); // intentional typo
            }
        }
    }

    private static final class ErrorTableNotFound extends Case {
        ErrorTableNotFound() { super("error_table_not_found"); }
        @Override public void execute(Connection conn, GoldenWriter golden) throws SQLException {
            try (Statement st = conn.createStatement()) {
                st.executeQuery("SELECT * FROM SYSIBM.GOJTOPEN_NOPE_NOT_HERE");
            }
        }
    }

    private Cases() { }
}
