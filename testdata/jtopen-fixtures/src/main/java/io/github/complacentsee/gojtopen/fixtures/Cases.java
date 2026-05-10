package io.github.complacentsee.gojtopen.fixtures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Declares every fixture case the harness will capture.
 *
 * Cases that need only system tables target {@code SYSIBM.SYSDUMMY1} so the
 * fixtures are reproducible across PUB400 accounts. Cases that need a real
 * table create {@code <schema>.GOJTOPEN_T1} in setup and drop it in teardown.
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
        // suffix-mangling that turned GOJTOPEN_T1 into GOJTO00001 and made
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
