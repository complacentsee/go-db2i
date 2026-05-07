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

    private static abstract class WithTable extends Case {
        // 10-char SQL name == 10-char IBM i system name, so we avoid the
        // suffix-mangling that turned GOJTOPEN_T1 into GOJTO00001 and made
        // the system commands below fragile.
        private static final String TABLE_SHORT = "GOJT_T1";

        // goJTOpen-specific journal + receiver. Created once in the user's
        // library and reused by all WithTable cases. Names are chosen so
        // it's obvious they belong to this project; the journal is left
        // in place between runs (creating a fresh receiver/journal each
        // time would just churn the system's catalog).
        private static final String JRN = "GOJTJRN";
        private static final String JRNRCV = "GOJTRCV1";

        protected final String schema;
        protected final String table;

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
                // Enable journaling so commitment-control cases work.
                // CRTJRNRCV / CRTJRN may have already happened on a prior
                // run -- "already exists" is fine, anything else gets
                // surfaced to stderr so the user can see what's going on.
                runOrLog(st, "CRTJRNRCV", "CALL QSYS2.QCMDEXC('CRTJRNRCV JRNRCV("
                        + schema + "/" + JRNRCV + ")')");
                runOrLog(st, "CRTJRN", "CALL QSYS2.QCMDEXC('CRTJRN JRN("
                        + schema + "/" + JRN + ") JRNRCV("
                        + schema + "/" + JRNRCV + ")')");
                runOrLog(st, "STRJRNPF", "CALL QSYS2.QCMDEXC('STRJRNPF FILE("
                        + schema + "/" + TABLE_SHORT + ") JRN("
                        + schema + "/" + JRN + ") IMAGES(*BOTH))')");
                seed(conn);
            }
        }
        @Override public void teardown(Connection conn) throws SQLException {
            try (Statement st = conn.createStatement()) {
                // ENDJRNPF before DROP so the table can be deleted cleanly.
                runOrLog(st, "ENDJRNPF", "CALL QSYS2.QCMDEXC('ENDJRNPF FILE("
                        + schema + "/" + TABLE_SHORT + "))')");
                try { st.execute("DROP TABLE " + table); } catch (SQLException ignored) { }
            }
        }
        protected void seed(Connection conn) throws SQLException { }

        private void runOrLog(Statement st, String label, String sql) {
            try {
                st.execute(sql);
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
