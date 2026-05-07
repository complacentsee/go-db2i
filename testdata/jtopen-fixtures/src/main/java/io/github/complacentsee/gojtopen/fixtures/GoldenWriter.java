package io.github.complacentsee.gojtopen.fixtures;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;

/**
 * Builds a deterministic JSON document describing the observable result of a
 * fixture case (column metadata, every row's typed values, any SQLException),
 * then writes it to {@code <name>.golden.json}.
 *
 * Goal: the Go driver, after replaying the matching {@code .trace}, produces
 * a structurally identical document.
 */
public class GoldenWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private final String caseName;
    private final ObjectNode root;
    private final ArrayNode resultSets;

    public GoldenWriter(String caseName) {
        this.caseName = caseName;
        this.root = MAPPER.createObjectNode();
        this.root.put("case", caseName);
        this.resultSets = root.putArray("resultSets");
    }

    public void recordResultSet(ResultSet rs) throws SQLException {
        ObjectNode rsNode = resultSets.addObject();
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();

        ArrayNode cols = rsNode.putArray("columns");
        for (int i = 1; i <= n; i++) {
            ObjectNode col = cols.addObject();
            col.put("name", md.getColumnName(i));
            col.put("label", md.getColumnLabel(i));
            col.put("typeName", md.getColumnTypeName(i));
            col.put("sqlType", md.getColumnType(i));
            col.put("displaySize", md.getColumnDisplaySize(i));
            col.put("precision", md.getPrecision(i));
            col.put("scale", md.getScale(i));
            col.put("nullable", md.isNullable(i));
            col.put("signed", md.isSigned(i));
            col.put("schema", md.getSchemaName(i));
            col.put("table", md.getTableName(i));
        }

        ArrayNode rows = rsNode.putArray("rows");
        while (rs.next()) {
            ArrayNode row = rows.addArray();
            for (int i = 1; i <= n; i++) {
                appendValue(row, rs, i, md.getColumnType(i), md.getColumnTypeName(i));
            }
        }
    }

    public void recordUpdateCount(int count) {
        root.put("updateCount", count);
    }

    public void recordError(SQLException e) {
        ObjectNode err = root.putObject("error");
        err.put("sqlState", e.getSQLState());
        err.put("errorCode", e.getErrorCode());
        err.put("message", e.getMessage());
    }

    public void recordNote(String key, String value) {
        if (!root.has("notes")) {
            root.putObject("notes");
        }
        ((ObjectNode) root.get("notes")).put(key, value);
    }

    public void writeTo(Path fixturesDir) throws IOException {
        Path out = fixturesDir.resolve(caseName + ".golden.json");
        Files.write(out, MAPPER.writeValueAsBytes(root));
    }

    private static void appendValue(ArrayNode arr, ResultSet rs, int col, int sqlType, String typeName) throws SQLException {
        // Read raw first so we can detect NULL without coercion noise.
        Object raw = rs.getObject(col);
        if (rs.wasNull() || raw == null) {
            arr.addNull();
            return;
        }
        // JDBC has no Types constant for DECFLOAT (IEEE 754-2008 decimal);
        // it surfaces as Types.OTHER. Identify by typeName so the value
        // round-trips as a canonical string (covers Infinity / NaN / -0
        // that BigDecimal can't represent).
        if ("DECFLOAT".equals(typeName)) {
            arr.add(rs.getString(col));
            return;
        }
        switch (sqlType) {
            case Types.SMALLINT:
            case Types.INTEGER:
                arr.add(rs.getInt(col));
                break;
            case Types.BIGINT:
                arr.add(rs.getLong(col));
                break;
            case Types.REAL:
                arr.add(rs.getFloat(col));
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                arr.add(rs.getDouble(col));
                break;
            case Types.DECIMAL:
            case Types.NUMERIC: {
                BigDecimal bd = rs.getBigDecimal(col);
                // Preserve precision/scale via string.
                arr.add(bd != null ? bd.toPlainString() : null);
                break;
            }
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                arr.add(rs.getString(col));
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                arr.add(toHex(rs.getBytes(col)));
                break;
            case Types.DATE: {
                Date d = rs.getDate(col);
                arr.add(d != null ? d.toLocalDate().toString() : null);
                break;
            }
            case Types.TIME: {
                Time t = rs.getTime(col);
                arr.add(t != null ? t.toLocalTime().toString() : null);
                break;
            }
            case Types.TIMESTAMP: {
                Timestamp ts = rs.getTimestamp(col);
                arr.add(ts != null ? ts.toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : null);
                break;
            }
            case Types.BOOLEAN:
            case Types.BIT:
                arr.add(rs.getBoolean(col));
                break;
            case Types.BLOB: {
                byte[] bytes = rs.getBytes(col);
                arr.add(toHex(bytes));
                break;
            }
            case Types.CLOB:
            case Types.NCLOB:
                arr.add(rs.getString(col));
                break;
            default:
                // Fall back to string form; record the SQL type for triage.
                ObjectNode unknown = arr.addObject();
                unknown.put("sqlType", sqlType);
                unknown.put("string", String.valueOf(raw));
                break;
        }
    }

    private static String toHex(byte[] b) {
        if (b == null) return null;
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(String.format("%02x", x & 0xFF));
        }
        return sb.toString();
    }

    JsonNode rootNodeForTesting() {
        return root;
    }
}
