package io.github.complacentsee.db2i.fixtures;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * One captured fixture case. The runner enables datastream tracing for the
 * scope of {@link #execute(Connection, GoldenWriter)} and asserts the result.
 *
 * Setup and teardown run untraced; only the inside of {@code execute} is
 * captured to {@code <name>.trace}.
 */
public abstract class Case {
    public final String name;

    protected Case(String name) {
        this.name = name;
    }

    /** Idempotent DDL/DML run before the traced section. May be a no-op. */
    public void setup(Connection conn) throws SQLException {
    }

    /** The traced operation. Write its observable result to {@code golden}. */
    public abstract void execute(Connection conn, GoldenWriter golden) throws SQLException;

    /** Idempotent cleanup run after the traced section. May be a no-op. */
    public void teardown(Connection conn) throws SQLException {
    }

    /**
     * Per-case JDBC connection properties applied on top of the runner's
     * defaults when opening the traced connection. Default is empty;
     * override to set things like {@code "lob threshold"} so the
     * captured trace reflects a JT400 client running with that knob
     * turned on.
     *
     * Only the {@code execute}-phase connection picks these up. Setup
     * and teardown run on the default-properties connection so the
     * pre/post-state plumbing doesn't get tangled in the case's
     * specific knobs.
     */
    public Map<String, String> extraConnectionProperties() {
        return Collections.emptyMap();
    }
}
