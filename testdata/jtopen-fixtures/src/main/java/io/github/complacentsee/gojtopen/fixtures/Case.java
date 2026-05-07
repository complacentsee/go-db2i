package io.github.complacentsee.gojtopen.fixtures;

import java.sql.Connection;
import java.sql.SQLException;

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
}
