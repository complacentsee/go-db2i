package io.github.complacentsee.gojtopen.fixtures;

import com.ibm.as400.access.Trace;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Entry point for the goJTOpen fixture-capture harness.
 *
 * For each {@link Case} declared in {@link Cases}, opens a fresh JDBC
 * connection, enables JTOpen datastream tracing to a per-case
 * {@code <name>.trace} file, runs the case, and writes the observable
 * result to {@code <name>.golden.json}. Both files are committed to the
 * goJTOpen repo so the Go driver's wire-replay tests work offline.
 *
 * Configuration (env vars):
 *   PUB400_HOST    (default: pub400.com)
 *   PUB400_USER    (required)
 *   PUB400_PWD     (required)
 *   PUB400_SCHEMA  (default: PUB400_USER uppercased)
 *   FIXTURES_DIR   (default: ./fixtures)
 *   ONLY           (optional, comma-separated case names to run)
 */
public final class Capture {

    public static void main(String[] args) throws Exception {
        Config cfg = Config.fromEnv();

        Path fixturesDir = Paths.get(cfg.fixturesDir).toAbsolutePath();
        Files.createDirectories(fixturesDir);

        // Tee System.err into fixtures/run.log so journal-setup messages
        // and other failures are committed alongside the trace files
        // instead of disappearing with the user's terminal session.
        Path runLog = fixturesDir.resolve("run.log");
        PrintStream originalErr = System.err;
        FileOutputStream logOut = new FileOutputStream(runLog.toFile(), false);
        PrintStream teeErr = new PrintStream(
                new TeeOutputStream(logOut, originalErr), true, StandardCharsets.UTF_8.name());
        System.setErr(teeErr);

        try {
            List<Case> cases = Cases.all(cfg.schema);
            if (!cfg.only.isEmpty()) {
                cases.removeIf(c -> !cfg.only.contains(c.name));
            }

            System.out.println("goJTOpen fixture capture");
            System.out.println("  host:     " + cfg.host);
            System.out.println("  user:     " + cfg.user);
            System.out.println("  schema:   " + cfg.schema);
            System.out.println("  fixtures: " + fixturesDir);
            System.out.println("  log:      " + runLog);
            System.out.println("  cases:    " + cases.size());
            System.out.println();

            int ok = 0, failed = 0;
            for (Case c : cases) {
                System.out.print("[ " + c.name + " ] ");
                try {
                    runCase(c, cfg, fixturesDir);
                    System.out.println("ok");
                    ok++;
                } catch (Exception e) {
                    System.out.println("FAILED: " + e.getMessage());
                    e.printStackTrace(System.out);
                    failed++;
                }
            }

            System.out.println();
            System.out.println("Done. " + ok + " ok, " + failed + " failed.");
            if (failed > 0) {
                System.exit(1);
            }
        } finally {
            try {
                teeErr.flush();
                logOut.close();
            } catch (IOException ignored) {
            }
            System.setErr(originalErr);
        }
    }

    /**
     * Writes to two streams. close() only closes the first ({@code logFile});
     * the original stderr is left open so the JVM can keep using it.
     */
    private static final class TeeOutputStream extends OutputStream {
        private final OutputStream logFile;
        private final OutputStream stderr;
        TeeOutputStream(OutputStream logFile, OutputStream stderr) {
            this.logFile = logFile;
            this.stderr = stderr;
        }
        @Override public void write(int b) throws IOException {
            logFile.write(b);
            stderr.write(b);
        }
        @Override public void write(byte[] buf, int off, int len) throws IOException {
            logFile.write(buf, off, len);
            stderr.write(buf, off, len);
        }
        @Override public void flush() throws IOException {
            logFile.flush();
            stderr.flush();
        }
        @Override public void close() throws IOException {
            logFile.close();
        }
    }

    private static void runCase(Case c, Config cfg, Path fixturesDir) throws Exception {
        // Drop any existing trace target so a stale partial file doesn't
        // contaminate the new capture.
        Path tracePath = fixturesDir.resolve(c.name + ".trace");
        Files.deleteIfExists(tracePath);

        try (Connection setupConn = openConnection(cfg)) {
            // Setup runs untraced.
            c.setup(setupConn);
        }

        GoldenWriter golden = new GoldenWriter(c.name);

        // Each case gets its own connection so the trace is self-contained
        // (sign-on -> traced operation -> disconnect).
        try {
            Trace.setFileName(tracePath.toString());
            // DATASTREAM emits the wire bytes ("Data stream sent
            // (connID=...)" + hex dumps). ERROR and WARNING surface
            // anything JTOpen itself flags as an error or warning during
            // the traced section, so a failure that doesn't bubble up as
            // a SQLException is still visible in the trace file.
            //
            // DIAGNOSTIC is intentionally OFF: it includes JTOpen's
            // finalize-time "Service disconnected" / "Resetting all
            // services" log spam, which lands non-deterministically in
            // whichever case's trace is open when the JVM happens to GC.
            Trace.setTraceDatastreamOn(true);
            Trace.setTraceErrorOn(true);
            Trace.setTraceWarningOn(true);
            Trace.setTraceOn(true);
            try (Connection conn = openConnection(cfg, c.extraConnectionProperties())) {
                try {
                    c.execute(conn, golden);
                } catch (SQLException e) {
                    // Negative-path cases expect to record the error.
                    golden.recordError(e);
                }
            }
        } finally {
            Trace.setTraceOn(false);
            Trace.setTraceDatastreamOn(false);
            Trace.setTraceErrorOn(false);
            Trace.setTraceWarningOn(false);
            // Switching back to System.out flushes & releases the file handle.
            try {
                Trace.setFileName(null);
            } catch (Exception ignored) {
            }
        }

        // Persist the golden BEFORE teardown so a teardown failure doesn't
        // discard a successful capture.
        golden.writeTo(fixturesDir);

        try (Connection teardownConn = openConnection(cfg)) {
            c.teardown(teardownConn);
        }
    }

    private static Connection openConnection(Config cfg) throws SQLException {
        return openConnection(cfg, java.util.Collections.emptyMap());
    }

    private static Connection openConnection(Config cfg,
                                             java.util.Map<String, String> extras) throws SQLException {
        String url = "jdbc:as400://" + cfg.host;
        Properties props = new Properties();
        props.setProperty("user", cfg.user);
        props.setProperty("password", cfg.pwd);
        // Match what a typical goJTOpen user would request: SQL naming
        // (period-qualified), default library = their schema.
        props.setProperty("naming", "sql");
        props.setProperty("libraries", cfg.schema);
        // Be nice to PUB400; don't leave idle connections lingering.
        props.setProperty("thread used", "false");
        // Connection-level socket read timeout in ms. Unlike
        // Statement.setQueryTimeout (which only affects execute, not
        // prepare or commit), JTOpen's "socket timeout" property bounds
        // every read off the JDBC socket -- including the PREPARE
        // round-trip. We had a run wedge for 30+ min inside
        // commonPrepare on DLTJRN because PUB400 had a held journal;
        // this caps that to 30s and surfaces an SQLException instead.
        props.setProperty("socket timeout", "30000");
        // Login + connect timeouts so a half-open TCP path can't hang
        // openConnection itself indefinitely.
        props.setProperty("login timeout", "30");
        // Apply per-case overrides last so they win over the defaults.
        for (java.util.Map.Entry<String, String> e : extras.entrySet()) {
            props.setProperty(e.getKey(), e.getValue());
        }
        // Force the JTOpen driver to load.
        try {
            Class.forName("com.ibm.as400.access.AS400JDBCDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("JTOpen driver not on classpath", e);
        }
        return DriverManager.getConnection(url, props);
    }

    private static final class Config {
        final String host;
        final String user;
        final String pwd;
        final String schema;
        final String fixturesDir;
        final List<String> only;

        private Config(String host, String user, String pwd, String schema,
                       String fixturesDir, List<String> only) {
            this.host = host;
            this.user = user;
            this.pwd = pwd;
            this.schema = schema;
            this.fixturesDir = fixturesDir;
            this.only = only;
        }

        static Config fromEnv() {
            String host = envOr("PUB400_HOST", "pub400.com");
            String user = required("PUB400_USER");
            String pwd = required("PUB400_PWD");
            String schema = envOr("PUB400_SCHEMA", user).toUpperCase(Locale.ROOT);
            String fixturesDir = envOr("FIXTURES_DIR", "fixtures");
            String onlyRaw = System.getenv("ONLY");
            List<String> only = onlyRaw == null || onlyRaw.isEmpty()
                    ? Collections.emptyList()
                    : new ArrayList<>(Arrays.asList(onlyRaw.split(",")));
            return new Config(host, user, pwd, schema, fixturesDir, only);
        }

        private static String envOr(String key, String def) {
            String v = System.getenv(key);
            return (v == null || v.isEmpty()) ? def : v;
        }

        private static String required(String key) {
            String v = System.getenv(key);
            if (v == null || v.isEmpty()) {
                throw new IllegalStateException("Missing required env var: " + key);
            }
            return v;
        }
    }
}
