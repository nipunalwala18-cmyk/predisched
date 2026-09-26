package com.predisched.common.db;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Locale;
import java.util.UUID;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * A fresh, empty PostgreSQL database for one test class, migrated or not.
 *
 * <ul>
 *   <li>With {@code PREDISCHED_TEST_DB_URL} set (a server's admin database, e.g.
 *       {@code jdbc:postgresql://localhost:5432/postgres}, user and password in
 *       {@code PREDISCHED_TEST_DB_USER} / {@code PREDISCHED_TEST_DB_PASSWORD}) it creates a
 *       throwaway database there and drops it afterwards. That is how tests run on a machine
 *       without Docker.</li>
 *   <li>Otherwise, with Docker available (CI), a Testcontainers {@code postgres:16}.</li>
 *   <li>With neither, the test is skipped, not failed.</li>
 * </ul>
 */
public final class TestDatabase implements AutoCloseable {

    private static final String IMAGE = "postgres:16";

    private final String url;
    private final String user;
    private final String password;
    private final Runnable cleanup;

    private TestDatabase(String url, String user, String password, Runnable cleanup) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.cleanup = cleanup;
    }

    public static TestDatabase create() throws Exception {
        String adminUrl = System.getenv("PREDISCHED_TEST_DB_URL");
        if (adminUrl != null && !adminUrl.isBlank()) {
            String user = env("PREDISCHED_TEST_DB_USER", "postgres");
            String password = env("PREDISCHED_TEST_DB_PASSWORD", "predisched");
            String name = "predisched_test_" + UUID.randomUUID().toString().replace("-", "")
                    .substring(0, 12).toLowerCase(Locale.ROOT);
            try (Connection admin = DriverManager.getConnection(adminUrl, user, password);
                    Statement statement = admin.createStatement()) {
                statement.execute("CREATE DATABASE " + name);
            }
            String url = adminUrl.substring(0, adminUrl.lastIndexOf('/') + 1) + name;
            return new TestDatabase(url, user, password, () -> {
                try (Connection admin = DriverManager.getConnection(adminUrl, user, password);
                        Statement statement = admin.createStatement()) {
                    statement.execute("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)");
                } catch (Exception e) {
                    // A leftover test database is harmless.
                }
            });
        }
        assumeTrue(dockerAvailable(),
                "no database for this test: set PREDISCHED_TEST_DB_URL or run Docker");
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>(IMAGE)
                .withDatabaseName("predisched")
                .withUsername("postgres")
                .withPassword("predisched");
        container.start();
        return new TestDatabase(container.getJdbcUrl(), container.getUsername(),
                container.getPassword(), container::stop);
    }

    public Db open(int poolSize, boolean migrate) {
        return Db.open(url, user, password, poolSize, "test", migrate);
    }

    public String url() {
        return url;
    }

    public String user() {
        return user;
    }

    public String password() {
        return password;
    }

    @Override
    public void close() {
        cleanup.run();
    }

    private static boolean dockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable e) {
            return false;
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
