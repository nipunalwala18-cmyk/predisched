package com.predisched.common.db;

import com.predisched.common.NodeConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A HikariCP pool over PostgreSQL, plain JDBC on top (prompt 11: no ORM). {@link #migrate()} applies
 * the Flyway migrations shipped in this jar ({@code db/migrations} at the repo root).
 */
public final class Db implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Db.class);

    private final HikariDataSource dataSource;

    private Db(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** A pool of {@code poolSize} connections; migrated first when {@code migrate} is set. */
    public static Db open(NodeConfig.DbConfig config, int poolSize, String poolName,
            boolean migrate) {
        return open(config.getUrl(), config.getUser(), config.getPassword(), poolSize, poolName,
                migrate);
    }

    public static Db open(String url, String user, String password, int poolSize, String poolName,
            boolean migrate) {
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(url);
        hikari.setUsername(user);
        hikari.setPassword(password);
        hikari.setMaximumPoolSize(Math.max(1, poolSize));
        hikari.setMinimumIdle(1);
        hikari.setPoolName(poolName);
        // Fail fast: a node without its database should say so, not hang.
        hikari.setConnectionTimeout(3_000);
        hikari.setInitializationFailTimeout(5_000);
        Db db = new Db(new HikariDataSource(hikari));
        if (migrate) {
            try {
                db.migrate();
            } catch (RuntimeException e) {
                db.close();
                throw e;
            }
        }
        return db;
    }

    /** Applies any pending migration; returns how many ran. */
    public int migrate() {
        MigrateResult result = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();
        log.info("Database schema at version {} ({} migrations applied now)",
                result.targetSchemaVersion == null ? "current" : result.targetSchemaVersion,
                result.migrationsExecuted);
        return result.migrationsExecuted;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
