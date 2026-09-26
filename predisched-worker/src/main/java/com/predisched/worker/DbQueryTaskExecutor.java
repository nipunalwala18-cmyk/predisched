package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Locale;
import javax.sql.DataSource;

/**
 * {@code DB_QUERY_TASK rows=N} (spec §7.2, prompt 11): count, sum, average and a per-category
 * maximum over the first N rows of the seeded {@code db_query_data} table.
 *
 * <p>It runs on the worker's own small connection pool ({@code db.queryPoolSize}, 2 by default),
 * so however many of these queue up they cannot take connections from anything else. Not
 * deterministic in the cache's sense: the answer depends on the database, not only on the input.
 */
public class DbQueryTaskExecutor implements TaskExecutor {

    private static final String QUERY = """
            SELECT count(*), sum(value), avg(value), count(DISTINCT category), max(value)
            FROM (SELECT category, value FROM db_query_data ORDER BY id LIMIT ?) AS slice
            """;

    private final DataSource pool;

    public DbQueryTaskExecutor(DataSource pool) {
        this.pool = pool;
    }

    @Override
    public TaskType type() {
        return TaskType.DB_QUERY_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.IO_BOUND;
    }

    @Override
    public boolean deterministic() {
        return false;
    }

    @Override
    public ExecutionResult execute(String input) throws Exception {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        long rows = Long.parseLong(InputParser.parse(input).get("rows"));
        try (Connection connection = pool.getConnection();
                PreparedStatement statement = connection.prepareStatement(QUERY)) {
            statement.setLong(1, rows);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return ExecutionResult.success(String.format(Locale.ROOT,
                        "rows=%d sum=%.2f avg=%.4f categories=%d max=%.2f",
                        result.getLong(1), result.getDouble(2), result.getDouble(3),
                        result.getLong(4), result.getDouble(5)));
            }
        }
    }
}
