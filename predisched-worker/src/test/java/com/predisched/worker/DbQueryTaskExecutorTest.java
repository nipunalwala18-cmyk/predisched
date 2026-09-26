package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.ExecutionResult;
import com.predisched.common.db.Db;
import com.predisched.common.db.TestDatabase;
import com.predisched.proto.TaskType;
import org.junit.jupiter.api.Test;

/** DB_QUERY_TASK over the seeded table, and which executors the result cache may use (F6). */
class DbQueryTaskExecutorTest {

    @Test
    void aggregatesTheFirstRowsOfTheSeededTable() throws Exception {
        try (TestDatabase database = TestDatabase.create(); Db db = database.open(2, true)) {
            DbQueryTaskExecutor executor = new DbQueryTaskExecutor(db.dataSource());
            ExecutionResult result = executor.execute("rows=1000");
            assertTrue(result.success(), result.errorMessage());
            assertTrue(result.output().startsWith("rows=1000 "), result.output());
            assertTrue(result.output().contains("categories=16"), result.output());
            assertEquals(result.output(), executor.execute("rows=1000").output(),
                    "the seeded data is the same every time");
            assertFalse(executor.execute("rows=0").success(), "rows is validated");
        }
    }

    @Test
    void onlyExecutorsWhoseOutputDependsOnTheInputAloneAreCacheable() {
        ExecutorRegistry registry = new ExecutorRegistry();
        for (TaskType type : new TaskType[] {TaskType.CPU_TASK, TaskType.MATRIX_TASK,
                TaskType.HASH_TASK, TaskType.MONTE_CARLO_TASK, TaskType.SORT_TASK,
                TaskType.COMPRESS_TASK, TaskType.GRAPH_TASK}) {
            assertTrue(registry.deterministic(type), type + " is cacheable");
        }
        for (TaskType type : new TaskType[] {TaskType.SLEEP_TASK, TaskType.HTTP_TASK,
                TaskType.FILE_IO_TASK, TaskType.DB_QUERY_TASK}) {
            assertFalse(registry.deterministic(type), type + " is not");
        }
        assertFalse(new DbQueryTaskExecutor(null).deterministic());
    }
}
