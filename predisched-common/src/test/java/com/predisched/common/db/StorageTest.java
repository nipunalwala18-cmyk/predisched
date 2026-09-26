package com.predisched.common.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Schema, history writer and SLA report against a real PostgreSQL (prompt 11). */
class StorageTest {

    private static TestDatabase database;
    private static Db db;

    @BeforeAll
    static void start() throws Exception {
        database = TestDatabase.create();
        db = database.open(4, true);
    }

    @AfterAll
    static void stop() {
        if (db != null) {
            db.close();
        }
        if (database != null) {
            database.close();
        }
    }

    @Test
    void migrationsCreateEverySpecTableAndSeedTheQueryData() throws Exception {
        for (String table : List.of("tasks", "workers", "worker_metrics", "execution_history",
                "predictions", "scheduling_decisions", "events", "replication_log", "clock_sync",
                "failures", "benchmark_runs", "result_cache", "db_query_data")) {
            assertEquals(1L, count("SELECT count(*) FROM information_schema.tables WHERE"
                    + " table_name = '" + table + "'"), table + " exists");
        }
        assertEquals(200_000L, count("SELECT count(*) FROM db_query_data"));
        assertEquals(0, db.migrate(), "a second run has nothing to apply");
    }

    @Test
    void tenThousandMetricRowsArriveInTheDatabase() throws Exception {
        long before = count("SELECT count(*) FROM worker_metrics WHERE worker_id = 'load'");
        try (HistoryWriter writer = new HistoryWriter(db.dataSource(), 50_000, 500, 50)) {
            writer.start();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10_000; i++) {
                writer.workerMetrics("load", start + i, 12.5, 40.0, 2, 1, i, 20.0);
            }
            assertTrue(writer.flush(30_000), "flushed");
            assertEquals(0, writer.droppedMetrics());
        }
        assertEquals(10_000L,
                count("SELECT count(*) FROM worker_metrics WHERE worker_id = 'load'") - before);
    }

    @Test
    void aFullQueueDropsTheOldestMetricsNotDecisions() throws Exception {
        try (HistoryWriter writer = new HistoryWriter(db.dataSource(), 100, 50, 50)) {
            // Not started yet: nothing drains, so the buffer fills.
            for (int i = 0; i < 300; i++) {
                writer.workerMetrics("full", i, 1, 1, 0, 0, i, 1);
                if (i % 5 == 0) {
                    writer.decision("full-" + i, "round_robin", "w1", 0.5,
                            Map.of("w1", 0.5), 12, i);
                }
            }
            assertEquals(100, writer.buffered());
            assertEquals(0, writer.droppedOther(), "no decision was dropped");
            writer.start();
            assertTrue(writer.flush(30_000));
            assertEquals(60L, count("SELECT count(*) FROM scheduling_decisions"
                    + " WHERE task_id LIKE 'full-%'"), "all 60 decisions written");
            assertEquals(40L, count("SELECT count(*) FROM worker_metrics WHERE worker_id = 'full'"));
            assertEquals(260, writer.droppedMetrics());
            assertEquals(299L, count("SELECT max(tasks_completed) FROM worker_metrics"
                    + " WHERE worker_id = 'full'"), "the newest metrics were kept");
        }
    }

    @Test
    void theSlaReportCountsTightAndLooseDeadlines() throws Exception {
        long now = System.currentTimeMillis();
        try (HistoryWriter writer = new HistoryWriter(db.dataSource(), 1_000, 100, 20)) {
            writer.start();
            // 10 tasks took 100 ms each: 4 had 50 ms (missed), 6 had 1 s (met). One is open.
            for (int i = 0; i < 10; i++) {
                long deadline = now + (i < 4 ? 50 : 1_000);
                TaskRecord done = TaskRecord.createQueued("sla-" + i, TaskType.CPU_TASK, "n=1", 5)
                        .withDeadlineAt(deadline)
                        .withStrategy(i % 2 == 0 ? "round_robin" : "least_loaded");
                done = restoreTimes(done, now, now + 100);
                writer.task(done);
            }
            writer.task(TaskRecord.createQueued("sla-open", TaskType.SORT_TASK, "n=1", 5)
                    .withDeadlineAt(now + 60_000));
            assertTrue(writer.flush(10_000));
        }
        Instant since = Instant.ofEpochMilli(now - 60_000);
        Map<String, SlaReport.Row> byStrategy = index(SlaReport.byStrategy(db.dataSource(), since));
        assertEquals(new SlaReport.Row("round_robin", 3, 2, 0), byStrategy.get("round_robin"));
        assertEquals(new SlaReport.Row("least_loaded", 3, 2, 0), byStrategy.get("least_loaded"));
        Map<String, SlaReport.Row> byType = index(SlaReport.byTaskType(db.dataSource(), since));
        assertEquals(60.0, byType.get("CPU_TASK").onTimePct(), 1e-9);
        assertEquals(1, byType.get("SORT_TASK").open());
        assertTrue(SlaReport.format(List.copyOf(byStrategy.values()),
                List.copyOf(byType.values())).contains("60.0"));
    }

    /** A COMPLETED record whose started/completed times are the given ones. */
    private static TaskRecord restoreTimes(TaskRecord queued, long startedAt, long completedAt) {
        TaskRecord running = TaskRecord.restore(queued.id(), queued.type(), queued.input(),
                queued.priority(), TaskStatus.COMPLETED, "w1", "ok", queued.submittedAt(),
                startedAt, completedAt, completedAt - startedAt, "", 0, -1, List.of());
        return running.withDeadlineAt(queued.deadlineAt()).withStrategy(queued.strategy());
    }

    private static Map<String, SlaReport.Row> index(List<SlaReport.Row> rows) {
        Map<String, SlaReport.Row> byKey = new java.util.LinkedHashMap<>();
        rows.forEach(row -> byKey.put(row.key(), row));
        return byKey;
    }

    private static long count(String sql) throws Exception {
        try (Connection connection = db.dataSource().getConnection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getLong(1);
        }
    }
}
