package com.predisched.common.db;

import com.predisched.common.TaskRecord;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The node's history, written to PostgreSQL asynchronously and in batches (prompt 11).
 *
 * <p>Every {@link HistorySink} call turns into a row in a bounded buffer and returns; one drain
 * thread takes up to {@code batchSize} rows at a time (or whatever arrived within
 * {@code flushMs}), groups them by statement and writes each group as one JDBC batch in one
 * transaction. The scheduler never waits on the database.
 *
 * <p>When the buffer is full the <em>oldest worker-metrics row</em> is dropped to make room:
 * metrics arrive every second from every worker and a gap in them costs little, whereas a
 * decision, an execution or a task state is written once. Only with no metrics left to drop is the
 * new row itself dropped. Both kinds of drop are counted, and nothing ever blocks.
 *
 * <p>Thread safety: the buffer is guarded by {@code lock}; counters are atomic; the drain thread
 * alone touches the database.
 */
public class HistoryWriter implements HistorySink, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HistoryWriter.class);

    /** One row to write: its statement and how to fill it in. */
    private interface Row {
        String sql();

        void bind(PreparedStatement statement) throws SQLException;

        default boolean isMetric() {
            return false;
        }
    }

    private final DataSource dataSource;
    private final int capacity;
    private final int batchSize;
    private final long flushMs;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition drained = lock.newCondition();
    private final ArrayDeque<Row> buffer = new ArrayDeque<>();
    private final AtomicLong written = new AtomicLong();
    private final AtomicLong droppedMetrics = new AtomicLong();
    private final AtomicLong droppedOther = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile boolean running;
    private boolean writing;
    private Thread drainer;

    public HistoryWriter(DataSource dataSource, int capacity, int batchSize, long flushMs) {
        this.dataSource = dataSource;
        this.capacity = Math.max(1, capacity);
        this.batchSize = Math.max(1, batchSize);
        this.flushMs = Math.max(1, flushMs);
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        drainer = new Thread(this::drain, "history-writer");
        drainer.setDaemon(true);
        drainer.start();
    }

    // ---- HistorySink ---------------------------------------------------------------------

    @Override
    public void task(TaskRecord r) {
        offer(new SimpleRow("""
                INSERT INTO tasks (task_id, type, input, priority, status, worker_id, submitted_at,
                    started_at, completed_at, result, exec_time_ms, attempts, deadline_at, sla_met,
                    strategy, client_id, trace_id, workflow_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (task_id) DO UPDATE SET type = EXCLUDED.type, input = EXCLUDED.input,
                    priority = EXCLUDED.priority, status = EXCLUDED.status,
                    worker_id = EXCLUDED.worker_id, submitted_at = EXCLUDED.submitted_at,
                    started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at,
                    result = EXCLUDED.result, exec_time_ms = EXCLUDED.exec_time_ms,
                    attempts = EXCLUDED.attempts, deadline_at = EXCLUDED.deadline_at,
                    sla_met = EXCLUDED.sla_met, strategy = EXCLUDED.strategy,
                    client_id = EXCLUDED.client_id, trace_id = EXCLUDED.trace_id,
                    workflow_id = EXCLUDED.workflow_id, updated_at = now()
                """, s -> {
            s.setString(1, r.id());
            s.setString(2, r.type().name());
            s.setString(3, r.input());
            s.setInt(4, r.priority());
            s.setString(5, r.status().name());
            text(s, 6, r.workerId());
            s.setTimestamp(7, new Timestamp(r.submittedAt()));
            time(s, 8, r.startedAt());
            time(s, 9, r.completedAt());
            text(s, 10, r.result());
            s.setLong(11, r.execTimeMs());
            s.setInt(12, r.attemptCount());
            time(s, 13, r.deadlineAt());
            if (r.deadlineAt() > 0 && r.isTerminal()) {
                s.setBoolean(14, r.slaMet());
            } else {
                s.setNull(14, Types.BOOLEAN);
            }
            text(s, 15, r.strategy());
            text(s, 16, r.clientId());
            text(s, 17, r.traceId());
            text(s, 18, r.workflowId());
        }));
    }

    @Override
    public void worker(String workerId, String host, int port, int cores, long memoryMb,
            int poolSize, String status, long lastHeartbeatMs) {
        offer(new SimpleRow("""
                INSERT INTO workers (worker_id, host, port, cores, memory_mb, pool_size, status,
                    last_heartbeat)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (worker_id) DO UPDATE SET host = EXCLUDED.host, port = EXCLUDED.port,
                    cores = EXCLUDED.cores, memory_mb = EXCLUDED.memory_mb,
                    pool_size = EXCLUDED.pool_size, status = EXCLUDED.status,
                    last_heartbeat = EXCLUDED.last_heartbeat
                """, s -> {
            s.setString(1, workerId);
            s.setString(2, host);
            s.setInt(3, port);
            s.setInt(4, cores);
            s.setLong(5, memoryMb);
            s.setInt(6, poolSize);
            s.setString(7, status);
            s.setTimestamp(8, new Timestamp(lastHeartbeatMs));
        }));
    }

    @Override
    public void workerMetrics(String workerId, long tsMs, double cpuPct, double memPct,
            int activeThreads, int queueLen, long tasksCompleted, double avgExecMs) {
        offer(new MetricRow(s -> {
            s.setString(1, workerId);
            s.setTimestamp(2, new Timestamp(tsMs));
            s.setDouble(3, cpuPct);
            s.setDouble(4, memPct);
            s.setInt(5, activeThreads);
            s.setInt(6, queueLen);
            s.setLong(7, tasksCompleted);
            s.setDouble(8, avgExecMs);
        }));
    }

    @Override
    public void decision(String taskId, String strategy, String chosenWorker, Double cost,
            Map<String, Double> scores, long decisionUs, long tsMs) {
        decision(taskId, strategy, chosenWorker, cost, scores, decisionUs, tsMs, null, false,
                null, null);
    }

    @Override
    public void decision(String taskId, String strategy, String chosenWorker, Double cost,
            Map<String, Double> scores, long decisionUs, long tsMs, String breakdownJson,
            boolean fallback, String fallbackReason, String modelVersions) {
        offer(new SimpleRow("""
                INSERT INTO scheduling_decisions (task_id, strategy, chosen_worker, cost, scores,
                    decision_us, ts, breakdown, fallback, fallback_reason, model_versions)
                VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?::jsonb, ?, ?, ?)
                """, s -> {
            s.setString(1, taskId);
            s.setString(2, strategy);
            s.setString(3, chosenWorker);
            number(s, 4, cost);
            s.setString(5, Json.object(scores));
            s.setLong(6, decisionUs);
            s.setTimestamp(7, new Timestamp(tsMs));
            s.setString(8, breakdownJson);
            s.setBoolean(9, fallback);
            s.setString(10, fallbackReason == null || fallbackReason.isEmpty()
                    ? null : fallbackReason);
            s.setString(11, modelVersions == null || modelVersions.isEmpty()
                    ? null : modelVersions);
        }));
    }

    @Override
    public void predictionOutcome(String taskId, String taskType, String workerId,
            double predExecMs, long actualExecMs, boolean coldStart, String modelVersions,
            double rollingMaeMs, double rollingTypeMaeMs, long tsMs) {
        offer(new SimpleRow("""
                INSERT INTO prediction_outcomes (task_id, task_type, worker_id, pred_exec_ms,
                    actual_exec_ms, abs_error_ms, cold_start, model_versions, rolling_mae_ms,
                    rolling_type_mae_ms, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, s -> {
            s.setString(1, taskId);
            s.setString(2, taskType);
            s.setString(3, workerId);
            s.setDouble(4, predExecMs);
            s.setLong(5, actualExecMs);
            s.setDouble(6, Math.abs(predExecMs - actualExecMs));
            s.setBoolean(7, coldStart);
            s.setString(8, modelVersions);
            number(s, 9, Double.isFinite(rollingMaeMs) ? rollingMaeMs : null);
            number(s, 10, Double.isFinite(rollingTypeMaeMs) ? rollingTypeMaeMs : null);
            s.setTimestamp(11, new Timestamp(tsMs));
        }));
    }

    @Override
    public void execution(ExecutionRow e) {
        offer(new SimpleRow("""
                INSERT INTO execution_history (task_id, task_type, resource_profile, attempt, status,
                    strategy, priority, input_size, worker_id, worker_cores,
                    concurrent_tasks_on_worker, cpu_pct, mem_pct, active_threads, queue_len,
                    arrival_rate, avg_exec_recent, dispatched_at, wait_time_ms, exec_time_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, s -> {
            s.setString(1, e.taskId());
            s.setString(2, e.taskType());
            text(s, 3, e.resourceProfile());
            s.setInt(4, e.attempt());
            s.setString(5, e.status());
            text(s, 6, e.strategy());
            s.setInt(7, e.priority());
            s.setLong(8, e.inputSize());
            s.setString(9, e.workerId());
            integer(s, 10, e.workerCores());
            integer(s, 11, e.concurrentTasksOnWorker());
            number(s, 12, e.cpuPct());
            number(s, 13, e.memPct());
            integer(s, 14, e.activeThreads());
            integer(s, 15, e.queueLen());
            number(s, 16, e.arrivalRate());
            number(s, 17, e.avgExecRecent());
            s.setTimestamp(18, new Timestamp(e.dispatchedAtMs()));
            s.setLong(19, e.waitTimeMs());
            s.setLong(20, e.execTimeMs());
        }));
    }

    @Override
    public void event(String nodeId, long lamportTime, long tsMs, String type, String taskId,
            String traceId, Map<String, String> details) {
        offer(new SimpleRow("""
                INSERT INTO events (node_id, lamport_time, ts, event_type, task_id, trace_id,
                    details)
                VALUES (?, ?, ?, ?, ?, ?, ?::jsonb)
                """, s -> {
            s.setString(1, nodeId);
            s.setLong(2, lamportTime);
            s.setTimestamp(3, new Timestamp(tsMs));
            s.setString(4, type);
            text(s, 5, taskId);
            text(s, 6, traceId);
            s.setString(7, Json.object(details));
        }));
    }

    @Override
    public void replication(long seqNo, String nodeId, String op, String taskId, long version,
            long lamportTime, long appliedAtMs) {
        offer(new SimpleRow("""
                INSERT INTO replication_log (seq_no, node_id, op, task_id, version, lamport_time,
                    applied_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, s -> {
            s.setLong(1, seqNo);
            s.setString(2, nodeId);
            s.setString(3, op);
            s.setString(4, taskId);
            s.setLong(5, version);
            s.setLong(6, lamportTime);
            s.setTimestamp(7, new Timestamp(appliedAtMs));
        }));
    }

    @Override
    public void clockSync(String nodeId, long tsMs, long offsetBeforeMs, long offsetAfterMs,
            String algorithm) {
        offer(new SimpleRow("""
                INSERT INTO clock_sync (node_id, ts, offset_before_ms, offset_after_ms, algorithm)
                VALUES (?, ?, ?, ?, ?)
                """, s -> {
            s.setString(1, nodeId);
            s.setTimestamp(2, new Timestamp(tsMs));
            s.setLong(3, offsetBeforeMs);
            s.setLong(4, offsetAfterMs);
            s.setString(5, algorithm);
        }));
    }

    @Override
    public void failure(String nodeId, String type, long detectedAtMs, String details) {
        offer(new SimpleRow("""
                INSERT INTO failures (node_id, type, detected_at, details) VALUES (?, ?, ?, ?)
                """, s -> {
            s.setString(1, nodeId);
            s.setString(2, type);
            s.setTimestamp(3, new Timestamp(detectedAtMs));
            text(s, 4, details);
        }));
    }

    @Override
    public void recovered(String nodeId, long recoveredAtMs) {
        offer(new SimpleRow("""
                UPDATE failures SET recovered_at = ? WHERE node_id = ? AND recovered_at IS NULL
                """, s -> {
            s.setTimestamp(1, new Timestamp(recoveredAtMs));
            s.setString(2, nodeId);
        }));
    }

    @Override
    public void cacheStore(String key, String taskType, String output) {
        offer(new SimpleRow("""
                INSERT INTO result_cache (key, task_type, output) VALUES (?, ?, ?)
                ON CONFLICT (key) DO NOTHING
                """, s -> {
            s.setString(1, key);
            s.setString(2, taskType);
            s.setString(3, output);
        }));
    }

    @Override
    public void cacheHit(String key) {
        offer(new SimpleRow("UPDATE result_cache SET hits = hits + 1 WHERE key = ?",
                s -> s.setString(1, key)));
    }

    // ---- buffer ----------------------------------------------------------------------------

    private void offer(Row row) {
        lock.lock();
        try {
            if (buffer.size() >= capacity && !dropOldestMetric()) {
                if (row.isMetric()) {
                    droppedMetrics.incrementAndGet();
                } else {
                    droppedOther.incrementAndGet();
                }
                return;
            }
            buffer.addLast(row);
            if (buffer.size() >= batchSize) {
                notEmpty.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Holds {@code lock}. */
    private boolean dropOldestMetric() {
        Iterator<Row> rows = buffer.iterator();
        while (rows.hasNext()) {
            if (rows.next().isMetric()) {
                rows.remove();
                droppedMetrics.incrementAndGet();
                return true;
            }
        }
        return false;
    }

    private void drain() {
        while (true) {
            List<Row> batch = new ArrayList<>();
            lock.lock();
            try {
                if (buffer.isEmpty() && running) {
                    notEmpty.await(flushMs, TimeUnit.MILLISECONDS);
                }
                if (buffer.isEmpty() && !running) {
                    drained.signalAll();
                    return;
                }
                while (!buffer.isEmpty() && batch.size() < batchSize) {
                    batch.add(buffer.pollFirst());
                }
                writing = !batch.isEmpty();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                lock.unlock();
            }
            if (!batch.isEmpty()) {
                write(batch);
            }
            lock.lock();
            try {
                writing = false;
                if (buffer.isEmpty()) {
                    drained.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /** Each statement's rows as one JDBC batch, all in one transaction, in arrival order. */
    private void write(List<Row> batch) {
        Map<String, List<Row>> byStatement = new LinkedHashMap<>();
        for (Row row : batch) {
            byStatement.computeIfAbsent(row.sql(), sql -> new ArrayList<>()).add(row);
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                for (Map.Entry<String, List<Row>> group : byStatement.entrySet()) {
                    try (PreparedStatement statement = connection.prepareStatement(group.getKey())) {
                        for (Row row : group.getValue()) {
                            row.bind(statement);
                            statement.addBatch();
                        }
                        statement.executeBatch();
                    }
                }
                connection.commit();
                written.addAndGet(batch.size());
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            failed.addAndGet(batch.size());
            log.warn("History batch of {} rows not written: {}", batch.size(), e.getMessage());
        }
    }

    /** Waits until everything offered so far is written (or failed); for tests and shutdown. */
    public boolean flush(long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        lock.lock();
        try {
            notEmpty.signal();
            while (!buffer.isEmpty() || writing) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                drained.awaitNanos(left);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public long written() {
        return written.get();
    }

    public long droppedMetrics() {
        return droppedMetrics.get();
    }

    public long droppedOther() {
        return droppedOther.get();
    }

    public long failed() {
        return failed.get();
    }

    public int buffered() {
        lock.lock();
        try {
            return buffer.size();
        } finally {
            lock.unlock();
        }
    }

    /** Writes what is buffered, then stops. */
    @Override
    public void close() {
        Thread thread;
        synchronized (this) {
            running = false;
            thread = drainer;
        }
        lock.lock();
        try {
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
        if (thread != null) {
            try {
                thread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("History writer closed: {} rows written, {} metrics dropped, {} other dropped,"
                + " {} failed", written(), droppedMetrics(), droppedOther(), failed());
    }

    // ---- rows ------------------------------------------------------------------------------

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private record SimpleRow(String sql, Binder binder) implements Row {
        @Override
        public void bind(PreparedStatement statement) throws SQLException {
            binder.bind(statement);
        }
    }

    private record MetricRow(Binder binder) implements Row {
        private static final String SQL = """
                INSERT INTO worker_metrics (worker_id, ts, cpu_pct, mem_pct, active_threads,
                    queue_len, tasks_completed, avg_exec_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;

        @Override
        public String sql() {
            return SQL;
        }

        @Override
        public void bind(PreparedStatement statement) throws SQLException {
            binder.bind(statement);
        }

        @Override
        public boolean isMetric() {
            return true;
        }
    }

    private static void text(PreparedStatement s, int index, String value) throws SQLException {
        if (value == null || value.isEmpty()) {
            s.setNull(index, Types.VARCHAR);
        } else {
            s.setString(index, value);
        }
    }

    private static void time(PreparedStatement s, int index, long ms) throws SQLException {
        if (ms <= 0) {
            s.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            s.setTimestamp(index, new Timestamp(ms));
        }
    }

    private static void number(PreparedStatement s, int index, Double value) throws SQLException {
        if (value == null) {
            s.setNull(index, Types.DOUBLE);
        } else {
            s.setDouble(index, value);
        }
    }

    private static void integer(PreparedStatement s, int index, Integer value) throws SQLException {
        if (value == null) {
            s.setNull(index, Types.INTEGER);
        } else {
            s.setInt(index, value);
        }
    }
}
