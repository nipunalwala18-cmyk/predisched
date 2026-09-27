package com.predisched.dashboard.repo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Every read the dashboard makes from PostgreSQL (spec §14 tables), as plain JdbcTemplate
 * queries returning rows as maps. One class keeps the SQL in one place; the controller tests mock
 * it.
 */
@Repository
public class Repositories {

    private final JdbcTemplate jdbc;

    public Repositories(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // --- tasks ---------------------------------------------------------------------------
    public List<Map<String, Object>> tasks(String status, String type, int limit) {
        StringBuilder sql = new StringBuilder("SELECT task_id, type, priority, status, worker_id,"
                + " submitted_at, started_at, completed_at, exec_time_ms, attempts, deadline_at,"
                + " sla_met, strategy, trace_id,"
                + " extract(epoch FROM (completed_at - submitted_at)) * 1000 AS latency_ms"
                + " FROM tasks WHERE true");
        List<Object> args = new ArrayList<>();
        if (status != null && !status.isBlank()) {
            sql.append(" AND status = ?");
            args.add(status.toUpperCase());
        }
        if (type != null && !type.isBlank()) {
            sql.append(" AND type = ?");
            args.add(type.toUpperCase());
        }
        sql.append(" ORDER BY submitted_at DESC LIMIT ?");
        args.add(Math.max(1, Math.min(limit, 5_000)));
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Map<String, Object> task(String taskId) {
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT * FROM tasks WHERE task_id = ?",
                taskId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Map<String, Object>> decisions(String taskId) {
        return jdbc.queryForList("SELECT strategy, chosen_worker, cost, scores::text AS scores,"
                + " breakdown::text AS breakdown, fallback, fallback_reason, model_versions,"
                + " decision_us, ts FROM scheduling_decisions WHERE task_id = ? ORDER BY ts",
                taskId);
    }

    public List<Map<String, Object>> taskEvents(String taskId) {
        return jdbc.queryForList("SELECT node_id, lamport_time, ts, event_type,"
                + " details::text AS details FROM events WHERE task_id = ?"
                + " ORDER BY lamport_time, ts LIMIT 500", taskId);
    }

    public List<Map<String, Object>> taskPredictions(String taskId) {
        return jdbc.queryForList("SELECT worker_id, pred_exec_ms, actual_exec_ms, abs_error_ms,"
                + " cold_start, model_versions, ts FROM prediction_outcomes WHERE task_id = ?",
                taskId);
    }

    /** Throughput, latency and SLA over completed tasks of the last {@code seconds}. */
    public Map<String, Object> kpis(int seconds) {
        return jdbc.queryForMap("SELECT count(*) AS completed,"
                + " avg(extract(epoch FROM (completed_at - submitted_at)) * 1000) AS mean_latency_ms,"
                + " percentile_cont(0.95) WITHIN GROUP (ORDER BY"
                + "   extract(epoch FROM (completed_at - submitted_at)) * 1000) AS p95_latency_ms,"
                + " count(*) FILTER (WHERE deadline_at IS NOT NULL) AS with_deadline,"
                + " count(*) FILTER (WHERE sla_met) AS sla_met"
                + " FROM tasks WHERE status = 'COMPLETED'"
                + " AND completed_at > now() - make_interval(secs => ?)", seconds);
    }

    // --- workers and queues --------------------------------------------------------------
    public List<Map<String, Object>> workersLatest() {
        return jdbc.queryForList("SELECT w.worker_id, w.host, w.port, w.cores, w.memory_mb,"
                + " w.pool_size, w.status, w.last_heartbeat, m.ts AS metrics_at, m.cpu_pct,"
                + " m.mem_pct, m.active_threads, m.queue_len, m.tasks_completed, m.avg_exec_ms"
                + " FROM workers w LEFT JOIN LATERAL (SELECT * FROM worker_metrics x"
                + "   WHERE x.worker_id = w.worker_id ORDER BY ts DESC LIMIT 1) m ON true"
                + " ORDER BY w.worker_id");
    }

    public List<Map<String, Object>> workerHistory(String workerId, int minutes) {
        return jdbc.queryForList("SELECT ts, cpu_pct, mem_pct, active_threads, queue_len,"
                + " tasks_completed, avg_exec_ms FROM worker_metrics WHERE worker_id = ?"
                + " AND ts > now() - make_interval(mins => ?) ORDER BY ts", workerId, minutes);
    }

    /** Queued tasks on all workers per second (each worker's mean in the second, summed). */
    public List<Map<String, Object>> queueActual(int minutes) {
        return jdbc.queryForList("SELECT b AS ts, sum(q) AS queue_len FROM ("
                + " SELECT date_trunc('second', ts) AS b, worker_id, avg(queue_len) AS q"
                + " FROM worker_metrics WHERE ts > now() - make_interval(mins => ?)"
                + " GROUP BY 1, 2) s GROUP BY b ORDER BY b", minutes);
    }

    /**
     * The M2 forecasts made each second, summed over workers and shifted by the 5 s horizon, so
     * each point sits where the forecast was about.
     */
    public List<Map<String, Object>> queuePredicted(int minutes) {
        return jdbc.queryForList("SELECT b + interval '5 seconds' AS ts, sum(q) AS queue_len FROM ("
                + " SELECT date_trunc('second', ts) AS b, worker_id, avg(pred_queue_len) AS q"
                + " FROM predictions WHERE pred_queue_len IS NOT NULL"
                + " AND ts > now() - make_interval(mins => ?) GROUP BY 1, 2) s"
                + " GROUP BY b ORDER BY b", minutes);
    }

    // --- predictions ---------------------------------------------------------------------
    public List<Map<String, Object>> accuracy(int limit) {
        return jdbc.queryForList("SELECT ts, task_id, task_type, worker_id, pred_exec_ms,"
                + " actual_exec_ms, abs_error_ms, cold_start, rolling_mae_ms, rolling_type_mae_ms"
                + " FROM prediction_outcomes ORDER BY ts DESC LIMIT ?", limit);
    }

    public List<Map<String, Object>> accuracyByType(int limit) {
        return jdbc.queryForList("SELECT task_type, count(*) AS n, avg(abs_error_ms) AS mae_ms"
                + " FROM (SELECT * FROM prediction_outcomes ORDER BY ts DESC LIMIT ?) r"
                + " GROUP BY task_type ORDER BY task_type", limit);
    }

    public List<Map<String, Object>> events(List<String> types, int limit) {
        String in = String.join(",", types.stream().map(t -> "?").toList());
        List<Object> args = new ArrayList<>(types);
        args.add(limit);
        return jdbc.queryForList("SELECT node_id, lamport_time, ts, event_type, task_id,"
                + " details::text AS details FROM events WHERE event_type IN (" + in + ")"
                + " ORDER BY ts DESC LIMIT ?", args.toArray());
    }

    // --- cluster, replication, clocks -----------------------------------------------------
    public List<Map<String, Object>> replication() {
        return jdbc.queryForList("SELECT node_id, max(seq_no) AS max_seq, count(*) AS entries,"
                + " max(applied_at) AS last_applied FROM replication_log GROUP BY node_id"
                + " ORDER BY node_id");
    }

    public List<Map<String, Object>> clockOffsets() {
        return jdbc.queryForList("SELECT DISTINCT ON (node_id) node_id, ts, offset_before_ms,"
                + " offset_after_ms, algorithm FROM clock_sync ORDER BY node_id, ts DESC");
    }

    public List<Map<String, Object>> failures(int limit) {
        return jdbc.queryForList("SELECT node_id, type, detected_at, recovered_at, details"
                + " FROM failures ORDER BY detected_at DESC LIMIT ?", limit);
    }

    // --- benchmarks ------------------------------------------------------------------------
    public List<Map<String, Object>> benchmarks(int limit) {
        return jdbc.queryForList("SELECT run_id, strategy, scenario, metrics::text AS metrics,"
                + " created_at FROM benchmark_runs WHERE scenario LIKE 'benchmark:%'"
                + " ORDER BY created_at DESC LIMIT ?", limit);
    }

    /** Suites with their run counts, newest first. */
    public List<Map<String, Object>> suites() {
        List<Map<String, Object>> rows = jdbc.queryForList("SELECT split_part(scenario, ':', 2)"
                + " AS suite, count(*) AS runs, min(created_at) AS started, max(created_at) AS"
                + " finished FROM benchmark_runs WHERE scenario LIKE 'benchmark:%'"
                + " GROUP BY 1 ORDER BY max(created_at) DESC");
        List<Map<String, Object>> out = new ArrayList<>();
        rows.forEach(r -> out.add(new LinkedHashMap<>(r)));
        return out;
    }
}
