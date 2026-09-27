package com.predisched.dashboard.web;

import com.predisched.dashboard.cluster.Cluster;
import com.predisched.dashboard.cluster.Protos;
import com.predisched.dashboard.repo.Repositories;
import com.predisched.proto.ClusterState;
import com.predisched.proto.WorkerEntry;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** {@code GET /api/overview}: the KPI strip and a topology snapshot (spec §15.2). */
@RestController
public class OverviewController {

    private final Cluster cluster;
    private final Repositories repo;

    public OverviewController(Cluster cluster, Repositories repo) {
        this.cluster = cluster;
        this.repo = repo;
    }

    @GetMapping("/api/overview")
    public Map<String, Object> overview(@RequestParam(defaultValue = "60") int windowSeconds) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("generatedAt", Instant.now().toString());
        Map<String, Object> db = repo.kpis(windowSeconds);
        long completed = ((Number) db.get("completed")).longValue();
        long withDeadline = ((Number) db.get("with_deadline")).longValue();
        long slaMet = ((Number) db.get("sla_met")).longValue();
        Map<String, Object> kpis = new LinkedHashMap<>();
        kpis.put("windowSeconds", windowSeconds);
        kpis.put("tasksPerSecond", completed / (double) windowSeconds);
        kpis.put("meanLatencyMs", db.get("mean_latency_ms"));
        kpis.put("p95LatencyMs", db.get("p95_latency_ms"));
        kpis.put("slaCompliancePct", withDeadline == 0 ? null : 100.0 * slaMet / withDeadline);
        try {
            ClusterState state = cluster.state();
            List<WorkerEntry> workers = state.getWorkersList();
            kpis.put("queueDepth", state.getQueueDepth());
            kpis.put("activeWorkers", workers.stream().filter(WorkerEntry::getHealthy).count());
            out.put("kpis", kpis);
            out.put("cluster", Protos.state(state));
        } catch (RuntimeException e) {
            kpis.put("queueDepth", null);
            kpis.put("activeWorkers", null);
            out.put("kpis", kpis);
            out.put("cluster", null);
            out.put("clusterError", e.getMessage());
        }
        return out;
    }
}
