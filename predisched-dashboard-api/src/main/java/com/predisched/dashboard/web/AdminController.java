package com.predisched.dashboard.web;

import com.predisched.dashboard.DashboardProperties;
import com.predisched.dashboard.cluster.Cluster;
import com.predisched.proto.AdminReply;
import com.predisched.proto.ChaosReply;
import com.predisched.proto.CpuRequest;
import com.predisched.proto.CrashRequest;
import com.predisched.proto.DrainRequest;
import com.predisched.proto.IsolateRequest;
import com.predisched.proto.LatencyRequest;
import com.predisched.proto.SetStrategyResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Admin (F7) and chaos (F16) controls, all behind {@link AdminGuard}: strategy, drain, pause,
 * election; kill, latency, CPU, partition, drain and burst.
 */
@RestController
public class AdminController {

    private final Cluster cluster;
    private final Jobs jobs;
    private final DashboardProperties props;

    public AdminController(Cluster cluster, Jobs jobs, DashboardProperties props) {
        this.cluster = cluster;
        this.jobs = jobs;
        this.props = props;
    }

    private static ResponseEntity<Map<String, Object>> reply(AdminReply r) {
        return ResponseEntity.status(r.getOk() ? HttpStatus.OK : HttpStatus.CONFLICT)
                .body(Map.of("ok", r.getOk(), "message", r.getMessage()));
    }

    @PostMapping("/api/admin/strategy")
    public ResponseEntity<Map<String, Object>> strategy(@RequestBody Map<String, Object> body) {
        Object name = body.get("strategy");
        if (name == null) {
            throw new IllegalArgumentException("body needs {\"strategy\": \"<name>\"}");
        }
        SetStrategyResponse r = cluster.setStrategy(String.valueOf(name));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", r.getOk());
        out.put("previous", r.getPrevious());
        out.put("current", r.getCurrent());
        out.put("message", r.getMessage());
        return ResponseEntity.status(r.getOk() ? HttpStatus.OK : HttpStatus.CONFLICT).body(out);
    }

    @PostMapping("/api/admin/drain/{workerId}")
    public ResponseEntity<Map<String, Object>> drain(@PathVariable String workerId) {
        return reply(cluster.drain(workerId));
    }

    /** {@code {"paused": true}} pauses, {@code false} resumes; no body pauses. */
    @PostMapping("/api/admin/pause")
    public ResponseEntity<Map<String, Object>> pause(
            @RequestBody(required = false) Map<String, Object> body) {
        boolean paused = body == null || !Boolean.FALSE.equals(body.get("paused"));
        return reply(cluster.pause(paused));
    }

    @PostMapping("/api/admin/election")
    public ResponseEntity<Map<String, Object>> election() {
        return reply(cluster.election());
    }

    @PostMapping("/api/chaos/{action}")
    public ResponseEntity<Map<String, Object>> chaos(@PathVariable String action,
            @RequestBody(required = false) Map<String, Object> body) throws Exception {
        Map<String, Object> b = body == null ? Map.of() : body;
        String node = b.containsKey("node") ? String.valueOf(b.get("node")) : null;
        long seconds = ((Number) b.getOrDefault("seconds", 30)).longValue();
        ChaosReply r = switch (action) {
            case "kill-primary" -> cluster.chaosOnPrimary(s -> s.crash(CrashRequest.newBuilder()
                    .setReason("dashboard kill-primary").build()));
            case "kill-worker" -> cluster.chaos(need(node), s -> s.crash(CrashRequest.newBuilder()
                    .setReason("dashboard kill-worker").build()));
            case "latency" -> cluster.chaos(need(node), s -> s.injectLatency(
                    LatencyRequest.newBuilder().setMs(((Number) b.getOrDefault("ms", 500))
                            .longValue()).setDurationS(seconds).build()));
            case "cpu" -> cluster.chaos(need(node), s -> s.spikeCpu(CpuRequest.newBuilder()
                    .setThreads(((Number) b.getOrDefault("threads", 0)).intValue())
                    .setDurationS(seconds).build()));
            case "partition" -> cluster.chaos(need(node), s -> s.isolate(
                    IsolateRequest.newBuilder().setDurationS(seconds).build()));
            case "drain" -> cluster.chaos(need(node), s -> s.drain(DrainRequest.getDefaultInstance()));
            case "burst" -> null;
            default -> throw new IllegalArgumentException("unknown chaos action " + action
                    + "; one of kill-worker, kill-primary, latency, cpu, partition, drain, burst");
        };
        if (r != null) {
            return ResponseEntity.ok(Map.of("action", action, "ok", r.getOk(),
                    "node", r.getNodeId(), "message", r.getMessage(), "untilMs", r.getUntilMs()));
        }
        // burst: the client's generator submits the tasks (predisched chaos burst).
        int tasks = ((Number) b.getOrDefault("tasks", 100)).intValue();
        String profile = String.valueOf(b.getOrDefault("profile", "bursty"));
        if (!profile.matches("[a-z_]+")) {
            throw new IllegalArgumentException("bad profile " + profile);
        }
        List<String> command = new ArrayList<>(List.of(jobs.java(), "-jar",
                "predisched-client/target/predisched-client.jar"));
        if (props.getClusterConfig() != null && !props.getClusterConfig().isBlank()) {
            command.addAll(List.of("--config", props.getClusterConfig()));
        } else {
            String[] hp = cluster.schedulers().get(0).split(":");
            command.addAll(List.of("--host", hp[0], "--port", hp[1]));
        }
        command.addAll(List.of("chaos", "burst", String.valueOf(tasks), "--profile", profile));
        Jobs.Result result = jobs.run(command, jobs.root().toFile(), 300_000);
        return ResponseEntity.status(result.exitCode() == 0 ? HttpStatus.OK
                : HttpStatus.BAD_GATEWAY).body(Map.of("action", "burst",
                "ok", result.exitCode() == 0, "message", result.output().strip()));
    }

    private static String need(String node) {
        if (node == null || node.isBlank()) {
            throw new IllegalArgumentException("this chaos action needs {\"node\": \"...\"}");
        }
        return node;
    }
}
