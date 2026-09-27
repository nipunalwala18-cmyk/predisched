package com.predisched.dashboard.web;

import com.predisched.dashboard.cluster.Cluster;
import com.predisched.dashboard.cluster.Protos;
import com.predisched.dashboard.repo.Repositories;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskType;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Tasks (spec §15.4), the queue forecast, and REST task submission (F24). */
@RestController
public class TaskController {

    private final Cluster cluster;
    private final Repositories repo;

    public TaskController(Cluster cluster, Repositories repo) {
        this.cluster = cluster;
        this.repo = repo;
    }

    @GetMapping("/api/tasks")
    public Map<String, Object> tasks(@RequestParam(required = false) String status,
            @RequestParam(required = false) String type,
            @RequestParam(defaultValue = "200") int limit) {
        return Map.of("tasks", repo.tasks(status, type, limit));
    }

    /** One task: its row, lifecycle events, every placement decision and prediction. */
    @GetMapping("/api/tasks/{id}")
    public ResponseEntity<Map<String, Object>> task(@PathVariable String id) {
        Map<String, Object> row = repo.task(id);
        if (row == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "unknown task " + id));
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("task", row);
        out.put("lifecycle", repo.taskEvents(id));
        out.put("decisions", repo.decisions(id));
        out.put("predictions", repo.taskPredictions(id));
        try {
            out.put("explanation", Protos.explanation(cluster.explain(id)));
        } catch (RuntimeException e) {
            out.put("explanation", null);   // the stored decisions above still explain it
        }
        return ResponseEntity.ok(out);
    }

    /** {@code POST /api/tasks}: forwarded to SubmitTask with the caller's Authorization. */
    @PostMapping("/api/tasks")
    public ResponseEntity<Map<String, Object>> submit(@RequestBody Map<String, Object> body,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        String type = String.valueOf(body.getOrDefault("type", "")).toUpperCase();
        TaskType taskType;
        try {
            taskType = TaskType.valueOf(type);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown task type '" + type + "'");
        }
        String id = String.valueOf(body.getOrDefault("taskId",
                "api-" + UUID.randomUUID().toString().substring(0, 8)));
        TaskRequest request = TaskRequest.newBuilder()
                .setTaskId(id)
                .setType(taskType)
                .setInput(String.valueOf(body.getOrDefault("input", "")))
                .setPriority(((Number) body.getOrDefault("priority", 5)).intValue())
                .setDeadlineMs(((Number) body.getOrDefault("deadlineMs", 0)).longValue())
                .setTimeoutMs(((Number) body.getOrDefault("timeoutMs", 0)).longValue())
                .setMaxRetries(((Number) body.getOrDefault("maxRetries", -1)).intValue())
                .build();
        TaskResponse response = cluster.submit(request, authorization);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", response.getTaskId());
        out.put("accepted", response.getAccepted());
        out.put("message", response.getMessage());
        return ResponseEntity.status(response.getAccepted() ? HttpStatus.ACCEPTED
                : HttpStatus.BAD_REQUEST).body(out);
    }

    /** Actual queued tasks per second next to the M2 forecast for that second. */
    @GetMapping("/api/queue/forecast")
    public Map<String, Object> forecast(@RequestParam(defaultValue = "5") int minutes) {
        int m = Math.max(1, Math.min(minutes, 120));
        return Map.of("minutes", m, "horizonSeconds", 5, "actual", repo.queueActual(m),
                "predicted", repo.queuePredicted(m));
    }
}
