package com.predisched.dashboard.web;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.predisched.dashboard.repo.Repositories;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Prediction accuracy and the model registry (spec §15.5; prompts 18 and 21). */
@RestController
public class ModelController {

    private final Repositories repo;
    private final Jobs jobs;
    // meta.json is written by Python, which spells a missing threshold NaN.
    private final ObjectMapper json = JsonMapper.builder()
            .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS).build();

    public ModelController(Repositories repo, Jobs jobs) {
        this.repo = repo;
        this.jobs = jobs;
    }

    @GetMapping("/api/predictions/accuracy")
    public Map<String, Object> accuracy(@RequestParam(defaultValue = "500") int limit) {
        int n = Math.max(1, Math.min(limit, 5_000));
        List<Map<String, Object>> rows = repo.accuracy(n);
        return Map.of("recent", rows, "byType", repo.accuracyByType(n),
                "rollingMaeMs", rows.isEmpty() ? null : rows.get(0).get("rolling_mae_ms"));
    }

    /** registry.json with each version's test metrics, plus the recent drift episodes. */
    @GetMapping("/api/models")
    @SuppressWarnings("unchecked")
    public Map<String, Object> models() throws Exception {
        Path dir = jobs.root().resolve("ml").resolve("models");
        Path registry = dir.resolve("registry.json");
        Map<String, Object> out = new LinkedHashMap<>();
        if (!Files.exists(registry)) {
            out.put("models", Map.of());
            out.put("error", "no " + registry + ": run python -m predisched_ml.train");
            return out;
        }
        Map<String, Object> reg = json.readValue(registry.toFile(), Map.class);
        Map<String, Object> models = new LinkedHashMap<>();
        ((Map<String, Map<String, Object>>) reg.get("models")).forEach((model, entry) -> {
            List<Map<String, Object>> versions = new ArrayList<>();
            for (Map<String, Object> v : (List<Map<String, Object>>) entry.get("versions")) {
                Map<String, Object> version = new LinkedHashMap<>(v);
                version.putIfAbsent("status", v.get("version").equals(entry.get("live"))
                        ? "live" : v.get("version").equals(entry.get("shadow")) ? "shadow"
                                : "retired");
                Path meta = dir.resolve(String.valueOf(v.get("path"))).resolve("meta.json");
                if (Files.exists(meta)) {
                    try {
                        Map<String, Object> m = json.readValue(meta.toFile(), Map.class);
                        version.put("metrics", m.get("metrics"));
                        version.put("reason", ((Map<String, Object>) m.getOrDefault("kept",
                                Map.of())).get("reason"));
                    } catch (Exception ignored) {
                        // metrics left out
                    }
                }
                versions.add(version);
            }
            models.put(model, Map.of("live", String.valueOf(entry.get("live")),
                    "shadow", String.valueOf(entry.get("shadow")), "versions", versions));
        });
        out.put("models", models);
        out.put("drift", repo.events(List.of("DRIFT"), 10));
        return out;
    }

    /** Runs the prompt 21 promotion rule; 409 when it refuses. */
    @PostMapping("/api/models/{version}/promote")
    public ResponseEntity<Map<String, Object>> promote(@PathVariable int version,
            @RequestParam(defaultValue = "m1") String model,
            @RequestParam(defaultValue = "false") boolean force) throws Exception {
        if (!model.matches("m[123]")) {
            throw new IllegalArgumentException("model must be m1, m2 or m3");
        }
        List<String> command = new ArrayList<>(List.of(jobs.python(), "-m",
                "predisched_ml.registry", "promote", model, String.valueOf(version)));
        if (force) {
            command.add("--force");
        }
        Jobs.Result r = jobs.run(command, jobs.root().resolve("ml").toFile(), 120_000);
        Map<String, Object> out = Map.of("model", model, "version", version,
                "promoted", r.exitCode() == 0, "output", r.output().strip());
        return ResponseEntity.status(r.exitCode() == 0 ? HttpStatus.OK : HttpStatus.CONFLICT)
                .body(out);
    }
}
