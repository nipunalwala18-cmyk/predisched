package com.predisched.benchmark.suite;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/** {@code configs/benchmark.yaml} (prompt 20): scenarios, strategies, worker sets, controls. */
public record SuiteConfig(
        String suite,
        String nodeConfig,
        String profilesFile,
        String traceDir,
        String httpBaseUrl,
        List<String> strategies,
        int reps,
        double speed,
        double warmupFraction,
        long deadlineMs,
        long runTimeoutMs,
        Map<String, List<WorkerSpec>> workerSets,
        List<Scenario> scenarios) {

    public record WorkerSpec(String id, int port, int poolSize, double slowdown) {}

    public record Scenario(String name, String profile, String pattern, double rate, int tasks,
            long seed, String workers, String killWorker, long killAtMs) {

        public String traceFile(String traceDir) {
            return traceDir + "/" + name + "-" + profile + "-" + pattern + "-" + seed + ".jsonl";
        }
    }

    @SuppressWarnings("unchecked")
    public static SuiteConfig load(Path path) throws IOException {
        Map<String, Object> y;
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            y = new Yaml().load(reader);
        }
        Map<String, List<WorkerSpec>> sets = new java.util.LinkedHashMap<>();
        ((Map<String, List<Map<String, Object>>>) y.get("workerSets")).forEach((name, list) -> {
            List<WorkerSpec> specs = new ArrayList<>();
            for (Map<String, Object> w : list) {
                specs.add(new WorkerSpec((String) w.get("id"), num(w.get("port")).intValue(),
                        num(w.get("poolSize")).intValue(), num(w.get("slowdown")).doubleValue()));
            }
            sets.put(name, specs);
        });
        List<Scenario> scenarios = new ArrayList<>();
        for (Map<String, Object> s : (List<Map<String, Object>>) y.get("scenarios")) {
            String workers = (String) s.get("workers");
            if (!sets.containsKey(workers)) {
                throw new IllegalArgumentException("scenario " + s.get("name")
                        + " uses unknown worker set " + workers);
            }
            scenarios.add(new Scenario((String) s.get("name"), (String) s.get("profile"),
                    (String) s.get("pattern"), num(s.get("rate")).doubleValue(),
                    num(s.get("tasks")).intValue(), num(s.get("seed")).longValue(), workers,
                    (String) s.getOrDefault("killWorker", ""),
                    num(s.getOrDefault("killAtMs", 0)).longValue()));
        }
        return new SuiteConfig((String) y.get("suite"), (String) y.get("nodeConfig"),
                (String) y.get("profilesFile"), (String) y.get("traceDir"),
                (String) y.get("httpBaseUrl"), (List<String>) y.get("strategies"),
                num(y.get("reps")).intValue(), num(y.get("speed")).doubleValue(),
                num(y.get("warmupFraction")).doubleValue(), num(y.get("deadlineMs")).longValue(),
                num(y.get("runTimeoutMs")).longValue(), sets, scenarios);
    }

    private static Number num(Object value) {
        return value instanceof Number n ? n : Double.parseDouble(String.valueOf(value));
    }

    public Scenario scenario(String name) {
        return scenarios.stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }
}
