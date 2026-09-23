package com.predisched.client.workload;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/**
 * A named workload profile from {@code configs/workloads.yaml}: task mix weights, size ranges,
 * priority and timeout distributions (spec §7.3).
 */
public class WorkloadProfile {

    /** One row of a profile's task mix: type, selection weight and size-knob range. */
    public record TaskMix(String typeName, double weight, long sizeMin, long sizeMax) {}

    private final String name;
    private final String description;
    private final List<TaskMix> tasks;
    private final int priorityMin;
    private final int priorityMax;
    private final double highFraction;
    private final int highMin;
    private final double timeoutFraction;
    private final long timeoutMinMs;
    private final long timeoutMaxMs;
    private final String httpBaseUrl;

    @SuppressWarnings("unchecked")
    public static Map<String, WorkloadProfile> load(Path path, String httpBaseUrlOverride)
            throws IOException {
        Map<String, Object> root;
        try (InputStream in = Files.newInputStream(path)) {
            root = new Yaml().load(in);
        }
        if (root == null || !root.containsKey("profiles")) {
            throw new IllegalArgumentException("workload config has no 'profiles': " + path);
        }
        String baseUrl = httpBaseUrlOverride != null ? httpBaseUrlOverride
                : String.valueOf(root.getOrDefault("httpBaseUrl", "http://localhost:8100"));
        Map<String, WorkloadProfile> profiles = new LinkedHashMap<>();
        Map<String, Object> rawProfiles = (Map<String, Object>) root.get("profiles");
        for (Map.Entry<String, Object> entry : rawProfiles.entrySet()) {
            profiles.put(entry.getKey(),
                    new WorkloadProfile(entry.getKey(), (Map<String, Object>) entry.getValue(),
                            baseUrl));
        }
        return profiles;
    }

    @SuppressWarnings("unchecked")
    private WorkloadProfile(String name, Map<String, Object> raw, String httpBaseUrl) {
        this.name = name;
        this.description = String.valueOf(raw.getOrDefault("description", ""));
        this.httpBaseUrl = httpBaseUrl;
        this.tasks = new ArrayList<>();
        for (Map<String, Object> task : (List<Map<String, Object>>) raw.get("tasks")) {
            Map<String, Object> size = (Map<String, Object>) task.get("size");
            tasks.add(new TaskMix(
                    String.valueOf(task.get("type")),
                    ((Number) task.get("weight")).doubleValue(),
                    ((Number) size.get("min")).longValue(),
                    ((Number) size.get("max")).longValue()));
        }
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("profile '" + name + "' has no tasks");
        }
        Map<String, Object> priority =
                (Map<String, Object>) raw.getOrDefault("priority", Map.of());
        this.priorityMin = number(priority, "min", 1).intValue();
        this.priorityMax = number(priority, "max", 10).intValue();
        this.highFraction = number(priority, "highFraction", 0.0).doubleValue();
        this.highMin = number(priority, "highMin", 8).intValue();
        Map<String, Object> timeout =
                (Map<String, Object>) raw.getOrDefault("timeout", Map.of());
        this.timeoutFraction = number(timeout, "fraction", 0.0).doubleValue();
        this.timeoutMinMs = number(timeout, "minMs", 0).longValue();
        this.timeoutMaxMs = number(timeout, "maxMs", 0).longValue();
    }

    private static Number number(Map<String, Object> map, String key, double fallback) {
        Object value = map.get(key);
        return value instanceof Number number ? number : fallback;
    }

    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public List<TaskMix> tasks() {
        return tasks;
    }

    public int priorityMin() {
        return priorityMin;
    }

    public int priorityMax() {
        return priorityMax;
    }

    public double highFraction() {
        return highFraction;
    }

    public int highMin() {
        return highMin;
    }

    public double timeoutFraction() {
        return timeoutFraction;
    }

    public long timeoutMinMs() {
        return timeoutMinMs;
    }

    public long timeoutMaxMs() {
        return timeoutMaxMs;
    }

    public String httpBaseUrl() {
        return httpBaseUrl;
    }
}
