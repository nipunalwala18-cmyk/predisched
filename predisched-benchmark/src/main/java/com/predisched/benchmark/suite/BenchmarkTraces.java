package com.predisched.benchmark.suite;

import com.predisched.client.workload.ArrivalPattern;
import com.predisched.client.workload.TraceEntry;
import com.predisched.client.workload.TraceIo;
import com.predisched.client.workload.WorkloadGenerator;
import com.predisched.client.workload.WorkloadProfile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The suite's traces (prompt 20): generated once into {@code workloads/benchmark/} with the
 * scenario seeds, then only ever read back. {@code manifest.txt} records each file's SHA-256;
 * a run refuses a trace whose checksum no longer matches, so every run of every strategy
 * replays byte-identical input.
 */
public final class BenchmarkTraces {

    public static final String MANIFEST = "manifest.txt";

    private BenchmarkTraces() {}

    /** Generates missing traces and records them; existing ones are left alone. */
    public static Map<String, String> ensure(SuiteConfig config, Path root) throws IOException {
        Path dir = root.resolve(config.traceDir());
        Files.createDirectories(dir);
        Map<String, String> manifest = readManifest(dir);
        Map<String, WorkloadProfile> profiles = WorkloadProfile.load(
                root.resolve(config.profilesFile()), config.httpBaseUrl());
        boolean changed = false;
        for (SuiteConfig.Scenario s : config.scenarios()) {
            Path trace = root.resolve(s.traceFile(config.traceDir()));
            String name = trace.getFileName().toString();
            if (!Files.exists(trace)) {
                WorkloadProfile profile = profiles.get(s.profile());
                if (profile == null) {
                    throw new IllegalArgumentException("unknown profile " + s.profile());
                }
                List<TraceEntry> entries = WorkloadGenerator.generate(profile,
                        ArrivalPattern.parse(s.pattern()), s.tasks(), s.seed(), s.rate());
                TraceIo.writeTrace(trace, entries);
                manifest.put(name, sha256(trace));
                changed = true;
            } else if (!manifest.containsKey(name)) {
                manifest.put(name, sha256(trace));
                changed = true;
            }
        }
        if (changed) {
            writeManifest(dir, manifest);
        }
        return manifest;
    }

    /** The trace of a scenario, after checking it is byte-identical to the manifest's. */
    public static List<TraceEntry> read(SuiteConfig config, Path root, SuiteConfig.Scenario s)
            throws IOException {
        Path trace = root.resolve(s.traceFile(config.traceDir()));
        Map<String, String> manifest = readManifest(root.resolve(config.traceDir()));
        String expected = manifest.get(trace.getFileName().toString());
        String actual = sha256(trace);
        if (expected == null || !expected.equals(actual)) {
            throw new IllegalStateException(trace + " does not match " + MANIFEST + " (expected "
                    + expected + ", found " + actual + "): benchmark traces are replayed,"
                    + " never regenerated");
        }
        return TraceIo.readTrace(trace);
    }

    public static String sha256(Path file) throws IOException {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static Map<String, String> readManifest(Path dir) throws IOException {
        Map<String, String> manifest = new TreeMap<>();
        Path file = dir.resolve(MANIFEST);
        if (Files.exists(file)) {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length == 2 && !line.startsWith("#")) {
                    manifest.put(parts[1], parts[0]);
                }
            }
        }
        return manifest;
    }

    private static void writeManifest(Path dir, Map<String, String> manifest) throws IOException {
        StringBuilder out = new StringBuilder("# SHA-256 of each benchmark trace (prompt 20);"
                + " sha256sum -c compatible\n");
        new LinkedHashMap<>(manifest).forEach((name, hash) ->
                out.append(hash).append("  ").append(name).append('\n'));
        Files.writeString(dir.resolve(MANIFEST), out.toString(), StandardCharsets.UTF_8);
    }

    public static Path root() {
        return Paths.get("").toAbsolutePath();
    }
}
