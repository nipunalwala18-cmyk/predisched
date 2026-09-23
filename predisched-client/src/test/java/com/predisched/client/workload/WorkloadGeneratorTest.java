package com.predisched.client.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.TaskType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Seeded generation, trace file round trips and arrival patterns (F8, rule 8). */
class WorkloadGeneratorTest {

    private static final String PROFILES = """
            httpBaseUrl: "http://localhost:8100"
            profiles:
              tiny:
                description: "test profile"
                tasks:
                  - {type: SLEEP_TASK, weight: 50, size: {min: 5, max: 20}}
                  - {type: CPU_TASK, weight: 50, size: {min: 100, max: 500}}
                priority: {min: 1, max: 10, highFraction: 0.2, highMin: 8}
                timeout: {fraction: 0.0, minMs: 0, maxMs: 0}
              all:
                description: "every executor, equal weight"
                tasks:
                  - {type: CPU_TASK, weight: 10, size: {min: 100, max: 500}}
                  - {type: HASH_TASK, weight: 10, size: {min: 100, max: 500}}
                  - {type: MONTE_CARLO_TASK, weight: 10, size: {min: 100, max: 500}}
                  - {type: MATRIX_TASK, weight: 10, size: {min: 5, max: 20}}
                  - {type: SORT_TASK, weight: 10, size: {min: 100, max: 500}}
                  - {type: COMPRESS_TASK, weight: 10, size: {min: 1, max: 2}}
                  - {type: GRAPH_TASK, weight: 10, size: {min: 100, max: 500}}
                  - {type: FILE_IO_TASK, weight: 10, size: {min: 1, max: 2}}
                  - {type: HTTP_TASK, weight: 10, size: {min: 20, max: 60}}
                  - {type: SLEEP_TASK, weight: 10, size: {min: 5, max: 20}}
                priority: {min: 1, max: 10, highFraction: 0.2, highMin: 8}
                timeout: {fraction: 0.0, minMs: 0, maxMs: 0}
              strict:
                description: "every task timed out and high priority"
                tasks:
                  - {type: SLEEP_TASK, weight: 100, size: {min: 5, max: 5}}
                priority: {min: 1, max: 10, highFraction: 1.0, highMin: 8}
                timeout: {fraction: 1.0, minMs: 2000, maxMs: 3000}
            """;

    private Map<String, WorkloadProfile> profiles(@TempDir Path dir) throws Exception {
        Path path = dir.resolve("workloads.yaml");
        Files.writeString(path, PROFILES, StandardCharsets.UTF_8);
        return WorkloadProfile.load(path, null);
    }

    @Test
    void sameSeedGivesByteIdenticalTrace(@TempDir Path dir) throws Exception {
        Map<String, WorkloadProfile> all = profiles(dir);
        List<TraceEntry> first =
                WorkloadGenerator.generate(all.get("tiny"), ArrivalPattern.BURSTY, 200, 42, 5.0);
        List<TraceEntry> second =
                WorkloadGenerator.generate(all.get("tiny"), ArrivalPattern.BURSTY, 200, 42, 5.0);
        Path a = dir.resolve("a.jsonl");
        Path b = dir.resolve("b.jsonl");
        TraceIo.writeTrace(a, first);
        TraceIo.writeTrace(b, second);
        assertEquals(Files.readString(a), Files.readString(b));
        // And the file reads back into the same entries.
        assertEquals(first, TraceIo.readTrace(a));
    }

    @Test
    void differentSeedGivesDifferentTrace(@TempDir Path dir) throws Exception {
        Map<String, WorkloadProfile> all = profiles(dir);
        List<TraceEntry> first =
                WorkloadGenerator.generate(all.get("tiny"), ArrivalPattern.STEADY, 200, 42, 5.0);
        List<TraceEntry> second =
                WorkloadGenerator.generate(all.get("tiny"), ArrivalPattern.STEADY, 200, 43, 5.0);
        assertNotEquals(first, second);
    }

    @Test
    void mixProportionsMatchConfigWithinFivePercent(@TempDir Path dir) throws Exception {
        Map<String, WorkloadProfile> all = profiles(dir);
        List<TraceEntry> entries =
                WorkloadGenerator.generate(all.get("all"), ArrivalPattern.STEADY, 5_000, 7, 50.0);
        Map<TaskType, Integer> counts = new HashMap<>();
        for (TraceEntry entry : entries) {
            counts.merge(entry.type(), 1, Integer::sum);
        }
        assertEquals(10, counts.size(), "every executor should appear: " + counts);
        for (Map.Entry<TaskType, Integer> count : counts.entrySet()) {
            double share = count.getValue() / 5_000.0;
            assertTrue(Math.abs(share - 0.10) <= 0.05,
                    count.getKey() + " share " + share + " is outside 10% +/- 5%");
        }
    }

    @Test
    void arrivalOffsetsAreSortedAndPatternShaped(@TempDir Path dir) throws Exception {
        Map<String, WorkloadProfile> all = profiles(dir);
        WorkloadProfile tiny = all.get("tiny");
        for (ArrivalPattern pattern : ArrivalPattern.values()) {
            List<TraceEntry> entries = WorkloadGenerator.generate(tiny, pattern, 500, 11, 10.0);
            for (int i = 1; i < entries.size(); i++) {
                assertTrue(entries.get(i).offsetMs() >= entries.get(i - 1).offsetMs(),
                        pattern + " offsets must be sorted");
            }
        }
        // Bursty: the last 10% arrive in the flood, far later on average than the first 10%.
        List<TraceEntry> bursty = WorkloadGenerator.generate(tiny, ArrivalPattern.BURSTY, 500, 11, 10.0);
        double firstMean = bursty.subList(0, 50).stream()
                .mapToLong(TraceEntry::offsetMs).average().orElseThrow();
        double lastMean = bursty.subList(450, 500).stream()
                .mapToLong(TraceEntry::offsetMs).average().orElseThrow();
        assertTrue(lastMean > firstMean * 2,
                "flood should arrive far later: first=" + firstMean + " last=" + lastMean);
        // Steady at 10/s: 500 tasks span roughly 50 s, give or take Poisson noise.
        List<TraceEntry> steady = WorkloadGenerator.generate(tiny, ArrivalPattern.STEADY, 500, 11, 10.0);
        long span = steady.get(499).offsetMs() - steady.get(0).offsetMs();
        assertTrue(span > 20_000 && span < 120_000, "steady span looks wrong: " + span + " ms");
    }

    @Test
    void timeoutAndPriorityDistributionsAreHonoured(@TempDir Path dir) throws Exception {
        Map<String, WorkloadProfile> all = profiles(dir);
        List<TraceEntry> entries =
                WorkloadGenerator.generate(all.get("strict"), ArrivalPattern.STEADY, 100, 3, 50.0);
        for (TraceEntry entry : entries) {
            assertTrue(entry.timeoutMs() >= 2000 && entry.timeoutMs() <= 3000,
                    "timeout out of range: " + entry.timeoutMs());
            assertTrue(entry.priority() >= 8, "priority should be high: " + entry.priority());
        }
    }

    @Test
    void generatedInputsMatchTheTaskTypes(@TempDir Path dir) throws Exception {
        Map<String, WorkloadProfile> all = profiles(dir);
        List<TraceEntry> entries =
                WorkloadGenerator.generate(all.get("all"), ArrivalPattern.STEADY, 300, 9, 50.0);
        for (TraceEntry entry : entries) {
            List<String> errors =
                    com.predisched.common.TaskInputSpec.validate(entry.type(), entry.input());
            assertTrue(errors.isEmpty(),
                    entry.type() + " " + entry.input() + " should validate: " + errors);
        }
    }

    @Test
    void reservedTypeInAProfileFailsFast(@TempDir Path dir) throws Exception {
        Map<String, WorkloadProfile> all = profiles(dir);
        assertThrows(IllegalArgumentException.class, () -> WorkloadGenerator.buildInput(
                all.get("tiny"), TaskType.WORKFLOW_TASK, 1, new Random(1)));
    }

    @Test
    void traceEntryJsonRoundTripsSpecialCharacters() {
        TraceEntry entry = new TraceEntry(123, "a\"b\\c", TaskType.HTTP_TASK,
                "url=http://h/delay?ms=5&x=\"q\", timeout=2000", 7, 1500);
        assertEquals(entry, TraceEntry.fromJson(entry.toJson()));
        // No timeout serialises without the key and reads back as none.
        TraceEntry plain = new TraceEntry(0, "t", TaskType.SLEEP_TASK, "ms=5", 5, 0);
        assertTrue(!plain.toJson().contains("timeout_ms"));
        assertEquals(plain, TraceEntry.fromJson(plain.toJson()));
        assertThrows(IllegalArgumentException.class, () -> TraceEntry.fromJson("not json"));
    }
}
