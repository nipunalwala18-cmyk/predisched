package com.predisched.client.workload;

import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Seeded workload generator (F8, rule 8). Every draw — task type, size, enums, priority,
 * timeout, arrivals — comes from one {@link Random}, so the same seed always yields the same
 * trace file byte for byte.
 */
public final class WorkloadGenerator {

    private static final List<String> SORT_ORDERS = List.of("random", "sorted", "reversed");
    private static final List<String> GRAPH_ALGOS = List.of("bfs", "pagerank");
    private static final List<String> FILE_IO_MODES = List.of("write", "read", "both");

    private WorkloadGenerator() {}

    /**
     * Generates {@code taskCount} entries for a profile and arrival pattern.
     *
     * @param ratePerSec base arrival rate in tasks per second (must be positive)
     */
    public static List<TraceEntry> generate(
            WorkloadProfile profile,
            ArrivalPattern pattern,
            int taskCount,
            long seed,
            double ratePerSec) {
        if (taskCount <= 0) {
            throw new IllegalArgumentException("tasks must be positive");
        }
        if (ratePerSec <= 0) {
            throw new IllegalArgumentException("rate must be positive");
        }
        Random random = new Random(seed);
        List<TraceEntry> entries = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            WorkloadProfile.TaskMix mix = pickType(profile, random);
            long size = mix.sizeMin()
                    + (long) (random.nextDouble() * (mix.sizeMax() - mix.sizeMin() + 1));
            TaskType type = TaskType.valueOf(mix.typeName());
            entries.add(new TraceEntry(
                    0,
                    String.format(Locale.ROOT, "%s-%d-%05d", profile.name(), seed, i),
                    type,
                    buildInput(profile, type, size, random),
                    drawPriority(profile, random),
                    drawTimeout(profile, random)));
        }
        List<Long> offsets = arrivalOffsets(pattern, taskCount, ratePerSec, random);
        List<TraceEntry> timed = new ArrayList<>(taskCount);
        for (int i = 0; i < taskCount; i++) {
            TraceEntry entry = entries.get(i);
            timed.add(new TraceEntry(
                    offsets.get(i), entry.taskId(), entry.type(), entry.input(),
                    entry.priority(), entry.timeoutMs()));
        }
        timed.sort(Comparator.comparingLong(TraceEntry::offsetMs));
        return timed;
    }

    private static WorkloadProfile.TaskMix pickType(WorkloadProfile profile, Random random) {
        double total = 0;
        for (WorkloadProfile.TaskMix mix : profile.tasks()) {
            total += mix.weight();
        }
        double draw = random.nextDouble() * total;
        for (WorkloadProfile.TaskMix mix : profile.tasks()) {
            draw -= mix.weight();
            if (draw < 0) {
                return mix;
            }
        }
        return profile.tasks().get(profile.tasks().size() - 1);
    }

    /** Builds the executor input for a type; the knob names must match TaskInputSpec. */
    static String buildInput(
            WorkloadProfile profile, TaskType type, long size, Random random) {
        return switch (type) {
            case CPU_TASK -> "n=" + size;
            case HASH_TASK -> "rounds=" + size;
            case MONTE_CARLO_TASK -> "samples=" + size + ", seed=" + random.nextInt(1_000_000);
            case MATRIX_TASK -> "size=" + size;
            case SORT_TASK -> "n=" + size + ", type=" + pick(SORT_ORDERS, random);
            case COMPRESS_TASK -> "size_mb=" + size + ", level=" + (1 + random.nextInt(9));
            case GRAPH_TASK -> "nodes=" + size + ", algo=" + pick(GRAPH_ALGOS, random)
                    + ", seed=" + random.nextInt(1_000_000);
            case FILE_IO_TASK -> "size_mb=" + size + ", mode=" + pick(FILE_IO_MODES, random);
            case HTTP_TASK -> "url=" + profile.httpBaseUrl() + "/delay?ms=" + size
                    + ", timeout=" + Math.min(120_000L, size * 10 + 2_000);
            case SLEEP_TASK -> "ms=" + size;
            default -> throw new IllegalArgumentException(
                    "profile must not contain " + type + ": it has no executor yet");
        };
    }

    private static String pick(List<String> options, Random random) {
        return options.get(random.nextInt(options.size()));
    }

    private static int drawPriority(WorkloadProfile profile, Random random) {
        if (random.nextDouble() < profile.highFraction()) {
            return profile.highMin() + random.nextInt(10 - profile.highMin() + 1);
        }
        return profile.priorityMin()
                + random.nextInt(profile.priorityMax() - profile.priorityMin() + 1);
    }

    private static long drawTimeout(WorkloadProfile profile, Random random) {
        if (random.nextDouble() < profile.timeoutFraction()) {
            long span = profile.timeoutMaxMs() - profile.timeoutMinMs() + 1;
            return profile.timeoutMinMs() + (long) (random.nextDouble() * span);
        }
        return 0;
    }

    /**
     * Arrival offsets in ms, sorted ascending. Steady is Poisson at the base rate; bursty spreads
     * 10% of tasks over the first 60% of the window and floods the rest; periodic thins a fast
     * Poisson process by a sine-modulated acceptance probability.
     */
    static List<Long> arrivalOffsets(
            ArrivalPattern pattern, int taskCount, double ratePerSec, Random random) {
        return switch (pattern) {
            case STEADY -> steady(taskCount, ratePerSec, random);
            case BURSTY -> bursty(taskCount, ratePerSec, random);
            case PERIODIC -> periodic(taskCount, ratePerSec, random);
        };
    }

    private static List<Long> steady(int taskCount, double ratePerSec, Random random) {
        List<Long> offsets = new ArrayList<>(taskCount);
        double now = 0;
        for (int i = 0; i < taskCount; i++) {
            now += -Math.log(1.0 - random.nextDouble()) / ratePerSec;
            offsets.add(Math.round(now * 1000));
        }
        return offsets;
    }

    private static List<Long> bursty(int taskCount, double ratePerSec, Random random) {
        double windowMs = taskCount / ratePerSec * 1000;
        double floodAt = windowMs * 0.6;
        int trickle = (int) (taskCount * 0.1);
        List<Long> offsets = new ArrayList<>(taskCount);
        for (int i = 0; i < trickle; i++) {
            offsets.add((long) (random.nextDouble() * floodAt));
        }
        for (int i = trickle; i < taskCount; i++) {
            offsets.add(Math.round(floodAt + random.nextDouble() * (windowMs - floodAt)));
        }
        offsets.sort(Long::compare);
        return offsets;
    }

    private static List<Long> periodic(int taskCount, double ratePerSec, Random random) {
        double amplitude = 0.7;
        double windowSec = taskCount / ratePerSec;
        double periodSec = Math.max(1.0, windowSec / 3);
        double maxRate = ratePerSec * (1 + amplitude);
        List<Long> offsets = new ArrayList<>(taskCount);
        double now = 0;
        int guard = 0;
        while (offsets.size() < taskCount && guard++ < taskCount * 1000) {
            now += -Math.log(1.0 - random.nextDouble()) / maxRate;
            double instant = (1 + amplitude * Math.sin(2 * Math.PI * now / periodSec)) / (1 + amplitude);
            if (random.nextDouble() <= instant) {
                offsets.add((long) Math.round(now * 1000));
            }
        }
        while (offsets.size() < taskCount) {
            now += 1.0 / ratePerSec;
            offsets.add((long) Math.round(now * 1000));
        }
        return offsets;
    }
}
