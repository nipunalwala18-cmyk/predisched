package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;

/**
 * Generates an int array of {@code n} elements and sorts it. Allocation-heavy, so it shows memory
 * pressure and GC effects that a CPU-only view of the worker would miss (spec §7.2).
 *
 * <p>{@code type} selects the starting order: {@code random} (seeded, the default),
 * {@code sorted} or {@code reversed}. The output checksum (sum, first and last element) is
 * identical for all three, which verifies the sort.
 */
public class SortTaskExecutor implements TaskExecutor {

    @Override
    public TaskType type() {
        return TaskType.SORT_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.MEMORY_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        int n = Integer.parseInt(params.get("n"));
        String order = params.getOrDefault("type", "random");
        int[] data = new int[n];
        switch (order) {
            case "sorted" -> {
                for (int i = 0; i < n; i++) {
                    data[i] = i;
                }
            }
            case "reversed" -> {
                for (int i = 0; i < n; i++) {
                    data[i] = n - i;
                }
            }
            default -> {
                Random random = new Random(42L);
                for (int i = 0; i < n; i++) {
                    if ((i & 0xFFFFF) == 0 && Thread.currentThread().isInterrupted()) {
                        throw new CancellationException("interrupted while generating");
                    }
                    data[i] = random.nextInt();
                }
            }
        }
        Arrays.sort(data);
        long sum = 0;
        for (int i = 1; i < n; i++) {
            if (data[i] < data[i - 1]) {
                return ExecutionResult.failure("sort verification failed at index " + i);
            }
            sum += data[i];
        }
        sum += data[0];
        return ExecutionResult.success(
                "n=" + n + " type=" + order + " sorted=true sum=" + sum
                        + " first=" + data[0] + " last=" + data[n - 1]);
    }
}
