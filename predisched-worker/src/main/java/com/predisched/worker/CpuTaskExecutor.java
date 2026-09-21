package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskInputSpec;
import com.predisched.common.TaskExecutor;
import com.predisched.proto.TaskType;
import java.util.Map;
import java.util.concurrent.CancellationException;

/** Counts primes below {@code n}. Baseline CPU-bound load. */
public class CpuTaskExecutor implements TaskExecutor {

    @Override
    public TaskType type() {
        return TaskType.CPU_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.CPU_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        int n = Integer.parseInt(InputParser.parse(input).get("n"));
        long count = countPrimesBelow(n);
        return ExecutionResult.success("primes_below_" + n + "=" + count);
    }

    static long countPrimesBelow(int n) {
        boolean[] composite = new boolean[n];
        long count = 0;
        for (int i = 2; i < n; i++) {
            // Checked every 65536 numbers so a cancelled or timed-out task actually stops (FR26).
            if ((i & 0xFFFF) == 0 && Thread.currentThread().isInterrupted()) {
                throw new CancellationException("interrupted after " + i);
            }
            if (!composite[i]) {
                count++;
                if ((long) i * i < n) {
                    for (int j = i * i; j < n; j += i) {
                        composite[j] = true;
                    }
                }
            }
        }
        return count;
    }
}
