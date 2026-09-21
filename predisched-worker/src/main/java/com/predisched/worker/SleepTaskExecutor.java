package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskInputSpec;
import com.predisched.common.TaskExecutor;
import com.predisched.proto.TaskType;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sleeps for a fixed time. The control case: known duration, near-zero resource use.
 *
 * <p>{@code failRate} and {@code seed} make it fail on purpose and reproducibly, which is what the
 * retry, dead-letter and (later) fault-tolerance demos need. The seed is derived from the task's
 * own input, so the same input always fails the same way (rule 8).
 */
public class SleepTaskExecutor implements TaskExecutor {

    /** Counts attempts so a retried task draws a new number instead of failing identically. */
    private final AtomicLong attempts = new AtomicLong();

    @Override
    public TaskType type() {
        return TaskType.SLEEP_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.IO_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        long ms = Long.parseLong(params.get("ms"));
        double failRate = params.containsKey("failRate")
                ? Double.parseDouble(params.get("failRate"))
                : 0.0;
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ExecutionResult.failure("interrupted after sleep request ms=" + ms);
        }
        if (failRate > 0) {
            long seed = params.containsKey("seed")
                    ? Long.parseLong(params.get("seed"))
                    : input.hashCode();
            // Draw once per attempt: a failRate of 1.0 always fails, 0.5 fails about half the
            // time across different tasks while staying the same for one input.
            double draw = new Random(seed + attempts.incrementAndGet()).nextDouble();
            if (draw < failRate) {
                return ExecutionResult.failure(
                        String.format(Locale.ROOT, "injected failure (failRate=%.2f, draw=%.3f)",
                                failRate, draw));
            }
        }
        return ExecutionResult.success("slept_ms=" + ms);
    }
}
