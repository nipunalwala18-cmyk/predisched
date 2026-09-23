package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;

/**
 * Estimates π by random sampling: {@code samples} points in the unit square, counting how many
 * fall in the quarter circle. Embarrassingly parallel and a good MPI/thread-pool demo (spec §7.2).
 * The {@code seed} makes the estimate deterministic and verifiable.
 */
public class MonteCarloTaskExecutor implements TaskExecutor {

    @Override
    public TaskType type() {
        return TaskType.MONTE_CARLO_TASK;
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
        Map<String, String> params = InputParser.parse(input);
        long samples = Long.parseLong(params.get("samples"));
        long seed = params.containsKey("seed") ? Long.parseLong(params.get("seed")) : 42L;
        Random random = new Random(seed);
        long inside = 0;
        for (long i = 0; i < samples; i++) {
            if ((i & 0xFFFFF) == 0 && Thread.currentThread().isInterrupted()) {
                throw new CancellationException("interrupted after " + i + " samples");
            }
            double x = random.nextDouble();
            double y = random.nextDouble();
            if (x * x + y * y <= 1.0) {
                inside++;
            }
        }
        double estimate = 4.0 * inside / samples;
        return ExecutionResult.success(String.format(Locale.ROOT,
                "samples=%d seed=%d inside=%d pi_estimate=%.6f", samples, seed, inside, estimate));
    }
}
