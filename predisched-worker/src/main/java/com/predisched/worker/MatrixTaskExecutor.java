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
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;

/**
 * Matrix multiplication of two seeded random matrices.
 *
 * <p>With {@code threads=k} the row blocks are multiplied on a {@link ForkJoinPool} of k threads,
 * which gives the thread-pool demo a CPU-heavy parallel task (lab Exp 2). The products are summed
 * afterwards in row-major order, so the checksum is identical however many threads ran, and stays
 * comparable with the MPI implementation in prompt 14.
 */
public class MatrixTaskExecutor implements TaskExecutor {

    static final long SEED = 42L;

    @Override
    public TaskType type() {
        return TaskType.MATRIX_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.PARALLEL;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        int size = Integer.parseInt(params.get("size"));
        int threads = params.containsKey("threads")
                ? Integer.parseInt(params.get("threads"))
                : 1;
        double checksum = threads > 1
                ? multiplyAndChecksum(size, threads)
                : multiplyAndChecksum(size);
        String output = String.format(Locale.ROOT, "size=%d threads=%d checksum=%.6f",
                size, threads, checksum);
        return ExecutionResult.success(output);
    }

    /** The {@code checksum=...} part of an output line, for comparing runs. */
    public static String checksumOf(String output) {
        int at = output.indexOf("checksum=");
        return at < 0 ? output : output.substring(at);
    }

    /** Row blocks in parallel, summed afterwards in the same order as the sequential version. */
    static double multiplyAndChecksum(int size, int threads) {
        double[][] a = new double[size][size];
        double[][] b = new double[size][size];
        fill(a, b, size);
        double[][] c = new double[size][size];
        ForkJoinPool pool = new ForkJoinPool(threads);
        try {
            pool.submit(() -> IntStream.range(0, size).parallel().forEach(i -> {
                for (int j = 0; j < size; j++) {
                    double acc = 0.0;
                    for (int k = 0; k < size; k++) {
                        acc += a[i][k] * b[k][j];
                    }
                    c[i][j] = acc;
                }
            })).join();
        } finally {
            pool.shutdown();
        }
        double sum = 0.0;
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                sum += c[i][j];
            }
        }
        return sum;
    }

    private static void fill(double[][] a, double[][] b, int size) {
        Random random = new Random(SEED);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                a[i][j] = random.nextDouble();
                b[i][j] = random.nextDouble();
            }
        }
    }

    static double multiplyAndChecksum(int size) {
        Random random = new Random(SEED);
        double[][] a = new double[size][size];
        double[][] b = new double[size][size];
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                a[i][j] = random.nextDouble();
                b[i][j] = random.nextDouble();
            }
        }
        double sum = 0.0;
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                double acc = 0.0;
                for (int k = 0; k < size; k++) {
                    acc += a[i][k] * b[k][j];
                }
                sum += acc;
            }
        }
        return sum;
    }
}
