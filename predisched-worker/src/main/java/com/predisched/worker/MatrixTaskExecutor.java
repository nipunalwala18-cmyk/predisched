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

/**
 * Single-threaded matrix multiplication of two seeded random matrices.
 * Output is the checksum of C, which is deterministic for a fixed size.
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
        int size = Integer.parseInt(InputParser.parse(input).get("size"));
        double checksum = multiplyAndChecksum(size);
        String output = String.format(Locale.ROOT, "size=%d checksum=%.6f", size, checksum);
        return ExecutionResult.success(output);
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
