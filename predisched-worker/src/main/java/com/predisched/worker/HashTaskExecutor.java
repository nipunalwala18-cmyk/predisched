package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CancellationException;

/**
 * Repeated SHA-256 hashing of a fixed payload. Very steady per-round cost, so execution time
 * scales linearly with {@code rounds} and prediction should be near-exact (spec §7.2).
 */
public class HashTaskExecutor implements TaskExecutor {

    private static final byte[] PAYLOAD = "predisched-hash-payload".getBytes(
            java.nio.charset.StandardCharsets.UTF_8);

    @Override
    public TaskType type() {
        return TaskType.HASH_TASK;
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
        int rounds = Integer.parseInt(params.get("rounds"));
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] digest = PAYLOAD;
            for (int i = 0; i < rounds; i++) {
                if ((i & 0xFFFF) == 0 && Thread.currentThread().isInterrupted()) {
                    throw new CancellationException("interrupted after " + i + " rounds");
                }
                digest = sha256.digest(digest);
            }
            return ExecutionResult.success(
                    "rounds=" + rounds + " digest=" + HexFormat.of().formatHex(digest));
        } catch (CancellationException e) {
            throw e;
        } catch (Exception e) {
            return ExecutionResult.failure("hashing failed: " + e.getMessage());
        }
    }
}
