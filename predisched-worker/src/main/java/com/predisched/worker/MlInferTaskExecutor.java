package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@code ML_INFER_TASK model=exec_time|queue_forecast|overload, batch=n, [seed=s]} (spec §7.2,
 * prompt 16): runs {@code python -m predisched_ml.infer} in {@code ml.workDir}. The Python side
 * loads the live model, predicts on a seeded batch of dataset rows and prints one JSON line. The
 * task returns its load and predict timings and a checksum of the predictions.
 *
 * <p>Disabled until {@code ml.python} is configured. Not cached: the answer depends on which model
 * version is live.
 */
public class MlInferTaskExecutor implements TaskExecutor {

    /** Where the ML package is. An empty {@code python} disables the executor. */
    public record Settings(String python, Path workDir, long timeoutMs) {

        public static final Settings DISABLED = new Settings("", Paths.get("ml"), 120_000);

        public boolean enabled() {
            return python != null && !python.isBlank();
        }
    }

    static final long SEED = 42L;
    private static final int OUTPUT_LINES_KEPT = 20;

    private final Settings settings;

    public MlInferTaskExecutor(Settings settings) {
        this.settings = settings;
    }

    @Override
    public TaskType type() {
        return TaskType.ML_INFER_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.CPU_BOUND;
    }

    @Override
    public boolean deterministic() {
        return false;
    }

    /** The command for one batch. */
    public List<String> command(String model, int batch, long seed) {
        List<String> command = new ArrayList<>();
        command.add(settings.python());
        command.add("-m");
        command.add("predisched_ml.infer");
        command.add("--model");
        command.add(model);
        command.add("--batch");
        command.add(Integer.toString(batch));
        command.add("--seed");
        command.add(Long.toString(seed));
        command.add("--json");
        return command;
    }

    @Override
    public ExecutionResult execute(String input) throws Exception {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        if (!settings.enabled()) {
            return ExecutionResult.failure(
                    "ML_INFER_TASK is disabled on this worker: ml.python is not configured");
        }
        Map<String, String> params = InputParser.parse(input);
        String model = params.get("model");
        int batch = Integer.parseInt(params.get("batch"));
        long seed = params.containsKey("seed") ? Long.parseLong(params.get("seed")) : SEED;

        ProcessBuilder builder = new ProcessBuilder(command(model, batch, seed))
                .directory(settings.workDir().toFile())
                .redirectErrorStream(true);
        builder.environment().put("PYTHONPATH", settings.workDir().toAbsolutePath().toString());
        builder.environment().put("PYTHONIOENCODING", "utf-8");
        Process process = builder.start();
        Deque<String> tail = new ArrayDeque<>();
        String[] json = {null};
        Thread reader = new Thread(() -> {
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.startsWith("{") && line.contains("\"checksum\"")) {
                        json[0] = line;
                    }
                    synchronized (tail) {
                        tail.addLast(line);
                        if (tail.size() > OUTPUT_LINES_KEPT) {
                            tail.removeFirst();
                        }
                    }
                }
            } catch (Exception e) {
                // The process was killed; what was read is kept.
            }
        }, "ml-infer-output");
        reader.setDaemon(true);
        reader.start();
        try {
            if (!process.waitFor(settings.timeoutMs(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                return ExecutionResult.failure(
                        "predisched_ml.infer timed out after " + settings.timeoutMs() + " ms");
            }
        } catch (InterruptedException e) {
            // Cancelled (FR26).
            process.destroyForcibly();
            throw e;
        }
        reader.join(5_000);
        if (process.exitValue() != 0 || json[0] == null) {
            String lastLines;
            synchronized (tail) {
                lastLines = String.join(" | ", tail);
            }
            return ExecutionResult.failure("predisched_ml.infer exited " + process.exitValue()
                    + ": " + lastLines);
        }
        Map<String, String> fields = MatrixTaskExecutor.parseJson(json[0]);
        return ExecutionResult.success(String.format(java.util.Locale.ROOT,
                "model=%s version=%s batch=%d seed=%d load_ms=%s predict_ms=%s per_row_us=%s"
                        + " checksum=%s",
                model, fields.get("version"), batch, seed, fields.get("load_ms"),
                fields.get("predict_ms"), fields.get("per_row_us"), fields.get("checksum")));
    }
}
