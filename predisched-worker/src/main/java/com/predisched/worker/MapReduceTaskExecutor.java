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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code MAPREDUCE_TASK dataset=<csv or jsonl>, job=avg_exec|wordcount} (spec §7.2, lab Exp 7,
 * prompt 12): runs one of the PySpark jobs in {@code spark/predisched_spark} with
 * {@code spark-submit --master local[*]} as a subprocess, with a timeout, and returns the output
 * directory and the row counts the job reports on its {@code ROWS ...} line.
 *
 * <p>Disabled until {@code spark.home} is configured: such a task fails validation here with that
 * reason. Not deterministic for the cache: the answer depends on the dataset file's contents.
 */
public class MapReduceTaskExecutor implements TaskExecutor {

    /** Where Spark and the jobs are. An empty {@code sparkHome} disables the executor. */
    public record Settings(String sparkHome, String python, Path jobsDir, Path outputDir,
            long timeoutMs) {

        public static final Settings DISABLED =
                new Settings("", "", Paths.get("spark", "predisched_spark"),
                        Paths.get("spark", "out", "tasks"), 600_000);
    }

    private static final Map<String, String> SCRIPTS =
            Map.of("avg_exec", "exec_stats.py", "wordcount", "wordcount.py");
    private static final int OUTPUT_LINES_KEPT = 20;

    private final Settings settings;
    private final AtomicLong runs = new AtomicLong();

    public MapReduceTaskExecutor(Settings settings) {
        this.settings = settings;
    }

    @Override
    public TaskType type() {
        return TaskType.MAPREDUCE_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.PARALLEL;
    }

    @Override
    public boolean deterministic() {
        return false;
    }

    public boolean enabled() {
        return settings.sparkHome() != null && !settings.sparkHome().isBlank();
    }

    /** The spark-submit command for one run. */
    public List<String> command(String dataset, String job, Path output) {
        boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
        Path submit = Paths.get(settings.sparkHome(), "bin",
                windows ? "spark-submit.cmd" : "spark-submit");
        List<String> command = new ArrayList<>();
        command.add(submit.toString());
        command.add("--master");
        command.add("local[*]");
        command.add(settings.jobsDir().resolve(SCRIPTS.get(job)).toString());
        command.add("--input");
        command.add(dataset);
        command.add("--output");
        command.add(output.toString());
        return command;
    }

    @Override
    public ExecutionResult execute(String input) throws Exception {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        if (!enabled()) {
            return ExecutionResult.failure(
                    "MAPREDUCE_TASK is disabled on this worker: spark.home is not configured");
        }
        Map<String, String> params = InputParser.parse(input);
        String job = params.get("job");
        Path output = settings.outputDir().resolve(
                job + "-" + System.currentTimeMillis() + "-" + runs.incrementAndGet());
        ProcessBuilder builder = new ProcessBuilder(command(params.get("dataset"), job, output))
                .redirectErrorStream(true);
        if (settings.python() != null && !settings.python().isBlank()) {
            builder.environment().put("PYSPARK_PYTHON", settings.python());
            builder.environment().put("PYSPARK_DRIVER_PYTHON", settings.python());
        }
        Process process = builder.start();
        Deque<String> tail = new ArrayDeque<>();
        String[] rows = {null};
        Thread reader = new Thread(() -> {
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.startsWith("ROWS ")) {
                        rows[0] = line.substring(5);
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
        }, "spark-submit-output");
        reader.setDaemon(true);
        reader.start();
        try {
            if (!process.waitFor(settings.timeoutMs(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                return ExecutionResult.failure("spark-submit timed out after "
                        + settings.timeoutMs() + " ms");
            }
        } catch (InterruptedException e) {
            // Cancelled (FR26): take the Spark job down with the task.
            process.destroyForcibly();
            throw e;
        }
        reader.join(5_000);
        if (process.exitValue() != 0 || rows[0] == null) {
            String lastLines;
            synchronized (tail) {
                lastLines = String.join(" | ", tail);
            }
            return ExecutionResult.failure("spark-submit exited " + process.exitValue() + ": "
                    + lastLines);
        }
        return ExecutionResult.success("job=" + job + " " + rows[0]);
    }
}
