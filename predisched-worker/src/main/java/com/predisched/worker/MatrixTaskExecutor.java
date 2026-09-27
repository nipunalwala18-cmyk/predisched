package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskInputSpec;
import com.predisched.common.TaskExecutor;
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
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

/**
 * Matrix multiplication of two seeded random matrices ({@code size=n, [seed=s], [threads=k],
 * [mode=local|threads|mpi], [procs=p]}).
 *
 * <ul>
 *   <li>{@code mode=local} (default with no threads) multiplies on the calling thread.
 *   <li>{@code threads=k} or {@code mode=threads} multiplies row blocks on a {@link ForkJoinPool}
 *       of k threads, the thread-pool demo's CPU-heavy parallel task (lab Exp 2).
 *   <li>{@code mode=mpi} runs {@code mpiexec -n <procs> python -m predisched_mpi.matmul} (prompt
 *       14, lab Exp 10) and reports its checksum and phase timings. Disabled until {@code mpi.exec}
 *       is configured.
 * </ul>
 *
 * <p>A and B are filled from {@code java.util.Random(seed)}, alternating a[i][j] and b[i][j]; the
 * Python side reproduces that stream. Products are summed in row-major order, so the local and
 * threaded checksums are identical; the MPI one (NumPy/BLAS order) agrees to about 1e-9 relative.
 */
public class MatrixTaskExecutor implements TaskExecutor {

    static final long SEED = 42L;

    /** Where MPI is for {@code mode=mpi}. An empty {@code mpiExec} disables that mode. */
    public record MpiSettings(String mpiExec, String python, Path workDir, int maxProcs,
            long timeoutMs) {

        public static final MpiSettings DISABLED =
                new MpiSettings("", "", Paths.get("mpi"), 8, 600_000);

        public boolean enabled() {
            return mpiExec != null && !mpiExec.isBlank();
        }
    }

    private static final Pattern JSON_NUMBER =
            Pattern.compile("\"(\\w+)\":\\s*(-?[0-9.eE+-]+|true|false)");
    private static final int OUTPUT_LINES_KEPT = 20;

    private final MpiSettings mpi;

    public MatrixTaskExecutor() {
        this(MpiSettings.DISABLED);
    }

    public MatrixTaskExecutor(MpiSettings mpi) {
        this.mpi = mpi;
    }

    @Override
    public TaskType type() {
        return TaskType.MATRIX_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.PARALLEL;
    }

    @Override
    public ExecutionResult execute(String input) throws Exception {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        int size = Integer.parseInt(params.get("size"));
        long seed = params.containsKey("seed") ? Long.parseLong(params.get("seed")) : SEED;
        String mode = params.getOrDefault("mode", "");
        if (mode.equals("mpi")) {
            int procs = params.containsKey("procs") ? Integer.parseInt(params.get("procs")) : 2;
            return runMpi(size, seed, procs);
        }
        int threads = params.containsKey("threads")
                ? Integer.parseInt(params.get("threads"))
                : mode.equals("threads") ? Runtime.getRuntime().availableProcessors() : 1;
        double checksum = threads > 1
                ? multiplyAndChecksum(size, threads, seed)
                : multiplyAndChecksum(size, seed);
        String output = String.format(Locale.ROOT, "size=%d threads=%d checksum=%.6f",
                size, threads, checksum);
        return ExecutionResult.success(output);
    }

    /** The {@code checksum=...} part of an output line, for comparing runs. */
    public static String checksumOf(String output) {
        int at = output.indexOf("checksum=");
        if (at < 0) {
            return output;
        }
        int end = output.indexOf(' ', at);
        return end < 0 ? output.substring(at) : output.substring(at, end);
    }

    /** The mpiexec command for one MPI run. */
    public List<String> mpiCommand(int size, long seed, int procs) {
        List<String> command = new ArrayList<>();
        command.add(mpi.mpiExec());
        command.add("-n");
        command.add(Integer.toString(procs));
        command.add(mpi.python() == null || mpi.python().isBlank() ? "python" : mpi.python());
        command.add("-m");
        command.add("predisched_mpi.matmul");
        command.add("--size");
        command.add(Integer.toString(size));
        command.add("--seed");
        command.add(Long.toString(seed));
        command.add("--json");
        return command;
    }

    private ExecutionResult runMpi(int size, long seed, int procs) throws Exception {
        if (!mpi.enabled()) {
            return ExecutionResult.failure(
                    "MATRIX_TASK mode=mpi is disabled on this worker: mpi.exec is not configured");
        }
        if (procs > mpi.maxProcs()) {
            return ExecutionResult.failure("procs=" + procs + " exceeds mpi.maxProcs="
                    + mpi.maxProcs() + " on this worker");
        }
        ProcessBuilder builder = new ProcessBuilder(mpiCommand(size, seed, procs))
                .directory(mpi.workDir().toFile())
                .redirectErrorStream(true);
        builder.environment().put("PYTHONPATH", mpi.workDir().toAbsolutePath().toString());
        // mpiexec and impi.dll from the impi_rt wheel sit next to each other; the ranks need it.
        Path execDir = Paths.get(mpi.mpiExec()).toAbsolutePath().getParent();
        if (execDir != null) {
            String path = builder.environment().getOrDefault("PATH",
                    builder.environment().getOrDefault("Path", ""));
            builder.environment().put("PATH", execDir + java.io.File.pathSeparator + path);
        }
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
        }, "mpiexec-output");
        reader.setDaemon(true);
        reader.start();
        try {
            if (!process.waitFor(mpi.timeoutMs(), TimeUnit.MILLISECONDS)) {
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
                return ExecutionResult.failure("mpiexec timed out after " + mpi.timeoutMs() + " ms");
            }
        } catch (InterruptedException e) {
            // Cancelled (FR26): take the ranks down with the task.
            process.descendants().forEach(ProcessHandle::destroyForcibly);
            process.destroyForcibly();
            throw e;
        }
        reader.join(5_000);
        if (process.exitValue() != 0 || json[0] == null) {
            String lastLines;
            synchronized (tail) {
                lastLines = String.join(" | ", tail);
            }
            return ExecutionResult.failure("mpiexec exited " + process.exitValue() + ": "
                    + lastLines);
        }
        Map<String, String> fields = parseJson(json[0]);
        if (!"true".equals(fields.get("verified"))) {
            return ExecutionResult.failure("MPI product does not match the sequential one: "
                    + json[0]);
        }
        return ExecutionResult.success(String.format(Locale.ROOT,
                "size=%d mode=mpi procs=%d checksum=%.6f verified=true scatter_bcast_ms=%s"
                        + " compute_ms=%s gather_ms=%s total_ms=%s",
                size, procs, Double.parseDouble(fields.get("checksum")),
                fields.get("scatter_bcast_ms"), fields.get("compute_ms"),
                fields.get("gather_ms"), fields.get("total_ms")));
    }

    /** The flat numeric/boolean fields of matmul.py's one-line JSON. */
    static Map<String, String> parseJson(String line) {
        Map<String, String> fields = new java.util.HashMap<>();
        Matcher m = JSON_NUMBER.matcher(line);
        while (m.find()) {
            fields.put(m.group(1), m.group(2));
        }
        return fields;
    }

    static double multiplyAndChecksum(int size, int threads) {
        return multiplyAndChecksum(size, threads, SEED);
    }

    /** Row blocks in parallel, summed afterwards in the same order as the sequential version. */
    static double multiplyAndChecksum(int size, int threads, long seed) {
        double[][] a = new double[size][size];
        double[][] b = new double[size][size];
        fill(a, b, size, seed);
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

    private static void fill(double[][] a, double[][] b, int size, long seed) {
        Random random = new Random(seed);
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                a[i][j] = random.nextDouble();
                b[i][j] = random.nextDouble();
            }
        }
    }

    static double multiplyAndChecksum(int size) {
        return multiplyAndChecksum(size, SEED);
    }

    static double multiplyAndChecksum(int size, long seed) {
        double[][] a = new double[size][size];
        double[][] b = new double[size][size];
        fill(a, b, size, seed);
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
