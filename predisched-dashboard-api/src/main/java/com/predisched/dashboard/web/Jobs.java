package com.predisched.dashboard.web;

import com.predisched.dashboard.DashboardProperties;
import com.predisched.dashboard.stream.StreamBridge;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * Subprocesses the API runs (prompt 22): benchmark suites in the background (progress to the
 * {@code events} topic), and short synchronous commands (the simulator, model promotion).
 */
@Component
public class Jobs {

    /** A command's exit code and output. */
    public record Result(int exitCode, String output) {}

    /** A background job's state. */
    public record Job(String id, List<String> command, String status, int exitCode,
            List<String> lastLines) {}

    private final DashboardProperties props;
    private final StreamBridge stream;
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();

    public Jobs(DashboardProperties props, StreamBridge stream) {
        this.props = props;
        this.stream = stream;
    }

    public Path root() {
        return Paths.get(props.getRepoRoot()).toAbsolutePath().normalize();
    }

    public String java() {
        return ProcessHandle.current().info().command().orElse("java");
    }

    public String python() {
        return root().resolve(props.getPython()).toString();
    }

    /** Runs to completion (at most {@code timeoutMs}) in {@code dir}, output captured. */
    public Result run(List<String> command, File dir, long timeoutMs) throws Exception {
        Process p = new ProcessBuilder(command).directory(dir).redirectErrorStream(true).start();
        StringBuilder out = new StringBuilder();
        Thread reader = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(),
                    StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    synchronized (out) {
                        out.append(line).append('\n');
                    }
                }
            } catch (Exception ignored) {
                // process ended
            }
        });
        reader.start();
        if (!p.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly();
            return new Result(-1, out + "timed out after " + timeoutMs + " ms");
        }
        reader.join(2_000);
        synchronized (out) {
            return new Result(p.exitValue(), out.toString());
        }
    }

    /** Starts {@code command} in the background; its lines go to the event stream. */
    public Job start(String kind, List<String> command) throws Exception {
        String id = kind + "-" + UUID.randomUUID().toString().substring(0, 8);
        Process p = new ProcessBuilder(command).directory(root().toFile())
                .redirectErrorStream(true).start();
        List<String> tail = new ArrayList<>();
        jobs.put(id, new Job(id, command, "running", 0, tail));
        Thread t = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(),
                    StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    synchronized (tail) {
                        tail.add(line);
                        if (tail.size() > 50) {
                            tail.remove(0);
                        }
                    }
                    if (line.startsWith("[")) {   // one line per finished run
                        stream.publish("events", Map.of("type", "BENCHMARK_PROGRESS",
                                "job", id, "line", line));
                    }
                }
                int code = p.waitFor();
                jobs.put(id, new Job(id, command, code == 0 ? "done" : "failed", code, tail));
                stream.publish("events", Map.of("type", "BENCHMARK_DONE", "job", id,
                        "exitCode", code));
            } catch (Exception e) {
                jobs.put(id, new Job(id, command, "failed", -1, tail));
            }
        }, "job-" + id);
        t.setDaemon(true);
        t.start();
        return jobs.get(id);
    }

    public Job job(String id) {
        return jobs.get(id);
    }
}
