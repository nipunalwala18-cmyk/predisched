package com.predisched.scheduler;

import com.predisched.common.NodeConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs the retraining command when drift is detected and {@code drift.autoRetrain} is on (prompt
 * 21, F15). One run at a time; the output goes to {@code logs/retrain-<time>.log}. The command
 * registers a shadow model, which the prediction server picks up by itself; it never makes one
 * live.
 */
public final class RetrainLauncher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RetrainLauncher.class);

    private final List<String> command;
    private final File workDir;
    private final AtomicBoolean running = new AtomicBoolean();

    public RetrainLauncher(NodeConfig.DriftConfig config) {
        this.command = Arrays.asList(config.getRetrainCommand().trim().split("\\s+"));
        this.workDir = new File(config.getRetrainWorkDir());
    }

    public List<String> command() {
        return command;
    }

    @Override
    public void run() {
        if (!running.compareAndSet(false, true)) {
            log.info("Retraining already running; this drift episode waits for it");
            return;
        }
        Thread t = new Thread(() -> {
            try {
                Path logs = Paths.get("logs");
                Files.createDirectories(logs);
                Path out = logs.resolve("retrain-" + LocalDateTime.now()
                        .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".log");
                log.warn("Drift: retraining with {} (in {}), output in {}", command, workDir, out);
                Process p = new ProcessBuilder(command).directory(workDir)
                        .redirectErrorStream(true).redirectOutput(out.toFile()).start();
                int code = p.waitFor();
                log.info("Retraining finished with exit code {}; a new shadow model is loaded by"
                        + " the prediction server if it succeeded", code);
            } catch (Exception e) {
                log.warn("Retraining could not run: {}", e.toString());
            } finally {
                running.set(false);
            }
        }, "retrain");
        t.setDaemon(true);
        t.start();
    }
}
