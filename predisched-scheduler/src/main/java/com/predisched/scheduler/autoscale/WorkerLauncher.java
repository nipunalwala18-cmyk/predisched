package com.predisched.scheduler.autoscale;

import com.predisched.common.NodeConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts and stops the workers the auto-scaler adds (prompt 21, F12). */
public interface WorkerLauncher {

    String name();

    /** Starts one worker; returns its id (it registers with the scheduler by itself). */
    String start() throws Exception;

    void stop(String workerId) throws Exception;

    /** The workers this launcher started that are still running, oldest first. */
    List<String> started();

    static WorkerLauncher from(NodeConfig.AutoscaleConfig config, String nodeConfigPath) {
        return "docker".equalsIgnoreCase(config.getLauncher())
                ? new Docker(config) : new LocalProcess(config, nodeConfigPath);
    }

    /** {@code java -jar} the worker jar on the next free port from {@code basePort}. */
    final class LocalProcess implements WorkerLauncher {
        private static final Logger log = LoggerFactory.getLogger(LocalProcess.class);
        private final NodeConfig.AutoscaleConfig config;
        private final String nodeConfig;
        private final Map<String, Process> running = new LinkedHashMap<>();
        private int next = 1;

        public LocalProcess(NodeConfig.AutoscaleConfig config, String nodeConfigPath) {
            this.config = config;
            this.nodeConfig = config.getWorkerConfig().isBlank()
                    ? nodeConfigPath : config.getWorkerConfig();
        }

        @Override
        public String name() {
            return "process";
        }

        List<String> command(String id, int port) {
            String java = ProcessHandle.current().info().command().orElse("java");
            return List.of(java, "-jar", config.getWorkerJar(), "--config", nodeConfig,
                    "--id", id, "--port", String.valueOf(port),
                    "--pool-size", String.valueOf(config.getPoolSize()));
        }

        @Override
        public synchronized String start() throws Exception {
            int n = next++;
            String id = "worker-a" + n;
            int port = config.getBasePort() + n;
            Path logs = Paths.get(config.getLogDir());
            Files.createDirectories(logs);
            File out = logs.resolve(id + ".log").toFile();
            Process p = new ProcessBuilder(command(id, port)).redirectErrorStream(true)
                    .redirectOutput(out).start();
            running.put(id, p);
            log.info("Started {} on port {} (pid {}, log {})", id, port, p.pid(), out);
            return id;
        }

        @Override
        public synchronized void stop(String workerId) throws Exception {
            Process p = running.remove(workerId);
            if (p == null) {
                return;
            }
            p.descendants().forEach(ProcessHandle::destroy);
            p.destroy();
            if (!p.waitFor(10, TimeUnit.SECONDS)) {
                p.destroyForcibly();
            }
            log.info("Stopped {} (pid {})", workerId, p.pid());
        }

        @Override
        public synchronized List<String> started() {
            running.entrySet().removeIf(e -> !e.getValue().isAlive());
            return new ArrayList<>(running.keySet());
        }
    }

    /**
     * {@code docker run} the worker image with host networking, {@code docker stop} to remove.
     * Untested here (no Docker on the build machine); the command is what prompt 24's compose
     * file runs per worker.
     */
    final class Docker implements WorkerLauncher {
        private final NodeConfig.AutoscaleConfig config;
        private final List<String> running = new ArrayList<>();
        private int next = 1;

        public Docker(NodeConfig.AutoscaleConfig config) {
            this.config = config;
        }

        @Override
        public String name() {
            return "docker";
        }

        List<String> runCommand(String id, int port) {
            return List.of("docker", "run", "-d", "--rm", "--network", "host", "--name",
                    "predisched-" + id, config.getDockerImage(), "--id", id, "--port",
                    String.valueOf(port), "--pool-size", String.valueOf(config.getPoolSize()));
        }

        @Override
        public synchronized String start() throws Exception {
            int n = next++;
            String id = "worker-a" + n;
            int code = new ProcessBuilder(runCommand(id, config.getBasePort() + n)).inheritIO()
                    .start().waitFor();
            if (code != 0) {
                throw new IllegalStateException("docker run exited " + code);
            }
            running.add(id);
            return id;
        }

        @Override
        public synchronized void stop(String workerId) throws Exception {
            if (running.remove(workerId)) {
                new ProcessBuilder("docker", "stop", "predisched-" + workerId).inheritIO().start()
                        .waitFor(30, TimeUnit.SECONDS);
            }
        }

        @Override
        public synchronized List<String> started() {
            return new ArrayList<>(running);
        }
    }
}
