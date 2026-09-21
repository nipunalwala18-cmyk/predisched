package com.predisched.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.yaml.snakeyaml.Yaml;

/** Loads a YAML node config file from {@code configs/} (SnakeYAML). */
public class NodeConfig {

    private SchedulerConfig scheduler = new SchedulerConfig();
    private WorkerConfig worker = new WorkerConfig();
    private ValidationConfig validation = new ValidationConfig();

    public SchedulerConfig getScheduler() {
        return scheduler;
    }

    public void setScheduler(SchedulerConfig scheduler) {
        this.scheduler = scheduler;
    }

    public WorkerConfig getWorker() {
        return worker;
    }

    public void setWorker(WorkerConfig worker) {
        this.worker = worker;
    }

    public ValidationConfig getValidation() {
        return validation;
    }

    public void setValidation(ValidationConfig validation) {
        this.validation = validation;
    }

    public static NodeConfig load(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            NodeConfig loaded = new Yaml().loadAs(in, NodeConfig.class);
            return loaded == null ? new NodeConfig() : loaded;
        }
    }

    public static class SchedulerConfig {
        private String id = "scheduler-1";
        private String host = "localhost";
        private int port = 51051;
        private int dispatchThreads = 4;
        private long workerStaleAfterMs = 5000;
        private long noWorkerRetryMs = 200;
        private long clusterReportIntervalMs = 5000;

        public long getClusterReportIntervalMs() {
            return clusterReportIntervalMs;
        }

        public void setClusterReportIntervalMs(long clusterReportIntervalMs) {
            this.clusterReportIntervalMs = clusterReportIntervalMs;
        }

        public int getDispatchThreads() {
            return dispatchThreads;
        }

        public void setDispatchThreads(int dispatchThreads) {
            this.dispatchThreads = dispatchThreads;
        }

        public long getWorkerStaleAfterMs() {
            return workerStaleAfterMs;
        }

        public void setWorkerStaleAfterMs(long workerStaleAfterMs) {
            this.workerStaleAfterMs = workerStaleAfterMs;
        }

        public long getNoWorkerRetryMs() {
            return noWorkerRetryMs;
        }

        public void setNoWorkerRetryMs(long noWorkerRetryMs) {
            this.noWorkerRetryMs = noWorkerRetryMs;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public static class WorkerConfig {
        private String id = "worker-1";
        private String host = "localhost";
        private int port = 51061;
        private int poolSize = 4;
        private int queueCapacity = 100;
        private long heartbeatIntervalMs = 1000;

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public long getHeartbeatIntervalMs() {
            return heartbeatIntervalMs;
        }

        public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public static class ValidationConfig {
        private int maxInputChars = 4096;

        public int getMaxInputChars() {
            return maxInputChars;
        }

        public void setMaxInputChars(int maxInputChars) {
            this.maxInputChars = maxInputChars;
        }
    }
}
