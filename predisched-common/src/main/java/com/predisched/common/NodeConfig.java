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
    private ClockConfig clock = new ClockConfig();
    private QueueConfig queue = new QueueConfig();

    public QueueConfig getQueue() {
        return queue;
    }

    public void setQueue(QueueConfig queue) {
        this.queue = queue;
    }

    public ClockConfig getClock() {
        return clock;
    }

    public void setClock(ClockConfig clock) {
        this.clock = clock;
    }

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
        private double outstandingPerWorkerFactor = 2.0;

        /**
         * How many tasks this scheduler will keep outstanding on one worker, as a multiple of that
         * worker's pool size. Above 1 keeps the pool fed; too high empties the scheduler's queue
         * into the worker's and makes priority ordering meaningless.
         */
        public double getOutstandingPerWorkerFactor() {
            return outstandingPerWorkerFactor;
        }

        public void setOutstandingPerWorkerFactor(double outstandingPerWorkerFactor) {
            this.outstandingPerWorkerFactor = outstandingPerWorkerFactor;
        }
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
        /** Temp dir for FILE_IO_TASK files; empty means the JVM temp dir. */
        private String fileIoDir = "";

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

        public String getFileIoDir() {
            return fileIoDir;
        }

        public void setFileIoDir(String fileIoDir) {
            this.fileIoDir = fileIoDir;
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

    /** Queue discipline: ageing, retries with backoff, dead letters and timeouts (F2, F4, F5). */
    public static class QueueConfig {
        private double ageingPerSecond = 0.1;
        private int maxPriority = 10;
        private int maxRetries = 3;
        private long retryBaseDelayMs = 200;
        private long retryMaxDelayMs = 30000;
        private double retryJitter = 0.2;
        private long defaultTimeoutMs = 0;
        private long timeoutCheckMs = 250;
        private long seed = 42;

        public double getAgeingPerSecond() {
            return ageingPerSecond;
        }

        public void setAgeingPerSecond(double ageingPerSecond) {
            this.ageingPerSecond = ageingPerSecond;
        }

        public int getMaxPriority() {
            return maxPriority;
        }

        public void setMaxPriority(int maxPriority) {
            this.maxPriority = maxPriority;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public long getRetryBaseDelayMs() {
            return retryBaseDelayMs;
        }

        public void setRetryBaseDelayMs(long retryBaseDelayMs) {
            this.retryBaseDelayMs = retryBaseDelayMs;
        }

        public long getRetryMaxDelayMs() {
            return retryMaxDelayMs;
        }

        public void setRetryMaxDelayMs(long retryMaxDelayMs) {
            this.retryMaxDelayMs = retryMaxDelayMs;
        }

        public double getRetryJitter() {
            return retryJitter;
        }

        public void setRetryJitter(double retryJitter) {
            this.retryJitter = retryJitter;
        }

        /** 0 disables timeouts unless a task asks for one. */
        public long getDefaultTimeoutMs() {
            return defaultTimeoutMs;
        }

        public void setDefaultTimeoutMs(long defaultTimeoutMs) {
            this.defaultTimeoutMs = defaultTimeoutMs;
        }

        public long getTimeoutCheckMs() {
            return timeoutCheckMs;
        }

        public void setTimeoutCheckMs(long timeoutCheckMs) {
            this.timeoutCheckMs = timeoutCheckMs;
        }

        /** Seed for retry jitter, so a replayed workload retries at the same moments (rule 8). */
        public long getSeed() {
            return seed;
        }

        public void setSeed(long seed) {
            this.seed = seed;
        }
    }

    /** Simulated clock skew and how it is corrected (spec section 11, Exp 3). */
    public static class ClockConfig {
        private long offsetMs = 0;
        private double driftPpm = 0;
        private String algorithm = "berkeley";
        private long syncIntervalMs = 10000;
        private long outlierMs = 1000;

        public long getOffsetMs() {
            return offsetMs;
        }

        public void setOffsetMs(long offsetMs) {
            this.offsetMs = offsetMs;
        }

        public double getDriftPpm() {
            return driftPpm;
        }

        public void setDriftPpm(double driftPpm) {
            this.driftPpm = driftPpm;
        }

        /** berkeley (scheduler drives), cristian (worker pulls) or none. */
        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public long getSyncIntervalMs() {
            return syncIntervalMs;
        }

        public void setSyncIntervalMs(long syncIntervalMs) {
            this.syncIntervalMs = syncIntervalMs;
        }

        public long getOutlierMs() {
            return outlierMs;
        }

        public void setOutlierMs(long outlierMs) {
            this.outlierMs = outlierMs;
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
