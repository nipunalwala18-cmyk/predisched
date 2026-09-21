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
