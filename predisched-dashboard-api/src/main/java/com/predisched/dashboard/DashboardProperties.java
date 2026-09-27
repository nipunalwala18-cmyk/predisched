package com.predisched.dashboard;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code dashboard.*} in application.yaml (prompt 22). */
@ConfigurationProperties(prefix = "dashboard")
public class DashboardProperties {

    /** Scheduler addresses (host:port); the primary is found among them. */
    private List<String> schedulers = new ArrayList<>(List.of("localhost:51051"));
    /** A node config whose election peers list the schedulers (overrides {@code schedulers}). */
    private String clusterConfig = "";
    /** Header {@code X-Admin-Key} required on admin, chaos, promote and benchmark-run calls. */
    private String adminKey = "dev-admin";
    /** Admin calls allowed per second per key (token bucket, burst of twice that). */
    private double adminRatePerSecond = 5;
    /** gRPC deadline for live calls to the cluster. */
    private long grpcTimeoutMs = 2_000;
    /** The repository root: where ml/, configs/ and the jars are. */
    private String repoRoot = ".";
    private String python = ".venv/Scripts/python.exe";
    /** Messages per second per WebSocket topic, at most (batched within each tick). */
    private int streamRatePerSecond = 4;

    public List<String> getSchedulers() {
        return schedulers;
    }

    public void setSchedulers(List<String> schedulers) {
        this.schedulers = schedulers;
    }

    public String getClusterConfig() {
        return clusterConfig;
    }

    public void setClusterConfig(String clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public String getAdminKey() {
        return adminKey;
    }

    public void setAdminKey(String adminKey) {
        this.adminKey = adminKey;
    }

    public double getAdminRatePerSecond() {
        return adminRatePerSecond;
    }

    public void setAdminRatePerSecond(double adminRatePerSecond) {
        this.adminRatePerSecond = adminRatePerSecond;
    }

    public long getGrpcTimeoutMs() {
        return grpcTimeoutMs;
    }

    public void setGrpcTimeoutMs(long grpcTimeoutMs) {
        this.grpcTimeoutMs = grpcTimeoutMs;
    }

    public String getRepoRoot() {
        return repoRoot;
    }

    public void setRepoRoot(String repoRoot) {
        this.repoRoot = repoRoot;
    }

    public String getPython() {
        return python;
    }

    public void setPython(String python) {
        this.python = python;
    }

    public int getStreamRatePerSecond() {
        return streamRatePerSecond;
    }

    public void setStreamRatePerSecond(int streamRatePerSecond) {
        this.streamRatePerSecond = streamRatePerSecond;
    }
}
