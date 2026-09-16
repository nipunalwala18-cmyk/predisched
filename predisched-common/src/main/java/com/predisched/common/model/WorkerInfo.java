package com.predisched.common.model;

import java.util.Objects;

/**
 * A worker known to the scheduler: static capacity plus the latest heartbeat snapshot.
 *
 * <p>Snapshot fields default to idle; Prompt 02 fills them from heartbeats. The scheduler
 * also keeps a local in-flight counter (Prompt 03) on top of these values.
 */
public class WorkerInfo {

  private final String workerId;
  private final String host;
  private final int port;
  private final int cores;
  private final long memoryMb;
  private final int poolSize;

  private volatile double cpuPct;
  private volatile double memPct;
  private volatile int activeThreads;
  private volatile int queueLen;
  private volatile long tasksCompleted;
  private volatile double avgExecMs;
  private volatile long lastSeen;

  public WorkerInfo(String workerId, String host, int port, int cores, long memoryMb, int poolSize) {
    this.workerId = Objects.requireNonNull(workerId);
    this.host = Objects.requireNonNull(host);
    this.port = port;
    this.cores = cores;
    this.memoryMb = memoryMb;
    this.poolSize = poolSize;
    // Never registered: not alive until the registry stamps lastSeen from its NodeClock.
    this.lastSeen = 0;
  }

  /** Copy, including the latest snapshot. */
  public WorkerInfo(WorkerInfo other) {
    this(
        other.workerId, other.host, other.port, other.cores, other.memoryMb,
        other.poolSize);
    this.cpuPct = other.cpuPct;
    this.memPct = other.memPct;
    this.activeThreads = other.activeThreads;
    this.queueLen = other.queueLen;
    this.tasksCompleted = other.tasksCompleted;
    this.avgExecMs = other.avgExecMs;
    this.lastSeen = other.lastSeen;
  }

  public String workerId() {
    return workerId;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public int cores() {
    return cores;
  }

  public long memoryMb() {
    return memoryMb;
  }

  public int poolSize() {
    return poolSize;
  }

  public double cpuPct() {
    return cpuPct;
  }

  public void cpuPct(double cpuPct) {
    this.cpuPct = cpuPct;
  }

  public double memPct() {
    return memPct;
  }

  public void memPct(double memPct) {
    this.memPct = memPct;
  }

  public int activeThreads() {
    return activeThreads;
  }

  public void activeThreads(int activeThreads) {
    this.activeThreads = activeThreads;
  }

  public int queueLen() {
    return queueLen;
  }

  public void queueLen(int queueLen) {
    this.queueLen = queueLen;
  }

  public long tasksCompleted() {
    return tasksCompleted;
  }

  public void tasksCompleted(long tasksCompleted) {
    this.tasksCompleted = tasksCompleted;
  }

  public double avgExecMs() {
    return avgExecMs;
  }

  public void avgExecMs(double avgExecMs) {
    this.avgExecMs = avgExecMs;
  }

  public long lastSeen() {
    return lastSeen;
  }

  public void lastSeen(long lastSeen) {
    this.lastSeen = lastSeen;
  }

  public String address() {
    return host + ":" + port;
  }
}
