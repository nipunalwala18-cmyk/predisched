package com.predisched.common.model;

import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import java.util.Objects;

/**
 * A task tracked by the scheduler.
 *
 * <p>Mutable but thread-safe via synchronization on the instance; the owning
 * {@code TaskStore} guards transitions.
 */
public class TaskRecord {

  private final String id;
  private final TaskType type;
  private final String input;
  private final int priority;
  private final long submittedAt;

  private volatile TaskStatus status;
  private volatile String workerId = "";
  private volatile String result = "";
  private volatile long startedAt;
  private volatile long completedAt;
  private volatile long waitTimeMs;
  private volatile long execTimeMs;

  public TaskRecord(String id, TaskType type, String input, int priority, long submittedAt) {
    this.id = Objects.requireNonNull(id);
    this.type = Objects.requireNonNull(type);
    this.input = Objects.requireNonNull(input);
    this.priority = priority;
    this.submittedAt = submittedAt;
    this.status = TaskStatus.QUEUED;
  }

  public String id() {
    return id;
  }

  public TaskType type() {
    return type;
  }

  public String input() {
    return input;
  }

  public int priority() {
    return priority;
  }

  public long submittedAt() {
    return submittedAt;
  }

  public TaskStatus status() {
    return status;
  }

  public void status(TaskStatus status) {
    this.status = status;
  }

  public String workerId() {
    return workerId;
  }

  public void workerId(String workerId) {
    this.workerId = workerId == null ? "" : workerId;
  }

  public String result() {
    return result;
  }

  public void result(String result) {
    this.result = result == null ? "" : result;
  }

  public long startedAt() {
    return startedAt;
  }

  public void startedAt(long startedAt) {
    this.startedAt = startedAt;
  }

  public long completedAt() {
    return completedAt;
  }

  public void completedAt(long completedAt) {
    this.completedAt = completedAt;
  }

  public long waitTimeMs() {
    return waitTimeMs;
  }

  public void waitTimeMs(long waitTimeMs) {
    this.waitTimeMs = waitTimeMs;
  }

  public long execTimeMs() {
    return execTimeMs;
  }

  public void execTimeMs(long execTimeMs) {
    this.execTimeMs = execTimeMs;
  }
}
