package com.predisched.common;

import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import java.util.Objects;

/**
 * Immutable view of a task. State changes return a new record.
 */
public final class TaskRecord {

    private final String id;
    private final TaskType type;
    private final String input;
    private final int priority;
    private final TaskStatus status;
    private final String workerId;
    private final String result;
    private final long submittedAt;
    private final long startedAt;
    private final long completedAt;
    private final long execTimeMs;

    public TaskRecord(
            String id,
            TaskType type,
            String input,
            int priority,
            TaskStatus status,
            String workerId,
            String result,
            long submittedAt,
            long startedAt,
            long completedAt,
            long execTimeMs) {
        this.id = Objects.requireNonNull(id, "id");
        this.type = Objects.requireNonNull(type, "type");
        this.input = input == null ? "" : input;
        this.priority = priority;
        this.status = Objects.requireNonNull(status, "status");
        this.workerId = workerId == null ? "" : workerId;
        this.result = result == null ? "" : result;
        this.submittedAt = submittedAt;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.execTimeMs = execTimeMs;
    }

    public static TaskRecord createQueued(String id, TaskType type, String input, int priority) {
        return new TaskRecord(
                id, type, input, priority, TaskStatus.QUEUED,
                "", "", System.currentTimeMillis(), 0L, 0L, 0L);
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

    public TaskStatus status() {
        return status;
    }

    public String workerId() {
        return workerId;
    }

    public String result() {
        return result;
    }

    public long submittedAt() {
        return submittedAt;
    }

    public long startedAt() {
        return startedAt;
    }

    public long completedAt() {
        return completedAt;
    }

    public long execTimeMs() {
        return execTimeMs;
    }

    /** Returns a copy with a new status after checking the transition. Timestamps are updated. */
    public TaskRecord withStatus(TaskStatus newStatus) {
        TaskStateMachine.checkTransition(this.status, newStatus);
        long now = System.currentTimeMillis();
        long started = this.startedAt;
        long completed = this.completedAt;
        if (newStatus == TaskStatus.RUNNING && started == 0L) {
            started = now;
        }
        if ((newStatus == TaskStatus.COMPLETED
                || newStatus == TaskStatus.FAILED
                || newStatus == TaskStatus.CANCELLED)
                && completed == 0L) {
            completed = now;
        }
        if (newStatus == TaskStatus.QUEUED) {
            // Re-queued after worker failure: clear worker binding, keep history otherwise.
            return new TaskRecord(
                    id, type, input, priority, newStatus, "", result,
                    submittedAt, started, 0L, execTimeMs);
        }
        return new TaskRecord(
                id, type, input, priority, newStatus, workerId, result,
                submittedAt, started, completed, execTimeMs);
    }

    public TaskRecord withWorkerId(String newWorkerId) {
        return new TaskRecord(
                id, type, input, priority, status, newWorkerId, result,
                submittedAt, startedAt, completedAt, execTimeMs);
    }

    public TaskRecord withResult(String newResult) {
        return new TaskRecord(
                id, type, input, priority, status, workerId, newResult,
                submittedAt, startedAt, completedAt, execTimeMs);
    }

    public TaskRecord withExecTimeMs(long newExecTimeMs) {
        return new TaskRecord(
                id, type, input, priority, status, workerId, result,
                submittedAt, startedAt, completedAt, newExecTimeMs);
    }

    public boolean isTerminal() {
        return status == TaskStatus.COMPLETED
                || status == TaskStatus.FAILED
                || status == TaskStatus.CANCELLED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TaskRecord)) {
            return false;
        }
        TaskRecord that = (TaskRecord) o;
        return priority == that.priority
                && submittedAt == that.submittedAt
                && startedAt == that.startedAt
                && completedAt == that.completedAt
                && execTimeMs == that.execTimeMs
                && id.equals(that.id)
                && type == that.type
                && input.equals(that.input)
                && status == that.status
                && workerId.equals(that.workerId)
                && result.equals(that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id, type, input, priority, status, workerId, result,
                submittedAt, startedAt, completedAt, execTimeMs);
    }

    @Override
    public String toString() {
        return "TaskRecord{id='" + id + "', type=" + type + ", status=" + status
                + ", priority=" + priority + ", workerId='" + workerId + "'}";
    }
}
