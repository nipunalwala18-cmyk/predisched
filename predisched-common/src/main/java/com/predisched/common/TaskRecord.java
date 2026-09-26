package com.predisched.common;

import com.predisched.common.time.Clocks;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable view of a task. Every state change returns a new record, so a reader never sees a
 * half-updated task (rule 5).
 *
 * <p>Copies go through {@link #copy()}, which keeps the growing field list in one place: the
 * constructor is private and every {@code withX} method changes exactly one thing.
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
    private final String traceId;
    private final long timeoutMs;
    private final int maxRetries;
    private final List<TaskAttempt> attempts;

    private TaskRecord(Builder builder) {
        this.id = Objects.requireNonNull(builder.id, "id");
        this.type = Objects.requireNonNull(builder.type, "type");
        this.input = builder.input == null ? "" : builder.input;
        this.priority = builder.priority;
        this.status = Objects.requireNonNull(builder.status, "status");
        this.workerId = builder.workerId == null ? "" : builder.workerId;
        this.result = builder.result == null ? "" : builder.result;
        this.submittedAt = builder.submittedAt;
        this.startedAt = builder.startedAt;
        this.completedAt = builder.completedAt;
        this.execTimeMs = builder.execTimeMs;
        this.traceId = builder.traceId == null ? "" : builder.traceId;
        this.timeoutMs = builder.timeoutMs;
        this.maxRetries = builder.maxRetries;
        this.attempts = builder.attempts == null
                ? List.of()
                : Collections.unmodifiableList(new ArrayList<>(builder.attempts));
    }

    public static TaskRecord createQueued(String id, TaskType type, String input, int priority) {
        return createQueued(id, type, input, priority, "");
    }

    public static TaskRecord createQueued(
            String id, TaskType type, String input, int priority, String traceId) {
        return createQueued(id, type, input, priority, traceId, 0L, -1);
    }

    /**
     * A freshly submitted task.
     *
     * @param timeoutMs  0 means the scheduler's configured default applies
     * @param maxRetries negative means the scheduler's configured default applies
     */
    public static TaskRecord createQueued(
            String id,
            TaskType type,
            String input,
            int priority,
            String traceId,
            long timeoutMs,
            int maxRetries) {
        Builder builder = new Builder();
        builder.id = id;
        builder.type = type;
        builder.input = input;
        builder.priority = priority;
        builder.status = TaskStatus.QUEUED;
        builder.submittedAt = Clocks.now();
        builder.traceId = traceId;
        builder.timeoutMs = timeoutMs;
        builder.maxRetries = maxRetries;
        return new TaskRecord(builder);
    }

    /**
     * Rebuilds a record exactly as another node held it, every field included. Only for codecs
     * that move records between nodes (replication, persistence); it skips the state machine,
     * because the record already went through it where it was written.
     */
    public static TaskRecord restore(
            String id, TaskType type, String input, int priority, TaskStatus status,
            String workerId, String result, long submittedAt, long startedAt, long completedAt,
            long execTimeMs, String traceId, long timeoutMs, int maxRetries,
            List<TaskAttempt> attempts) {
        Builder builder = new Builder();
        builder.id = id;
        builder.type = type;
        builder.input = input;
        builder.priority = priority;
        builder.status = status;
        builder.workerId = workerId;
        builder.result = result;
        builder.submittedAt = submittedAt;
        builder.startedAt = startedAt;
        builder.completedAt = completedAt;
        builder.execTimeMs = execTimeMs;
        builder.traceId = traceId;
        builder.timeoutMs = timeoutMs;
        builder.maxRetries = maxRetries;
        builder.attempts = new ArrayList<>(attempts);
        return new TaskRecord(builder);
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

    /** The id that follows this task across nodes (F10). Empty when the client sent none. */
    public String traceId() {
        return traceId;
    }

    /** Per-task timeout in ms; 0 means the scheduler's default (FR26). */
    public long timeoutMs() {
        return timeoutMs;
    }

    /** Per-task retry limit; negative means the scheduler's default (FR25). */
    public int maxRetries() {
        return maxRetries;
    }

    /** Every try so far, oldest first (F4). */
    public List<TaskAttempt> attempts() {
        return attempts;
    }

    public int attemptCount() {
        return attempts.size();
    }

    /** Returns a copy with a new status after checking the transition. Timestamps are updated. */
    public TaskRecord withStatus(TaskStatus newStatus) {
        TaskStateMachine.checkTransition(this.status, newStatus);
        long now = Clocks.now();
        Builder builder = copy();
        builder.status = newStatus;
        if (newStatus == TaskStatus.RUNNING && startedAt == 0L) {
            builder.startedAt = now;
        }
        if ((newStatus == TaskStatus.COMPLETED
                || newStatus == TaskStatus.FAILED
                || newStatus == TaskStatus.CANCELLED)
                && completedAt == 0L) {
            builder.completedAt = now;
        }
        if (newStatus == TaskStatus.QUEUED) {
            // Re-queued after a failed attempt or a lost worker: drop the worker binding and the
            // completion time, but keep the attempt history.
            builder.workerId = "";
            builder.completedAt = 0L;
        }
        return new TaskRecord(builder);
    }

    public TaskRecord withWorkerId(String newWorkerId) {
        Builder builder = copy();
        builder.workerId = newWorkerId;
        return new TaskRecord(builder);
    }

    public TaskRecord withResult(String newResult) {
        Builder builder = copy();
        builder.result = newResult;
        return new TaskRecord(builder);
    }

    public TaskRecord withExecTimeMs(long newExecTimeMs) {
        Builder builder = copy();
        builder.execTimeMs = newExecTimeMs;
        return new TaskRecord(builder);
    }

    /** Appends one try to the history (F4). */
    public TaskRecord withAttempt(TaskAttempt attempt) {
        Builder builder = copy();
        builder.attempts.add(attempt);
        return new TaskRecord(builder);
    }

    public boolean isTerminal() {
        return status == TaskStatus.COMPLETED
                || status == TaskStatus.FAILED
                || status == TaskStatus.CANCELLED;
    }

    private Builder copy() {
        Builder builder = new Builder();
        builder.id = id;
        builder.type = type;
        builder.input = input;
        builder.priority = priority;
        builder.status = status;
        builder.workerId = workerId;
        builder.result = result;
        builder.submittedAt = submittedAt;
        builder.startedAt = startedAt;
        builder.completedAt = completedAt;
        builder.execTimeMs = execTimeMs;
        builder.traceId = traceId;
        builder.timeoutMs = timeoutMs;
        builder.maxRetries = maxRetries;
        builder.attempts = new ArrayList<>(attempts);
        return builder;
    }

    private static final class Builder {
        private String id;
        private TaskType type;
        private String input;
        private int priority;
        private TaskStatus status;
        private String workerId;
        private String result;
        private long submittedAt;
        private long startedAt;
        private long completedAt;
        private long execTimeMs;
        private String traceId;
        private long timeoutMs;
        private int maxRetries = -1;
        private List<TaskAttempt> attempts = new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TaskRecord that)) {
            return false;
        }
        return priority == that.priority
                && submittedAt == that.submittedAt
                && startedAt == that.startedAt
                && completedAt == that.completedAt
                && execTimeMs == that.execTimeMs
                && timeoutMs == that.timeoutMs
                && maxRetries == that.maxRetries
                && id.equals(that.id)
                && type == that.type
                && input.equals(that.input)
                && status == that.status
                && workerId.equals(that.workerId)
                && result.equals(that.result)
                && traceId.equals(that.traceId)
                && attempts.equals(that.attempts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id, type, input, priority, status, workerId, result,
                submittedAt, startedAt, completedAt, execTimeMs, traceId,
                timeoutMs, maxRetries, attempts);
    }

    @Override
    public String toString() {
        return "TaskRecord{id='" + id + "', type=" + type + ", status=" + status
                + ", priority=" + priority + ", workerId='" + workerId
                + "', attempts=" + attempts.size() + "}";
    }
}
