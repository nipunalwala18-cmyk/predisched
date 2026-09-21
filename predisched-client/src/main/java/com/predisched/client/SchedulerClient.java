package com.predisched.client;

import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.obs.TraceContext;
import com.predisched.common.time.Clocks;
import com.predisched.common.time.LamportClock;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.DeadLetterRequest;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;

/** Thin wrapper over the scheduler blocking stub. */
public class SchedulerClient implements AutoCloseable {

    private final io.grpc.ManagedChannel channel;
    private final SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;
    private final LamportClock clock = Clocks.lamport();

    public SchedulerClient(String host, int port) {
        this.channel = io.grpc.ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .intercept(LamportInterceptors.client(clock))
                .build();
        this.stub = SchedulerServiceGrpc.newBlockingStub(channel);
    }

    SchedulerClient(SchedulerServiceGrpc.SchedulerServiceBlockingStub stub) {
        this.channel = null;
        this.stub = stub;
    }

    public TaskResponse submitTask(String taskId, TaskType type, String input, int priority) {
        return submitTask(taskId, type, input, priority, TraceContext.newTraceId());
    }

    /** Submits with an explicit trace id, which follows the task to the worker (F10). */
    public TaskResponse submitTask(
            String taskId, TaskType type, String input, int priority, String traceId) {
        return submitTask(taskId, type, input, priority, traceId, 0L, -1);
    }

    /**
     * Full submit.
     *
     * @param timeoutMs  0 leaves the scheduler's default in place (F5)
     * @param maxRetries negative leaves the scheduler's default in place (F4)
     */
    public TaskResponse submitTask(
            String taskId,
            TaskType type,
            String input,
            int priority,
            String traceId,
            long timeoutMs,
            int maxRetries) {
        TraceContext.set(traceId);
        try {
            return stub.submitTask(TaskRequest.newBuilder()
                    .setTaskId(taskId)
                    .setType(type)
                    .setInput(input)
                    .setPriority(priority)
                    .setLamportTime(clock.current())
                    .setTraceId(traceId)
                    .setTimeoutMs(timeoutMs)
                    .setMaxRetries(maxRetries)
                    .build());
        } finally {
            TraceContext.clear();
        }
    }

    public DeadLetterList listDeadLetters(int limit) {
        return stub.listDeadLetters(DeadLetterRequest.newBuilder().setLimit(limit).build());
    }

    public TaskResponse retryDeadLetter(String taskId) {
        return stub.retryDeadLetter(TaskStatusRequest.newBuilder().setTaskId(taskId).build());
    }

    public TaskStatusResponse getStatus(String taskId) {
        return stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(taskId).build());
    }

    public TaskResponse cancelTask(String taskId) {
        return stub.cancelTask(TaskStatusRequest.newBuilder().setTaskId(taskId).build());
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }
}
