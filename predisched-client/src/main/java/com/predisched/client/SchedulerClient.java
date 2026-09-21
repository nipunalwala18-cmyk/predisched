package com.predisched.client;

import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;

/** Thin wrapper over the scheduler blocking stub. */
public class SchedulerClient implements AutoCloseable {

    private final io.grpc.ManagedChannel channel;
    private final SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;

    public SchedulerClient(String host, int port) {
        this.channel = io.grpc.ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = SchedulerServiceGrpc.newBlockingStub(channel);
    }

    SchedulerClient(SchedulerServiceGrpc.SchedulerServiceBlockingStub stub) {
        this.channel = null;
        this.stub = stub;
    }

    public TaskResponse submitTask(String taskId, TaskType type, String input, int priority) {
        return stub.submitTask(TaskRequest.newBuilder()
                .setTaskId(taskId)
                .setType(type)
                .setInput(input)
                .setPriority(priority)
                .setLamportTime(0L)
                .build());
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
