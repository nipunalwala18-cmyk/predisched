package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;

/** Runs the matching executor synchronously (prompt 02 adds thread pools). */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final ExecutorRegistry registry;
    private final String workerId;

    public WorkerServiceImpl(ExecutorRegistry registry, String workerId) {
        this.registry = registry;
        this.workerId = workerId;
    }

    @Override
    public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
        long start = System.nanoTime();
        String taskId = request.getTask().getTaskId();
        ExecutionResult result = registry.execute(request.getTask().getType(), request.getTask().getInput());
        long execMs = (System.nanoTime() - start) / 1_000_000L;
        String output = result.success() ? result.output() : "error: " + result.errorMessage();
        ExecuteResult reply = ExecuteResult.newBuilder()
                .setTaskId(taskId)
                .setSuccess(result.success())
                .setOutput(output)
                .setExecTimeMs(execMs)
                .setWaitTimeMs(0L)
                .setLamportTime(0L)
                .build();
        observer.onNext(reply);
        observer.onCompleted();
    }

    public String workerId() {
        return workerId;
    }
}
