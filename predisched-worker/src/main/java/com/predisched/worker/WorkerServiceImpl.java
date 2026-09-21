package com.predisched.worker;

import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Hands the task to the {@link ExecutionEngine} and answers when it finishes, so one gRPC call
 * never blocks a pool thread while others are free (lab Exp 2).
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final ExecutionEngine engine;
    private final String workerId;

    public WorkerServiceImpl(ExecutionEngine engine, String workerId) {
        this.engine = engine;
        this.workerId = workerId;
    }

    @Override
    public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
        String taskId = request.getTask().getTaskId();
        engine.submit(taskId, request.getTask().getType(), request.getTask().getInput(),
                        request.getTask().getTraceId())
                .thenAccept(outcome -> {
                    observer.onNext(ExecuteResult.newBuilder()
                            .setTaskId(taskId)
                            .setSuccess(outcome.success())
                            .setOutput(outcome.output())
                            .setExecTimeMs(outcome.execMs())
                            .setWaitTimeMs(outcome.waitMs())
                            .setRejected(outcome.rejected())
                            .setLamportTime(com.predisched.common.time.Clocks.lamport().current())
                            .build());
                    observer.onCompleted();
                });
    }

    public String workerId() {
        return workerId;
    }
}
