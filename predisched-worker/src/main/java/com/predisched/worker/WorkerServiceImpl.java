package com.predisched.worker;

import com.predisched.proto.Ack;
import com.predisched.proto.CancelRequest;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.ExecutionState;
import com.predisched.proto.ExecutionStatus;
import com.predisched.proto.QueryExecutionRequest;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Hands the task to the {@link ExecutionEngine} and answers when it finishes, so one gRPC call
 * never blocks a pool thread while others are free (lab Exp 2). A dispatch id makes the call
 * idempotent, and {@code QueryExecution} reports on one (prompt 10).
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
        engine.submit(taskId, request.getDispatchId(), request.getTask().getType(),
                        request.getTask().getInput(), request.getTask().getTraceId())
                .thenAccept(outcome -> {
                    observer.onNext(toResult(taskId, outcome));
                    observer.onCompleted();
                });
    }

    @Override
    public void queryExecution(QueryExecutionRequest request,
            StreamObserver<ExecutionStatus> observer) {
        ExecutionStatus.Builder reply = ExecutionStatus.newBuilder();
        switch (engine.query(request.getDispatchId())) {
            case RUNNING -> reply.setState(ExecutionState.EXECUTION_RUNNING);
            case FINISHED -> {
                reply.setState(ExecutionState.EXECUTION_FINISHED);
                engine.finishedOutcome(request.getDispatchId()).ifPresent(outcome ->
                        reply.setResult(toResult(request.getTaskId(), outcome)));
            }
            default -> reply.setState(ExecutionState.EXECUTION_UNKNOWN);
        }
        observer.onNext(reply.build());
        observer.onCompleted();
    }

    private static ExecuteResult toResult(String taskId, ExecutionEngine.Outcome outcome) {
        return ExecuteResult.newBuilder()
                .setTaskId(taskId)
                .setSuccess(outcome.success())
                .setOutput(outcome.output())
                .setExecTimeMs(outcome.execMs())
                .setWaitTimeMs(outcome.waitMs())
                .setRejected(outcome.rejected())
                .setLamportTime(com.predisched.common.time.Clocks.lamport().current())
                .build();
    }

    @Override
    public void cancelExecution(CancelRequest request, StreamObserver<Ack> observer) {
        boolean stopped = engine.cancel(request.getTaskId(), request.getReason());
        observer.onNext(Ack.newBuilder()
                .setOk(stopped)
                .setMessage(stopped
                        ? "cancelled " + request.getTaskId()
                        : "not running: " + request.getTaskId())
                .build());
        observer.onCompleted();
    }

    public String workerId() {
        return workerId;
    }
}
