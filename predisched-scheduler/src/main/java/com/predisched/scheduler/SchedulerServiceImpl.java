package com.predisched.scheduler;

import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Validates, stores as QUEUED and hands tasks to the {@link Dispatcher}.
 * Invalid requests get {@code accepted=false} with every validation message.
 */
public class SchedulerServiceImpl extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    private final TaskStore store;
    private final TaskValidator validator;
    private final BlockingQueue<String> dispatchQueue;

    public SchedulerServiceImpl(TaskStore store, TaskValidator validator, BlockingQueue<String> dispatchQueue) {
        this.store = store;
        this.validator = validator;
        this.dispatchQueue = dispatchQueue;
    }

    @Override
    public void submitTask(TaskRequest request, StreamObserver<TaskResponse> observer) {
        List<String> errors = validator.validate(request, store);
        if (!errors.isEmpty()) {
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request == null ? "" : request.getTaskId())
                    .setAccepted(false)
                    .setMessage(String.join("; ", errors))
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
            return;
        }
        TaskRecord record = TaskRecord.createQueued(
                request.getTaskId(), request.getType(), request.getInput(), request.getPriority());
        try {
            store.put(record);
        } catch (IllegalStateException e) {
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setAccepted(false)
                    .setMessage("task_id already exists: " + request.getTaskId())
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
            return;
        }
        dispatchQueue.offer(record.id());
        observer.onNext(TaskResponse.newBuilder()
                .setTaskId(record.id())
                .setAccepted(true)
                .setMessage("queued")
                .setLamportTime(0L)
                .build());
        observer.onCompleted();
    }

    @Override
    public void getTaskStatus(TaskStatusRequest request, StreamObserver<TaskStatusResponse> observer) {
        TaskRecord record = store.get(request.getTaskId());
        if (record == null) {
            observer.onError(Status.NOT_FOUND
                    .withDescription("unknown task: " + request.getTaskId())
                    .asRuntimeException());
            return;
        }
        observer.onNext(TaskStatusResponse.newBuilder()
                .setTaskId(record.id())
                .setStatus(record.status())
                .setResult(record.result())
                .setWorkerId(record.workerId())
                .setExecTimeMs(record.execTimeMs())
                .build());
        observer.onCompleted();
    }

    @Override
    public void cancelTask(TaskStatusRequest request, StreamObserver<TaskResponse> observer) {
        TaskRecord record = store.get(request.getTaskId());
        if (record == null) {
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setAccepted(false)
                    .setMessage("unknown task: " + request.getTaskId())
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
            return;
        }
        if (record.status() != TaskStatus.QUEUED) {
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setAccepted(false)
                    .setMessage("only QUEUED tasks can be cancelled (status=" + record.status() + ")")
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
            return;
        }
        try {
            store.update(request.getTaskId(), r -> r.withStatus(TaskStatus.CANCELLED));
        } catch (IllegalStateException e) {
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setAccepted(false)
                    .setMessage(e.getMessage())
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
            return;
        }
        dispatchQueue.remove(request.getTaskId());
        observer.onNext(TaskResponse.newBuilder()
                .setTaskId(request.getTaskId())
                .setAccepted(true)
                .setMessage("cancelled")
                .setLamportTime(0L)
                .build());
        observer.onCompleted();
    }
}
