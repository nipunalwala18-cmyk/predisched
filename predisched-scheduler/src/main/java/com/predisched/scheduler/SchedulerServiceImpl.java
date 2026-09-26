package com.predisched.scheduler;

import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskAttempt;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.TraceContext;
import com.predisched.common.time.Clocks;
import com.predisched.proto.DeadLetterEntry;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.DeadLetterRequest;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.proto.DeadLetterEntry;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.DeadLetterRequest;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates, stores as QUEUED and hands tasks to the {@link Dispatcher}.
 * Invalid requests get {@code accepted=false} with every validation message.
 */
public class SchedulerServiceImpl extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(SchedulerServiceImpl.class);

    private final TaskStore store;
    private final TaskValidator validator;
    private final TaskQueue dispatchQueue;
    private final RetryCoordinator retries;
    private final Leadership leadership;

    public SchedulerServiceImpl(
            TaskStore store,
            TaskValidator validator,
            TaskQueue dispatchQueue,
            RetryCoordinator retries) {
        this(store, validator, dispatchQueue, retries, Leadership.ALONE);
    }

    public SchedulerServiceImpl(
            TaskStore store,
            TaskValidator validator,
            TaskQueue dispatchQueue,
            RetryCoordinator retries,
            Leadership leadership) {
        this.store = store;
        this.validator = validator;
        this.dispatchQueue = dispatchQueue;
        this.retries = retries;
        this.leadership = leadership;
    }

    @Override
    public void submitTask(TaskRequest request, StreamObserver<TaskResponse> observer) {
        if (!leadership.isLeader()) {
            String leader = leadership.leader().map(String::valueOf).orElse("unknown");
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request == null ? "" : request.getTaskId())
                    .setAccepted(false)
                    .setMessage("not the leader; leader=" + leader)
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
            return;
        }
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
        String traceId = request.getTraceId().isEmpty()
                ? TraceContext.newTraceId()
                : request.getTraceId();
        TraceContext.set(traceId);
        TaskRecord record = TaskRecord.createQueued(
                request.getTaskId(), request.getType(), request.getInput(), request.getPriority(),
                traceId,
                request.getTimeoutMs(),
                // proto3 cannot tell "0 retries" from "unset", so 0 means the default and a
                // client that really wants no retries sends -1 through --max-retries 0 handling
                // in the CLI.
                request.getMaxRetries() == 0 ? -1 : request.getMaxRetries());
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
        } catch (RuntimeException e) {
            // A replicated store that cannot reach its write quorum (prompt 07): not accepted.
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setAccepted(false)
                    .setMessage("could not store the task: " + e.getMessage())
                    .setLamportTime(0L)
                    .build());
            observer.onCompleted();
            return;
        }
        EventLog.get().event(EventLog.SUBMIT, record.id(), Map.of(
                "task_type", record.type().name(),
                "priority", String.valueOf(record.priority())));
        dispatchQueue.add(record.id(), record.priority(), record.submittedAt());
        EventLog.get().event(EventLog.ENQUEUE, record.id(),
                Map.of("queue_len", String.valueOf(dispatchQueue.size())));
        log.info("Accepted {} ({}) and queued it", record.id(), record.type());
        observer.onNext(TaskResponse.newBuilder()
                .setTaskId(record.id())
                .setAccepted(true)
                .setMessage("queued")
                .setLamportTime(Clocks.lamport().current())
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
                .setTraceId(record.traceId())
                .setLamportTime(Clocks.lamport().current())
                .setAttempt(record.attemptCount())
                .addAllAttemptHistory(record.attempts().stream()
                        .map(TaskAttempt::toString)
                        .toList())
                .build());
        observer.onCompleted();
    }

    @Override
    public void listDeadLetters(DeadLetterRequest request, StreamObserver<DeadLetterList> observer) {
        DeadLetterList.Builder reply = DeadLetterList.newBuilder();
        for (DeadLetterQueue.Entry entry : retries.deadLetters().list(request.getLimit())) {
            reply.addEntries(DeadLetterEntry.newBuilder()
                    .setTaskId(entry.record().id())
                    .setType(entry.record().type())
                    .setInput(entry.record().input())
                    .setAttempts(entry.record().attemptCount())
                    .setLastError(entry.lastError())
                    .setFailedAtMs(entry.failedAtMs())
                    .build());
        }
        observer.onNext(reply.build());
        observer.onCompleted();
    }

    @Override
    public void retryDeadLetter(TaskStatusRequest request, StreamObserver<TaskResponse> observer) {
        boolean requeued = retries.retryDeadLetter(request.getTaskId());
        observer.onNext(TaskResponse.newBuilder()
                .setTaskId(request.getTaskId())
                .setAccepted(requeued)
                .setMessage(requeued
                        ? "re-queued from the dead-letter queue"
                        : "not in the dead-letter queue: " + request.getTaskId())
                .setLamportTime(Clocks.lamport().current())
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
                .setLamportTime(Clocks.lamport().current())
                .build());
        observer.onCompleted();
    }
}
