package com.predisched.scheduler;

import com.predisched.common.InputParser;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.common.WorkflowDag;
import com.predisched.common.auth.AuthInterceptor;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.TraceContext;
import com.predisched.common.time.Clocks;
import com.predisched.proto.DeadLetterEntry;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.DeadLetterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkflowRequest;
import com.predisched.proto.WorkflowStatusRequest;
import com.predisched.proto.WorkflowStatusResponse;
import com.predisched.scheduler.auth.ClientLimits;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.TaskQueue;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates, stores as QUEUED and hands tasks to the {@link Dispatcher}. Invalid requests get
 * {@code accepted=false} with every validation message. Workflows (F1) are validated as a whole
 * and handed to the {@link WorkflowManager}. With auth on, every task records the client that
 * submitted it, and the client's quota of unfinished tasks is checked (F9).
 */
public class SchedulerServiceImpl extends SchedulerServiceGrpc.SchedulerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(SchedulerServiceImpl.class);

    private final TaskStore store;
    private final TaskValidator validator;
    private final TaskQueue dispatchQueue;
    private final RetryCoordinator retries;
    private final Leadership leadership;
    private final WorkflowManager workflows;
    private final ClientLimits limits;

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
        this(store, validator, dispatchQueue, retries, leadership, null);
    }

    /** @param limits null when auth is off: no quotas */
    public SchedulerServiceImpl(
            TaskStore store,
            TaskValidator validator,
            TaskQueue dispatchQueue,
            RetryCoordinator retries,
            Leadership leadership,
            ClientLimits limits) {
        this.store = store;
        this.validator = validator;
        this.dispatchQueue = dispatchQueue;
        this.retries = retries;
        this.leadership = leadership;
        this.limits = limits;
        this.workflows = new WorkflowManager(store, dispatchQueue, retries);
        if (limits != null) {
            retries.addTerminalListener(record -> limits.release(record.clientId(), 1));
        }
    }

    public WorkflowManager workflows() {
        return workflows;
    }

    @Override
    public void submitTask(TaskRequest request, StreamObserver<TaskResponse> observer) {
        String refusal = notLeader();
        if (refusal == null && request.getDependsOnCount() > 0) {
            refusal = "depends_on is only allowed inside SubmitWorkflow";
        }
        if (refusal != null) {
            reply(observer, request.getTaskId(), false, refusal);
            return;
        }
        List<String> errors = validator.validate(request, store);
        if (!errors.isEmpty()) {
            reply(observer, request.getTaskId(), false, String.join("; ", errors));
            return;
        }
        String traceId = request.getTraceId().isEmpty()
                ? TraceContext.newTraceId()
                : request.getTraceId();
        TraceContext.set(traceId);
        if (request.getType() == TaskType.WORKFLOW_TASK) {
            submitWorkflowTask(request, traceId, observer);
            return;
        }
        String clientId = callerClientId();
        String overQuota = limits == null ? null : limits.reserve(clientId, 1);
        if (overQuota != null) {
            reply(observer, request.getTaskId(), false, overQuota);
            return;
        }
        TaskRecord record = TaskRecord.createQueued(
                request.getTaskId(), request.getType(), request.getInput(), request.getPriority(),
                traceId,
                request.getTimeoutMs(),
                // proto3 cannot tell "0 retries" from "unset", so 0 means the default and a
                // client that really wants no retries sends -1 through --max-retries 0 handling
                // in the CLI.
                request.getMaxRetries() == 0 ? -1 : request.getMaxRetries())
                .withClientId(clientId);
        try {
            store.put(record);
        } catch (IllegalStateException e) {
            releaseReservation(clientId, 1);
            reply(observer, request.getTaskId(), false,
                    "task_id already exists: " + request.getTaskId());
            return;
        } catch (RuntimeException e) {
            // A replicated store that cannot reach its write quorum (prompt 07): not accepted.
            releaseReservation(clientId, 1);
            reply(observer, request.getTaskId(), false,
                    "could not store the task: " + e.getMessage());
            return;
        }
        EventLog.get().event(EventLog.SUBMIT, record.id(), Map.of(
                "task_type", record.type().name(),
                "priority", String.valueOf(record.priority()),
                "client", clientId));
        dispatchQueue.add(record.id(), record.priority(), record.submittedAt());
        EventLog.get().event(EventLog.ENQUEUE, record.id(),
                Map.of("queue_len", String.valueOf(dispatchQueue.size())));
        log.info("Accepted {} ({}) and queued it", record.id(), record.type());
        reply(observer, record.id(), true, "queued");
    }

    @Override
    public void submitWorkflow(WorkflowRequest request, StreamObserver<TaskResponse> observer) {
        String refusal = notLeader();
        if (refusal != null) {
            reply(observer, request.getWorkflowId(), false, refusal);
            return;
        }
        String traceId = TraceContext.newTraceId();
        TraceContext.set(traceId);
        acceptWorkflow(request, traceId, observer);
    }

    /**
     * A WORKFLOW_TASK expands its {@code dag=<path>} file into a workflow whose id is the task's
     * id, plus the task itself as the last node, depending on every other: its status is the
     * workflow's.
     */
    private void submitWorkflowTask(TaskRequest request, String traceId,
            StreamObserver<TaskResponse> observer) {
        WorkflowDag dag;
        String path = InputParser.parse(request.getInput()).get("dag");
        try {
            dag = WorkflowDag.load(Path.of(path));
        } catch (Exception e) {
            reply(observer, request.getTaskId(), false,
                    "cannot load workflow " + path + ": " + e.getMessage());
            return;
        }
        WorkflowRequest expanded = dag.toRequest(request.getTaskId(), traceId);
        TaskRequest.Builder self = request.toBuilder().clearDependsOn();
        expanded.getTasksList().forEach(task -> self.addDependsOn(task.getTaskId()));
        acceptWorkflow(expanded.toBuilder().addTasks(self).build(), traceId, observer);
    }

    private void acceptWorkflow(WorkflowRequest request, String traceId,
            StreamObserver<TaskResponse> observer) {
        List<String> errors = workflows.validate(request, validator);
        if (!errors.isEmpty()) {
            reply(observer, request.getWorkflowId(), false, String.join("; ", errors));
            return;
        }
        String clientId = callerClientId();
        int size = request.getTasksCount();
        String overQuota = limits == null ? null : limits.reserve(clientId, size);
        if (overQuota != null) {
            reply(observer, request.getWorkflowId(), false, overQuota);
            return;
        }
        try {
            workflows.submit(request, clientId, traceId);
        } catch (RuntimeException e) {
            releaseReservation(clientId, size);
            reply(observer, request.getWorkflowId(), false,
                    "could not store the workflow: " + e.getMessage());
            return;
        }
        reply(observer, request.getWorkflowId(), true,
                "workflow accepted: " + size + " tasks");
    }

    @Override
    public void getWorkflowStatus(WorkflowStatusRequest request,
            StreamObserver<WorkflowStatusResponse> observer) {
        List<TaskRecord> tasks = workflows.status(request.getWorkflowId());
        WorkflowStatusResponse.Builder reply = WorkflowStatusResponse.newBuilder()
                .setWorkflowId(request.getWorkflowId())
                .setFound(!tasks.isEmpty());
        tasks.forEach(task -> reply.addTasks(statusOf(task)));
        observer.onNext(reply.build());
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
        observer.onNext(statusOf(record));
        observer.onCompleted();
    }

    private static TaskStatusResponse statusOf(TaskRecord record) {
        return TaskStatusResponse.newBuilder()
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
                .build();
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
        reply(observer, request.getTaskId(), requeued, requeued
                ? "re-queued from the dead-letter queue"
                : "not in the dead-letter queue: " + request.getTaskId());
    }

    /** Cancels a QUEUED task, or a BLOCKED one waiting in a workflow; its descendants follow. */
    @Override
    public void cancelTask(TaskStatusRequest request, StreamObserver<TaskResponse> observer) {
        TaskRecord record = store.get(request.getTaskId());
        if (record == null) {
            reply(observer, request.getTaskId(), false, "unknown task: " + request.getTaskId());
            return;
        }
        if (record.status() != TaskStatus.QUEUED && record.status() != TaskStatus.BLOCKED) {
            reply(observer, request.getTaskId(), false,
                    "only QUEUED or BLOCKED tasks can be cancelled (status=" + record.status() + ")");
            return;
        }
        TaskRecord cancelled;
        try {
            cancelled = store.update(request.getTaskId(), r -> r.withStatus(TaskStatus.CANCELLED));
        } catch (IllegalStateException e) {
            reply(observer, request.getTaskId(), false, e.getMessage());
            return;
        }
        dispatchQueue.remove(request.getTaskId());
        retries.notifyTerminal(cancelled);
        reply(observer, request.getTaskId(), true, "cancelled");
    }

    /** Null when this node may take work, else why not (only the leader accepts, FR9). */
    private String notLeader() {
        if (leadership.isLeader()) {
            return null;
        }
        return "not the leader; leader=" + leadership.leader().map(String::valueOf).orElse("unknown");
    }

    /** The authenticated client, or empty with auth off or for a node. */
    private static String callerClientId() {
        return AuthInterceptor.current()
                .filter(principal -> !principal.node())
                .map(AuthInterceptor.Principal::clientId)
                .orElse("");
    }

    private void releaseReservation(String clientId, int count) {
        if (limits != null) {
            limits.release(clientId, count);
        }
    }

    private static void reply(StreamObserver<TaskResponse> observer, String taskId,
            boolean accepted, String message) {
        observer.onNext(TaskResponse.newBuilder()
                .setTaskId(taskId == null ? "" : taskId)
                .setAccepted(accepted)
                .setMessage(message)
                .setLamportTime(accepted ? Clocks.lamport().current() : 0L)
                .build());
        observer.onCompleted();
    }
}
