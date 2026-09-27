package com.predisched.scheduler;

import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.db.History;
import com.predisched.common.obs.EventLog;
import com.predisched.common.time.Clocks;
import com.predisched.fault.InDoubtActions;
import com.predisched.fault.PrimaryRole;
import com.predisched.fault.Recovery;
import com.predisched.fault.WorkerFailureDetector;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.ExecutionStatus;
import com.predisched.proto.QueryExecutionRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.scheduler.auth.ClientLimits;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import io.grpc.StatusRuntimeException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The scheduler's side of fault tolerance (prompt 10): what a promotion rebuilds, how an in-doubt
 * dispatch is settled, and what happens to a worker declared dead. The decisions of when live in
 * {@code predisched-fault}; this class owns the queue, the dispatcher and the registry they act on.
 */
public class SchedulerFailover
        implements PrimaryRole, InDoubtActions, WorkerFailureDetector.Workers {

    private static final Logger log = LoggerFactory.getLogger(SchedulerFailover.class);

    private final TaskStore store;
    private final TaskQueue queue;
    private final RunningTasks running;
    private final Dispatcher dispatcher;
    private final WorkerRegistry workers;
    private final WorkerStubs stubs;
    private final RetryCoordinator retries;
    private final WorkflowManager workflows;
    private final ClientLimits limits;
    private final long queryTimeoutMs;

    /** @param limits null when auth is off */
    public SchedulerFailover(
            TaskStore store,
            TaskQueue queue,
            RunningTasks running,
            Dispatcher dispatcher,
            WorkerRegistry workers,
            WorkerStubs stubs,
            RetryCoordinator retries,
            WorkflowManager workflows,
            ClientLimits limits,
            long queryTimeoutMs) {
        this.store = store;
        this.queue = queue;
        this.running = running;
        this.dispatcher = dispatcher;
        this.workers = workers;
        this.stubs = stubs;
        this.retries = retries;
        this.workflows = workflows;
        this.limits = limits;
        this.queryTimeoutMs = queryTimeoutMs;
    }

    // ---- promotion ----------------------------------------------------------------------

    @Override
    public void rebuild(Recovery recovery) {
        queue.clear();
        running.clear();
        retries.deadLetters().clear();
        for (TaskRecord task : recovery.queued()) {
            queue.add(task.id(), task.priority(), task.submittedAt());
        }
        workflows.restore(recovery.all());
        int parked = 0;
        Map<String, Integer> unfinishedByClient = new HashMap<>();
        for (TaskRecord task : recovery.all()) {
            if (task.status() == TaskStatus.FAILED) {
                retries.deadLetters().add(task, lastReason(task), task.completedAt());
                parked++;
            }
            if (!task.isTerminal() && !task.clientId().isEmpty()) {
                unfinishedByClient.merge(task.clientId(), 1, Integer::sum);
            }
        }
        if (limits != null) {
            limits.restore(unfinishedByClient);
        }
        log.info("Queue rebuilt from the store: {} queued, {} in the dead-letter queue",
                queue.size(), parked);
    }

    @Override
    public void startDispatching() {
        dispatcher.start();
    }

    @Override
    public void stopDispatching() {
        dispatcher.stop();
    }

    // ---- in-doubt dispatches --------------------------------------------------------------

    @Override
    public Optional<ExecutionStatus> query(String workerId, String taskId, String dispatchId) {
        Optional<WorkerInfo> worker = workers.get(workerId);
        if (worker.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(stubs.stubFor(worker.get())
                    .withDeadlineAfter(queryTimeoutMs, TimeUnit.MILLISECONDS)
                    .queryExecution(QueryExecutionRequest.newBuilder()
                            .setTaskId(taskId)
                            .setDispatchId(dispatchId)
                            .build()));
        } catch (StatusRuntimeException e) {
            log.info("Worker {} did not answer about {}: {}", workerId, dispatchId,
                    e.getStatus().getCode());
            return Optional.empty();
        }
    }

    @Override
    public void reattach(TaskRecord task, int attempt) {
        Optional<WorkerInfo> worker = workers.get(task.workerId());
        if (worker.isEmpty()) {
            workerLost(task, attempt, "worker " + task.workerId() + " is no longer registered");
            return;
        }
        dispatcher.reattach(task, worker.get(), attempt);
    }

    @Override
    public void takeResult(TaskRecord task, int attempt, ExecuteResult result) {
        dispatcher.takeResult(task, attempt, result);
    }

    @Override
    public void requeue(TaskRecord task, String reason) {
        TaskRecord queued = store.update(task.id(), record -> record.status() == TaskStatus.RUNNING
                ? record.withStatus(TaskStatus.QUEUED)
                : record);
        if (queued.status() == TaskStatus.QUEUED) {
            queue.add(queued.id(), queued.priority(), queued.submittedAt());
            log.info("Task {} re-queued: {}", task.id(), reason);
        }
    }

    @Override
    public void workerLost(TaskRecord task, int attempt, String reason) {
        retries.failed(task.id(), new TaskAttempt(attempt, task.workerId(),
                TaskAttempt.Outcome.WORKER_LOST, reason, task.startedAt(), Clocks.now(), 0L),
                reason);
    }

    // ---- worker failure -------------------------------------------------------------------

    @Override
    public Map<String, Long> lastHeartbeats() {
        Map<String, Long> last = new HashMap<>();
        for (WorkerInfo worker : workers.all()) {
            last.put(worker.id(), worker.lastHeartbeatMs());
        }
        return last;
    }

    /**
     * Drops the worker, closes its channel (so a call still waiting on it fails at once), and
     * re-queues every task it was running with reason WORKER_LOST. The retry rules then place
     * them on another worker; if the worker comes back it registers as new.
     */
    @Override
    public void declareDead(String workerId, long silentMs, int missed) {
        Optional<WorkerInfo> worker = workers.get(workerId);
        workers.remove(workerId);
        worker.ifPresent(stubs::forget);
        List<RunningTasks.Running> lost = running.settleAllOn(workerId);
        log.warn("Worker {} missed {} heartbeats ({} ms silent): marked DEAD; re-queueing its {}"
                + " running tasks", workerId, missed, silentMs, lost.size());
        worker.ifPresent(dead -> History.get().worker(dead.id(), dead.host(), dead.port(),
                dead.cores(), dead.memoryMb(), dead.poolSize(), "DEAD", dead.lastHeartbeatMs()));
        History.get().failure(workerId, "WORKER_DEAD", Clocks.now(),
                "missed " + missed + " heartbeats, " + lost.size() + " tasks re-queued");
        EventLog.get().event("WORKER_DEAD", "", Map.of(
                "worker", workerId,
                "silent_ms", String.valueOf(silentMs),
                "requeued", String.valueOf(lost.size())));
        String reason = "WORKER_LOST: " + workerId + " missed " + missed + " heartbeats";
        for (RunningTasks.Running task : lost) {
            boolean partnerRunning = task.speculative()
                    ? running.get(task.taskId()).isPresent()
                    : running.speculative(task.taskId()).isPresent();
            if (partnerRunning) {
                // Its other speculative copy is still running elsewhere: that one finishes it.
                log.info("{} lost its copy on dead worker {}; the other copy carries on",
                        task.taskId(), workerId);
                store.update(task.taskId(), r -> r.withAttempt(new TaskAttempt(task.attempt(),
                        workerId, TaskAttempt.Outcome.WORKER_LOST, reason, task.startedAtMs(),
                        Clocks.now(), 0L)));
                continue;
            }
            log.info("Reassigning {} (attempt {}) from dead worker {}", task.taskId(),
                    task.attempt(), workerId);
            try {
                retries.failed(task.taskId(), new TaskAttempt(task.attempt(), workerId,
                        TaskAttempt.Outcome.WORKER_LOST, reason, task.startedAtMs(),
                        Clocks.now(), 0L), reason);
            } catch (RuntimeException e) {
                log.warn("Could not re-queue {}: {}", task.taskId(), e.getMessage());
            }
        }
    }

    private static String lastReason(TaskRecord task) {
        List<TaskAttempt> attempts = task.attempts();
        return attempts.isEmpty() ? task.result() : attempts.get(attempts.size() - 1).reason();
    }
}
