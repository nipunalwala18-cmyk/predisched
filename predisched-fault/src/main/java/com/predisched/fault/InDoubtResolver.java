package com.predisched.fault;

import com.predisched.common.TaskRecord;
import com.predisched.common.obs.EventLog;
import com.predisched.proto.ExecutionStatus;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Settles every task a dead primary left RUNNING (prompt 10). The new primary cannot know whether
 * the worker got the call, is still running it, or finished it while no one was listening, so it
 * asks: still running means re-attach, finished means take the result, unknown means it never
 * arrived and goes back to the queue. A worker that does not answer lost the attempt.
 *
 * <p>An attempt is named by its dispatch id, {@code <taskId>#<attempt>}. The attempt number is
 * the record's attempt count plus one, which the old primary used too, so both sides name the
 * same attempt without storing anything extra, and the worker never runs one attempt twice.
 */
public class InDoubtResolver {

    private static final Logger log = LoggerFactory.getLogger(InDoubtResolver.class);

    /** How one in-doubt task was settled. */
    public enum Resolution { REATTACHED, TOOK_RESULT, REQUEUED, WORKER_LOST }

    private final InDoubtActions actions;

    public InDoubtResolver(InDoubtActions actions) {
        this.actions = actions;
    }

    /** The id of one attempt at a task, as the dispatcher sends it. */
    public static String dispatchId(String taskId, int attempt) {
        return taskId + "#" + attempt;
    }

    /** Settles every task; returns how many ended each way. */
    public Map<Resolution, Integer> resolve(List<TaskRecord> running) {
        Map<Resolution, Integer> counts = new EnumMap<>(Resolution.class);
        // One unanswered query is enough: the rest on that worker would each wait out a deadline.
        Set<String> unreachable = new HashSet<>();
        for (TaskRecord task : running) {
            Resolution resolution = resolve(task, unreachable);
            counts.merge(resolution, 1, Integer::sum);
        }
        return counts;
    }

    Resolution resolve(TaskRecord task, Set<String> unreachable) {
        int attempt = task.attemptCount() + 1;
        String dispatchId = dispatchId(task.id(), attempt);
        String worker = task.workerId();
        Optional<ExecutionStatus> status = worker.isEmpty() || unreachable.contains(worker)
                ? Optional.empty()
                : actions.query(worker, task.id(), dispatchId);
        Resolution resolution;
        if (status.isEmpty()) {
            if (!worker.isEmpty()) {
                unreachable.add(worker);
            }
            resolution = Resolution.WORKER_LOST;
            actions.workerLost(task, attempt, "worker " + worker + " unreachable after failover");
        } else {
            switch (status.get().getState()) {
                case EXECUTION_RUNNING -> {
                    resolution = Resolution.REATTACHED;
                    actions.reattach(task, attempt);
                }
                case EXECUTION_FINISHED -> {
                    resolution = Resolution.TOOK_RESULT;
                    actions.takeResult(task, attempt, status.get().getResult());
                }
                default -> {
                    resolution = Resolution.REQUEUED;
                    actions.requeue(task, "worker " + worker + " never received " + dispatchId);
                }
            }
        }
        log.info("In-doubt {} on {} (dispatch {}): {}", task.id(),
                worker.isEmpty() ? "no worker" : worker, dispatchId, describe(resolution));
        EventLog.get().event("IN_DOUBT", task.id(), Map.of(
                "worker", worker,
                "dispatch", dispatchId,
                "resolution", resolution.name()));
        return resolution;
    }

    private static String describe(Resolution resolution) {
        return switch (resolution) {
            case REATTACHED -> "still running there, re-attached";
            case TOOK_RESULT -> "finished there, took its result";
            case REQUEUED -> "never arrived, re-queued";
            case WORKER_LOST -> "worker did not answer, attempt lost";
        };
    }
}
