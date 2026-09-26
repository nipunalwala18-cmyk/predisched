package com.predisched.fault;

import com.predisched.common.TaskRecord;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.ExecutionStatus;
import java.util.Optional;

/**
 * What the {@link InDoubtResolver} can do about a task the old primary left RUNNING. The scheduler
 * implements it with its worker stubs and dispatcher.
 */
public interface InDoubtActions {

    /**
     * Asks the task's worker about one dispatch ({@code WorkerService.QueryExecution}). Empty when
     * the worker is not registered here or does not answer.
     */
    Optional<ExecutionStatus> query(String workerId, String taskId, String dispatchId);

    /** Still running there: send the same dispatch again, which waits for that run's result. */
    void reattach(TaskRecord task, int attempt);

    /** Finished there: record its result as the outcome of that attempt. */
    void takeResult(TaskRecord task, int attempt, ExecuteResult result);

    /** The worker never received it: back to QUEUED, no attempt used. */
    void requeue(TaskRecord task, String reason);

    /** The worker is gone: the attempt ended WORKER_LOST, and the retry rules decide. */
    void workerLost(TaskRecord task, int attempt, String reason);
}
