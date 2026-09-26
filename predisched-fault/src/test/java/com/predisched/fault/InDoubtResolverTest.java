package com.predisched.fault;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.ExecutionState;
import com.predisched.proto.ExecutionStatus;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The three in-doubt cases, plus a worker that does not answer. */
class InDoubtResolverTest {

    /** Worker answers by dispatch id; records what the resolver asked for. */
    private static final class FakeActions implements InDoubtActions {
        final Map<String, ExecutionStatus> answers = new HashMap<>();
        final List<String> calls = new ArrayList<>();
        final List<String> queried = new ArrayList<>();

        @Override
        public Optional<ExecutionStatus> query(String workerId, String taskId, String dispatchId) {
            queried.add(dispatchId);
            return Optional.ofNullable(answers.get(dispatchId));
        }

        @Override
        public void reattach(TaskRecord task, int attempt) {
            calls.add("reattach " + task.id() + " #" + attempt);
        }

        @Override
        public void takeResult(TaskRecord task, int attempt, ExecuteResult result) {
            calls.add("result " + task.id() + " #" + attempt + " " + result.getOutput());
        }

        @Override
        public void requeue(TaskRecord task, String reason) {
            calls.add("requeue " + task.id());
        }

        @Override
        public void workerLost(TaskRecord task, int attempt, String reason) {
            calls.add("lost " + task.id() + " #" + attempt);
        }
    }

    @Test
    void stillRunningReattachesFinishedTakesTheResultUnknownRequeues() {
        FakeActions worker = new FakeActions();
        worker.answers.put("run#1", state(ExecutionState.EXECUTION_RUNNING));
        worker.answers.put("done#1", ExecutionStatus.newBuilder()
                .setState(ExecutionState.EXECUTION_FINISHED)
                .setResult(ExecuteResult.newBuilder().setSuccess(true).setOutput("42"))
                .build());
        worker.answers.put("lost#1", state(ExecutionState.EXECUTION_UNKNOWN));

        Map<InDoubtResolver.Resolution, Integer> counts = new InDoubtResolver(worker).resolve(List.of(
                running("run", "worker-1"), running("done", "worker-1"),
                running("lost", "worker-1")));

        assertEquals(List.of("reattach run #1", "result done #1 42", "requeue lost"), worker.calls);
        assertEquals(1, counts.get(InDoubtResolver.Resolution.REATTACHED));
        assertEquals(1, counts.get(InDoubtResolver.Resolution.TOOK_RESULT));
        assertEquals(1, counts.get(InDoubtResolver.Resolution.REQUEUED));
    }

    @Test
    void theDispatchIdNamesTheAttemptTheOldPrimaryWasRunning() {
        FakeActions worker = new FakeActions();
        worker.answers.put("t#3", state(ExecutionState.EXECUTION_RUNNING));
        TaskRecord twiceFailed = running("t", "worker-2")
                .withAttempt(new TaskAttempt(1, "worker-1", TaskAttempt.Outcome.FAILED, "x", 0, 0, 0))
                .withAttempt(new TaskAttempt(2, "worker-1", TaskAttempt.Outcome.FAILED, "x", 0, 0, 0));

        new InDoubtResolver(worker).resolve(List.of(twiceFailed));

        assertEquals(List.of("t#3"), worker.queried);
        assertEquals(List.of("reattach t #3"), worker.calls);
    }

    @Test
    void anUnreachableWorkerLosesTheAttemptAndIsAskedOnlyOnce() {
        FakeActions worker = new FakeActions();   // answers nothing: unreachable

        Map<InDoubtResolver.Resolution, Integer> counts = new InDoubtResolver(worker).resolve(
                List.of(running("a", "worker-3"), running("b", "worker-3")));

        assertEquals(List.of("lost a #1", "lost b #1"), worker.calls);
        assertEquals(1, worker.queried.size(), "the second task skips the dead worker");
        assertEquals(2, counts.get(InDoubtResolver.Resolution.WORKER_LOST));
    }

    private static ExecutionStatus state(ExecutionState state) {
        return ExecutionStatus.newBuilder().setState(state).build();
    }

    private static TaskRecord running(String id, String worker) {
        return TaskRecord.createQueued(id, TaskType.SLEEP_TASK, "ms=10", 5)
                .withWorkerId(worker)
                .withStatus(TaskStatus.RUNNING);
    }
}
