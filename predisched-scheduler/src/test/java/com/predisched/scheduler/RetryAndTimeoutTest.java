package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.proto.Ack;
import com.predisched.proto.CancelRequest;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.DeadLetterRequest;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Retries with backoff, the dead-letter queue and timeouts end to end (F4, F5, FR25, FR26). */
class RetryAndTimeoutTest {

    /** A worker that fails the first {@code failures} attempts of each task, then succeeds. */
    static class FlakyWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        private final int failures;
        final ConcurrentHashMap<String, AtomicInteger> calls = new ConcurrentHashMap<>();

        FlakyWorker(int failures) {
            this.failures = failures;
        }

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            String taskId = request.getTask().getTaskId();
            int call = calls.computeIfAbsent(taskId, id -> new AtomicInteger()).incrementAndGet();
            boolean ok = call > failures;
            observer.onNext(ExecuteResult.newBuilder()
                    .setTaskId(taskId)
                    .setSuccess(ok)
                    .setOutput(ok ? "ok on call " + call : "injected failure on call " + call)
                    .setExecTimeMs(1)
                    .build());
            observer.onCompleted();
        }
    }

    /** A worker that never answers until its execution is cancelled. */
    static class HangingWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        final CopyOnWriteArrayList<String> cancelled = new CopyOnWriteArrayList<>();
        private final ConcurrentHashMap<String, StreamObserver<ExecuteResult>> waiting =
                new ConcurrentHashMap<>();

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            waiting.put(request.getTask().getTaskId(), observer);
        }

        @Override
        public void cancelExecution(CancelRequest request, StreamObserver<Ack> observer) {
            cancelled.add(request.getTaskId());
            StreamObserver<ExecuteResult> pending = waiting.remove(request.getTaskId());
            if (pending != null) {
                pending.onNext(ExecuteResult.newBuilder()
                        .setTaskId(request.getTaskId())
                        .setSuccess(false)
                        .setOutput("error: cancelled (" + request.getReason() + ")")
                        .build());
                pending.onCompleted();
            }
            observer.onNext(Ack.newBuilder().setOk(pending != null).build());
            observer.onCompleted();
        }
    }

    private static TaskRequest submit(String id, long timeoutMs, int maxRetries) {
        return TaskRequest.newBuilder()
                .setTaskId(id)
                .setType(TaskType.SLEEP_TASK)
                .setInput("ms=1")
                .setPriority(5)
                .setTimeoutMs(timeoutMs)
                .setMaxRetries(maxRetries)
                .build();
    }

    private static TaskRecord awaitStatus(
            SchedulerHarness harness, String taskId, TaskStatus wanted) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            TaskRecord record = harness.store.get(taskId);
            if (record != null && record.status() == wanted) {
                return record;
            }
            Thread.sleep(20);
        }
        TaskRecord record = harness.store.get(taskId);
        throw new AssertionError("Timed out waiting for " + wanted + ", was "
                + (record == null ? "absent" : record.status() + " " + record.attempts()));
    }

    @Test
    void aFlakyTaskSucceedsOnARetryAndKeepsItsHistory() throws Exception {
        try (SchedulerHarness harness = new SchedulerHarness(new FlakyWorker(2))) {
            harness.start();
            String id = "flaky-" + UUID.randomUUID();
            assertTrue(harness.client.submitTask(submit(id, 0, -1)).getAccepted());

            TaskRecord record = awaitStatus(harness, id, TaskStatus.COMPLETED);
            assertEquals(3, record.attemptCount(), "two failures then a success");
            assertEquals(TaskAttempt.Outcome.FAILED, record.attempts().get(0).outcome());
            assertEquals(TaskAttempt.Outcome.FAILED, record.attempts().get(1).outcome());
            assertTrue(record.attempts().get(2).succeeded());
            assertEquals("ok on call 3", record.result());
            assertEquals(0, harness.deadLetters.size());
        }
    }

    @Test
    void aTaskThatAlwaysFailsEndsInTheDeadLetterQueueAndCanBeRetried() throws Exception {
        try (SchedulerHarness harness = new SchedulerHarness(new FlakyWorker(Integer.MAX_VALUE))) {
            harness.start();
            String id = "doomed-" + UUID.randomUUID();
            // maxRetries 2 means three attempts in total.
            assertTrue(harness.client.submitTask(submit(id, 0, 2)).getAccepted());

            TaskRecord failed = awaitStatus(harness, id, TaskStatus.FAILED);
            assertEquals(3, failed.attemptCount(), "one try plus two retries");
            assertEquals(1, harness.deadLetters.size());

            DeadLetterList listed = harness.client.listDeadLetters(
                    DeadLetterRequest.newBuilder().setLimit(10).build());
            assertEquals(1, listed.getEntriesCount());
            assertEquals(id, listed.getEntries(0).getTaskId());
            assertEquals(3, listed.getEntries(0).getAttempts());

            TaskStatusResponse status = harness.client.getTaskStatus(
                    TaskStatusRequest.newBuilder().setTaskId(id).build());
            assertEquals(3, status.getAttempt());
            assertEquals(3, status.getAttemptHistoryCount(), "history is visible to clients");

            TaskResponse retried = harness.client.retryDeadLetter(
                    TaskStatusRequest.newBuilder().setTaskId(id).build());
            assertTrue(retried.getAccepted());
            assertEquals(0, harness.deadLetters.size());
            // It fails again, since the worker still fails everything, but it did run again.
            awaitStatus(harness, id, TaskStatus.FAILED);
            assertTrue(harness.store.get(id).attemptCount() >= 1);
        }
    }

    @Test
    void retryingATaskThatIsNotParkedIsRefused() throws Exception {
        try (SchedulerHarness harness = new SchedulerHarness(new FlakyWorker(0))) {
            TaskResponse response = harness.client.retryDeadLetter(
                    TaskStatusRequest.newBuilder().setTaskId("never-existed").build());
            assertFalse(response.getAccepted());
            assertTrue(response.getMessage().contains("not in the dead-letter queue"));
        }
    }

    @Test
    void aTaskPastItsTimeoutIsCancelledOnTheWorkerAndRetried() throws Exception {
        SchedulerHarness.Options options = new SchedulerHarness.Options();
        options.timeoutCheckMs = 50;
        options.maxRetries = 1;
        HangingWorker worker = new HangingWorker();
        try (SchedulerHarness harness = new SchedulerHarness(worker, options)) {
            harness.start();
            String id = "slow-" + UUID.randomUUID();
            assertTrue(harness.client.submitTask(submit(id, 300, 1)).getAccepted());

            TaskRecord failed = awaitStatus(harness, id, TaskStatus.FAILED);
            List<TaskAttempt> attempts = failed.attempts();
            assertEquals(2, attempts.size(), "one try plus one retry, both timed out");
            assertTrue(attempts.stream()
                            .allMatch(a -> a.outcome() == TaskAttempt.Outcome.TIMED_OUT),
                    "attempts were " + attempts);
            assertTrue(worker.cancelled.contains(id), "the worker was told to stop");
            assertEquals(1, harness.deadLetters.size());
        }
    }

    @Test
    void aRejectedTaskIsRetriedWithoutSpendingARetry() throws Exception {
        SchedulerHarness.Options options = new SchedulerHarness.Options();
        options.maxRetries = 0;
        AtomicInteger calls = new AtomicInteger();
        WorkerServiceGrpc.WorkerServiceImplBase rejectTwice =
                new WorkerServiceGrpc.WorkerServiceImplBase() {
                    @Override
                    public void executeTask(
                            ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
                        int call = calls.incrementAndGet();
                        observer.onNext(ExecuteResult.newBuilder()
                                .setTaskId(request.getTask().getTaskId())
                                .setSuccess(call > 2)
                                .setRejected(call <= 2)
                                .setOutput(call > 2 ? "ok" : "queue full")
                                .build());
                        observer.onCompleted();
                    }
                };
        try (SchedulerHarness harness = new SchedulerHarness(rejectTwice, options)) {
            harness.start();
            String id = "rejected-" + UUID.randomUUID();
            assertTrue(harness.client.submitTask(submit(id, 0, 0)).getAccepted());

            TaskRecord record = awaitStatus(harness, id, TaskStatus.COMPLETED);
            assertEquals(3, record.attemptCount());
            assertEquals(TaskAttempt.Outcome.REJECTED, record.attempts().get(0).outcome());
            assertEquals(TaskAttempt.Outcome.REJECTED, record.attempts().get(1).outcome());
            assertTrue(record.attempts().get(2).succeeded(),
                    "with 0 retries allowed, rejections still did not kill the task");
        }
    }
}
