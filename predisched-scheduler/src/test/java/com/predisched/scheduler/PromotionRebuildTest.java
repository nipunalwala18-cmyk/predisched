package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.fault.Recovery;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Promotion step 2: a new primary rebuilds its scheduling state from the replicated store. */
class PromotionRebuildTest {

    @Test
    void queuedTasksAreQueuedBlockedTasksRelinkedAndFailedOnesParked() {
        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
        DeadLetterQueue deadLetters = new DeadLetterQueue();
        RetryCoordinator retries = new RetryCoordinator(store, queue,
                new RetryPolicy(10, 50, 0, 3, 42), deadLetters);
        WorkflowManager workflows = new WorkflowManager(store, queue, retries);
        queue.add("stale-from-an-earlier-term", 5, 0);

        // As a dying primary left them: a plain task queued, and a workflow where a finished
        // but its child b was never released, c waits on x too, and d's parent f failed.
        put(store, plain("q"));
        put(store, step("f", List.of()).withWorkerId("w").withStatus(TaskStatus.RUNNING)
                .withAttempt(new TaskAttempt(1, "w", TaskAttempt.Outcome.FAILED, "boom", 0, 0, 0))
                .withStatus(TaskStatus.FAILED));
        put(store, step("a", List.of()).withWorkerId("w").withStatus(TaskStatus.RUNNING)
                .withResult("A").withStatus(TaskStatus.COMPLETED));
        put(store, step("x", List.of()));
        put(store, step("b", List.of("a")).asBlocked());
        put(store, step("c", List.of("a", "x")).asBlocked());
        put(store, step("d", List.of("f")).asBlocked());

        SchedulerFailover failover = new SchedulerFailover(store, queue, new RunningTasks(), null,
                new WorkerRegistry(60_000), worker -> null, retries, workflows, null, 300);
        failover.rebuild(Recovery.of(store.list()));

        assertEquals(TaskStatus.QUEUED, store.get("b").status(), "all its parents had completed");
        assertEquals(TaskStatus.BLOCKED, store.get("c").status(), "x has not run yet");
        assertEquals(TaskStatus.CANCELLED, store.get("d").status(), "its parent failed");
        assertEquals(List.of("b", "q", "x"), queue.snapshot().stream().sorted().toList(),
                "the stale entry is gone; queued work is back");
        assertEquals(1, deadLetters.size());
        assertEquals("boom", deadLetters.get("f").orElseThrow().lastError());
        assertEquals(List.of("a", "b", "c", "d", "f", "x"),
                workflows.status("wf").stream().map(TaskRecord::id).sorted().toList(),
                "workflow status works on the new primary");

        // When x completes, c is released like any workflow child.
        store.update("x", r -> r.withWorkerId("w").withStatus(TaskStatus.RUNNING)
                .withResult("X").withStatus(TaskStatus.COMPLETED));
        retries.notifyTerminal(store.get("x"));
        assertEquals(TaskStatus.QUEUED, store.get("c").status());
        retries.close();
    }

    private static void put(TaskStore store, TaskRecord record) {
        store.replace(record);
    }

    private static TaskRecord plain(String id) {
        return TaskRecord.createQueued(id, TaskType.SLEEP_TASK, "ms=10", 5);
    }

    private static TaskRecord step(String id, List<String> parents) {
        return plain(id).withWorkflowId("wf").withDependsOn(parents);
    }
}
