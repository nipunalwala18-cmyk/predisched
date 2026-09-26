package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkflowRequest;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.TaskQueue;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class WorkflowManagerTest {

    private final TaskStore store = new InMemoryTaskStore();
    private final TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
    // No retries: a failure is final at once, which is what the cancellation test needs.
    private final RetryCoordinator retries = new RetryCoordinator(
            store, queue, new RetryPolicy(10, 100, 0, 0, 1), new DeadLetterQueue());
    private final WorkflowManager workflows = new WorkflowManager(store, queue, retries);
    private final TaskValidator validator = new TaskValidator(4096);

    @AfterEach
    void stop() {
        retries.close();
    }

    private static TaskRequest task(String id, String input, String... parents) {
        return TaskRequest.newBuilder()
                .setTaskId(id).setType(TaskType.SLEEP_TASK).setInput(input).setPriority(5)
                .addAllDependsOn(List.of(parents))
                .build();
    }

    private void submit(String workflowId, TaskRequest... tasks) {
        WorkflowRequest request = WorkflowRequest.newBuilder()
                .setWorkflowId(workflowId).addAllTasks(List.of(tasks)).build();
        List<String> errors = workflows.validate(request, validator);
        assertTrue(errors.isEmpty(), errors.toString());
        workflows.submit(request, "client-a", "trace-1");
    }

    /** Runs a task the way the dispatcher would, with the given worker output. */
    private void complete(String id, String output) {
        store.update(id, record -> record.withStatus(TaskStatus.RUNNING));
        retries.succeeded(id, new TaskAttempt(1, "w", TaskAttempt.Outcome.SUCCEEDED, "", 0, 1, 1),
                output, 1);
    }

    private void fail(String id) {
        store.update(id, record -> record.withStatus(TaskStatus.RUNNING));
        retries.failed(id, new TaskAttempt(1, "w", TaskAttempt.Outcome.FAILED, "boom", 0, 1, 1),
                "boom");
    }

    private Set<String> queued() {
        return Set.copyOf(queue.snapshot());
    }

    private TaskStatus status(String id) {
        return store.get(id).status();
    }

    @Test
    void tasksAreReleasedInTopologicalOrder() {
        // a -> b, a -> c, (b, c) -> d
        submit("wf", task("a", "ms=1"), task("b", "ms=1", "a"), task("c", "ms=1", "a"),
                task("d", "ms=1", "b", "c"));
        assertEquals(Set.of("a"), queued());
        assertEquals(TaskStatus.BLOCKED, status("d"));
        assertEquals("client-a", store.get("d").clientId());
        assertEquals("wf", store.get("d").workflowId());

        queue.remove("a");
        complete("a", "done");
        assertEquals(Set.of("b", "c"), queued());
        queue.remove("b");
        complete("b", "done");
        assertEquals(Set.of("c"), queued(), "d waits for c as well");
        assertEquals(TaskStatus.BLOCKED, status("d"));
        queue.remove("c");
        complete("c", "done");
        assertEquals(Set.of("d"), queued());
        assertEquals(TaskStatus.QUEUED, status("d"));
    }

    @Test
    void aCycleIsRejected() {
        WorkflowRequest cyclic = WorkflowRequest.newBuilder().setWorkflowId("loop")
                .addTasks(task("x", "ms=1", "z"))
                .addTasks(task("y", "ms=1", "x"))
                .addTasks(task("z", "ms=1", "y"))
                .addTasks(task("free", "ms=1"))
                .build();
        List<String> errors = workflows.validate(cyclic, validator);
        assertEquals(1, errors.size(), errors.toString());
        assertTrue(errors.get(0).contains("cycle through [x, y, z]"), errors.get(0));

        Map<String, List<String>> selfLoopFree = new LinkedHashMap<>();
        selfLoopFree.put("p", List.of());
        selfLoopFree.put("q", List.of("p"));
        assertEquals(List.of("p", "q"), WorkflowManager.topologicalOrder(selfLoopFree));
        Map<String, List<String>> twoCycle = Map.of("p", List.of("q"), "q", List.of("p"));
        assertThrows(IllegalArgumentException.class, () -> WorkflowManager.topologicalOrder(twoCycle));
    }

    @Test
    void unknownParentsAndBadReferencesAreRejected() {
        WorkflowRequest bad = WorkflowRequest.newBuilder().setWorkflowId("bad")
                .addTasks(task("a", "ms=1", "ghost"))
                .addTasks(task("b", "ms=${a.result}"))
                .build();
        List<String> errors = workflows.validate(bad, validator);
        assertTrue(errors.stream().anyMatch(e -> e.contains("depends on unknown task ghost")),
                errors.toString());
        assertTrue(errors.stream().anyMatch(e -> e.contains("not one of its depends_on")),
                errors.toString());
    }

    @Test
    void aFailedParentCancelsEveryDescendant() {
        // a -> b -> c, a -> d, and e is independent
        submit("wf2", task("a", "ms=1"), task("b", "ms=1", "a"), task("c", "ms=1", "b"),
                task("d", "ms=1", "a"), task("e", "ms=1"));
        queue.remove("a");
        fail("a");
        assertEquals(TaskStatus.FAILED, status("a"));
        for (String id : List.of("b", "c", "d")) {
            assertEquals(TaskStatus.CANCELLED, status(id), id);
            assertTrue(store.get(id).result().startsWith(WorkflowManager.UPSTREAM_FAILED),
                    store.get(id).result());
        }
        assertEquals(TaskStatus.QUEUED, status("e"), "an independent branch keeps running");
    }

    @Test
    void aParentsResultIsSubstitutedIntoItsChildsInput() {
        submit("wf3", task("wf3.size", "ms=1"), task("wf3.use", "ms=${wf3.size.result}", "wf3.size"));
        queue.remove("wf3.size");
        complete("wf3.size", "42");
        assertEquals("ms=42", store.get("wf3.use").input());
        assertEquals(TaskStatus.QUEUED, status("wf3.use"));
    }

    @Test
    void aSubstitutionThatBreaksTheInputCancelsTheChild() {
        submit("wf4", task("p", "ms=1"), task("c", "ms=${p.result}", "p"));
        queue.remove("p");
        complete("p", "not-a-number");
        assertEquals(TaskStatus.CANCELLED, status("c"));
        assertTrue(store.get("c").result().contains("input after substitution is invalid"),
                store.get("c").result());
        assertEquals(Set.of(), queue.snapshot().stream().collect(Collectors.toSet()));
    }
}
