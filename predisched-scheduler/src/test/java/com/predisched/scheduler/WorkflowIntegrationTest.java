package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.TaskRecord;
import com.predisched.common.WorkflowDag;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkflowStatusRequest;
import com.predisched.proto.WorkflowStatusResponse;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The shipped map-reduce DAG through the real service, dispatcher and a fake worker. */
class WorkflowIntegrationTest {

    // Tests run from the module directory.
    private static final Path MAP_REDUCE = Path.of("../workloads/dags/map-reduce.json");
    private SchedulerHarness harness;

    @BeforeEach
    void start() throws Exception {
        harness = new SchedulerHarness(new SchedulerIntegrationTest.FakeWorkerService());
        harness.start();
    }

    @AfterEach
    void stop() {
        harness.close();
    }

    private void awaitCompleted(List<String> ids) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (ids.stream().allMatch(id -> harness.store.get(id) != null
                    && harness.store.get(id).status() == TaskStatus.COMPLETED)) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("not all completed: " + ids.stream()
                .map(id -> id + "=" + harness.store.get(id)).toList());
    }

    @Test
    void theMapReduceWorkflowCompletesWithTheReduceLast() throws Exception {
        TaskResponse accepted = harness.client.submitWorkflow(
                WorkflowDag.load(MAP_REDUCE).toRequest("mr-1", "trace-mr"));
        assertTrue(accepted.getAccepted(), accepted.getMessage());
        assertEquals("workflow accepted: 5 tasks", accepted.getMessage());

        List<String> maps = List.of("mr-1.map-1", "mr-1.map-2", "mr-1.map-3", "mr-1.map-4");
        awaitCompleted(List.of("mr-1.map-1", "mr-1.map-2", "mr-1.map-3", "mr-1.map-4",
                "mr-1.reduce"));
        TaskRecord reduce = harness.store.get("mr-1.reduce");
        for (String map : maps) {
            assertTrue(reduce.startedAt() >= harness.store.get(map).completedAt(),
                    "reduce started before " + map + " finished");
        }
        WorkflowStatusResponse status = harness.client.getWorkflowStatus(
                WorkflowStatusRequest.newBuilder().setWorkflowId("mr-1").build());
        assertEquals(5, status.getTasksCount());
        assertEquals("mr-1.reduce", status.getTasks(4).getTaskId(), "topological order");
    }

    @Test
    void aWorkflowTaskExpandsItsDagAndCompletesWithIt() throws Exception {
        TaskResponse accepted = harness.client.submitTask(TaskRequest.newBuilder()
                .setTaskId("wft-1").setType(TaskType.WORKFLOW_TASK)
                .setInput("dag=" + MAP_REDUCE).setPriority(5).build());
        assertTrue(accepted.getAccepted(), accepted.getMessage());
        awaitCompleted(List.of("wft-1.reduce", "wft-1"));
        TaskRecord workflowTask = harness.store.get("wft-1");
        assertTrue(workflowTask.result().contains("all tasks completed"), workflowTask.result());
        assertEquals("", workflowTask.workerId(), "no worker ever ran the WORKFLOW_TASK itself");
    }

    @Test
    void dependsOnOutsideAWorkflowIsRefused() {
        TaskResponse refused = harness.client.submitTask(TaskRequest.newBuilder()
                .setTaskId("lonely").setType(TaskType.SLEEP_TASK).setInput("ms=1")
                .setPriority(5).addDependsOn("other").build());
        assertTrue(!refused.getAccepted()
                && refused.getMessage().contains("only allowed inside SubmitWorkflow"),
                refused.getMessage());
    }
}
