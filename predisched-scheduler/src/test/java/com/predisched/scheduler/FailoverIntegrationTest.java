package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.client.SchedulerClient;
import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Primary-backup failover on three in-process schedulers (prompt 10, lab Exp 8). */
class FailoverIntegrationTest {

    private InProcessSchedulerCluster cluster;
    private InProcessSchedulerCluster.FakeWorker worker;

    @AfterEach
    void stop() {
        if (cluster != null) {
            cluster.close();
        }
        if (worker != null) {
            worker.close();
        }
    }

    @Test
    void aPrimaryCrashBetweenBackupAckAndClientAckLosesNothingAndDuplicatesNothing()
            throws Exception {
        worker = new InProcessSchedulerCluster.FakeWorker(150);
        cluster = InProcessSchedulerCluster.start(List.of(1, 2, 3), worker);
        int primary = cluster.awaitPrimary(10_000).orElseThrow();
        assertEquals(3, primary, "Bully elects the highest id");
        assertTrue(cluster.awaitAllBackupsLive(primary, 10_000), "both backups joined");

        try (SchedulerClient client = new SchedulerClient(cluster.clientChannels(),
                new SchedulerClient.Failover(100, 50, 400, 2_000))) {
            List<String> ids = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                ids.add(submit(client, "t" + i).getTaskId());
            }

            // t6 is stored and acked by both backups, then the primary dies before replying.
            cluster.crashBeforeNextSubmitReply(primary);
            TaskResponse retried = submit(client, "t6");
            ids.add("t6");
            assertTrue(retried.getMessage().startsWith("already accepted"),
                    "the retry found the replicated task: " + retried.getMessage());

            for (int i = 7; i <= 10; i++) {
                ids.add(submit(client, "t" + i).getTaskId());
            }
            int newPrimary = cluster.awaitPrimary(10_000).orElseThrow();
            assertEquals(2, newPrimary, "the highest surviving id took over");

            for (String id : ids) {
                TaskStatusResponse done = awaitTerminal(client, id);
                assertEquals(TaskStatus.COMPLETED, done.getStatus(), id + " completed");
                long successes = done.getAttemptHistoryList().stream()
                        .filter(line -> line.contains("SUCCEEDED"))
                        .count();
                assertEquals(1, successes, id + " completed once: " + done.getAttemptHistoryList());
                assertEquals(1, worker.runsOf(id), id + " ran once on the worker");
            }
            List<TaskRecord> stored = cluster.member(newPrimary).store.list();
            assertEquals(1, stored.stream().filter(task -> task.id().equals("t6")).count());
            assertEquals(ids.size(), stored.size(), "no task lost, none added");
        }
    }

    private static TaskResponse submit(SchedulerClient client, String id) {
        TaskResponse response = client.submitTask(id, TaskType.SLEEP_TASK, "ms=10", 5);
        assertTrue(response.getAccepted(), id + ": " + response.getMessage());
        return response;
    }

    private static TaskStatusResponse awaitTerminal(SchedulerClient client, String id)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        TaskStatusResponse status = client.getStatus(id);
        while (System.currentTimeMillis() < deadline
                && status.getStatus() != TaskStatus.COMPLETED
                && status.getStatus() != TaskStatus.FAILED) {
            Thread.sleep(20);
            status = client.getStatus(id);
        }
        return status;
    }
}
