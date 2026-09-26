package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.TaskQueue;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Only the leader accepts tasks; a follower refuses and names the leader (FR9). */
class LeadershipTest {

    @Test
    void aFollowerRefusesSubmitsAndPointsAtTheLeaderUntilItIsElected() throws Exception {
        AtomicBoolean leading = new AtomicBoolean(false);
        Leadership follower = new Leadership() {
            @Override
            public boolean isLeader() {
                return leading.get();
            }

            @Override
            public Optional<Integer> leader() {
                return Optional.of(leading.get() ? 2 : 5);
            }
        };
        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
        RetryCoordinator retries = new RetryCoordinator(
                store, queue, new RetryPolicy(10, 100, 0, 3, 1), new DeadLetterQueue());
        String name = "leadership-" + UUID.randomUUID();
        Server server = InProcessServerBuilder.forName(name)
                .addService(new SchedulerServiceImpl(
                        store, new TaskValidator(4096), queue, retries, follower))
                .directExecutor().build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            SchedulerServiceGrpc.SchedulerServiceBlockingStub stub =
                    SchedulerServiceGrpc.newBlockingStub(channel);
            TaskResponse refused = stub.submitTask(sleep("while-follower"));
            assertFalse(refused.getAccepted());
            assertEquals("not the leader; leader=5", refused.getMessage());
            assertFalse(store.contains("while-follower"), "a follower must not store the task");
            assertEquals(0, queue.size(), "nor queue it");

            leading.set(true);
            TaskResponse accepted = stub.submitTask(sleep("once-leader"));
            assertTrue(accepted.getAccepted(), accepted.getMessage());
            assertTrue(store.contains("once-leader"));
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    private static TaskRequest sleep(String id) {
        return TaskRequest.newBuilder()
                .setTaskId(id).setType(TaskType.SLEEP_TASK).setInput("ms=5").setPriority(5)
                .build();
    }
}
