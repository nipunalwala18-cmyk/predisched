package com.predisched.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Quorum mode, N = 3, W = 2, R = 2: a successful write is visible to every quorum read. */
class StrongConsistencyTest {

    private InProcessReplicaCluster cluster;

    @AfterEach
    void stop() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void afterEverySuccessfulWriteAReadThroughAnyReplicaReturnsIt() throws Exception {
        cluster = InProcessReplicaCluster.start("strong", 3, 2, 2, 2_000);
        // Replica 3 lags: every change reaches it 25 ms late, so its own copy is often stale.
        cluster.setDelay(1, 3, 25);
        cluster.setDelay(2, 3, 25);
        Random random = new Random(7);
        List<String> ids = new ArrayList<>();
        for (int step = 0; step < 150; step++) {
            // Random writer and random mix of new tasks and updates.
            ReplicatedTaskStore writer = cluster.store(1 + random.nextInt(2));
            String value = "v" + step;
            String id;
            if (ids.isEmpty() || random.nextInt(3) == 0) {
                id = "t" + ids.size();
                ids.add(id);
                writer.put(TaskRecord.createQueued(id, TaskType.SLEEP_TASK, "ms=5", 5)
                        .withResult(value));
            } else {
                id = ids.get(random.nextInt(ids.size()));
                writer.update(id, record -> record.withResult(value));
            }
            if (random.nextBoolean()) {
                Thread.sleep(random.nextInt(10));
            }
            // Every replica's quorum read overlaps the write quorum.
            for (int reader = 1; reader <= 3; reader++) {
                TaskRecord read = cluster.store(reader).get(id);
                assertNotNull(read, "replica " + reader + " lost " + id + " at step " + step);
                assertEquals(value, read.result(),
                        "stale read of " + id + " through replica " + reader + " at step " + step);
            }
        }
    }

    @Test
    void withTwoOfThreeReplicasDownWritesFailAndLeaveNothingBehind() {
        cluster = InProcessReplicaCluster.start("strong", 3, 2, 2, 300);
        cluster.isolate(2);
        cluster.isolate(3);
        TaskRecord task = TaskRecord.createQueued("lost", TaskType.SLEEP_TASK, "ms=5", 5);

        assertThrows(ReplicationException.class, () -> cluster.store(1).put(task));
        assertNull(cluster.store(1).local().get("lost"), "the writer applied a failed write");

        cluster.heal(2);
        cluster.heal(3);
        for (int id = 1; id <= 3; id++) {
            assertNull(cluster.store(id).local().get("lost"), "replica " + id + " holds it");
            assertNull(cluster.store(id).get("lost"), "a quorum read through " + id + " found it");
        }
    }

    @Test
    void aQuorumReadRepairsTheStaleReplicaItConsulted() throws Exception {
        cluster = InProcessReplicaCluster.start("strong", 3, 2, 2, 2_000);
        cluster.setDelay(1, 3, 2_000);
        cluster.store(1).put(TaskRecord.createQueued("r", TaskType.SLEEP_TASK, "ms=5", 5));
        assertNull(cluster.store(3).local().get("r"), "replica 3 should still be behind");
        assertNotNull(cluster.store(3).get("r"), "a quorum read through 3 must see the write");
        assertNotNull(cluster.store(3).local().get("r"), "and repair 3's own copy");
    }
}
