package com.predisched.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskType;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Eventual mode: local writes, async replication, last-writer-wins, catch-up. */
class EventualConsistencyTest {

    private InProcessReplicaCluster cluster;

    @AfterEach
    void stop() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void aLaggingReplicaReadsStaleThenConvergesWithinDelayPlusOneSecond() throws Exception {
        cluster = InProcessReplicaCluster.start("eventual", 3, 2, 2, 1_000);
        cluster.store(1).put(task("t").withResult("old"));
        assertTrue(cluster.awaitConverged(1_000) >= 0);

        cluster.setDelay(1, 3, 2_000);
        long wroteAt = System.currentTimeMillis();
        cluster.store(1).update("t", record -> record.withResult("new"));

        assertEquals("new", cluster.store(1).get("t").result());
        assertEquals("old", cluster.store(3).get("t").result(), "replica 3 should be stale");
        long convergedMs = cluster.awaitConverged(3_000);
        assertTrue(convergedMs >= 0, "replicas did not converge within 3 s");
        assertTrue(System.currentTimeMillis() - wroteAt <= 3_000);
        for (int id = 1; id <= 3; id++) {
            assertEquals("new", cluster.store(id).get("t").result(), "replica " + id);
        }
    }

    @Test
    void concurrentWritesOnTwoNodesConvergeToOneValueEverywhere() throws Exception {
        cluster = InProcessReplicaCluster.start("eventual", 3, 2, 2, 1_000);
        cluster.store(1).put(task("c"));
        assertTrue(cluster.awaitConverged(1_000) >= 0);

        CountDownLatch go = new CountDownLatch(1);
        Thread one = new Thread(() -> await(go, () ->
                cluster.store(1).update("c", record -> record.withResult("from-1"))));
        Thread two = new Thread(() -> await(go, () ->
                cluster.store(2).update("c", record -> record.withResult("from-2"))));
        one.start();
        two.start();
        go.countDown();
        one.join();
        two.join();

        assertTrue(cluster.awaitConverged(2_000) >= 0, "replicas did not converge");
        String winner = cluster.store(1).get("c").result();
        for (int id = 1; id <= 3; id++) {
            assertEquals(winner, cluster.store(id).get("c").result(), "replica " + id);
        }
        VersionedRecord kept = cluster.store(3).local().get("c");
        assertEquals("from-" + kept.originNode().substring("node-".length()), winner,
                "the value kept must be the one written by the winning origin");
    }

    @Test
    void syncFromBringsAFreshReplicaToTheLeadersSequenceNumber() throws Exception {
        cluster = InProcessReplicaCluster.start("eventual", 3, 2, 2, 1_000);
        for (int i = 0; i < 20; i++) {
            cluster.store(1).put(task("s" + i));
            cluster.store(1).update("s" + i, record -> record.withResult("done"));
        }
        ReplicatedTaskStore leader = cluster.store(1);
        ReplicatedTaskStore fresh = cluster.freshReplica(4);
        try {
            long last = fresh.syncFrom(1, 1);
            assertEquals(leader.local().log().lastSeq(), last);
            assertEquals(leader.local().log().lastSeq(), fresh.local().log().lastSeq());
            for (int i = 0; i < 20; i++) {
                assertEquals(leader.get("s" + i), fresh.get("s" + i), "s" + i);
            }
        } finally {
            fresh.mode().close();
            fresh.replicas().cluster().close();
        }
    }

    @Test
    void aSessionTokenGivesReadYourWritesOnALaggingReplica() throws Exception {
        cluster = InProcessReplicaCluster.start("eventual", 3, 2, 2, 1_000);
        cluster.store(1).put(task("y"));
        assertTrue(cluster.awaitConverged(1_000) >= 0);
        cluster.setDelay(1, 3, 2_000);

        SessionToken session = new SessionToken();
        cluster.store(1).update("y", record -> record.withResult("mine"), session);

        assertNotEquals("mine", cluster.store(3).get("y").result(), "plain read is stale");
        assertEquals("mine", cluster.store(3).get("y", session).result(),
                "a session read must see its own write");
    }

    private static TaskRecord task(String id) {
        return TaskRecord.createQueued(id, TaskType.SLEEP_TASK, "ms=5", 5);
    }

    private static void await(CountDownLatch go, Runnable action) {
        try {
            go.await();
            action.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
