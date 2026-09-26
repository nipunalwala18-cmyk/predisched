package com.predisched.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.TaskRecord;
import com.predisched.proto.Ack;
import com.predisched.proto.JoinRequest;
import com.predisched.proto.JoinResponse;
import com.predisched.proto.TaskType;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Synchronous primary-backup replication (prompt 10), node 1 as the primary. */
class PrimaryBackupTest {

    private InProcessReplicaCluster cluster;

    @AfterEach
    void stop() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void aBackupAppliesOutOfOrderChangesInSequenceOrder() throws Exception {
        cluster = InProcessReplicaCluster.start("primary-backup", 3, 2, 2, 2_000);
        PrimaryBackupReplication backup = mode(2);
        VersionedRecord one = stamped("a", 1);
        VersionedRecord two = stamped("b", 2);
        VersionedRecord three = stamped("c", 3);

        CompletableFuture<Ack> third = backup.receive(three.toRequest(3));
        CompletableFuture<Ack> second = backup.receive(two.toRequest(2));
        assertFalse(third.isDone(), "seq 3 must wait for seq 1 and 2");
        assertFalse(second.isDone(), "seq 2 must wait for seq 1");
        assertEquals(0, backup.lastSeq(), "nothing applied ahead of the gap");
        assertNull(cluster.store(2).local().get("b"));

        CompletableFuture<Ack> first = backup.receive(one.toRequest(1));
        assertTrue(first.get(1, TimeUnit.SECONDS).getOk());
        assertTrue(second.get(1, TimeUnit.SECONDS).getOk());
        assertTrue(third.get(1, TimeUnit.SECONDS).getOk());
        assertEquals(3, backup.lastSeq());
        List<ReplicationLog.Entry> log = cluster.store(2).local().log().from(1);
        assertEquals(List.of("a", "b", "c"),
                log.stream().map(entry -> entry.record().taskId()).toList());
        assertEquals(List.of(1L, 2L, 3L), log.stream().map(ReplicationLog.Entry::seqNo).toList());

        // A repeat of an applied change is acknowledged and not applied again.
        assertTrue(backup.receive(two.toRequest(2)).get(1, TimeUnit.SECONDS).getOk());
        assertEquals(3, backup.lastSeq());
    }

    @Test
    void aWriteReachesEveryLiveBackupBeforeItReturns() {
        cluster = InProcessReplicaCluster.start("primary-backup", 3, 2, 2, 2_000);
        promote(1);
        assertTrue(mode(2).syncWithPrimary(1));
        assertTrue(mode(3).syncWithPrimary(1));
        assertEquals(Set.of(2, 3), mode(1).liveBackups());

        for (int i = 0; i < 20; i++) {
            cluster.store(1).put(task("t" + i));
            // Synchronous: the moment put returns, both backups hold the write.
            assertNotNull(cluster.store(2).local().get("t" + i), "backup 2 has t" + i);
            assertNotNull(cluster.store(3).local().get("t" + i), "backup 3 has t" + i);
        }
        assertEquals(mode(1).lastSeq(), mode(2).lastSeq());
        assertEquals(mode(1).lastSeq(), mode(3).lastSeq());
    }

    @Test
    void aRestartedBackupCatchesUpWithSyncFromBeforeItRejoinsTheLiveSet() {
        cluster = InProcessReplicaCluster.start("primary-backup", 3, 2, 2, 500);
        promote(1);
        assertTrue(mode(2).syncWithPrimary(1));
        assertTrue(mode(3).syncWithPrimary(1));
        for (int i = 0; i < 10; i++) {
            cluster.store(1).put(task("before" + i));
        }

        cluster.kill(3);
        for (int i = 0; i < 10; i++) {
            cluster.store(1).put(task("while-down" + i));
        }
        assertEquals(Set.of(2), mode(1).liveBackups(), "the dead backup was dropped");

        ReplicatedTaskStore restarted = cluster.restart(3);
        assertEquals(0, mode(3).lastSeq(), "a restarted replica starts empty");
        JoinResponse early = restarted.replicas().stub(1).join(
                JoinRequest.newBuilder().setNodeId(3).setLastSeq(0).build());
        assertFalse(early.getJoined(), "not in step yet: " + early.getMessage());
        assertEquals(Set.of(2), mode(1).liveBackups());

        assertTrue(mode(3).syncWithPrimary(1), "caught up, then joined");
        assertEquals(mode(1).lastSeq(), mode(3).lastSeq());
        assertEquals(Set.of(2, 3), mode(1).liveBackups());
        for (int i = 0; i < 10; i++) {
            assertEquals(cluster.store(1).get("while-down" + i), restarted.get("while-down" + i));
        }
        cluster.store(1).put(task("after"));
        assertNotNull(restarted.local().get("after"), "live again: writes reach it synchronously");
    }

    @Test
    void aBackupWhoseLogForkedDropsItAndCopiesThePrimarys() {
        cluster = InProcessReplicaCluster.start("primary-backup", 3, 2, 2, 2_000);
        promote(1);
        assertTrue(mode(2).syncWithPrimary(1));
        cluster.store(1).put(task("shared"));
        // Node 2 keeps a write the primary never made, as a demoted primary would.
        mode(2).standDown();
        LocalReplica backup = cluster.store(2).local();
        backup.applySequenced(backup.log().lastSeq() + 1, stamped("stray", 99));
        mode(1).becomePrimary();   // forget node 2 as live
        mode(1).serve();
        cluster.store(1).put(task("next"));

        assertTrue(mode(2).syncWithPrimary(1));
        assertNull(backup.get("stray"), "the forked write is gone");
        assertNotNull(backup.get("next"));
        assertEquals(mode(1).lastSeq(), mode(2).lastSeq());
    }

    @Test
    void aNewPrimaryRefusesJoinsUntilItsLogIsComplete() {
        cluster = InProcessReplicaCluster.start("primary-backup", 3, 2, 2, 2_000);
        mode(1).becomePrimary();
        JoinResponse reply = mode(1).join(JoinRequest.newBuilder().setNodeId(2).build());
        assertFalse(reply.getReady());
        assertFalse(reply.getJoined());
        assertFalse(mode(2).syncWithPrimary(1), "a backup waits rather than copy a partial log");
        mode(1).serve();
        assertTrue(mode(2).syncWithPrimary(1));
    }

    private void promote(int id) {
        mode(id).becomePrimary();
        mode(id).serve();
    }

    private PrimaryBackupReplication mode(int id) {
        return (PrimaryBackupReplication) cluster.store(id).mode();
    }

    private static TaskRecord task(String id) {
        return TaskRecord.createQueued(id, TaskType.CPU_TASK, "n=1", 5);
    }

    private static VersionedRecord stamped(String id, long lamport) {
        return new VersionedRecord(id, TaskCodec.encode(task(id)), 1, lamport, "node-1",
                VersionedRecord.ENQUEUE);
    }
}
