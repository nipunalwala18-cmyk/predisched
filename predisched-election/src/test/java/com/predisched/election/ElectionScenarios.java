package com.predisched.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The scenarios both algorithms must pass, run over five real gRPC nodes in process. Subclasses
 * name the algorithm and add their own message-count bound.
 */
abstract class ElectionScenarios {

    static final List<Integer> IDS = List.of(1, 2, 3, 4, 5);
    static final long TIMEOUT_MS = 300;
    static final long PING_MS = 100;
    static final int MISSES = 2;
    static final long AGREE_MS = 5_000;

    InProcessElectionCluster cluster;

    abstract String algorithm();

    InProcessElectionCluster start(long pingMs) {
        cluster = InProcessElectionCluster.start(algorithm(), IDS, TIMEOUT_MS, pingMs, MISSES);
        return cluster;
    }

    @AfterEach
    void stop() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void initialElectionPicksTheHighestId() throws Exception {
        start(PING_MS);
        assertEquals(Optional.of(5), cluster.awaitAgreedLeader(AGREE_MS));
        assertExactlyOneLeader();
    }

    @Test
    void killingTheLeaderElectsTheNextHighestWithinFiveSeconds() throws Exception {
        start(PING_MS);
        assertEquals(Optional.of(5), cluster.awaitAgreedLeader(AGREE_MS));
        long killedAt = System.currentTimeMillis();
        cluster.kill(5);
        assertEquals(Optional.of(4), cluster.awaitAgreedLeader(AGREE_MS));
        long tookMs = System.currentTimeMillis() - killedAt;
        assertTrue(tookMs < 5_000, "new leader took " + tookMs + " ms");
        assertExactlyOneLeader();
    }

    @Test
    void killingTheTwoHighestElectsThree() throws Exception {
        start(PING_MS);
        assertEquals(Optional.of(5), cluster.awaitAgreedLeader(AGREE_MS));
        cluster.kill(4);
        cluster.kill(5);
        assertEquals(Optional.of(3), cluster.awaitAgreedLeader(AGREE_MS));
        assertExactlyOneLeader();
    }

    @Test
    void threeAndFourDetectingTheFailureTogetherStillYieldOneLeader() throws Exception {
        // Monitors off, so exactly these two nodes start elections, at the same instant.
        start(0);
        assertEquals(Optional.of(5), cluster.awaitAgreedLeader(AGREE_MS));
        settle();
        cluster.kill(5);
        CountDownLatch go = new CountDownLatch(1);
        Thread three = new Thread(() -> awaitThen(go, () -> cluster.node(3).leaderFailed()));
        Thread four = new Thread(() -> awaitThen(go, () -> cluster.node(4).leaderFailed()));
        three.start();
        four.start();
        go.countDown();
        three.join();
        four.join();
        assertEquals(Optional.of(4), cluster.awaitAgreedLeader(AGREE_MS));
        // And it stays that way: no second leader appears once the elections have settled.
        Thread.sleep(3 * TIMEOUT_MS);
        assertEquals(Optional.of(4), cluster.awaitAgreedLeader(AGREE_MS));
        assertExactlyOneLeader();
    }

    /**
     * One election on four survivors, started by node 1 alone after leader 5 dies. Returns the
     * election messages every node sent for it.
     */
    long messagesForOneElectionFromTheLowestNode() throws Exception {
        start(0);
        assertEquals(Optional.of(5), cluster.awaitAgreedLeader(AGREE_MS));
        settle();
        cluster.resetStats();
        cluster.kill(5);
        cluster.node(1).leaderFailed();
        assertEquals(Optional.of(4), cluster.awaitAgreedLeader(AGREE_MS));
        // Late Coordinator deliveries to the dead node count too; let them finish.
        Thread.sleep(2 * TIMEOUT_MS);
        long messages = cluster.electionMessages();
        System.out.printf("%s: node 1 detects, n=%d nodes (1 dead) -> %d election messages%n",
                algorithm(), IDS.size(), messages);
        return messages;
    }

    /**
     * At start-up every node runs its own election; their late Coordinator messages can land
     * after a test has begun the election it wants to measure. Wait them out first.
     */
    static void settle() throws InterruptedException {
        Thread.sleep(3 * TIMEOUT_MS);
    }

    void assertExactlyOneLeader() {
        Map<Integer, Optional<Integer>> leaders = cluster.leaders();
        long selfLeaders = leaders.entrySet().stream()
                .filter(entry -> entry.getValue().equals(Optional.of(entry.getKey())))
                .count();
        assertEquals(1, selfLeaders, "nodes that believe they lead: " + leaders);
    }

    private static void awaitThen(CountDownLatch go, Runnable action) {
        try {
            go.await();
            action.run();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
