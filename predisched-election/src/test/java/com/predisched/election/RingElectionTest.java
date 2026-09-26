package com.predisched.election;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class RingElectionTest extends ElectionScenarios {

    @Override
    String algorithm() {
        return "ring";
    }

    @Test
    void oneRoundIsLinearInTheRingSize() throws Exception {
        long messages = messagesForOneElectionFromTheLowestNode();
        int n = IDS.size();
        // One RingPass per hop plus one Coordinator per hop, dead hops included: at most 2n.
        assertTrue(messages <= 2L * n, "ring round should be O(n), was " + messages);
    }

    @Test
    void aDeadNodeInTheMiddleOfTheRingIsSkipped() throws Exception {
        start(0);
        assertEquals(Optional.of(5), cluster.awaitAgreedLeader(AGREE_MS));
        settle();
        cluster.kill(3);
        cluster.resetStats();
        cluster.node(1).leaderFailed();
        assertEquals(Optional.of(5), cluster.awaitAgreedLeader(AGREE_MS));
        // Node 2's successor is dead, so node 2 tried it and then went on to node 4.
        assertTrue(cluster.node(2).stats().count(ElectionStats.Message.RING_PASS) >= 2,
                "node 2 should have skipped node 3: " + cluster.node(2).stats());
        assertExactlyOneLeader();
    }
}
