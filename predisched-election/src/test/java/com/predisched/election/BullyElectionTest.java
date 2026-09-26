package com.predisched.election;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BullyElectionTest extends ElectionScenarios {

    @Override
    String algorithm() {
        return "bully";
    }

    @Test
    void theLowestNodeDetectingIsTheQuadraticWorstCase() throws Exception {
        long messages = messagesForOneElectionFromTheLowestNode();
        int n = IDS.size();
        // Node 1 elects to 4 higher nodes, each of those elects to the ones above it, and every
        // Election to a live node earns an OK: at least n(n-1)/2 messages before any Coordinator.
        assertTrue(messages >= (long) n * (n - 1) / 2,
                "bully worst case should be O(n^2), was " + messages);
    }
}
