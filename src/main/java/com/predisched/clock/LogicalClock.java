package com.predisched.clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * ================================================================
 *  PrediSched — Experiment 3: Clock Synchronization
 *  LogicalClock.java — Lamport Logical Clock Implementation
 * ================================================================
 *
 *  Implements Lamport's Logical Clock algorithm for distributed event ordering:
 *    Rule 1 (Local event):  clock = clock + 1
 *    Rule 2 (Send message): clock = clock + 1 (attach timestamp to message)
 *    Rule 3 (Receive msg):  clock = max(localClock, receivedTimestamp) + 1
 */
public class LogicalClock {

    private final String nodeId;
    private final AtomicLong clock;

    public LogicalClock(String nodeId) {
        this(nodeId, 0L);
    }

    public LogicalClock(String nodeId, long initialValue) {
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("nodeId cannot be null or empty");
        }
        this.nodeId = nodeId;
        this.clock = new AtomicLong(initialValue);
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getTime() {
        return clock.get();
    }

    /**
     * Rule 1 — Local event: clock = clock + 1
     */
    public synchronized long increment() {
        return clock.incrementAndGet();
    }

    /**
     * Rule 1 — Alias for local event increment
     */
    public long localEvent() {
        return increment();
    }

    /**
     * Rule 2 — Sending a message: clock = clock + 1
     * Returns the timestamp to attach to the outgoing message.
     */
    public synchronized long sendEvent() {
        return clock.incrementAndGet();
    }

    /**
     * Rule 3 — Receiving a message with timestamp T:
     * clock = max(localClock, T) + 1
     *
     * @param receivedTimestamp Timestamp attached to the incoming message
     * @return New local logical clock value after update
     */
    public synchronized long receiveEvent(long receivedTimestamp) {
        long current = clock.get();
        long maxVal = Math.max(current, receivedTimestamp);
        long next = maxVal + 1;
        clock.set(next);
        return next;
    }

    /**
     * Alias for receiveEvent(receivedTimestamp)
     */
    public long update(long receivedTimestamp) {
        return receiveEvent(receivedTimestamp);
    }

    @Override
    public String toString() {
        return String.format("[%s LogicalClock L=%d]", nodeId, clock.get());
    }
}
