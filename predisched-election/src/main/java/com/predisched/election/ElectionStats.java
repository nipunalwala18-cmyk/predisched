package com.predisched.election;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Messages this node has sent, per type, and how long its last election took, measured from the
 * moment it detected the need for one to the moment it learned the new leader. Lock-free: every
 * counter is an {@link AtomicLong}, written from gRPC, election and monitor threads.
 */
public class ElectionStats {

    /** Every message type the two algorithms send. OK is the Bully reply to an Election. */
    public enum Message { ELECTION, OK, COORDINATOR, RING_PASS, PING }

    private final Map<Message, AtomicLong> sent = new EnumMap<>(Message.class);
    private final AtomicLong detectedAtNanos = new AtomicLong();
    private final AtomicLong lastDurationMs = new AtomicLong(-1);
    private final AtomicLong elections = new AtomicLong();

    public ElectionStats() {
        for (Message message : Message.values()) {
            sent.put(message, new AtomicLong());
        }
    }

    public void sent(Message message) {
        sent.get(message).incrementAndGet();
    }

    public long count(Message message) {
        return sent.get(message).get();
    }

    /** Messages that belong to electing a leader: everything except liveness pings. */
    public long electionMessages() {
        return count(Message.ELECTION) + count(Message.OK) + count(Message.COORDINATOR)
                + count(Message.RING_PASS);
    }

    /** Starts the election clock, unless one is already running for this election. */
    public void markDetected() {
        detectedAtNanos.compareAndSet(0, System.nanoTime());
    }

    /**
     * Stops the election clock when the new leader is known. Returns the duration in ms, or -1
     * when this node learned the leader without having detected anything (a plain announcement).
     */
    public long markLeaderKnown() {
        long detected = detectedAtNanos.getAndSet(0);
        if (detected == 0) {
            return -1;
        }
        long ms = (System.nanoTime() - detected) / 1_000_000;
        lastDurationMs.set(ms);
        elections.incrementAndGet();
        return ms;
    }

    /** Duration of the last election this node took part in from detection, or -1. */
    public long lastDurationMs() {
        return lastDurationMs.get();
    }

    public long elections() {
        return elections.get();
    }

    public void reset() {
        sent.values().forEach(counter -> counter.set(0));
        detectedAtNanos.set(0);
        lastDurationMs.set(-1);
        elections.set(0);
    }

    @Override
    public String toString() {
        return "election=" + count(Message.ELECTION) + " ok=" + count(Message.OK)
                + " coordinator=" + count(Message.COORDINATOR)
                + " ring_pass=" + count(Message.RING_PASS) + " ping=" + count(Message.PING);
    }
}
