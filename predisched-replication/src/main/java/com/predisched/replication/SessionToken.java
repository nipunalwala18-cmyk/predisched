package com.predisched.replication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A client's memory of the newest version it has seen per task (client-centric consistency).
 * Reading with it gives read-your-writes and monotonic reads even in eventual mode: a replica
 * that holds an older version than the token fetches a fresher copy from a peer.
 */
public class SessionToken {

    private final Map<String, Long> seen = new ConcurrentHashMap<>();

    public long lastSeen(String taskId) {
        return seen.getOrDefault(taskId, 0L);
    }

    /** Remembers a version; never moves backwards. */
    public void saw(String taskId, long version) {
        seen.merge(taskId, version, Math::max);
    }
}
