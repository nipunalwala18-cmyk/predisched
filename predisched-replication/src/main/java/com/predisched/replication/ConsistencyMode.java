package com.predisched.replication;

import java.util.Locale;

/**
 * How a replicated write becomes durable and what a read returns (FR10). The store calls these
 * two methods and nothing else, so the mode is a strategy chosen by config, not an if-chain.
 */
public interface ConsistencyMode extends AutoCloseable {

    String name();

    /** Makes the write take effect as this mode defines, or throws {@link ReplicationException}. */
    void write(VersionedRecord record);

    /** The copy this mode returns for a read, or null when the task is unknown. */
    VersionedRecord read(String taskId);

    @Override
    void close();

    /** strong (quorum W/R) or eventual (local write, async replication, local reads). */
    static ConsistencyMode create(
            String name, LocalReplica local, ReplicaSet replicas, int writeQuorum,
            int readQuorum, long writeTimeoutMs) {
        return switch (name == null ? "strong" : name.toLowerCase(Locale.ROOT)) {
            case "strong" -> new StrongConsistency(
                    local, replicas, writeQuorum, readQuorum, writeTimeoutMs);
            case "eventual" -> new EventualConsistency(local, replicas);
            default -> throw new IllegalArgumentException(
                    "replication.mode must be strong or eventual, got " + name);
        };
    }
}
