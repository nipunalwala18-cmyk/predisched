package com.predisched.replication;

import com.predisched.proto.Ack;
import com.predisched.proto.JoinRequest;
import com.predisched.proto.JoinResponse;
import com.predisched.proto.ReplicateRequest;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

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

    /**
     * A change a peer pushed to this replica. Null, the default, means the store keeps it by
     * last-writer-wins and acks at once; primary-backup applies it in sequence order instead.
     */
    default CompletableFuture<Ack> receive(ReplicateRequest request) {
        return null;
    }

    /** A backup asking to rejoin the live set; only primary-backup has one. */
    default JoinResponse join(JoinRequest request) {
        return JoinResponse.newBuilder()
                .setJoined(false)
                .setMessage(name() + " replication has no live set")
                .build();
    }

    @Override
    void close();

    /**
     * strong (quorum W/R), eventual (local write, async replication, local reads) or
     * primary-backup (the leader forwards every write to every live backup before it counts).
     */
    static ConsistencyMode create(
            String name, LocalReplica local, ReplicaSet replicas, int writeQuorum,
            int readQuorum, long writeTimeoutMs) {
        return switch (name == null ? "strong" : name.toLowerCase(Locale.ROOT)) {
            case "strong" -> new StrongConsistency(
                    local, replicas, writeQuorum, readQuorum, writeTimeoutMs);
            case "eventual" -> new EventualConsistency(local, replicas);
            case "primary-backup" -> new PrimaryBackupReplication(local, replicas, writeTimeoutMs);
            default -> throw new IllegalArgumentException(
                    "replication.mode must be strong, eventual or primary-backup, got " + name);
        };
    }
}
