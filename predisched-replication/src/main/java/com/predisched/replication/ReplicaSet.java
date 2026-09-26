package com.predisched.replication;

import com.predisched.election.ClusterView;
import com.predisched.proto.ReplicationServiceGrpc;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This node's replication peers: every other scheduler in the {@link ClusterView}, reached over
 * the same channels the election uses. For the Exp 5 demos a peer can be given an injected delay
 * (replication traffic to it is held back that long) or isolated (calls to it fail at once, as if
 * the network were cut). Both are thread-safe switches read on every call.
 */
public class ReplicaSet {

    private final ClusterView cluster;
    private final long callTimeoutMs;
    private final Map<Integer, Long> delayMs = new ConcurrentHashMap<>();
    private final Set<Integer> isolated = ConcurrentHashMap.newKeySet();

    public ReplicaSet(ClusterView cluster, long callTimeoutMs) {
        this.cluster = cluster;
        this.callTimeoutMs = callTimeoutMs;
    }

    public int selfId() {
        return cluster.selfId();
    }

    /** Replicas in the set, this node included: the N of the quorum arithmetic. */
    public int size() {
        return cluster.size();
    }

    public List<Integer> peers() {
        return cluster.others();
    }

    public void setDelay(int peer, long ms) {
        delayMs.put(peer, Math.max(0, ms));
    }

    public long delay(int peer) {
        return delayMs.getOrDefault(peer, 0L);
    }

    public void isolate(int peer) {
        isolated.add(peer);
    }

    public void heal(int peer) {
        isolated.remove(peer);
    }

    public ClusterView cluster() {
        return cluster;
    }

    /** A stub for one call to a peer; an isolated peer fails the call with UNAVAILABLE. */
    public ReplicationServiceGrpc.ReplicationServiceBlockingStub stub(int peer) {
        if (isolated.contains(peer)) {
            throw Status.UNAVAILABLE
                    .withDescription("peer " + peer + " is isolated from node " + selfId())
                    .asRuntimeException();
        }
        return ReplicationServiceGrpc.newBlockingStub(cluster.channel(peer))
                .withDeadlineAfter(callTimeoutMs, TimeUnit.MILLISECONDS);
    }
}
