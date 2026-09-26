package com.predisched.election;

import com.predisched.common.NodeConfig;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.LamportClock;
import com.predisched.proto.ElectionServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * Every scheduler node in the cluster by election id, and a lazily opened channel to each peer.
 * Ids are fixed by config; liveness is discovered by calling a peer, never assumed.
 */
public class ClusterView implements AutoCloseable {

    private final int selfId;
    private final List<Integer> ids;
    private final IntFunction<ManagedChannel> connect;
    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();

    /**
     * @param ids every node id in the cluster, including {@code selfId}
     * @param connect opens a channel to a peer id; tests pass in-process channels
     */
    public ClusterView(int selfId, Collection<Integer> ids, IntFunction<ManagedChannel> connect) {
        if (!ids.contains(selfId)) {
            throw new IllegalArgumentException("node " + selfId + " is not in the cluster " + ids);
        }
        this.selfId = selfId;
        this.ids = ids.stream().sorted().distinct().toList();
        this.connect = connect;
    }

    /** Plaintext gRPC channels to the configured peers, carrying Lamport time on every call. */
    public static ClusterView fromConfig(
            int selfId, List<NodeConfig.PeerConfig> peers, LamportClock clock) {
        Map<Integer, NodeConfig.PeerConfig> byId = new TreeMap<>();
        for (NodeConfig.PeerConfig peer : peers) {
            byId.put(peer.getId(), peer);
        }
        return new ClusterView(selfId, byId.keySet(), id -> {
            NodeConfig.PeerConfig peer = byId.get(id);
            return ManagedChannelBuilder.forAddress(peer.getHost(), peer.getPort())
                    .usePlaintext()
                    .intercept(LamportInterceptors.client(clock))
                    .build();
        });
    }

    public int selfId() {
        return selfId;
    }

    /** All node ids, ascending. */
    public List<Integer> ids() {
        return ids;
    }

    public int size() {
        return ids.size();
    }

    /** Ids above this node's, ascending: the nodes a Bully election is sent to. */
    public List<Integer> higher() {
        return ids.stream().filter(id -> id > selfId).toList();
    }

    /** Every id except this node's. */
    public List<Integer> others() {
        return ids.stream().filter(id -> id != selfId).toList();
    }

    /**
     * The ring after this node, in order: the successor first, wrapping around, ending just
     * before this node. A Ring hop tries these in turn, which is how it skips a dead successor.
     */
    public List<Integer> successors() {
        List<Integer> order = new ArrayList<>(ids.size() - 1);
        int self = ids.indexOf(selfId);
        for (int step = 1; step < ids.size(); step++) {
            order.add(ids.get((self + step) % ids.size()));
        }
        return order;
    }

    /** A blocking stub to a peer with a per-call deadline. */
    public ElectionServiceGrpc.ElectionServiceBlockingStub stub(int peerId, long deadlineMs) {
        return ElectionServiceGrpc.newBlockingStub(channel(peerId))
                .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
    }

    /** The shared channel to a peer, for other services on the same node (replication). */
    public ManagedChannel channel(int peerId) {
        return channels.computeIfAbsent(peerId, connect::apply);
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }
}
