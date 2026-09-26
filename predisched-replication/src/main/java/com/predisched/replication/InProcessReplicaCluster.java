package com.predisched.replication;

import com.predisched.common.time.LamportClock;
import com.predisched.election.ClusterView;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

/**
 * N replicas in one JVM, each a real {@link ReplicatedTaskStore} behind its own in-process gRPC
 * {@link ReplicationServiceImpl}, each with its own Lamport clock. Killing a replica shuts its
 * server down. Used by the replication tests and the {@code consistency-compare} benchmark; the
 * live cluster runs the same classes inside the schedulers.
 */
public final class InProcessReplicaCluster implements AutoCloseable {

    private record Node(ReplicatedTaskStore store, Server server, ClusterView view) {}

    private final String prefix = "replica-" + UUID.randomUUID() + "-";
    private final List<Integer> ids;
    private final String mode;
    private final int writeQuorum;
    private final int readQuorum;
    private final long timeoutMs;
    private final Map<Integer, Node> nodes = new TreeMap<>();
    private final Map<Integer, Node> live = new TreeMap<>();

    private InProcessReplicaCluster(
            List<Integer> ids, String mode, int writeQuorum, int readQuorum, long timeoutMs) {
        this.ids = List.copyOf(ids);
        this.mode = mode;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        this.timeoutMs = timeoutMs;
    }

    public static InProcessReplicaCluster start(
            String mode, int replicas, int writeQuorum, int readQuorum, long timeoutMs) {
        List<Integer> ids = new ArrayList<>();
        for (int id = 1; id <= replicas; id++) {
            ids.add(id);
        }
        InProcessReplicaCluster cluster =
                new InProcessReplicaCluster(ids, mode, writeQuorum, readQuorum, timeoutMs);
        for (int id : ids) {
            ReplicatedTaskStore store = cluster.newStore(id, ids);
            try {
                Server server = InProcessServerBuilder.forName(cluster.prefix + id)
                        .addService(new ReplicationServiceImpl(store))
                        .build()
                        .start();
                Node node = new Node(store, server, store.replicas().cluster());
                cluster.nodes.put(id, node);
                cluster.live.put(id, node);
            } catch (IOException e) {
                cluster.close();
                throw new UncheckedIOException(e);
            }
        }
        return cluster;
    }

    private ReplicatedTaskStore newStore(int id, List<Integer> members) {
        ClusterView view = new ClusterView(id, members,
                peer -> InProcessChannelBuilder.forName(prefix + peer).build());
        ReplicaSet replicas = new ReplicaSet(view, timeoutMs);
        LocalReplica local = new LocalReplica("node-" + id, new LamportClock());
        ConsistencyMode consistency = ConsistencyMode.create(
                mode, local, replicas, writeQuorum, readQuorum, timeoutMs);
        return new ReplicatedTaskStore(local, replicas, consistency);
    }

    public ReplicatedTaskStore store(int id) {
        return nodes.get(id).store();
    }

    /** Holds back replication traffic from one replica to another. */
    public void setDelay(int from, int to, long ms) {
        store(from).replicas().setDelay(to, ms);
    }

    /** Cuts every replica's link to {@code id} both ways, without stopping it. */
    public void isolate(int id) {
        for (int other : ids) {
            if (other != id) {
                store(other).replicas().isolate(id);
                store(id).replicas().isolate(other);
            }
        }
    }

    public void heal(int id) {
        for (int other : ids) {
            if (other != id) {
                store(other).replicas().heal(id);
                store(id).replicas().heal(other);
            }
        }
    }

    /** Crashes a replica: its server stops answering. */
    public void kill(int id) {
        Node node = live.remove(id);
        if (node != null) {
            node.server().shutdownNow();
        }
    }

    public List<Integer> liveIds() {
        return List.copyOf(live.keySet());
    }

    /**
     * A new, empty replica with id {@code id} that can reach this cluster but that no one
     * replicates to yet: the starting point of a {@code SyncFrom} catch-up.
     */
    public ReplicatedTaskStore freshReplica(int id) {
        List<Integer> members = new ArrayList<>(ids);
        members.add(id);
        return newStore(id, members);
    }

    /** True when every live replica holds the same copy of every task. */
    public boolean converged() {
        Map<String, VersionedRecord> reference = null;
        for (Node node : live.values()) {
            Map<String, VersionedRecord> copies = new HashMap<>();
            for (VersionedRecord copy : node.store().local().all()) {
                copies.put(copy.taskId(), copy);
            }
            if (reference == null) {
                reference = copies;
            } else if (!sameCopies(reference, copies)) {
                return false;
            }
        }
        return true;
    }

    /** Milliseconds until {@link #converged()}, or -1 if not within the timeout. */
    public long awaitConverged(long timeoutMs) throws InterruptedException {
        long start = System.nanoTime();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (converged()) {
                return (System.nanoTime() - start) / 1_000_000;
            }
            Thread.sleep(2);
        }
        return converged() ? (System.nanoTime() - start) / 1_000_000 : -1;
    }

    private static boolean sameCopies(
            Map<String, VersionedRecord> a, Map<String, VersionedRecord> b) {
        if (!a.keySet().equals(b.keySet())) {
            return false;
        }
        for (Map.Entry<String, VersionedRecord> entry : a.entrySet()) {
            VersionedRecord other = b.get(entry.getKey());
            if (entry.getValue().lamportTime() != other.lamportTime()
                    || !Objects.equals(entry.getValue().originNode(), other.originNode())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        for (Node node : nodes.values()) {
            node.server().shutdownNow();
            node.store().mode().close();
            node.view().close();
        }
        live.clear();
    }
}
