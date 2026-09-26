package com.predisched.election;

import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * N election nodes in one JVM, each a real {@link ElectionServiceImpl} behind its own in-process
 * gRPC server, talking through real stubs. Killing a node shuts its server down, so peers see the
 * same UNAVAILABLE a crashed process would give them. Used by the election tests and by the
 * {@code election-compare} benchmark; the live cluster runs the same classes inside schedulers.
 */
public final class InProcessElectionCluster implements AutoCloseable {

    private record Node(ElectionAlgorithm algorithm, LeaderMonitor monitor, Server server) {}

    private final Map<Integer, Node> live = new TreeMap<>();
    private final Map<Integer, ElectionAlgorithm> all = new TreeMap<>();

    private InProcessElectionCluster() {}

    /**
     * Starts every node, then lets them elect. {@code pingIntervalMs <= 0} disables the leader
     * monitors, so the only elections are the ones a caller starts.
     */
    public static InProcessElectionCluster start(
            String algorithm, List<Integer> ids, long timeoutMs, long pingIntervalMs,
            int pingMisses) {
        InProcessElectionCluster cluster = new InProcessElectionCluster();
        String prefix = "election-" + UUID.randomUUID() + "-";
        try {
            for (int id : ids) {
                ClusterView view = new ClusterView(id, ids,
                        peer -> InProcessChannelBuilder.forName(prefix + peer).build());
                ElectionAlgorithm node = ElectionAlgorithm.create(algorithm, view, timeoutMs);
                Server server = InProcessServerBuilder.forName(prefix + id)
                        .addService(new ElectionServiceImpl(node))
                        .build()
                        .start();
                LeaderMonitor monitor = pingIntervalMs > 0
                        ? new LeaderMonitor(node, pingIntervalMs, pingMisses)
                        : null;
                cluster.live.put(id, new Node(node, monitor, server));
                cluster.all.put(id, node);
            }
        } catch (IOException e) {
            cluster.close();
            throw new UncheckedIOException(e);
        }
        for (Node node : cluster.live.values()) {
            node.algorithm().start();
            if (node.monitor() != null) {
                node.monitor().start();
            }
        }
        return cluster;
    }

    /** Crashes a node: its server stops answering and its algorithm stops acting. */
    public synchronized void kill(int id) {
        Node node = live.remove(id);
        if (node == null) {
            return;
        }
        node.server().shutdownNow();
        if (node.monitor() != null) {
            node.monitor().close();
        }
        node.algorithm().close();
    }

    public synchronized ElectionAlgorithm node(int id) {
        return all.get(id);
    }

    public synchronized List<Integer> liveIds() {
        return List.copyOf(live.keySet());
    }

    /** What each live node believes the leader is. */
    public synchronized Map<Integer, Optional<Integer>> leaders() {
        Map<Integer, Optional<Integer>> view = new TreeMap<>();
        live.forEach((id, node) -> view.put(id, node.algorithm().leader()));
        return view;
    }

    /**
     * Waits until every live node names the same live leader and no election is running.
     * Returns that leader, or empty on timeout.
     */
    public Optional<Integer> awaitAgreedLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Optional<Integer> agreed = agreedLeader();
            if (agreed.isPresent()) {
                return agreed;
            }
            Thread.sleep(5);
        }
        return Optional.empty();
    }

    private synchronized Optional<Integer> agreedLeader() {
        Integer leader = null;
        for (Node node : live.values()) {
            Optional<Integer> seen = node.algorithm().leader();
            if (seen.isEmpty() || node.algorithm().electionInProgress()) {
                return Optional.empty();
            }
            if (leader == null) {
                leader = seen.get();
            } else if (!leader.equals(seen.get())) {
                return Optional.empty();
            }
        }
        return leader != null && live.containsKey(leader) ? Optional.of(leader) : Optional.empty();
    }

    /** Election messages sent by every node, dead ones included, since the last reset. */
    public synchronized long electionMessages() {
        return all.values().stream().mapToLong(node -> node.stats().electionMessages()).sum();
    }

    public synchronized void resetStats() {
        all.values().forEach(node -> node.stats().reset());
    }

    @Override
    public synchronized void close() {
        for (int id : List.copyOf(live.keySet())) {
            kill(id);
        }
    }
}
