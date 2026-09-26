package com.predisched.election;

import com.predisched.proto.Ack;
import com.predisched.proto.CoordinatorMsg;
import com.predisched.proto.ElectionMsg;
import java.util.Optional;
import java.util.function.IntConsumer;

/**
 * A coordinator election among the scheduler nodes (FR9). One instance per node; the gRPC
 * {@link ElectionServiceImpl} feeds it incoming messages and {@link LeaderMonitor} tells it when
 * the leader has gone quiet. Implementations are selected by config, never by an if-chain.
 */
public interface ElectionAlgorithm extends AutoCloseable {

    /** bully or ring, as written in config. */
    String name();

    /** Joins the cluster: elects a leader if none is known yet. */
    void start();

    /** Starts an election now, unless this node is already running one. */
    void startElection();

    /** The leader stopped answering: forget it and elect a new one. */
    void leaderFailed();

    Ack onElection(ElectionMsg message);

    Ack onRingPass(ElectionMsg message);

    Ack onCoordinator(CoordinatorMsg message);

    /** The leader this node currently believes in, if any. */
    Optional<Integer> leader();

    boolean electionInProgress();

    /** Called with the new leader's id every time this node's view of the leader changes. */
    void addLeaderListener(IntConsumer listener);

    ElectionStats stats();

    ClusterView cluster();

    @Override
    void close();

    /** The algorithm named in config: bully (default) or ring. */
    static ElectionAlgorithm create(String name, ClusterView cluster, long timeoutMs) {
        return switch (name == null ? "bully" : name.toLowerCase(java.util.Locale.ROOT)) {
            case "bully" -> new BullyElection(cluster, timeoutMs);
            case "ring" -> new RingElection(cluster, timeoutMs);
            default -> throw new IllegalArgumentException(
                    "election.algorithm must be bully or ring, got " + name);
        };
    }
}
