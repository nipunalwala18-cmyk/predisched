package com.predisched.scheduler;

import com.predisched.common.NodeConfig;
import com.predisched.common.time.LamportClock;
import com.predisched.election.ClusterView;
import com.predisched.election.ElectionAlgorithm;
import com.predisched.election.ElectionServiceImpl;
import com.predisched.election.LeaderMonitor;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This scheduler's place in the cluster (FR9): its election id, the configured algorithm and the
 * leader monitor. Only the leader accepts submits and dispatches; the rest are followers.
 *
 * <p>TODO(prompt 10): a new leader starts with an empty queue. Primary-backup replication hands
 * the task state over.
 */
public class SchedulerNode implements Leadership, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SchedulerNode.class);

    private final int selfId;
    private final ElectionAlgorithm election;
    private final LeaderMonitor monitor;

    private SchedulerNode(int selfId, ElectionAlgorithm election, LeaderMonitor monitor) {
        this.selfId = selfId;
        this.election = election;
        this.monitor = monitor;
    }

    /** A cluster member when election peers are configured; otherwise null (run alone). */
    public static SchedulerNode fromConfig(
            int selfId, NodeConfig.ElectionConfig config, LamportClock clock) {
        if (config.getPeers().isEmpty()) {
            return null;
        }
        ClusterView cluster = ClusterView.fromConfig(selfId, config.getPeers(), clock);
        ElectionAlgorithm election =
                ElectionAlgorithm.create(config.getAlgorithm(), cluster, config.getTimeoutMs());
        LeaderMonitor monitor = new LeaderMonitor(
                election, config.getPingIntervalMs(), config.getPingMisses());
        election.addLeaderListener(leader -> log.info(leader == selfId
                ? "This scheduler (node " + selfId + ") is now the leader: accepting tasks"
                : "Node " + selfId + " follows leader " + leader + ": submits are redirected"));
        return new SchedulerNode(selfId, election, monitor);
    }

    /**
     * The election id: {@code --node-id} if given, else the digits at the end of the scheduler id
     * ({@code scheduler-3} is node 3).
     */
    public static int electionId(String nodeIdOption, String schedulerId) {
        if (nodeIdOption != null) {
            return Integer.parseInt(nodeIdOption);
        }
        String digits = schedulerId.replaceAll("^.*?(\\d+)$", "$1");
        if (digits.isEmpty() || !digits.chars().allMatch(Character::isDigit)) {
            throw new IllegalArgumentException(
                    "cannot derive an election id from '" + schedulerId + "'; pass --node-id");
        }
        return Integer.parseInt(digits);
    }

    public ElectionServiceImpl service() {
        return new ElectionServiceImpl(election);
    }

    /** Call once the gRPC server is up, so peers can answer this node's first election. */
    public void start() {
        election.start();
        monitor.start();
        log.info("Node {} joined a {}-node cluster running {} election",
                selfId, election.cluster().size(), election.name());
    }

    public ElectionAlgorithm election() {
        return election;
    }

    @Override
    public boolean isLeader() {
        return election.leader().map(leader -> leader == selfId).orElse(false);
    }

    @Override
    public Optional<Integer> leader() {
        return election.leader();
    }

    @Override
    public void close() {
        monitor.close();
        election.close();
    }
}
