package com.predisched.scheduler;

import com.predisched.common.NodeConfig;
import com.predisched.common.TaskStore;
import com.predisched.common.time.LamportClock;
import com.predisched.election.ClusterView;
import com.predisched.election.ElectionAlgorithm;
import com.predisched.election.ElectionServiceImpl;
import com.predisched.election.LeaderMonitor;
import com.predisched.fault.InDoubtResolver;
import com.predisched.fault.PrimaryBackupCoordinator;
import com.predisched.fault.WorkerFailureDetector;
import com.predisched.replication.ConsistencyMode;
import com.predisched.replication.LocalReplica;
import com.predisched.replication.PrimaryBackupReplication;
import com.predisched.replication.ReplicaSet;
import com.predisched.replication.ReplicatedTaskStore;
import com.predisched.replication.ReplicationServiceImpl;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This scheduler's place in the cluster (FR9): its election id, the configured algorithm and the
 * leader monitor. Only the primary accepts submits and dispatches; the rest are backups.
 *
 * <p>With failover enabled (prompt 10) the elected leader is not the primary until the
 * {@link PrimaryBackupCoordinator} has promoted it: log caught up, queue rebuilt from the replicated
 * store, dispatcher started. Until then it refuses submits like a follower, naming itself leader.
 */
public class SchedulerNode implements Leadership, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SchedulerNode.class);

    private final int selfId;
    private final ElectionAlgorithm election;
    private final LeaderMonitor monitor;
    private final ReplicatedTaskStore replicated;
    private volatile PrimaryBackupCoordinator coordinator;

    private SchedulerNode(int selfId, ElectionAlgorithm election, LeaderMonitor monitor,
            ReplicatedTaskStore replicated) {
        this.selfId = selfId;
        this.election = election;
        this.monitor = monitor;
        this.replicated = replicated;
    }

    /**
     * A cluster member when election peers are configured; otherwise null (run alone). With
     * {@code replication.enabled} its task state is replicated to the same peers (Exp 5).
     */
    public static SchedulerNode fromConfig(
            int selfId, String nodeName, NodeConfig config, LamportClock clock) {
        NodeConfig.ElectionConfig electionConfig = config.getElection();
        if (electionConfig.getPeers().isEmpty()) {
            return null;
        }
        return inCluster(selfId, nodeName, config,
                ClusterView.fromConfig(selfId, electionConfig.getPeers(), clock), clock);
    }

    /** A cluster member reaching its peers through {@code cluster}; tests pass in-process ones. */
    public static SchedulerNode inCluster(
            int selfId, String nodeName, NodeConfig config, ClusterView cluster,
            LamportClock clock) {
        NodeConfig.ElectionConfig electionConfig = config.getElection();
        ElectionAlgorithm election = ElectionAlgorithm.create(
                electionConfig.getAlgorithm(), cluster, electionConfig.getTimeoutMs());
        LeaderMonitor monitor = new LeaderMonitor(election,
                electionConfig.getPingIntervalMs(), electionConfig.getPingMisses());
        election.addLeaderListener(leader -> log.info(leader == selfId
                ? "This scheduler (node " + selfId + ") is now the leader"
                : "Node " + selfId + " follows leader " + leader + ": submits are redirected"));
        ReplicatedTaskStore replicated = null;
        NodeConfig.ReplicationConfig replication = config.getReplication();
        if (replication.isEnabled()) {
            ReplicaSet replicas = new ReplicaSet(cluster, replication.getWriteTimeoutMs());
            for (int peer : cluster.others()) {
                replicas.setDelay(peer, replication.delayTo(peer));
            }
            LocalReplica local = new LocalReplica(nodeName, clock);
            replicated = new ReplicatedTaskStore(local, replicas, ConsistencyMode.create(
                    replication.getMode(), local, replicas, replication.getWriteQuorum(),
                    replication.getReadQuorum(), replication.getWriteTimeoutMs()));
            if (replicated.mode() instanceof PrimaryBackupReplication) {
                log.info("Task state replicated to {} peers, primary-backup: every write reaches"
                        + " every live backup before it is acknowledged", cluster.others().size());
            } else {
                log.info("Task state replicated to {} peers, {} consistency (W={} R={} of N={})",
                        cluster.others().size(), replication.getMode(),
                        replication.getWriteQuorum(), replication.getReadQuorum(), cluster.size());
            }
        }
        return new SchedulerNode(selfId, election, monitor, replicated);
    }

    /**
     * Hands promotion, demotion and worker failure handling to a {@link PrimaryBackupCoordinator}
     * (prompt 10). Call before {@link #start()}, so the first election result is seen.
     */
    public void enableFailover(SchedulerFailover failover, WorkerFailureDetector detector,
            TaskStore store, long catchUpIntervalMs) {
        PrimaryBackupReplication sequenced = replicated != null
                && replicated.mode() instanceof PrimaryBackupReplication primaryBackup
                ? primaryBackup
                : null;
        coordinator = new PrimaryBackupCoordinator(selfId, election, store, sequenced, failover,
                new InDoubtResolver(failover), detector, catchUpIntervalMs);
        coordinator.start();
    }

    public PrimaryBackupCoordinator coordinator() {
        return coordinator;
    }

    /** The replicated store when replication is on, else the given local one. */
    public TaskStore taskStore(TaskStore unreplicated) {
        return replicated != null ? replicated : unreplicated;
    }

    /** The replication service to serve, or null when replication is off. */
    public ReplicationServiceImpl replicationService() {
        return replicated == null ? null : new ReplicationServiceImpl(replicated);
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

    /** With failover on, true only once the promotion to primary has finished. */
    @Override
    public boolean isLeader() {
        boolean elected = election.leader().map(leader -> leader == selfId).orElse(false);
        PrimaryBackupCoordinator current = coordinator;
        return elected && (current == null || current.isPrimary());
    }

    @Override
    public Optional<Integer> leader() {
        return election.leader();
    }

    @Override
    public void close() {
        if (coordinator != null) {
            coordinator.close();
        }
        monitor.close();
        if (replicated != null) {
            replicated.mode().close();
        }
        election.close();
    }
}
