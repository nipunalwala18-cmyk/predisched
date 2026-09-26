package com.predisched.election;

import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What Bully and Ring share: the leader this node believes in, its listeners, the stats, and two
 * thread pools. Election logic runs on one {@code election} thread per node, so an algorithm's
 * own state changes happen in order; peer calls that may block go to {@code rpc} so one dead peer
 * cannot hold up the others. gRPC handlers only hand work to those threads and return at once.
 */
abstract class AbstractElection implements ElectionAlgorithm {

    private static final Logger log = LoggerFactory.getLogger(AbstractElection.class);

    protected final ClusterView cluster;
    protected final long timeoutMs;
    protected final ElectionStats stats = new ElectionStats();
    protected final ScheduledExecutorService election;
    protected final ExecutorService rpc;
    protected final AtomicBoolean running = new AtomicBoolean();

    private final AtomicReference<Integer> leader = new AtomicReference<>();
    private final List<IntConsumer> listeners = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    AbstractElection(ClusterView cluster, long timeoutMs) {
        this.cluster = cluster;
        this.timeoutMs = timeoutMs;
        int self = cluster.selfId();
        this.election = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "election-" + self);
            thread.setDaemon(true);
            return thread;
        });
        this.rpc = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "election-rpc-" + self);
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void start() {
        if (leader().isEmpty()) {
            startElection();
        }
    }

    @Override
    public void leaderFailed() {
        Integer lost = leader.getAndSet(null);
        log.warn("Leader {} stopped answering; node {} starts a {} election",
                lost, cluster.selfId(), name());
        startElection();
    }

    @Override
    public Optional<Integer> leader() {
        return Optional.ofNullable(leader.get());
    }

    @Override
    public boolean electionInProgress() {
        return running.get();
    }

    @Override
    public void addLeaderListener(IntConsumer listener) {
        listeners.add(listener);
    }

    @Override
    public ElectionStats stats() {
        return stats;
    }

    @Override
    public ClusterView cluster() {
        return cluster;
    }

    protected boolean isClosed() {
        return closed;
    }

    /** Records the start of an election on this node: stats clock, log and event. */
    protected void electionStarted() {
        stats.markDetected();
        log.info("Node {} starts a {} election", cluster.selfId(), name());
        EventLog.get().event(EventLog.ELECTION_START, "", Map.of(
                "algorithm", name(), "initiator", String.valueOf(cluster.selfId())));
    }

    /** Adopts a leader. Listeners hear about it only when it differs from the previous one. */
    protected void setLeader(int id) {
        Integer previous = leader.getAndSet(id);
        long tookMs = stats.markLeaderKnown();
        if (previous != null && previous == id) {
            return;
        }
        if (tookMs >= 0) {
            log.info("Leader is now {} ({}): elected in {} ms; sent by this node since start: {}",
                    id, name(), tookMs, stats);
        } else {
            log.info("Leader is now {} ({}), announced by the cluster", id, name());
        }
        EventLog.get().event(EventLog.LEADER_ELECTED, "", Map.of(
                "algorithm", name(),
                "leader", String.valueOf(id),
                "election_ms", String.valueOf(tookMs)));
        for (IntConsumer listener : listeners) {
            listener.accept(id);
        }
    }

    /** Runs election logic on this node's election thread, with the node's MDC applied. */
    protected void onElectionThread(Runnable work) {
        if (closed) {
            return;
        }
        election.execute(() -> {
            LamportInterceptors.applyMdc();
            try {
                work.run();
            } catch (RuntimeException e) {
                log.warn("Election step failed on node {}: {}", cluster.selfId(), e.toString());
            }
        });
    }

    @Override
    public void close() {
        closed = true;
        election.shutdownNow();
        rpc.shutdownNow();
        cluster.close();
    }
}
