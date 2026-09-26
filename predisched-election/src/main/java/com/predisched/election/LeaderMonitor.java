package com.predisched.election;

import com.predisched.common.obs.LamportInterceptors;
import com.predisched.proto.Ack;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A follower's failure detector: ping the leader every {@code pingIntervalMs}; after
 * {@code pingMisses} failures in a row, declare it dead and start an election. With no leader
 * known and no election running, it starts one. The miss counter is confined to the single
 * monitor thread.
 */
public class LeaderMonitor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LeaderMonitor.class);

    private final ElectionAlgorithm algorithm;
    private final long pingIntervalMs;
    private final int pingMisses;
    private final ScheduledExecutorService timer;
    private int missed;

    public LeaderMonitor(ElectionAlgorithm algorithm, long pingIntervalMs, int pingMisses) {
        this.algorithm = algorithm;
        this.pingIntervalMs = pingIntervalMs;
        this.pingMisses = Math.max(1, pingMisses);
        int self = algorithm.cluster().selfId();
        this.timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "leader-monitor-" + self);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        timer.scheduleWithFixedDelay(this::tick, pingIntervalMs, pingIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    void tick() {
        LamportInterceptors.applyMdc();
        if (algorithm.electionInProgress()) {
            missed = 0;
            return;
        }
        Optional<Integer> leader = algorithm.leader();
        if (leader.isEmpty()) {
            algorithm.startElection();
            return;
        }
        int leaderId = leader.get();
        if (leaderId == algorithm.cluster().selfId()) {
            missed = 0;
            return;
        }
        algorithm.stats().sent(ElectionStats.Message.PING);
        try {
            algorithm.cluster().stub(leaderId, pingIntervalMs).ping(Ack.getDefaultInstance());
            missed = 0;
        } catch (StatusRuntimeException e) {
            missed++;
            log.debug("Ping to leader {} failed ({} of {}): {}",
                    leaderId, missed, pingMisses, e.getStatus().getCode());
            if (missed >= pingMisses) {
                missed = 0;
                algorithm.leaderFailed();
            }
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
