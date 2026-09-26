package com.predisched.fault;

import com.predisched.common.TaskStore;
import com.predisched.common.db.History;
import com.predisched.common.time.Clocks;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.election.ElectionAlgorithm;
import com.predisched.replication.PrimaryBackupReplication;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes the elected leader the primary scheduler (FR12, lab Exp 8, prompt 10).
 *
 * <p>On becoming leader it promotes this node in three steps: (1) bring its replication log up to
 * the highest sequence number any reachable backup holds, pulling the gap with {@code SyncFrom};
 * (2) rebuild the queue from the store, QUEUED tasks queued, BLOCKED tasks re-linked to their
 * parents, RUNNING tasks resolved with their workers ({@link InDoubtResolver}); (3) start the
 * dispatcher and the worker failure detector. Only then does it accept submits. On losing the
 * leadership it stops dispatching at once.
 *
 * <p>As a backup it keeps itself in the primary's live set: every {@code catchUpIntervalMs} it
 * catches up from the primary and asks to join, which is a no-op once it is live.
 *
 * <p>Threading: leader changes are handled one at a time on the {@code failover} thread, in the
 * order the election reported them; a generation counter lets a promotion notice a newer change
 * and stop. The backup loop has its own thread so a long catch-up never delays a promotion.
 */
public class PrimaryBackupCoordinator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PrimaryBackupCoordinator.class);

    private final int selfId;
    private final ElectionAlgorithm election;
    private final TaskStore store;
    private final PrimaryBackupReplication replication;
    private final PrimaryRole role;
    private final InDoubtResolver inDoubt;
    private final WorkerFailureDetector detector;
    private final long catchUpIntervalMs;
    private final ScheduledExecutorService transitions;
    private final ScheduledExecutorService backupLoop;
    private final AtomicLong generation = new AtomicLong();
    private volatile boolean primary;
    private volatile boolean liveBackup;
    private volatile long lastPromotionMs = -1;
    /** The leader before the current one; confined to the transitions thread. */
    private int previousLeader = -1;

    /**
     * @param replication the primary-backup log, or null when the cluster replicates another way
     *     (or not at all): promotion then skips step 1 and uses the store as it is
     */
    public PrimaryBackupCoordinator(
            int selfId,
            ElectionAlgorithm election,
            TaskStore store,
            PrimaryBackupReplication replication,
            PrimaryRole role,
            InDoubtResolver inDoubt,
            WorkerFailureDetector detector,
            long catchUpIntervalMs) {
        this.selfId = selfId;
        this.election = election;
        this.store = store;
        this.replication = replication;
        this.role = role;
        this.inDoubt = inDoubt;
        this.detector = detector;
        this.catchUpIntervalMs = catchUpIntervalMs;
        this.transitions = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "failover-" + selfId);
            thread.setDaemon(true);
            return thread;
        });
        this.backupLoop = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "backup-sync-" + selfId);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Call before the election starts, so the first leader it announces is not missed. */
    public void start() {
        election.addLeaderListener(leader -> {
            long changeGeneration = generation.incrementAndGet();
            long changedAtMs = System.currentTimeMillis();
            transitions.execute(() -> onLeader(leader, changeGeneration, changedAtMs));
        });
        if (replication != null && catchUpIntervalMs > 0) {
            backupLoop.scheduleWithFixedDelay(this::backupTick, catchUpIntervalMs,
                    catchUpIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /** True once this node has finished promoting and dispatches; submits wait for this. */
    public boolean isPrimary() {
        return primary;
    }

    /** Milliseconds the last promotion took, from the leader change to dispatching; -1 if none. */
    public long lastPromotionMs() {
        return lastPromotionMs;
    }

    private void onLeader(int leader, long changeGeneration, long changedAtMs) {
        LamportInterceptors.applyMdc();
        int lost = previousLeader;
        previousLeader = leader;
        try {
            if (leader == selfId) {
                if (lost > 0 && lost != selfId) {
                    History.get().failure("scheduler-" + lost, "PRIMARY_LOST", Clocks.now(),
                            "scheduler " + selfId + " promoted in its place");
                }
                promote(changeGeneration, changedAtMs);
            } else {
                if (primary) {
                    demote(leader);
                }
                liveBackup = false;
                log.info("Node {} is a backup of primary {}", selfId, leader);
            }
        } catch (RuntimeException e) {
            log.error("Handling the change to leader {} failed on node {}", leader, selfId, e);
        }
    }

    private void promote(long changeGeneration, long changedAtMs) {
        log.info("Node {} won the election: promoting to primary", selfId);
        liveBackup = false;

        // 1. Replication log at least as far as any reachable backup's.
        if (replication != null) {
            replication.becomePrimary();
            long before = replication.lastSeq();
            List<Integer> unreachable = new ArrayList<>();
            for (int peer : election.cluster().others()) {
                if (stale(changeGeneration)) {
                    return;
                }
                if (replication.catchUpFrom(peer, false) < 0) {
                    unreachable.add(peer);
                }
            }
            replication.serve();
            log.info("Promotion 1/3: replication log at seq {} (was {}); unreachable or skipped:"
                    + " {}", replication.lastSeq(), before, unreachable);
        } else {
            log.info("Promotion 1/3: no primary-backup log in this cluster; using the store as it"
                    + " is");
        }
        if (stale(changeGeneration)) {
            return;
        }

        // 2. Scheduling state from the store; RUNNING tasks settled with their workers.
        Recovery recovery = Recovery.of(store.list());
        role.rebuild(recovery);
        log.info("Promotion 2/3: rebuilt from the store: {}", recovery);
        if (!recovery.running().isEmpty()) {
            Map<InDoubtResolver.Resolution, Integer> settled = inDoubt.resolve(recovery.running());
            log.info("Promotion 2/3: in-doubt dispatches settled: {}", settled);
        }
        if (stale(changeGeneration)) {
            return;
        }

        // 3. Dispatch.
        role.startDispatching();
        detector.start();
        primary = true;
        lastPromotionMs = System.currentTimeMillis() - changedAtMs;
        log.info("Promotion 3/3: node {} is the primary and dispatching, {} ms after the leader"
                + " change", selfId, lastPromotionMs);
        EventLog.get().event("PROMOTED", "", Map.of(
                "node", String.valueOf(selfId),
                "took_ms", String.valueOf(lastPromotionMs),
                "seq", String.valueOf(replication == null ? 0 : replication.lastSeq()),
                "queued", String.valueOf(recovery.queued().size()),
                "in_doubt", String.valueOf(recovery.running().size())));
    }

    private void demote(int leader) {
        primary = false;
        role.stopDispatching();
        detector.stop();
        if (replication != null) {
            replication.standDown();
        }
        log.warn("Node {} lost primacy to node {}: dispatch stopped; it rejoins as a backup",
                selfId, leader);
        EventLog.get().event("DEMOTED", "", Map.of(
                "node", String.valueOf(selfId), "leader", String.valueOf(leader)));
    }

    /** A newer leader change is queued behind this one: let it decide. */
    private boolean stale(long changeGeneration) {
        if (generation.get() == changeGeneration) {
            return false;
        }
        log.info("Leadership changed again during the promotion of node {}; stopping it", selfId);
        return true;
    }

    /** One pass of the backup's rejoin loop. */
    private void backupTick() {
        LamportInterceptors.applyMdc();
        try {
            Optional<Integer> leader = election.leader();
            if (primary || leader.isEmpty() || leader.get() == selfId) {
                return;
            }
            boolean live = replication.syncWithPrimary(leader.get());
            if (live != liveBackup) {
                liveBackup = live;
                if (live) {
                    log.info("Node {} is a live backup of primary {} at seq {}",
                            selfId, leader.get(), replication.lastSeq());
                } else {
                    log.warn("Node {} left primary {}'s live set; catching up to rejoin",
                            selfId, leader.get());
                }
            }
        } catch (RuntimeException e) {
            log.debug("Backup sync on node {} failed: {}", selfId, e.toString());
        }
    }

    @Override
    public void close() {
        backupLoop.shutdownNow();
        transitions.shutdownNow();
        detector.close();
    }
}
