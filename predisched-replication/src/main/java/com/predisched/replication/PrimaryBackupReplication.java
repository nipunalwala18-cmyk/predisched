package com.predisched.replication;

import com.predisched.common.db.History;
import com.predisched.common.obs.EventLog;
import com.predisched.common.time.Clocks;
import com.predisched.proto.Ack;
import com.predisched.proto.JoinRequest;
import com.predisched.proto.JoinResponse;
import com.predisched.proto.ReplicateRequest;
import com.predisched.proto.SyncRequest;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous primary-backup replication (FR12, lab Exp 8, prompt 10).
 *
 * <p>Primary: a write takes the next sequence number, is applied and logged here, then forwarded
 * to every backup in the <em>live set</em>; it returns, and the client is acknowledged, only once
 * every one of them has acked. A backup that does not ack within the timeout is dropped from the
 * live set, so one slow or dead backup cannot stall the cluster.
 *
 * <p>Backup: changes are applied strictly in sequence order. Writes are forwarded from many
 * threads at once, so seq 7 may arrive before seq 6; it waits in a buffer and its ack is held
 * until seq 6 has been applied, which means an ack always says "this and everything before it".
 *
 * <p>Rejoining: a backup that is new, restarted or was dropped pulls what it lacks with
 * {@code SyncFrom}, then calls {@code Join} with its last sequence number. The primary adds it to
 * the live set only if that equals its own last sequence number, checked under the same lock
 * that numbers writes, so no write can fall between the catch-up and the join. A node that has
 * just been elected is not {@link #serve() serving} until it has pulled whatever the backups hold
 * beyond its own log; until then it refuses joins, so no backup mistakes the new primary's
 * shorter log for the truth and throws its own away.
 *
 * <p>Thread safety: {@code sequence} guards numbering, sequenced applies, the buffer and joins;
 * the live set is a concurrent set read without the lock when a write fans out.
 */
public class PrimaryBackupReplication implements ConsistencyMode {

    private static final Logger log = LoggerFactory.getLogger(PrimaryBackupReplication.class);

    /** A catch-up stream may carry the whole log, so it gets far longer than one write. */
    private static final int SYNC_DEADLINE_FACTOR = 30;

    private final LocalReplica local;
    private final ReplicaSet replicas;
    private final long ackTimeoutMs;
    private final ExecutorService rpc;
    private final Set<Integer> live = ConcurrentHashMap.newKeySet();
    /** True while this node is the primary and its log is complete: only then may backups join. */
    private volatile boolean serving;
    private final Object sequence = new Object();
    /** Backup side: changes that arrived ahead of a gap, by sequence number. */
    private final TreeMap<Long, Buffered> buffered = new TreeMap<>();

    private record Buffered(VersionedRecord record, CompletableFuture<Ack> ack) {}

    public PrimaryBackupReplication(LocalReplica local, ReplicaSet replicas, long ackTimeoutMs) {
        this.local = local;
        this.replicas = replicas;
        this.ackTimeoutMs = ackTimeoutMs;
        this.rpc = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "primary-backup-" + local.nodeName());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public String name() {
        return "primary-backup";
    }

    // ---- primary side -------------------------------------------------------------------

    @Override
    public void write(VersionedRecord record) {
        long seq;
        List<Integer> targets;
        synchronized (sequence) {
            seq = local.log().lastSeq() + 1;
            local.applySequenced(seq, record);
            targets = List.copyOf(live);
        }
        if (targets.isEmpty()) {
            return;
        }
        ReplicateRequest request = record.toRequest(seq);
        Map<Integer, Future<Boolean>> acks = new LinkedHashMap<>();
        for (int peer : targets) {
            acks.put(peer, rpc.submit(() -> push(peer, request)));
        }
        long deadline = System.currentTimeMillis() + ackTimeoutMs;
        for (Map.Entry<Integer, Future<Boolean>> ack : acks.entrySet()) {
            if (!await(ack.getValue(), deadline)) {
                drop(ack.getKey(), seq);
            }
        }
    }

    /** The primary reads its own copy: every write it acknowledged went through it first. */
    @Override
    public VersionedRecord read(String taskId) {
        return local.get(taskId);
    }

    @Override
    public JoinResponse join(JoinRequest request) {
        int node = request.getNodeId();
        synchronized (sequence) {
            long last = local.log().lastSeq();
            JoinResponse.Builder reply = JoinResponse.newBuilder()
                    .setPrimaryLastSeq(last)
                    .setReady(serving);
            if (!serving) {
                return reply.setJoined(false)
                        .setMessage("node " + replicas.selfId() + " is not serving as primary")
                        .build();
            }
            if (request.getLastSeq() == last && !sameChangeAt(last, request)) {
                live.remove(node);
                return reply.setJoined(false)
                        .setMessage("forked at seq " + last + ": resync from scratch").build();
            }
            if (request.getLastSeq() == last) {
                if (live.add(node)) {
                    log.info("Backup {} caught up to seq {}: joined the live set, now {}",
                            node, last, new TreeSet<>(live));
                    EventLog.get().event("BACKUP_JOINED", "", Map.of(
                            "backup", String.valueOf(node), "seq", String.valueOf(last)));
                    History.get().recovered("scheduler-" + node, Clocks.now());
                    return reply.setJoined(true).setMessage("joined at seq " + last).build();
                }
                return reply.setJoined(true).setMessage("already live").build();
            }
            if (live.contains(node) && request.getLastSeq() < last) {
                // Writes numbered after its last one are still on their way to it.
                return reply.setJoined(true).setMessage("already live").build();
            }
            live.remove(node);
            return reply.setJoined(false).setMessage(request.getLastSeq() < last
                    ? "behind: catch up from seq " + (request.getLastSeq() + 1)
                    : "ahead of the primary's seq " + last + ": resync from scratch").build();
        }
    }

    /** True if the joiner's last change is the one this log holds at {@code seq}. */
    private boolean sameChangeAt(long seq, JoinRequest request) {
        if (seq == 0) {
            return true;
        }
        return local.log().entry(seq)
                .map(entry -> entry.record().lamportTime() == request.getLastLamport()
                        && entry.record().originNode().equals(request.getLastOrigin()))
                .orElse(false);
    }

    /**
     * This node has just been elected: clear the live set (backups must join it) and refuse
     * joins until {@link #serve()}.
     */
    public void becomePrimary() {
        serving = false;
        live.clear();
        failBuffered("this node is now the primary");
    }

    /** The log is complete (promotion step 1 done): backups may catch up from it and join. */
    public void serve() {
        serving = true;
    }

    /** This node lost primacy: stop forwarding, so a stale write reaches no backup. */
    public void standDown() {
        serving = false;
        live.clear();
    }

    public Set<Integer> liveBackups() {
        return Set.copyOf(live);
    }

    public long lastSeq() {
        return local.log().lastSeq();
    }

    private boolean push(int peer, ReplicateRequest request) {
        try {
            long delay = replicas.delay(peer);
            if (delay > 0) {
                Thread.sleep(delay);
            }
            return replicas.stub(peer).replicate(request).getOk();
        } catch (StatusRuntimeException e) {
            log.debug("Backup {} did not take seq {}: {}", peer, request.getSeqNo(),
                    e.getStatus().getCode());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void drop(int peer, long seq) {
        if (live.remove(peer)) {
            log.warn("Backup {} did not ack seq {} within {} ms: dropped from the live set;"
                    + " it rejoins after catching up with SyncFrom", peer, seq, ackTimeoutMs);
            EventLog.get().event("BACKUP_DROPPED", "", Map.of(
                    "backup", String.valueOf(peer), "seq", String.valueOf(seq)));
        }
    }

    // ---- backup side --------------------------------------------------------------------

    /**
     * A change forwarded by the primary. Unsequenced changes (seq 0, from a strong or eventual
     * peer) are left to the store's last-writer-wins path.
     */
    @Override
    public CompletableFuture<Ack> receive(ReplicateRequest request) {
        long seq = request.getSeqNo();
        if (seq <= 0) {
            return null;
        }
        VersionedRecord record = VersionedRecord.from(request);
        synchronized (sequence) {
            long last = local.log().lastSeq();
            if (seq <= last) {
                return CompletableFuture.completedFuture(ack(true, "already have seq " + seq));
            }
            if (seq > last + 1) {
                CompletableFuture<Ack> ack = new CompletableFuture<>();
                Buffered earlier = buffered.put(seq, new Buffered(record, ack));
                if (earlier != null) {
                    earlier.ack().complete(ack(false, "seq " + seq + " was sent again"));
                }
                log.debug("Seq {} arrived before seq {}: buffered ({} waiting)",
                        seq, last + 1, buffered.size());
                // If the gap never fills, the primary has dropped this node; answer so the
                // call does not hang, and the rejoin loop catches up.
                return ack.completeOnTimeout(
                        ack(false, "seq " + (last + 1) + " never arrived"),
                        ackTimeoutMs, TimeUnit.MILLISECONDS);
            }
            local.applySequenced(seq, record);
            drainBuffered();
            return CompletableFuture.completedFuture(ack(true, "applied seq " + seq));
        }
    }

    /**
     * Pulls what {@code peer} has beyond this node's log and applies it in order. It first asks
     * for the entry at this node's own last seq and checks it is the same change: if not, the
     * two logs forked (this node kept writes a demoted primary made, say). With
     * {@code resetOnFork} this node then drops its log and copies the peer's from seq 1 (the
     * primary is always right, and a log longer than the primary's is a fork too); without it
     * (a new primary pulling from a backup) a forked peer is skipped and a shorter one has
     * nothing to give.
     *
     * @return changes applied, or -1 if the peer was unreachable or forked and skipped
     */
    public long catchUpFrom(int peer, boolean resetOnFork) {
        long last = lastSeq();
        long applied = 0;
        try {
            Iterator<ReplicateRequest> changes = pull(peer, Math.max(1, last));
            ReplicateRequest overlap = last > 0 && changes.hasNext() ? changes.next() : null;
            if (last > 0 && overlap == null && !resetOnFork) {
                return 0;   // a shorter log than this node's: nothing to pull from it
            }
            if (last > 0 && !sameAsMine(overlap, last)) {
                if (!resetOnFork) {
                    log.warn("Node {}'s log forks from this node's at seq {}; not pulling from it",
                            peer, last);
                    return -1;
                }
                log.warn("This node's log forks from node {}'s at seq {} (it kept writes the"
                        + " cluster never acknowledged): dropping it and copying node {}'s",
                        peer, last, peer);
                synchronized (sequence) {
                    local.reset();
                    failBuffered("resync from scratch");
                }
                changes = pull(peer, 1);
            }
            while (changes.hasNext()) {
                ReplicateRequest change = changes.next();
                if (applyInOrder(change.getSeqNo(), VersionedRecord.from(change))) {
                    applied++;
                }
            }
        } catch (StatusRuntimeException e) {
            log.debug("Catch-up from node {} failed: {}", peer, e.getStatus().getCode());
            return -1;
        }
        if (applied > 0) {
            log.info("Caught up from node {}: applied {} changes, seq {} -> {}",
                    peer, applied, last, lastSeq());
        }
        return applied;
    }

    /**
     * One step of a backup's rejoin loop: ask to join; if the primary is serving but this node is
     * not in step with it, catch up from it and ask again. True once this node is live.
     */
    public boolean syncWithPrimary(int primary) {
        Optional<JoinResponse> first = askToJoin(primary);
        if (first.isEmpty() || first.get().getJoined() || !first.get().getReady()) {
            return first.map(JoinResponse::getJoined).orElse(false);
        }
        if (catchUpFrom(primary, true) < 0) {
            return false;
        }
        Optional<JoinResponse> second = askToJoin(primary);
        second.filter(reply -> !reply.getJoined()).ifPresent(reply ->
                log.debug("Primary {} did not take this node yet: {}", primary, reply.getMessage()));
        return second.map(JoinResponse::getJoined).orElse(false);
    }

    private Optional<JoinResponse> askToJoin(int primary) {
        JoinRequest.Builder request = JoinRequest.newBuilder().setNodeId(replicas.selfId());
        synchronized (sequence) {
            long last = lastSeq();
            request.setLastSeq(last);
            local.log().entry(last).ifPresent(entry -> request
                    .setLastLamport(entry.record().lamportTime())
                    .setLastOrigin(entry.record().originNode()));
        }
        try {
            return Optional.of(replicas.stub(primary).join(request.build()));
        } catch (StatusRuntimeException e) {
            log.debug("Join with primary {} failed: {}", primary, e.getStatus().getCode());
            return Optional.empty();
        }
    }

    private Iterator<ReplicateRequest> pull(int peer, long fromSeq) {
        return replicas.stub(peer, ackTimeoutMs * SYNC_DEADLINE_FACTOR)
                .syncFrom(SyncRequest.newBuilder().setFromSeq(fromSeq).build());
    }

    /** True if {@code theirs} is the change this node holds at {@code seq}. */
    private boolean sameAsMine(ReplicateRequest theirs, long seq) {
        if (theirs == null || theirs.getSeqNo() != seq) {
            return false;
        }
        Optional<ReplicationLog.Entry> mine = local.log().entry(seq);
        if (mine.isEmpty()) {
            return false;
        }
        VersionedRecord record = mine.get().record();
        return record.taskId().equals(theirs.getTaskId())
                && record.lamportTime() == theirs.getLamportTime()
                && record.originNode().equals(theirs.getOriginNode());
    }

    /** Applies a caught-up change if it is the next one; true if it was applied. */
    private boolean applyInOrder(long seq, VersionedRecord record) {
        synchronized (sequence) {
            if (seq != local.log().lastSeq() + 1) {
                return false;
            }
            local.applySequenced(seq, record);
            drainBuffered();
            return true;
        }
    }

    /** Applies buffered changes that are now next in line. Holds {@code sequence}. */
    private void drainBuffered() {
        while (!buffered.isEmpty()) {
            long next = local.log().lastSeq() + 1;
            Map.Entry<Long, Buffered> first = buffered.firstEntry();
            if (first.getKey() > next) {
                return;
            }
            buffered.pollFirstEntry();
            if (first.getKey() < next) {
                first.getValue().ack().complete(ack(true, "already have seq " + first.getKey()));
                continue;
            }
            local.applySequenced(next, first.getValue().record());
            first.getValue().ack().complete(ack(true, "applied seq " + next + " after buffering"));
        }
    }

    private void failBuffered(String why) {
        synchronized (sequence) {
            buffered.values().forEach(entry -> entry.ack().complete(ack(false, why)));
            buffered.clear();
        }
    }

    private static Ack ack(boolean ok, String message) {
        return Ack.newBuilder().setOk(ok).setMessage(message).build();
    }

    private static boolean await(Future<Boolean> reply, long deadline) {
        long remaining = deadline - System.currentTimeMillis();
        try {
            return remaining > 0 && reply.get(remaining, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException e) {
            reply.cancel(true);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {
        rpc.shutdownNow();
        failBuffered("closing");
    }
}
