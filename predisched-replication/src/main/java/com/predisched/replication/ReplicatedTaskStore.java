package com.predisched.replication;

import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.proto.ReadRequest;
import com.predisched.proto.ReplicateRequest;
import com.predisched.proto.SyncRequest;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

/**
 * The scheduler's {@link TaskStore}, replicated across the cluster (FR10, Exp 5). Every change
 * becomes a {@link VersionedRecord} stamped with this node's Lamport time and handed to the
 * {@link ConsistencyMode}, which decides when it counts as written and what a read returns.
 *
 * <p>Read-modify-write ({@link #update}) holds a lock striped by task id, so two updates of one
 * task on this node cannot interleave; different tasks proceed in parallel. {@link #list} and
 * {@link #contains} look at the local replica only.
 */
public class ReplicatedTaskStore implements TaskStore {

    private final LocalReplica local;
    private final ReplicaSet replicas;
    private final ConsistencyMode mode;
    private final Object[] locks = new Object[64];

    public ReplicatedTaskStore(LocalReplica local, ReplicaSet replicas, ConsistencyMode mode) {
        this.local = local;
        this.replicas = replicas;
        this.mode = mode;
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
    }

    public LocalReplica local() {
        return local;
    }

    public ReplicaSet replicas() {
        return replicas;
    }

    public ConsistencyMode mode() {
        return mode;
    }

    @Override
    public void put(TaskRecord record) {
        synchronized (lockFor(record.id())) {
            if (local.contains(record.id())) {
                throw new IllegalStateException("Task already exists: " + record.id());
            }
            mode.write(stamp(record, 1, VersionedRecord.ENQUEUE));
        }
    }

    @Override
    public TaskRecord get(String taskId) {
        VersionedRecord copy = mode.read(taskId);
        return copy == null ? null : copy.task();
    }

    /**
     * A read that honours the session: if the copy this node's mode returns is older than what
     * the session has already seen, a peer that has the newer version answers instead.
     */
    public TaskRecord get(String taskId, SessionToken session) {
        VersionedRecord copy = mode.read(taskId);
        long seen = session.lastSeen(taskId);
        if ((copy == null ? 0 : copy.version()) < seen) {
            copy = freshestPeerCopy(taskId, seen);
        }
        if (copy == null) {
            return null;
        }
        session.saw(taskId, copy.version());
        return copy.task();
    }

    @Override
    public TaskRecord update(String taskId, UnaryOperator<TaskRecord> fn) {
        synchronized (lockFor(taskId)) {
            VersionedRecord current = mode.read(taskId);
            if (current == null) {
                throw new NoSuchElementException("Unknown task: " + taskId);
            }
            TaskRecord next = fn.apply(current.task());
            if (next == null) {
                throw new IllegalStateException("Update function returned null for task: " + taskId);
            }
            mode.write(stamp(next, current.version() + 1, VersionedRecord.UPDATE_STATUS));
            return next;
        }
    }

    /** Update that also records the new version in the session (read-your-writes). */
    public TaskRecord update(String taskId, UnaryOperator<TaskRecord> fn, SessionToken session) {
        TaskRecord next = update(taskId, fn);
        session.saw(taskId, local.get(taskId).version());
        return next;
    }

    @Override
    public void replace(TaskRecord record) {
        synchronized (lockFor(record.id())) {
            VersionedRecord current = mode.read(record.id());
            long version = current == null ? 1 : current.version() + 1;
            mode.write(stamp(record, version, VersionedRecord.UPDATE_STATUS));
        }
    }

    @Override
    public List<TaskRecord> list() {
        return local.all().stream().map(VersionedRecord::task).toList();
    }

    @Override
    public boolean contains(String taskId) {
        return local.contains(taskId);
    }

    /**
     * Catch-up for a new or recovered replica: streams every change the peer logged from
     * {@code fromSeq} on and applies it here. Returns the peer's last sequence number received.
     */
    public long syncFrom(int peer, long fromSeq) {
        long last = fromSeq - 1;
        Iterator<ReplicateRequest> changes = replicas.stub(peer)
                .syncFrom(SyncRequest.newBuilder().setFromSeq(fromSeq).build());
        while (changes.hasNext()) {
            ReplicateRequest change = changes.next();
            local.apply(VersionedRecord.from(change));
            last = change.getSeqNo();
        }
        return last;
    }

    private VersionedRecord freshestPeerCopy(String taskId, long atLeast) {
        VersionedRecord best = null;
        for (int peer : replicas.peers()) {
            try {
                VersionedRecord copy = VersionedRecord.from(replicas.stub(peer).read(
                        ReadRequest.newBuilder().setTaskId(taskId).setLocalOnly(true).build()));
                if (copy != null && copy.isNewerThan(best)) {
                    best = copy;
                }
            } catch (StatusRuntimeException e) {
                // Try the next replica.
            }
        }
        if (best == null || best.version() < atLeast) {
            throw new ReplicationException("no reachable replica has " + taskId + " at version "
                    + atLeast + " or later");
        }
        local.apply(best);
        return best;
    }

    private VersionedRecord stamp(TaskRecord record, long version, String op) {
        return new VersionedRecord(record.id(), TaskCodec.encode(record), version,
                local.clock().tick(), local.nodeName(), op);
    }

    private Object lockFor(String taskId) {
        return locks[Math.floorMod(taskId.hashCode(), locks.length)];
    }
}
