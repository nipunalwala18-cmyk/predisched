package com.predisched.replication;

import com.predisched.common.time.LamportClock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This node's own copy of the replicated task state, plus the log of every change it applied.
 * {@link #apply} keeps a copy only if it is newer by the last-writer-wins rule, atomically per
 * task ({@code ConcurrentHashMap.compute}); an applied change is logged inside the same compute,
 * so the log order matches the order changes took effect.
 */
public class LocalReplica {

    private final String nodeName;
    private final LamportClock clock;
    private final ConcurrentHashMap<String, VersionedRecord> records = new ConcurrentHashMap<>();
    private final ReplicationLog log = new ReplicationLog();

    public LocalReplica(String nodeName, LamportClock clock) {
        this.nodeName = nodeName;
        this.clock = clock;
    }

    public String nodeName() {
        return nodeName;
    }

    public LamportClock clock() {
        return clock;
    }

    /**
     * Applies a copy if it is newer than the one held. Receiving a write merges its Lamport time,
     * so any later write made here is ordered after it. Returns true if it was applied.
     */
    public boolean apply(VersionedRecord incoming) {
        clock.update(incoming.lamportTime());
        boolean[] applied = {false};
        records.compute(incoming.taskId(), (id, held) -> {
            if (!incoming.isNewerThan(held)) {
                return held;
            }
            log.append(incoming);
            applied[0] = true;
            return incoming;
        });
        return applied[0];
    }

    public VersionedRecord get(String taskId) {
        return records.get(taskId);
    }

    public boolean contains(String taskId) {
        return records.containsKey(taskId);
    }

    public List<VersionedRecord> all() {
        return new ArrayList<>(records.values());
    }

    public ReplicationLog log() {
        return log;
    }
}
