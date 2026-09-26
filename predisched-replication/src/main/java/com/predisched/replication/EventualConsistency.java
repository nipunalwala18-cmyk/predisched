package com.predisched.replication;

import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Eventual consistency: a write is applied here and acknowledged at once, then sent to each peer
 * after that peer's injected delay. Each peer has its own single-threaded sender, so changes reach
 * it in the order they were made. Reads are local and may be stale until the changes arrive;
 * conflicting writes converge by last-writer-wins ({@link VersionedRecord#isNewerThan}).
 *
 * <p>A change that cannot be delivered (the peer is down) is dropped and counted; a replica that
 * missed changes catches up with {@code SyncFrom}.
 */
public class EventualConsistency implements ConsistencyMode {

    private static final Logger log = LoggerFactory.getLogger(EventualConsistency.class);

    private final LocalReplica local;
    private final ReplicaSet replicas;
    private final Map<Integer, ScheduledExecutorService> senders = new ConcurrentHashMap<>();
    private final AtomicLong undelivered = new AtomicLong();

    public EventualConsistency(LocalReplica local, ReplicaSet replicas) {
        this.local = local;
        this.replicas = replicas;
    }

    @Override
    public String name() {
        return "eventual";
    }

    @Override
    public void write(VersionedRecord record) {
        local.apply(record);
        for (int peer : replicas.peers()) {
            sender(peer).schedule(() -> push(peer, record),
                    replicas.delay(peer), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public VersionedRecord read(String taskId) {
        return local.get(taskId);
    }

    /** Changes that could not be delivered to a peer since start. */
    public long undelivered() {
        return undelivered.get();
    }

    private void push(int peer, VersionedRecord record) {
        try {
            replicas.stub(peer).replicate(record.toRequest(0));
        } catch (StatusRuntimeException e) {
            undelivered.incrementAndGet();
            log.debug("Replica {} missed {} v{}: {}", peer, record.taskId(), record.version(),
                    e.getStatus().getCode());
        }
    }

    private ScheduledExecutorService sender(int peer) {
        return senders.computeIfAbsent(peer, id -> Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable,
                            "replicate-" + local.nodeName() + "-to-" + id);
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    @Override
    public void close() {
        senders.values().forEach(ScheduledExecutorService::shutdownNow);
    }
}
