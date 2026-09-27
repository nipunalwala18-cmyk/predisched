package com.predisched.scheduler;

import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * The scheduler's live view of its workers, updated by {@code Register} and {@code SendHeartbeat}
 * (FR6, FR7). Backed by a {@link ConcurrentHashMap} of immutable {@link WorkerInfo} records.
 */
public class WorkerRegistry {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(WorkerRegistry.class);
    private final java.util.Set<String> draining = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    /** Workers whose last call failed as unreachable, until their next heartbeat (prompt 10). */
    private final Set<String> suspected = ConcurrentHashMap.newKeySet();
    private final long staleAfterMs;
    private final LongSupplier clock;

    public WorkerRegistry(long staleAfterMs) {
        this(staleAfterMs, System::currentTimeMillis);
    }

    /** Test seam: an injected clock keeps staleness tests free of sleeps. */
    public WorkerRegistry(long staleAfterMs, LongSupplier clock) {
        this.staleAfterMs = staleAfterMs;
        this.clock = clock;
    }

    public WorkerInfo register(RegisterRequest request) {
        long now = clock.getAsLong();
        suspected.remove(request.getWorkerId());
        draining.remove(request.getWorkerId());
        return workers.compute(request.getWorkerId(), (id, existing) ->
                WorkerInfo.fromRegistration(request, now));
    }

    /** Returns empty when the worker is unknown, which tells it to register again. */
    public Optional<WorkerInfo> heartbeat(Heartbeat heartbeat) {
        long now = clock.getAsLong();
        WorkerInfo updated = workers.computeIfPresent(heartbeat.getWorkerId(),
                (id, existing) -> existing.withHeartbeat(heartbeat, now));
        if (updated != null) {
            suspected.remove(heartbeat.getWorkerId());
            if (heartbeat.getDraining()) {
                if (draining.add(heartbeat.getWorkerId())) {
                    log.info("Worker {} is draining: no new dispatches to it",
                            heartbeat.getWorkerId());
                }
            } else {
                draining.remove(heartbeat.getWorkerId());
            }
        }
        return Optional.ofNullable(updated);
    }

    public Optional<WorkerInfo> get(String workerId) {
        return Optional.ofNullable(workers.get(workerId));
    }

    /** Every registered worker, healthy or not, ordered by id. */
    public List<WorkerInfo> all() {
        return workers.values().stream()
                .sorted(Comparator.comparing(WorkerInfo::id))
                .collect(Collectors.toList());
    }

    /**
     * Workers that have sent a heartbeat recently enough to be dispatched to, and are not
     * suspected, ordered by id.
     */
    public List<WorkerInfo> healthy() {
        long now = clock.getAsLong();
        return workers.values().stream()
                .filter(worker -> worker.isHealthy(now, staleAfterMs))
                .filter(worker -> !suspected.contains(worker.id()))
                .sorted(Comparator.comparing(WorkerInfo::id))
                .collect(Collectors.toList());
    }

    public int size() {
        return workers.size();
    }

    public void remove(String workerId) {
        workers.remove(workerId);
        suspected.remove(workerId);
        draining.remove(workerId);
    }

    /**
     * A call to this worker failed as unreachable: leave it out of dispatch until it heartbeats
     * again. Declaring it dead stays with the failure detector, after its missed heartbeats.
     */
    public void suspect(String workerId) {
        if (workers.containsKey(workerId)) {
            suspected.add(workerId);
        }
    }

    public boolean isSuspected(String workerId) {
        return suspected.contains(workerId);
    }

    /** The worker's last heartbeat said it is draining (prompt 19): it gets no new tasks. */
    public boolean isDraining(String workerId) {
        return draining.contains(workerId);
    }
}
