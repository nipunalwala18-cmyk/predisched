package com.predisched.scheduler;

import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * The scheduler's live view of its workers, updated by {@code Register} and {@code SendHeartbeat}
 * (FR6, FR7). Backed by a {@link ConcurrentHashMap} of immutable {@link WorkerInfo} records.
 */
public class WorkerRegistry {

    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
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
        return workers.compute(request.getWorkerId(), (id, existing) ->
                WorkerInfo.fromRegistration(request, now));
    }

    /** Returns empty when the worker is unknown, which tells it to register again. */
    public Optional<WorkerInfo> heartbeat(Heartbeat heartbeat) {
        long now = clock.getAsLong();
        WorkerInfo updated = workers.computeIfPresent(heartbeat.getWorkerId(),
                (id, existing) -> existing.withHeartbeat(heartbeat, now));
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

    /** Workers that have sent a heartbeat recently enough to be dispatched to, ordered by id. */
    public List<WorkerInfo> healthy() {
        long now = clock.getAsLong();
        return workers.values().stream()
                .filter(worker -> worker.isHealthy(now, staleAfterMs))
                .sorted(Comparator.comparing(WorkerInfo::id))
                .collect(Collectors.toList());
    }

    public int size() {
        return workers.size();
    }

    public void remove(String workerId) {
        workers.remove(workerId);
    }
}
