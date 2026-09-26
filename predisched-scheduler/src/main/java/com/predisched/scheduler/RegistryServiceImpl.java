package com.predisched.scheduler;

import com.predisched.common.db.History;
import com.predisched.proto.Ack;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.RegistryServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serves worker registration and heartbeats on the scheduler (FR6, FR7). */
public class RegistryServiceImpl extends RegistryServiceGrpc.RegistryServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RegistryServiceImpl.class);

    private final WorkerRegistry registry;
    private final BooleanSupplier primary;

    public RegistryServiceImpl(WorkerRegistry registry) {
        this(registry, () -> true);
    }

    /**
     * @param primary whether this node records worker state and metrics in the database (prompt
     *     11): workers heartbeat every scheduler, and only the primary's copy is kept
     */
    public RegistryServiceImpl(WorkerRegistry registry, BooleanSupplier primary) {
        this.registry = registry;
        this.primary = primary;
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<Ack> observer) {
        WorkerInfo worker = registry.register(request);
        log.info("Registered worker {} at {} ({} cores, pool {}, {} MB)",
                worker.id(), worker.address(), worker.cores(), worker.poolSize(),
                worker.memoryMb());
        if (primary.getAsBoolean()) {
            record(worker, "UP");
            History.get().recovered(worker.id(), worker.registeredAtMs());
        }
        observer.onNext(Ack.newBuilder()
                .setOk(true)
                .setMessage("registered " + worker.id())
                .build());
        observer.onCompleted();
    }

    private static void record(WorkerInfo worker, String status) {
        History.get().worker(worker.id(), worker.host(), worker.port(), worker.cores(),
                worker.memoryMb(), worker.poolSize(), status, worker.lastHeartbeatMs());
    }

    @Override
    public void sendHeartbeat(Heartbeat request, StreamObserver<Ack> observer) {
        java.util.Optional<WorkerInfo> updated = registry.heartbeat(request);
        boolean known = updated.isPresent();
        if (known && primary.getAsBoolean()) {
            WorkerInfo worker = updated.get();
            History.get().workerMetrics(worker.id(), worker.lastHeartbeatMs(), worker.cpuPct(),
                    worker.memPct(), worker.activeThreads(), worker.queueLen(),
                    worker.tasksCompleted(), worker.avgExecMs());
            record(worker, "UP");
        }
        observer.onNext(Ack.newBuilder()
                .setOk(known)
                .setMessage(known ? "ok" : "unknown worker " + request.getWorkerId())
                .build());
        observer.onCompleted();
    }
}
