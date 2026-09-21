package com.predisched.scheduler;

import com.predisched.proto.Ack;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.RegistryServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serves worker registration and heartbeats on the scheduler (FR6, FR7). */
public class RegistryServiceImpl extends RegistryServiceGrpc.RegistryServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RegistryServiceImpl.class);

    private final WorkerRegistry registry;

    public RegistryServiceImpl(WorkerRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void register(RegisterRequest request, StreamObserver<Ack> observer) {
        WorkerInfo worker = registry.register(request);
        log.info("Registered worker {} at {} ({} cores, pool {}, {} MB)",
                worker.id(), worker.address(), worker.cores(), worker.poolSize(),
                worker.memoryMb());
        observer.onNext(Ack.newBuilder()
                .setOk(true)
                .setMessage("registered " + worker.id())
                .build());
        observer.onCompleted();
    }

    @Override
    public void sendHeartbeat(Heartbeat request, StreamObserver<Ack> observer) {
        boolean known = registry.heartbeat(request).isPresent();
        observer.onNext(Ack.newBuilder()
                .setOk(known)
                .setMessage(known ? "ok" : "unknown worker " + request.getWorkerId())
                .build());
        observer.onCompleted();
    }
}
