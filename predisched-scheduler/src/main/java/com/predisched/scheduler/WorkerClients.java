package com.predisched.scheduler;

import com.predisched.common.net.Transport;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.Clocks;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * One gRPC channel per worker, created on first use and reused afterwards. Channels are expensive
 * to build and safe to share across threads, so the dispatch pool goes through this cache.
 */
public class WorkerClients implements WorkerStubs, AutoCloseable {

    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    @Override
    public WorkerServiceGrpc.WorkerServiceBlockingStub stubFor(WorkerInfo worker) {
        ManagedChannel channel = channels.computeIfAbsent(worker.address(), address ->
                Transport.get().channel(worker.host(), worker.port(),
                        LamportInterceptors.client(Clocks.lamport())));
        return WorkerServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() {
        channels.values().forEach(channel -> {
            channel.shutdown();
            try {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        channels.clear();
    }
}
