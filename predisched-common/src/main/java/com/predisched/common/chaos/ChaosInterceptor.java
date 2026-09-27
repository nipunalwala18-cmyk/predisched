package com.predisched.common.chaos;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Applies a node's chaos state to incoming calls (prompt 19): injected latency delays every call
 * before it is handled, and isolation refuses replication calls with {@code UNAVAILABLE}.
 * ChaosService calls themselves are never delayed or refused, so a fault can always be changed.
 */
public final class ChaosInterceptor implements ServerInterceptor {

    static final String CHAOS_SERVICE = "predisched.ChaosService";
    static final String REPLICATION_SERVICE = "predisched.ReplicationService";

    private final ChaosController chaos;

    public ChaosInterceptor(ChaosController chaos) {
        this.chaos = chaos;
    }

    @Override
    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
            ServerCallHandler<Q, R> next) {
        String service = call.getMethodDescriptor().getServiceName();
        if (CHAOS_SERVICE.equals(service)) {
            return next.startCall(call, headers);
        }
        if (REPLICATION_SERVICE.equals(service) && chaos.isIsolated()) {
            call.close(Status.UNAVAILABLE.withDescription(
                    "isolated by chaos: replication traffic refused"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        long delay = chaos.currentLatencyMs();
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return next.startCall(call, headers);
    }
}
