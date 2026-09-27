package com.predisched.common.chaos;

import com.predisched.proto.ChaosReply;
import com.predisched.proto.ChaosServiceGrpc;
import com.predisched.proto.CpuRequest;
import com.predisched.proto.CrashRequest;
import com.predisched.proto.DrainRequest;
import com.predisched.proto.IsolateRequest;
import com.predisched.proto.LatencyRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** The gRPC face of a node's {@link ChaosController} (prompt 19, F16). */
public final class ChaosServiceImpl extends ChaosServiceGrpc.ChaosServiceImplBase {

    /** Durations are capped, so a typo cannot leave a node broken for a day. */
    static final long MAX_DURATION_S = 3_600;
    static final long MAX_LATENCY_MS = 60_000;

    private final ChaosController chaos;

    public ChaosServiceImpl(ChaosController chaos) {
        this.chaos = chaos;
    }

    @Override
    public void injectLatency(LatencyRequest request, StreamObserver<ChaosReply> observer) {
        if (!valid(request.getDurationS(), observer)) {
            return;
        }
        if (request.getMs() < 0 || request.getMs() > MAX_LATENCY_MS) {
            invalid(observer, "ms must be 0.." + MAX_LATENCY_MS);
            return;
        }
        long until = chaos.injectLatency(request.getMs(), request.getDurationS());
        reply(observer, true, "+" + request.getMs() + " ms on every call for "
                + request.getDurationS() + " s", until);
    }

    @Override
    public void spikeCpu(CpuRequest request, StreamObserver<ChaosReply> observer) {
        if (!valid(request.getDurationS(), observer)) {
            return;
        }
        if (request.getThreads() < 0 || request.getThreads() > 256) {
            invalid(observer, "threads must be 0..256 (0 = one per core)");
            return;
        }
        long until = chaos.spikeCpu(request.getThreads(), request.getDurationS());
        reply(observer, true, "CPU spike for " + request.getDurationS() + " s", until);
    }

    @Override
    public void isolate(IsolateRequest request, StreamObserver<ChaosReply> observer) {
        if (!valid(request.getDurationS(), observer)) {
            return;
        }
        long until = chaos.isolate(request.getDurationS());
        reply(observer, true, "replication traffic refused for " + request.getDurationS() + " s",
                until);
    }

    @Override
    public void drain(DrainRequest request, StreamObserver<ChaosReply> observer) {
        boolean drained = chaos.drain();
        reply(observer, drained, drained
                ? "draining: running tasks finish, new ones are refused"
                : "nothing to drain on " + chaos.nodeId(), 0);
    }

    @Override
    public void crash(CrashRequest request, StreamObserver<ChaosReply> observer) {
        reply(observer, true, "crashing", 0);
        chaos.crash(request.getReason());
    }

    private boolean valid(long durationS, StreamObserver<ChaosReply> observer) {
        if (durationS <= 0 || durationS > MAX_DURATION_S) {
            invalid(observer, "duration_s must be 1.." + MAX_DURATION_S);
            return false;
        }
        return true;
    }

    private static void invalid(StreamObserver<ChaosReply> observer, String message) {
        observer.onError(Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException());
    }

    private void reply(StreamObserver<ChaosReply> observer, boolean ok, String message,
            long until) {
        observer.onNext(ChaosReply.newBuilder().setOk(ok).setNodeId(chaos.nodeId())
                .setMessage(message).setUntilMs(until).build());
        observer.onCompleted();
    }
}
