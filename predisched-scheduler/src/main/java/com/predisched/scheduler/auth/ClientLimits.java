package com.predisched.scheduler.auth;

import com.predisched.common.auth.AuthInterceptor;
import com.predisched.common.auth.ClientDirectory;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-client limits once a caller is authenticated (F9): a request rate enforced as a gRPC
 * interceptor ({@code RESOURCE_EXHAUSTED} when over), and a quota of unfinished tasks the
 * scheduler checks on submit. Nodes are exempt from both. Counters are atomic per client.
 */
public class ClientLimits implements ServerInterceptor {

    private final ClientDirectory directory;
    private final RateLimiter rates;
    private final Map<String, AtomicInteger> unfinished = new ConcurrentHashMap<>();

    public ClientLimits(ClientDirectory directory, RateLimiter rates) {
        this.directory = directory;
        this.rates = rates;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        Optional<AuthInterceptor.Principal> caller = AuthInterceptor.current();
        if (caller.isPresent() && !caller.get().node()) {
            Optional<ClientDirectory.Client> client = directory.byId(caller.get().clientId());
            if (client.isPresent() && !rates.tryAcquire(client.get().id(),
                    client.get().ratePerSecond(), client.get().burst())) {
                call.close(Status.RESOURCE_EXHAUSTED.withDescription(String.format(
                        "rate limit for %s: %.0f requests/s, burst %d", client.get().id(),
                        client.get().ratePerSecond(), client.get().burst())), new Metadata());
                return new ServerCall.Listener<>() {};
            }
        }
        return next.startCall(call, headers);
    }

    /**
     * Reserves {@code count} task slots for a client; returns null on success or the reason it
     * is over quota. An unknown or empty client id is not limited.
     */
    public String reserve(String clientId, int count) {
        Optional<ClientDirectory.Client> client = directory.byId(clientId);
        if (clientId.isEmpty() || client.isEmpty()) {
            return null;
        }
        int max = client.get().maxQueued();
        AtomicInteger used = unfinished.computeIfAbsent(clientId, id -> new AtomicInteger());
        while (true) {
            int now = used.get();
            if (now + count > max) {
                return "quota for " + clientId + ": at most " + max + " unfinished tasks ("
                        + now + " now, " + count + " requested)";
            }
            if (used.compareAndSet(now, now + count)) {
                return null;
            }
        }
    }

    /** Gives back one slot when a client's task finishes, or a reservation that was not used. */
    public void release(String clientId, int count) {
        AtomicInteger used = unfinished.get(clientId);
        if (used != null) {
            used.updateAndGet(value -> Math.max(0, value - count));
        }
    }

    public int unfinished(String clientId) {
        AtomicInteger used = unfinished.get(clientId);
        return used == null ? 0 : used.get();
    }
}
