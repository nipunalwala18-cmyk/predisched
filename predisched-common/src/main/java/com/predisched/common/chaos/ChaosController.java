package com.predisched.common.chaos;

import com.predisched.common.obs.EventLog;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The fault state of one node (prompt 19, F16): injected latency, a CPU spike, isolation from
 * replication traffic, draining, and a crash. {@link ChaosInterceptor} applies latency and
 * isolation to incoming calls; {@link ChaosServiceImpl} is how a client turns them on. Every
 * effect with a duration ends by itself: the state carries its end time, and nothing has to
 * remember to switch it off.
 *
 * <p>Every action writes a {@code CHAOS} event (the event log stamps it with the node's Lamport
 * time), so charts can mark where each fault began.
 */
public final class ChaosController {

    private static final Logger log = LoggerFactory.getLogger(ChaosController.class);

    public static final String EVENT = "CHAOS";

    private final String nodeId;
    private final LongSupplier clock;
    private final Runnable drain;
    private final IntConsumer exit;
    private volatile long latencyMs;
    private volatile long latencyUntilMs;
    private volatile long isolatedUntilMs;
    private volatile long cpuUntilMs;
    private final List<Thread> cpuThreads = new ArrayList<>();

    /**
     * @param drain what "drain" means on this node (a worker stops accepting tasks); null where
     *      it means nothing (a scheduler)
     */
    public ChaosController(String nodeId, Runnable drain) {
        this(nodeId, drain, System::currentTimeMillis, code -> Runtime.getRuntime().halt(code));
    }

    public ChaosController(String nodeId, Runnable drain, LongSupplier clock, IntConsumer exit) {
        this.nodeId = nodeId;
        this.drain = drain;
        this.clock = clock;
        this.exit = exit;
    }

    public String nodeId() {
        return nodeId;
    }

    /** Adds {@code ms} to every incoming call for {@code durationS} seconds. */
    public long injectLatency(long ms, long durationS) {
        long until = clock.getAsLong() + TimeUnit.SECONDS.toMillis(durationS);
        latencyMs = Math.max(0, ms);
        latencyUntilMs = until;
        record("latency", Map.of("ms", String.valueOf(ms), "duration_s",
                String.valueOf(durationS)));
        return until;
    }

    /** The latency to add to a call arriving now; 0 when none is active. */
    public long currentLatencyMs() {
        return clock.getAsLong() < latencyUntilMs ? latencyMs : 0;
    }

    /** Refuses replication traffic for {@code durationS} seconds. */
    public long isolate(long durationS) {
        long until = clock.getAsLong() + TimeUnit.SECONDS.toMillis(durationS);
        isolatedUntilMs = until;
        record("isolate", Map.of("duration_s", String.valueOf(durationS)));
        return until;
    }

    public boolean isIsolated() {
        return clock.getAsLong() < isolatedUntilMs;
    }

    /**
     * Fails this node's own outgoing replication calls while it is isolated, so the partition
     * cuts both ways: it neither receives the primary's pushes nor pulls a catch-up. Install with
     * {@code Transport.addOutgoing}.
     */
    public io.grpc.ClientInterceptor outgoingInterceptor() {
        return new io.grpc.ClientInterceptor() {
            @Override
            public <Q, R> io.grpc.ClientCall<Q, R> interceptCall(
                    io.grpc.MethodDescriptor<Q, R> method, io.grpc.CallOptions options,
                    io.grpc.Channel next) {
                if (!ChaosInterceptor.REPLICATION_SERVICE.equals(method.getServiceName())
                        || !isIsolated()) {
                    return next.newCall(method, options);
                }
                return new io.grpc.ClientCall<>() {
                    @Override
                    public void start(Listener<R> listener, io.grpc.Metadata headers) {
                        listener.onClose(io.grpc.Status.UNAVAILABLE.withDescription(
                                "isolated by chaos: outgoing replication refused"),
                                new io.grpc.Metadata());
                    }

                    @Override
                    public void request(int numMessages) {}

                    @Override
                    public void cancel(String message, Throwable cause) {}

                    @Override
                    public void halfClose() {}

                    @Override
                    public void sendMessage(Q message) {}
                };
            }
        };
    }

    /** Busy-loops {@code threads} threads (0: one per core) for {@code durationS} seconds. */
    public synchronized long spikeCpu(int threads, long durationS) {
        int n = threads > 0 ? threads : Runtime.getRuntime().availableProcessors();
        long until = clock.getAsLong() + TimeUnit.SECONDS.toMillis(durationS);
        cpuUntilMs = until;
        cpuThreads.removeIf(t -> !t.isAlive());
        for (int i = 0; i < n; i++) {
            Thread t = new Thread(() -> {
                long x = 0;
                while (clock.getAsLong() < cpuUntilMs && !Thread.currentThread().isInterrupted()) {
                    x += Long.numberOfTrailingZeros(x + 1); // work the JIT cannot drop
                }
                if (x == 42) {
                    log.trace("unreachable");
                }
            }, "chaos-cpu-" + i);
            t.setDaemon(true);
            t.start();
            cpuThreads.add(t);
        }
        record("cpu", Map.of("threads", String.valueOf(n), "duration_s",
                String.valueOf(durationS)));
        return until;
    }

    /** Spike threads still running. */
    public synchronized int cpuThreadsRunning() {
        cpuThreads.removeIf(t -> !t.isAlive());
        return cpuThreads.size();
    }

    /** False when this node has nothing to drain. */
    public boolean drain() {
        if (drain == null) {
            return false;
        }
        record("drain", Map.of());
        drain.run();
        return true;
    }

    /** Exits the process shortly after this returns, so the caller still gets its reply. */
    public void crash(String reason) {
        record("crash", Map.of("reason", reason == null ? "" : reason));
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignored) {
                // exit anyway
            }
            log.error("CHAOS: {} crashing on request ({})", nodeId, reason);
            exit.accept(137);
        }, "chaos-crash");
        t.setDaemon(false);
        t.start();
    }

    private void record(String action, Map<String, String> details) {
        Map<String, String> all = new LinkedHashMap<>();
        all.put("action", action);
        all.put("node", nodeId);
        all.putAll(details);
        EventLog.get().event(EVENT, "", all);
        log.warn("CHAOS {} on {} {}", action, nodeId, details);
    }
}
