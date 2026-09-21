package com.predisched.common.time;

import com.predisched.common.obs.LamportInterceptors;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.TimeRequest;
import com.predisched.proto.TimeResponse;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cristian's algorithm (lab Exp 3, the alternative to Berkeley): ask a time server for its time and
 * set the local clock to {@code server_time + RTT / 2}, assuming the request and the reply took the
 * same time.
 *
 * <p>Unlike Berkeley this is one-sided: the server's time wins, and the server never moves.
 */
public class CristianClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CristianClient.class);

    private final ClockServiceGrpc.ClockServiceBlockingStub server;
    private final PhysicalClock clock;
    private final String nodeId;
    private final long intervalMs;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService scheduler;

    public CristianClient(
            ClockServiceGrpc.ClockServiceBlockingStub server,
            PhysicalClock clock,
            String nodeId,
            long intervalMs) {
        this(server, clock, nodeId, intervalMs, System::nanoTime);
    }

    /** Test seam: an injected nano clock makes the round-trip time deterministic. */
    public CristianClient(
            ClockServiceGrpc.ClockServiceBlockingStub server,
            PhysicalClock clock,
            String nodeId,
            long intervalMs,
            LongSupplier nanoTime) {
        this.server = server;
        this.clock = clock;
        this.nodeId = nodeId;
        this.intervalMs = intervalMs;
        this.nanoTime = nanoTime;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, nodeId + "-cristian");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        scheduler.scheduleAtFixedRate(
                this::syncOnce, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** One round. Returns the correction applied in ms, or 0 if the server could not be reached. */
    public long syncOnce() {
        LamportInterceptors.applyMdc();
        try {
            long startNanos = nanoTime.getAsLong();
            TimeResponse response = server.getTime(TimeRequest.newBuilder()
                    .setRequesterTimeMs(clock.now())
                    .build());
            long rttMs = (nanoTime.getAsLong() - startNanos) / 1_000_000L;
            long estimatedServerNow = response.getNodeTimeMs() + rttMs / 2;
            long before = clock.now();
            long correction = estimatedServerNow - before;
            clock.adjust(correction);
            log.info("Cristian sync with {}: rtt={} ms, offset was {} ms, corrected to {} ms",
                    response.getNodeId(), rttMs, correction, clock.now() - estimatedServerNow);
            return correction;
        } catch (Exception e) {
            log.debug("Cristian sync for {} failed: {}", nodeId, e.getMessage());
            return 0L;
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
