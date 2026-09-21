package com.predisched.common.time;

import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.proto.ClockAdjust;
import com.predisched.proto.TimeRequest;
import com.predisched.proto.TimeResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Berkeley clock synchronisation (lab Exp 3, FR8). The coordinator is the time daemon: it polls
 * every node, corrects each reading for the round trip, discards outliers, averages what is left,
 * and tells each node how far to move.
 *
 * <p>Unlike Cristian's algorithm no clock is treated as correct: the daemon moves its own clock too,
 * so the cluster converges on the average of the healthy clocks rather than on one node's time.
 */
public class BerkeleyDaemon implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyDaemon.class);

    /** What one round observed and did, for logs, tests and the demo. */
    public record Round(
            Map<String, Long> offsetsBeforeMs,
            Map<String, Long> correctionsMs,
            List<String> excluded,
            long averageOffsetMs) {

        public long spreadBeforeMs() {
            return spread(offsetsBeforeMs.values());
        }

        /** Where each node's clock lands if it applies its correction. */
        public Map<String, Long> offsetsAfterMs() {
            Map<String, Long> after = new LinkedHashMap<>();
            offsetsBeforeMs.forEach((node, before) ->
                    after.put(node, before + correctionsMs.getOrDefault(node, 0L)));
            return after;
        }

        public long spreadAfterMs() {
            return spread(offsetsAfterMs().values());
        }

        private static long spread(java.util.Collection<Long> values) {
            if (values.isEmpty()) {
                return 0L;
            }
            long min = values.stream().mapToLong(Long::longValue).min().orElse(0);
            long max = values.stream().mapToLong(Long::longValue).max().orElse(0);
            return max - min;
        }
    }

    private final String nodeId;
    private final PhysicalClock ownClock;
    private final Supplier<List<ClockPeer>> peers;
    private final long intervalMs;
    private final long outlierMs;
    private final LongSupplier nanoTime;
    private final ScheduledExecutorService scheduler;

    public BerkeleyDaemon(
            String nodeId,
            PhysicalClock ownClock,
            Supplier<List<ClockPeer>> peers,
            long intervalMs,
            long outlierMs) {
        this(nodeId, ownClock, peers, intervalMs, outlierMs, System::nanoTime);
    }

    /** Test seam: an injected nano clock makes round-trip corrections deterministic. */
    public BerkeleyDaemon(
            String nodeId,
            PhysicalClock ownClock,
            Supplier<List<ClockPeer>> peers,
            long intervalMs,
            long outlierMs,
            LongSupplier nanoTime) {
        this.nodeId = nodeId;
        this.ownClock = ownClock;
        this.peers = peers;
        this.intervalMs = intervalMs;
        this.outlierMs = outlierMs;
        this.nanoTime = nanoTime;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, nodeId + "-berkeley");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        if (intervalMs <= 0) {
            return;
        }
        scheduler.scheduleAtFixedRate(() -> {
            try {
                round();
            } catch (Exception e) {
                log.warn("Berkeley round failed: {}", e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /** One synchronisation round. Offsets are relative to the daemon's own clock. */
    public Round round() {
        LamportInterceptors.applyMdc();
        Map<String, Long> offsets = new LinkedHashMap<>();
        Map<String, ClockPeer> reachable = new LinkedHashMap<>();
        offsets.put(nodeId, 0L);

        for (ClockPeer peer : peers.get()) {
            try {
                // Each peer is compared against the coordinator's clock read around that call,
                // not once at the start of the round: the first call on a channel includes
                // connection setup, and sampling once would charge that delay to every peer as a
                // clock offset.
                long sentAtMs = ownClock.now();
                long startNanos = nanoTime.getAsLong();
                TimeResponse response = peer.stub().getTime(TimeRequest.newBuilder()
                        .setRequesterTimeMs(sentAtMs)
                        .build());
                long rttMs = (nanoTime.getAsLong() - startNanos) / 1_000_000L;
                long receivedAtMs = ownClock.now();
                // The peer answered somewhere in the middle of the round trip, so compare its
                // time with the midpoint of ours.
                long coordinatorAtReply = (sentAtMs + receivedAtMs) / 2;
                offsets.put(peer.id(), response.getNodeTimeMs() - coordinatorAtReply);
                reachable.put(peer.id(), peer);
                log.debug("Berkeley polled {}: rtt {} ms, offset {} ms",
                        peer.id(), rttMs, offsets.get(peer.id()));
            } catch (Exception e) {
                log.debug("Berkeley poll of {} failed: {}", peer.id(), e.getMessage());
            }
        }

        if (offsets.size() == 1) {
            log.debug("Berkeley round skipped: no peers answered");
            return new Round(offsets, Map.of(), List.of(), 0L);
        }

        // Outliers are measured against the median, which one bad clock cannot drag far.
        List<Long> sorted = new ArrayList<>(offsets.values());
        sorted.sort(Long::compareTo);
        long median = sorted.get(sorted.size() / 2);

        List<String> excluded = new ArrayList<>();
        long sum = 0;
        int counted = 0;
        for (Map.Entry<String, Long> entry : offsets.entrySet()) {
            if (Math.abs(entry.getValue() - median) > outlierMs) {
                excluded.add(entry.getKey());
                continue;
            }
            sum += entry.getValue();
            counted++;
        }
        long average = counted == 0 ? median : sum / counted;

        // Every node, outliers included, is moved onto the agreed time.
        Map<String, Long> corrections = new LinkedHashMap<>();
        for (Map.Entry<String, Long> entry : offsets.entrySet()) {
            corrections.put(entry.getKey(), average - entry.getValue());
        }

        Round result = new Round(offsets, corrections, excluded, average);
        log.info("Berkeley round: offsets before {} (spread {} ms), average {} ms, excluded {}",
                offsets, result.spreadBeforeMs(), average, excluded);

        ownClock.adjust(corrections.getOrDefault(nodeId, 0L));
        for (Map.Entry<String, ClockPeer> entry : reachable.entrySet()) {
            long correction = corrections.getOrDefault(entry.getKey(), 0L);
            try {
                entry.getValue().stub().adjust(
                        ClockAdjust.newBuilder().setOffsetMs(correction).build());
            } catch (Exception e) {
                log.warn("Could not correct {}: {}", entry.getKey(), e.getMessage());
            }
        }
        log.info("Berkeley round: corrections {}, offsets after {} (spread {} ms)",
                corrections, result.offsetsAfterMs(), result.spreadAfterMs());
        EventLog.get().event(EventLog.CLOCK_SYNC, "", Map.of(
                "algorithm", "berkeley",
                "spread_before_ms", String.valueOf(result.spreadBeforeMs()),
                "spread_after_ms", String.valueOf(result.spreadAfterMs()),
                "excluded", String.join("|", excluded)));
        return result;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
