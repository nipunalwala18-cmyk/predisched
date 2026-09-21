package com.predisched.common.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.ClockServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Berkeley and Cristian over in-process gRPC with fake node clocks. The system clock is frozen and
 * the nano clock is scripted, so these assertions are about the algorithms, not about timing.
 */
class ClockSyncTest {

    private static final long FROZEN_SYSTEM_MS = 1_700_000_000_000L;

    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    void tearDown() {
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }

    /** A node whose clock is skewed by {@code offsetMs}, served over in-process gRPC. */
    private ClockPeer node(String id, PhysicalClock clock) throws Exception {
        String name = id + "-" + UUID.randomUUID();
        Server server = InProcessServerBuilder.forName(name)
                .addService(new ClockServiceImpl(id, clock))
                .directExecutor()
                .build()
                .start();
        servers.add(server);
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        channels.add(channel);
        return new ClockPeer(id, ClockServiceGrpc.newBlockingStub(channel));
    }

    @Test
    void berkeleyRoundBringsSkewedClocksTogether() throws Exception {
        Map<String, PhysicalClock> clocks = new LinkedHashMap<>();
        clocks.put("coordinator", new PhysicalClock(0, 0, () -> FROZEN_SYSTEM_MS));
        clocks.put("worker-a", new PhysicalClock(-300, 0, () -> FROZEN_SYSTEM_MS));
        clocks.put("worker-b", new PhysicalClock(200, 0, () -> FROZEN_SYSTEM_MS));

        List<ClockPeer> peers = new ArrayList<>();
        peers.add(node("worker-a", clocks.get("worker-a")));
        peers.add(node("worker-b", clocks.get("worker-b")));

        BerkeleyDaemon daemon = new BerkeleyDaemon(
                "coordinator", clocks.get("coordinator"), () -> peers, 0, 1000, () -> 0L);
        BerkeleyDaemon.Round round = daemon.round();

        assertEquals(500, round.spreadBeforeMs(), "-300 to +200 before the round");
        long spread = spreadOf(clocks);
        assertTrue(spread < 10, "clocks within 10 ms of each other, spread was " + spread + " ms");
        assertEquals(List.of(), round.excluded());
        // The average of -300, 0 and +200 is about -33, and everyone lands there.
        assertEquals(-33, clocks.get("coordinator").offsetFromSystemMs());
    }

    @Test
    void anOutlierIsExcludedFromTheAverageButStillCorrected() throws Exception {
        Map<String, PhysicalClock> clocks = new LinkedHashMap<>();
        clocks.put("coordinator", new PhysicalClock(0, 0, () -> FROZEN_SYSTEM_MS));
        clocks.put("worker-a", new PhysicalClock(-300, 0, () -> FROZEN_SYSTEM_MS));
        clocks.put("worker-b", new PhysicalClock(200, 0, () -> FROZEN_SYSTEM_MS));
        clocks.put("worker-broken", new PhysicalClock(500_000, 0, () -> FROZEN_SYSTEM_MS));

        List<ClockPeer> peers = new ArrayList<>();
        peers.add(node("worker-a", clocks.get("worker-a")));
        peers.add(node("worker-b", clocks.get("worker-b")));
        peers.add(node("worker-broken", clocks.get("worker-broken")));

        BerkeleyDaemon daemon = new BerkeleyDaemon(
                "coordinator", clocks.get("coordinator"), () -> peers, 0, 1000, () -> 0L);
        BerkeleyDaemon.Round round = daemon.round();

        assertEquals(List.of("worker-broken"), round.excluded());
        assertTrue(Math.abs(round.averageOffsetMs()) < 200,
                "the broken clock did not drag the average: " + round.averageOffsetMs());
        long spread = spreadOf(clocks);
        assertTrue(spread < 10,
                "the outlier was corrected onto the agreed time too, spread " + spread + " ms");
    }

    @Test
    void cristianSetsTheClockToServerTimePlusHalfTheRoundTrip() throws Exception {
        PhysicalClock serverClock = new PhysicalClock(0, 0, () -> FROZEN_SYSTEM_MS);
        ClockPeer server = node("scheduler", serverClock);

        PhysicalClock workerClock = new PhysicalClock(-300, 0, () -> FROZEN_SYSTEM_MS);
        // Scripted nano clock: the call appears to take 40 ms, so the correction adds 20 ms.
        long[] nanos = {0L};
        CristianClient client = new CristianClient(
                server.stub(), workerClock, "worker-a", 0,
                () -> nanos[0] == 0 ? (nanos[0] = 1) - 1 : 40_000_000L);

        long correction = client.syncOnce();
        assertEquals(320, correction, "300 ms behind plus half of a 40 ms round trip");
        assertEquals(FROZEN_SYSTEM_MS + 20, workerClock.now());
        client.close();
    }

    private static long spreadOf(Map<String, PhysicalClock> clocks) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (PhysicalClock clock : clocks.values()) {
            long now = clock.now();
            min = Math.min(min, now);
            max = Math.max(max, now);
        }
        return max - min;
    }
}
