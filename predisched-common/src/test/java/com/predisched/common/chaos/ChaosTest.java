package com.predisched.common.chaos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.obs.EventLog;
import com.predisched.common.time.ClockServiceImpl;
import com.predisched.common.time.PhysicalClock;
import com.predisched.proto.ChaosServiceGrpc;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.CpuRequest;
import com.predisched.proto.CrashRequest;
import com.predisched.proto.DrainRequest;
import com.predisched.proto.IsolateRequest;
import com.predisched.proto.LatencyRequest;
import com.predisched.proto.ReadRequest;
import com.predisched.proto.ReadResponse;
import com.predisched.proto.ReplicationServiceGrpc;
import com.predisched.proto.TimeRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Every chaos action on an in-process node has its effect and ends after its duration. */
class ChaosTest {

    /** Answers Read, so isolation has replication traffic to refuse. */
    static final class FakeReplica extends ReplicationServiceGrpc.ReplicationServiceImplBase {
        @Override
        public void read(ReadRequest request, StreamObserver<ReadResponse> observer) {
            observer.onNext(ReadResponse.newBuilder().setTaskId(request.getTaskId()).build());
            observer.onCompleted();
        }
    }

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final AtomicBoolean drained = new AtomicBoolean();
    private final AtomicInteger exitCode = new AtomicInteger(-1);
    private ChaosController chaos;
    private Server server;
    private ManagedChannel channel;
    private EventLog previous;

    @TempDir
    Path dir;

    @BeforeEach
    void start() throws Exception {
        chaos = new ChaosController("worker-9", () -> drained.set(true), now::get,
                exitCode::set);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .addService(new ChaosServiceImpl(chaos))
                .addService(new ClockServiceImpl("worker-9", PhysicalClock.system()))
                .addService(new FakeReplica())
                .intercept(new ChaosInterceptor(chaos))
                .executor(java.util.concurrent.Executors.newCachedThreadPool())
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        previous = EventLog.get();
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        now.addAndGet(10_000_000L); // ends any spike threads
    }

    private ChaosServiceGrpc.ChaosServiceBlockingStub chaosStub() {
        return ChaosServiceGrpc.newBlockingStub(channel);
    }

    private long timedGetTime() {
        long started = System.nanoTime();
        ClockServiceGrpc.newBlockingStub(channel).getTime(TimeRequest.getDefaultInstance());
        return (System.nanoTime() - started) / 1_000_000;
    }

    @Test
    void latencyDelaysCallsUntilItsDurationEnds() {
        assertTrue(timedGetTime() < 150);
        var reply = chaosStub().injectLatency(
                LatencyRequest.newBuilder().setMs(300).setDurationS(60).build());
        assertTrue(reply.getOk());
        assertEquals(now.get() + 60_000, reply.getUntilMs());
        assertTrue(timedGetTime() >= 290, "delayed while active");
        long chaosCall = System.nanoTime();
        chaosStub().injectLatency(LatencyRequest.newBuilder().setMs(300).setDurationS(60).build());
        assertTrue((System.nanoTime() - chaosCall) / 1_000_000 < 250,
                "ChaosService itself is never delayed");
        now.addAndGet(60_001);
        assertTrue(timedGetTime() < 150, "over after its duration");
    }

    @Test
    void isolationRefusesReplicationTrafficOnly() {
        var replica = ReplicationServiceGrpc.newBlockingStub(channel);
        replica.read(ReadRequest.newBuilder().setTaskId("t").build());
        chaosStub().isolate(IsolateRequest.newBuilder().setDurationS(30).build());
        StatusRuntimeException refused = assertThrows(StatusRuntimeException.class,
                () -> replica.read(ReadRequest.newBuilder().setTaskId("t").build()));
        assertEquals(Status.Code.UNAVAILABLE, refused.getStatus().getCode());
        assertTrue(timedGetTime() < 150, "other services still answer");
        now.addAndGet(30_001);
        assertEquals("t", replica.read(ReadRequest.newBuilder().setTaskId("t").build())
                .getTaskId());
    }

    @Test
    void isolationAlsoCutsTheNodesOwnReplicationCalls() {
        var outgoing = ReplicationServiceGrpc.newBlockingStub(io.grpc.ClientInterceptors
                .intercept(channel, chaos.outgoingInterceptor()));
        assertEquals("t", outgoing.read(ReadRequest.newBuilder().setTaskId("t").build())
                .getTaskId());
        chaos.isolate(10);
        StatusRuntimeException refused = assertThrows(StatusRuntimeException.class,
                () -> outgoing.read(ReadRequest.newBuilder().setTaskId("t").build()));
        assertEquals(Status.Code.UNAVAILABLE, refused.getStatus().getCode());
        assertTrue(refused.getStatus().getDescription().contains("outgoing"));
        var clock = ClockServiceGrpc.newBlockingStub(io.grpc.ClientInterceptors
                .intercept(channel, chaos.outgoingInterceptor()));
        clock.getTime(TimeRequest.getDefaultInstance()); // other calls go out
        now.addAndGet(10_001);
        assertEquals("t", outgoing.read(ReadRequest.newBuilder().setTaskId("t").build())
                .getTaskId());
    }

    @Test
    void cpuSpikeRunsItsThreadsUntilItsDurationEnds() throws Exception {
        chaosStub().spikeCpu(CpuRequest.newBuilder().setThreads(2).setDurationS(5).build());
        assertEquals(2, chaos.cpuThreadsRunning());
        now.addAndGet(5_001);
        long deadline = System.currentTimeMillis() + 5_000;
        while (chaos.cpuThreadsRunning() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(0, chaos.cpuThreadsRunning());
    }

    @Test
    void drainRunsTheNodesDrainAndCrashExits() throws Exception {
        assertTrue(chaosStub().drain(DrainRequest.getDefaultInstance()).getOk());
        assertTrue(drained.get());
        assertFalse(new ChaosController("scheduler-1", null).drain(),
                "a scheduler has nothing to drain");
        assertTrue(chaosStub().crash(CrashRequest.newBuilder().setReason("test").build())
                .getOk(), "the reply goes out before the exit");
        long deadline = System.currentTimeMillis() + 5_000;
        while (exitCode.get() < 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(137, exitCode.get());
    }

    @Test
    void badDurationsAreRejectedAndEveryActionIsAnEvent() throws Exception {
        StatusRuntimeException bad = assertThrows(StatusRuntimeException.class, () ->
                chaosStub().injectLatency(LatencyRequest.newBuilder().setMs(10).setDurationS(0)
                        .build()));
        assertEquals(Status.Code.INVALID_ARGUMENT, bad.getStatus().getCode());
        Path file = dir.resolve("events.jsonl");
        EventLog log = new EventLog("worker-9", file);
        java.lang.reflect.Field field = EventLog.class.getDeclaredField("instance");
        field.setAccessible(true);
        field.set(null, log);
        try {
            chaosStub().isolate(IsolateRequest.newBuilder().setDurationS(5).build());
            chaosStub().drain(DrainRequest.getDefaultInstance());
        } finally {
            field.set(null, previous);
            log.close();
        }
        String events = Files.readString(file);
        assertEquals(2, events.lines().filter(l -> l.contains("\"CHAOS\"")).count(), events);
        assertTrue(events.contains("isolate") && events.contains("drain"), events);
        assertTrue(events.contains("lamport"), events);
    }
}
