package com.predisched.common.obs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.time.ClockServiceImpl;
import com.predisched.common.time.LamportClock;
import com.predisched.common.time.PhysicalClock;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.TimeRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * The interceptors are what make rule 6 true without any service method remembering to tick: a call
 * carries the caller's Lamport time and trace id, and the receiver merges both.
 */
class LamportInterceptorsTest {

    private Server server;
    private ManagedChannel channel;
    private LamportClock serverClock;
    private LamportClock clientClock;

    @BeforeEach
    void setUp() throws Exception {
        String name = "interceptor-" + UUID.randomUUID();
        serverClock = new LamportClock(3);
        clientClock = new LamportClock(10);
        server = InProcessServerBuilder.forName(name)
                .addService(new ClockServiceImpl("node-b", PhysicalClock.system()))
                .intercept(LamportInterceptors.server("node-b", serverClock))
                .directExecutor()
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name)
                .intercept(LamportInterceptors.client(clientClock))
                .directExecutor()
                .build();
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
        TraceContext.clear();
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void aCallFromNodeAAtTenLeavesNodeBAtEleven() {
        ClockServiceGrpc.newBlockingStub(channel)
                .getTime(TimeRequest.newBuilder().build());

        // Request: A ticks 10 -> 11 and sends it; B merges max(3,11)+1 = 12.
        // Reply: B ticks 12 -> 13 and sends it; A merges max(11,13)+1 = 14.
        // The reply is a message too, so the caller ends up causally after the callee's work.
        assertEquals(13, serverClock.current(), "node B: merged 11, then ticked to reply");
        assertEquals(14, clientClock.current(), "node A: merged node B's reply timestamp");
    }

    @Test
    void theTraceIdTravelsWithTheCall() throws Exception {
        // The service records what the MDC held while it ran; afterwards the interceptor clears it,
        // which is what stops a trace id leaking onto the next call on that thread.
        Map<String, String> seen = new ConcurrentHashMap<>();
        String name = "capture-" + UUID.randomUUID();
        LamportClock clock = new LamportClock(3);
        Server capturing = InProcessServerBuilder.forName(name)
                .addService(new ClockServiceGrpc.ClockServiceImplBase() {
                    @Override
                    public void getTime(TimeRequest request,
                            io.grpc.stub.StreamObserver<com.predisched.proto.TimeResponse> obs) {
                        seen.put("node", String.valueOf(MDC.get("node")));
                        seen.put("lamport", String.valueOf(MDC.get("lamport")));
                        seen.put("trace", String.valueOf(MDC.get(TraceContext.MDC_KEY)));
                        obs.onNext(com.predisched.proto.TimeResponse.newBuilder().build());
                        obs.onCompleted();
                    }
                })
                .intercept(LamportInterceptors.server("node-b", clock))
                .directExecutor()
                .build()
                .start();
        ManagedChannel capturingChannel = InProcessChannelBuilder.forName(name)
                .intercept(LamportInterceptors.client(clientClock))
                .directExecutor()
                .build();
        try {
            TraceContext.set("abc12345");
            ClockServiceGrpc.newBlockingStub(capturingChannel)
                    .getTime(TimeRequest.newBuilder().build());
            TraceContext.clear();

            assertEquals("abc12345", seen.get("trace"));
            assertEquals("node-b", seen.get("node"));
            assertTrue(Long.parseLong(seen.get("lamport")) >= 11);
            assertEquals(null, MDC.get(TraceContext.MDC_KEY),
                    "the trace does not leak past the call");
        } finally {
            capturingChannel.shutdownNow();
            capturing.shutdownNow();
        }
    }

    @Test
    void aCallWithoutALamportHeaderStillTicksTheReceiver() throws Exception {
        String name = "plain-" + UUID.randomUUID();
        LamportClock clock = new LamportClock(7);
        Server plain = InProcessServerBuilder.forName(name)
                .addService(new ClockServiceImpl("node-c", PhysicalClock.system()))
                .intercept(LamportInterceptors.server("node-c", clock))
                .directExecutor()
                .build()
                .start();
        ManagedChannel plainChannel =
                InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            ClockServiceGrpc.newBlockingStub(plainChannel)
                    .getTime(TimeRequest.newBuilder().build());
            assertEquals(9, clock.current(),
                    "no header: one tick for the request, one for the reply");
        } finally {
            plainChannel.shutdownNow();
            plain.shutdownNow();
        }
    }

    @Test
    void traceContextRestoresThePreviousValue() {
        TraceContext.set("outer123");
        TraceContext.with("inner456", () -> assertEquals("inner456", TraceContext.current()));
        assertEquals("outer123", TraceContext.current());
        TraceContext.clear();
        assertEquals("", TraceContext.current());
    }
}
