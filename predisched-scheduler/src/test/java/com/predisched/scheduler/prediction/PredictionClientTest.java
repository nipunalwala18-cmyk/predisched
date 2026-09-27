package com.predisched.scheduler.prediction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.HealthRequest;
import com.predisched.proto.HealthResponse;
import com.predisched.proto.PredictRequest;
import com.predisched.proto.PredictResponse;
import com.predisched.proto.PredictionServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerPrediction;
import com.predisched.proto.WorkerState;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Deadline, breaker and never-throw behaviour against an in-process fake server (prompt 17). */
class PredictionClientTest {

    /** Answers one prediction per worker after an injectable delay, or fails on demand. */
    static final class FakeServer extends PredictionServiceGrpc.PredictionServiceImplBase {
        volatile long delayMs;
        volatile Status failWith;
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void predict(PredictRequest request, StreamObserver<PredictResponse> observer) {
            calls.incrementAndGet();
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (failWith != null) {
                observer.onError(failWith.asRuntimeException());
                return;
            }
            PredictResponse.Builder response = PredictResponse.newBuilder()
                    .setModelVersion(3).setModelVersions("m1=v3,m2=v1,m3=v2").setServerMs(0.4);
            for (WorkerState w : request.getWorkersList()) {
                response.addPredictions(WorkerPrediction.newBuilder().setWorkerId(w.getWorkerId())
                        .setPredExecMs(10.0 * w.getQueueLen() + 5).setOverloadProb(0.1));
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }

        @Override
        public void health(HealthRequest request, StreamObserver<HealthResponse> observer) {
            observer.onNext(HealthResponse.newBuilder().setReady(true).build());
            observer.onCompleted();
        }
    }

    private final FakeServer fake = new FakeServer();
    private final AtomicLong now = new AtomicLong();
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void start() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(fake).executor(
                java.util.concurrent.Executors.newCachedThreadPool()).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private PredictionClient client(long timeoutMs, int threshold) {
        return new PredictionClient(channel, timeoutMs,
                new CircuitBreaker(threshold, 1_000, now::get), 0);
    }

    private static final TaskRequest TASK = TaskRequest.newBuilder().setTaskId("t1")
            .setType(TaskType.CPU_TASK).setInput("n=1000").setPriority(5).build();
    private static final List<WorkerState> WORKERS = List.of(
            WorkerState.newBuilder().setWorkerId("worker-1").setQueueLen(2).build(),
            WorkerState.newBuilder().setWorkerId("worker-2").setQueueLen(0).build());

    @Test
    void returnsOnePredictionPerWorker() {
        PredictionClient.Result result = client(1_000, 3).predict(TASK, WORKERS);
        assertFalse(result.isEmpty(), result.failure());
        assertEquals(25.0, result.byWorker().get("worker-1").getPredExecMs());
        assertEquals(5.0, result.byWorker().get("worker-2").getPredExecMs());
        assertEquals(3, result.modelVersion());
        assertTrue(result.clientMs() > 0);
    }

    @Test
    void returnsEmptyOnDeadlineExceeded() {
        fake.delayMs = 200;
        PredictionClient client = client(20, 3);
        long started = System.nanoTime();
        PredictionClient.Result result = client.predict(TASK, WORKERS);
        long tookMs = (System.nanoTime() - started) / 1_000_000;
        assertTrue(result.isEmpty());
        assertTrue(result.failure().startsWith("DEADLINE_EXCEEDED"), result.failure());
        assertTrue(tookMs < 150, "gave up at the deadline, not the server's 200 ms: " + tookMs);
        assertEquals(1, client.failures());
    }

    @Test
    void opensAfterKFailuresThenHalfOpensAfterTheCoolDown() {
        fake.delayMs = 300;
        // A deadline with room to spare for the healthy trial call on a busy build machine.
        PredictionClient client = client(60, 3);
        for (int i = 0; i < 3; i++) {
            assertTrue(client.predict(TASK, WORKERS).isEmpty());
        }
        assertEquals(CircuitBreaker.State.OPEN, client.breakerState());
        int callsWhenOpened = fake.calls.get();
        PredictionClient.Result shortCircuit = client.predict(TASK, WORKERS);
        assertTrue(shortCircuit.isEmpty());
        assertEquals("circuit breaker open", shortCircuit.failure());
        assertEquals(callsWhenOpened, fake.calls.get(), "an open breaker makes no call");
        assertEquals(1, client.shortCircuited());

        // After the cool-down one trial goes out; the server is healthy again, so it closes.
        fake.delayMs = 0;
        now.addAndGet(1_000_000_000L);
        PredictionClient.Result trial = client.predict(TASK, WORKERS);
        assertFalse(trial.isEmpty(), trial.failure());
        assertEquals(CircuitBreaker.State.CLOSED, client.breakerState());
    }

    @Test
    void aFailedTrialReopensTheBreaker() {
        fake.failWith = Status.UNAVAILABLE;
        PredictionClient client = client(1_000, 2);
        client.predict(TASK, WORKERS);
        client.predict(TASK, WORKERS);
        assertEquals(CircuitBreaker.State.OPEN, client.breakerState());
        now.addAndGet(1_000_000_000L);
        assertTrue(client.predict(TASK, WORKERS).isEmpty());
        assertEquals(CircuitBreaker.State.OPEN, client.breakerState());
    }

    @Test
    void invalidArgumentDoesNotOpenTheBreaker() {
        fake.failWith = Status.INVALID_ARGUMENT.withDescription("workers is empty");
        PredictionClient client = client(1_000, 1);
        PredictionClient.Result result = client.predict(TASK, WORKERS);
        assertTrue(result.isEmpty());
        assertTrue(result.failure().contains("workers is empty"), result.failure());
        assertEquals(CircuitBreaker.State.CLOSED, client.breakerState());
    }

    @Test
    void neverThrowsWhenTheServerIsGone() {
        server.shutdownNow();
        PredictionClient client = client(50, 5);
        PredictionClient.Result result = client.predict(TASK, WORKERS);
        assertTrue(result.isEmpty());
        assertTrue(client.predict(TASK, List.of()).isEmpty(), "no workers: nothing to ask");
    }

    @Test
    void halfOpenLetsOnlyOneTrialThrough() {
        CircuitBreaker breaker = new CircuitBreaker(1, 100, now::get);
        breaker.onFailure();
        assertFalse(breaker.allowRequest());
        now.addAndGet(100_000_000L);
        assertTrue(breaker.allowRequest());
        assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.state());
        assertFalse(breaker.allowRequest(), "second caller waits for the trial");
        breaker.onSuccess();
        assertTrue(breaker.allowRequest());
        assertEquals(1, breaker.opens());
    }

    @Test
    void latencyPercentilesComeFromRecentCalls() {
        com.predisched.scheduler.LatencyWindow window = new com.predisched.scheduler.LatencyWindow(4);
        for (double v : new double[] {9, 1, 2, 3, 4}) {
            window.add(v);
        }
        double[] p = window.percentiles(50, 100);
        assertEquals(2.0, p[0]);
        assertEquals(4.0, p[1], "the oldest value (9) has left the window");
    }
}
