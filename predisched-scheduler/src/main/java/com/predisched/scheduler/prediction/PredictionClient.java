package com.predisched.scheduler.prediction;

import com.predisched.common.NodeConfig;
import com.predisched.scheduler.LatencyWindow;
import com.predisched.proto.HealthRequest;
import com.predisched.proto.HealthResponse;
import com.predisched.proto.PredictRequest;
import com.predisched.proto.PredictResponse;
import com.predisched.proto.PredictionServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.WorkerPrediction;
import com.predisched.proto.WorkerState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The scheduler's client for the prediction server (prompt 17, spec 12.3).
 *
 * <p>Every call has a strict deadline ({@code prediction.timeoutMs}, default 10 ms) and goes
 * through a {@link CircuitBreaker}. The client never throws into the dispatcher: it returns the
 * predictions, or {@link Result#EMPTY}, and the caller then falls back to a reactive strategy
 * (FR19, prompt 18).
 *
 * <p>An {@code INVALID_ARGUMENT} answer is the caller's fault, not the server's, so it does not
 * count towards opening the breaker. Timeouts, unavailability and internal errors do.
 */
public final class PredictionClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PredictionClient.class);

    /** Per-worker predictions of one call, with the client- and server-side latency. */
    public record Result(Map<String, WorkerPrediction> byWorker, int modelVersion,
            String modelVersions, double clientMs, double serverMs, String failure) {

        public static final Result EMPTY =
                new Result(Map.of(), 0, "", 0, 0, "no prediction");

        static Result failed(String why, double clientMs) {
            return new Result(Map.of(), 0, "", clientMs, 0, why);
        }

        public boolean isEmpty() {
            return byWorker.isEmpty();
        }
    }

    private final ManagedChannel channel;
    private final boolean ownsChannel;
    private final PredictionServiceGrpc.PredictionServiceBlockingStub stub;
    private final long timeoutMs;
    private final CircuitBreaker breaker;
    private final int latencyLogEvery;
    private final LatencyWindow latencies = new LatencyWindow(1024);
    private final AtomicLong calls = new AtomicLong();
    private volatile double testMaeMs;
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong shortCircuited = new AtomicLong();

    public PredictionClient(ManagedChannel channel, long timeoutMs, CircuitBreaker breaker,
            int latencyLogEvery) {
        this(channel, false, timeoutMs, breaker, latencyLogEvery);
    }

    private PredictionClient(ManagedChannel channel, boolean ownsChannel, long timeoutMs,
            CircuitBreaker breaker, int latencyLogEvery) {
        this.channel = channel;
        this.ownsChannel = ownsChannel;
        this.stub = PredictionServiceGrpc.newBlockingStub(channel);
        this.timeoutMs = timeoutMs;
        this.breaker = breaker;
        this.latencyLogEvery = latencyLogEvery;
    }

    /** A client on its own plaintext channel to {@code prediction.host:port}. */
    public static PredictionClient create(NodeConfig.PredictionConfig config) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(config.getHost(), config.getPort())
                .usePlaintext()
                .build();
        channel.getState(true); // start connecting now, not on the first 10 ms call
        return new PredictionClient(channel, true, config.getTimeoutMs(),
                new CircuitBreaker(config.getFailureThreshold(), config.getCoolDownMs()),
                config.getLatencyLogEvery());
    }

    /**
     * Connects and asks {@code Health} with a generous deadline, outside the dispatch path: the
     * first call on a new channel pays for the TCP and HTTP/2 set-up, which alone can exceed the
     * 10 ms Predict deadline. Returns the server's model versions, or null when it is not up.
     */
    public String warmUp(long timeoutMs) {
        try {
            HealthResponse health = stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .health(HealthRequest.getDefaultInstance());
            log.info("Prediction server ready: {} ({} predictions served so far){}",
                    health.getModelVersions(), health.getPredictions(),
                    health.getShadowVersions().isEmpty() ? ""
                            : "; shadow " + health.getShadowVersions());
            testMaeMs = health.getM1TestMaeMs();
            return health.getModelVersions();
        } catch (StatusRuntimeException e) {
            log.warn("Prediction server not reachable yet: {}", e.getStatus().getCode());
            return null;
        }
    }

    /** Predictions for {@code task} on each of {@code workers}; never throws. */
    public Result predict(TaskRequest task, Collection<WorkerState> workers) {
        return predict(task, workers, 0.0, "");
    }

    public Result predict(TaskRequest task, Collection<WorkerState> workers, double typeMeanMs,
            String requestId) {
        if (workers.isEmpty()) {
            return Result.failed("no workers", 0);
        }
        if (!breaker.allowRequest()) {
            shortCircuited.incrementAndGet();
            return Result.failed("circuit breaker open", 0);
        }
        long started = System.nanoTime();
        try {
            PredictResponse response = stub
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .predict(PredictRequest.newBuilder()
                            .setTask(task)
                            .addAllWorkers(workers)
                            .setTypeMeanMs(typeMeanMs)
                            .setRequestId(requestId)
                            .build());
            double clientMs = (System.nanoTime() - started) / 1e6;
            breaker.onSuccess();
            record(clientMs);
            Map<String, WorkerPrediction> byWorker = new LinkedHashMap<>();
            for (WorkerPrediction p : response.getPredictionsList()) {
                byWorker.put(p.getWorkerId(), p);
            }
            return new Result(byWorker, response.getModelVersion(), response.getModelVersions(),
                    clientMs, response.getServerMs(), null);
        } catch (StatusRuntimeException e) {
            double clientMs = (System.nanoTime() - started) / 1e6;
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.INVALID_ARGUMENT) {
                breaker.onSuccess(); // the server is fine; the request was not
                log.warn("Prediction request rejected: {}", e.getStatus().getDescription());
            } else {
                breaker.onFailure();
                failures.incrementAndGet();
                log.debug("Prediction failed after {} ms: {}", String.format("%.2f", clientMs),
                        code);
            }
            return Result.failed(code + (e.getStatus().getDescription() == null ? ""
                    : ": " + e.getStatus().getDescription()), clientMs);
        } catch (RuntimeException e) {
            breaker.onFailure();
            failures.incrementAndGet();
            return Result.failed(e.toString(), (System.nanoTime() - started) / 1e6);
        }
    }

    private void record(double clientMs) {
        latencies.add(clientMs);
        long n = calls.incrementAndGet();
        if (latencyLogEvery > 0 && n % latencyLogEvery == 0) {
            double[] p = latencies.percentiles(50, 95, 99);
            log.info("Prediction latency (client, last {} calls): p50={} ms p95={} ms p99={} ms;"
                            + " {} calls, {} failures, {} short-circuited, breaker {}",
                    latencies.size(), fmt(p[0]), fmt(p[1]), fmt(p[2]), n, failures.get(),
                    shortCircuited.get(), breaker.state());
        }
    }

    private static String fmt(double ms) {
        return String.format(java.util.Locale.ROOT, "%.2f", ms);
    }

    /** The live M1's test MAE from the last Health answer; 0 before one (drift baseline). */
    public double testMaeMs() {
        return testMaeMs;
    }

    public CircuitBreaker.State breakerState() {
        return breaker.state();
    }

    /** p-th percentiles of the recent client-observed latencies (ms). */
    public double[] latencyPercentiles(double... p) {
        return latencies.percentiles(p);
    }

    public long calls() {
        return calls.get();
    }

    public long failures() {
        return failures.get();
    }

    public long shortCircuited() {
        return shortCircuited.get();
    }

    @Override
    public void close() {
        if (ownsChannel) {
            channel.shutdownNow();
        }
    }
}
