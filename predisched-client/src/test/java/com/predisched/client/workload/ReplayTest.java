package com.predisched.client.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Replays a 50-task trace against an in-process scheduler: paced submits with the trace timing,
 * polling to terminal states, and a results CSV.
 */
class ReplayTest {

    /** Minimal scheduler: accepts everything and completes each task ~15 ms later. */
    static final class FakeScheduler extends SchedulerServiceGrpc.SchedulerServiceImplBase {
        record State(TaskStatus status, String worker, long execMs) {}

        final Map<String, State> tasks = new ConcurrentHashMap<>();
        final ScheduledExecutorService pool = Executors.newScheduledThreadPool(2);

        @Override
        public void submitTask(TaskRequest request, StreamObserver<TaskResponse> observer) {
            tasks.put(request.getTaskId(), new State(TaskStatus.RUNNING, "", 0));
            long at = System.nanoTime();
            pool.schedule(() -> tasks.put(request.getTaskId(),
                    new State(TaskStatus.COMPLETED, "fake-worker-1",
                            (System.nanoTime() - at) / 1_000_000L)),
                    15, TimeUnit.MILLISECONDS);
            observer.onNext(TaskResponse.newBuilder()
                    .setTaskId(request.getTaskId()).setAccepted(true).setMessage("queued").build());
            observer.onCompleted();
        }

        @Override
        public void getTaskStatus(TaskStatusRequest request, StreamObserver<TaskStatusResponse> observer) {
            State state = tasks.getOrDefault(
                    request.getTaskId(), new State(TaskStatus.QUEUED, "", 0));
            observer.onNext(TaskStatusResponse.newBuilder()
                    .setTaskId(request.getTaskId())
                    .setStatus(state.status())
                    .setResult(state.status() == TaskStatus.COMPLETED ? "fake-ok" : "")
                    .setWorkerId(state.worker())
                    .setExecTimeMs(state.execMs())
                    .build());
            observer.onCompleted();
        }
    }

    @Test
    void replayingAFiftyTaskTraceCompletesAllOfThem(@TempDir Path dir) throws Exception {
        Path profiles = dir.resolve("workloads.yaml");
        Files.writeString(profiles, """
                httpBaseUrl: "http://localhost:8100"
                profiles:
                  tiny:
                    description: "test"
                    tasks:
                      - {type: SLEEP_TASK, weight: 50, size: {min: 5, max: 20}}
                      - {type: CPU_TASK, weight: 50, size: {min: 100, max: 500}}
                    priority: {min: 1, max: 10, highFraction: 0.2, highMin: 8}
                    timeout: {fraction: 0.0, minMs: 0, maxMs: 0}
                """, StandardCharsets.UTF_8);
        WorkloadProfile profile =
                WorkloadProfile.load(profiles, null).get("tiny");
        List<TraceEntry> trace =
                WorkloadGenerator.generate(profile, ArrivalPattern.STEADY, 50, 1, 200.0);

        String name = "inprocess-" + UUID.randomUUID();
        FakeScheduler fake = new FakeScheduler();
        Server server = InProcessServerBuilder.forName(name)
                .addService(fake).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        PrintStream quiet = new PrintStream(OutputStream.nullOutputStream());
        try {
            SchedulerGateway gateway = gateway(channel);
            Path csv = dir.resolve("replay.csv");
            Replayer.Summary summary =
                    Replayer.replay(trace, 20.0, gateway, csv, 10, 60_000, quiet);
            assertEquals(50, summary.totalCompleted(), "all 50 tasks should complete");
            assertEquals(0, summary.totalFailed());
            List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
            assertEquals(51, lines.size(), "header plus one row per task");
            assertTrue(lines.get(0).startsWith("task_id,task_type,"),
                    "CSV should have the documented header");
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            fake.pool.shutdownNow();
        }
    }

    @Test
    void anEarlyTaskIsTimedWhileTheRestOfTheTraceIsStillSubmitting(@TempDir Path dir)
            throws Exception {
        // The first task is done ~15 ms after its submit, the last is submitted 2 s later. Polling
        // only after the whole trace (the old behaviour) stamps the first task with a 2 s latency
        // that is an artefact of when the poller got to it, not of when it ran.
        List<TraceEntry> trace = new ArrayList<>();
        trace.add(new TraceEntry(0, "early", TaskType.SLEEP_TASK, "ms=5", 5, 0));
        trace.add(new TraceEntry(2000, "late", TaskType.SLEEP_TASK, "ms=5", 5, 0));

        String name = "inprocess-" + UUID.randomUUID();
        FakeScheduler fake = new FakeScheduler();
        Server server = InProcessServerBuilder.forName(name)
                .addService(fake).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        PrintStream quiet = new PrintStream(OutputStream.nullOutputStream());
        try {
            SchedulerGateway gateway = gateway(channel);
            Path csv = dir.resolve("early.csv");
            Replayer.Summary summary =
                    Replayer.replay(trace, 1.0, gateway, csv, 20, 60_000, quiet);
            assertEquals(2, summary.totalCompleted());
            assertEquals(0, summary.totalFailed());

            List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
            Row early = row(lines, "early");
            Row late = row(lines, "late");
            assertEquals(TaskStatus.COMPLETED.name(), early.status());
            assertTrue(early.endMs() - early.submitMs() < 1000,
                    "the early task finished before the late one was even submitted, so its "
                            + "latency must be far below the 2000 ms gap, was "
                            + (early.endMs() - early.submitMs()) + " ms");
            assertTrue(late.submitMs() >= 1900, "the late task was submitted at the end of the "
                    + "trace, was " + late.submitMs() + " ms");
            assertTrue(late.endMs() - late.submitMs() < 1000,
                    "and it too must be timed from its own submit, was "
                            + (late.endMs() - late.submitMs()));
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            fake.pool.shutdownNow();
        }
    }

    @Test
    void replayRejectsBadArguments(@TempDir Path dir) {
        List<TraceEntry> trace = new ArrayList<>();
        trace.add(new TraceEntry(0, "t", TaskType.SLEEP_TASK, "ms=5", 5, 0));
        SchedulerGateway unused = new SchedulerGateway() {
            @Override
            public TaskResponse submit(String taskId, TaskType type, String input, int priority,
                    String traceId, long timeoutMs, int maxRetries) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TaskStatusResponse status(String taskId) {
                throw new UnsupportedOperationException();
            }
        };
        PrintStream quiet = new PrintStream(OutputStream.nullOutputStream());
        assertTrue(throwsIo(() -> Replayer.replay(new ArrayList<>(), 1.0, unused,
                dir.resolve("x.csv"), 10, 1000, quiet)));
        assertTrue(throwsIo(() -> Replayer.replay(trace, 0.0, unused,
                dir.resolve("x.csv"), 10, 1000, quiet)));
    }

    private static SchedulerGateway gateway(ManagedChannel channel) {
        SchedulerServiceGrpc.SchedulerServiceBlockingStub stub =
                SchedulerServiceGrpc.newBlockingStub(channel);
        return new SchedulerGateway() {
            @Override
            public TaskResponse submit(String taskId, TaskType type, String input,
                    int priority, String traceId, long timeoutMs, int maxRetries) {
                return stub.submitTask(TaskRequest.newBuilder()
                        .setTaskId(taskId).setType(type).setInput(input)
                        .setPriority(priority).setTraceId(traceId)
                        .setTimeoutMs(timeoutMs).setMaxRetries(maxRetries).build());
            }

            @Override
            public TaskStatusResponse status(String taskId) {
                return stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(taskId).build());
            }
        };
    }

    /** One CSV row, by task id. */
    private record Row(long offsetMs, long submitMs, long startMs, long endMs, String status) {}

    private static Row row(List<String> lines, String taskId) {
        for (String line : lines) {
            String[] f = line.split(",");
            if (f.length == 11 && f[0].equals(taskId)) {
                return new Row(Long.parseLong(f[3]), Long.parseLong(f[4]), Long.parseLong(f[5]),
                        Long.parseLong(f[6]), f[8]);
            }
        }
        throw new AssertionError("no CSV row for " + taskId);
    }

    private static boolean throwsIo(IoRunnable runnable) {
        try {
            runnable.run();
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private interface IoRunnable {
        void run() throws Exception;
    }
}
