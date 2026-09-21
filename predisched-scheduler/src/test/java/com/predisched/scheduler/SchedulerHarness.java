package com.predisched.scheduler;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A scheduler and one fake worker over in-process gRPC, for tests.
 *
 * <p>The worker is always a test double implementing the generated service, so the scheduler module
 * never imports worker classes (rule 3); real executors are covered in the worker module.
 */
final class SchedulerHarness implements AutoCloseable {

    final TaskStore store = new InMemoryTaskStore();
    final TaskQueue queue;
    final DeadLetterQueue deadLetters = new DeadLetterQueue();
    final RetryCoordinator retries;
    final RunningTasks running = new RunningTasks();
    final WorkerRegistry workers = new WorkerRegistry(60_000);
    final Dispatcher dispatcher;
    final SchedulerServiceGrpc.SchedulerServiceBlockingStub client;
    final TimeoutWatcher timeouts;

    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    static final class Options {
        int maxRetries = 3;
        long retryBaseDelayMs = 20;
        long retryMaxDelayMs = 200;
        double retryJitter = 0;
        long defaultTimeoutMs = 0;
        long timeoutCheckMs = 0;
        double ageingPerSecond = 0.1;
        double outstandingPerWorkerFactor = 2.0;
    }

    SchedulerHarness(BindableService fakeWorker) throws Exception {
        this(fakeWorker, new Options());
    }

    SchedulerHarness(BindableService fakeWorker, Options options) throws Exception {
        queue = new AgeingPriorityQueue(options.ageingPerSecond, 10);
        retries = new RetryCoordinator(
                store,
                queue,
                new RetryPolicy(options.retryBaseDelayMs, options.retryMaxDelayMs,
                        options.retryJitter, options.maxRetries, 42),
                deadLetters);

        String workerName = "worker-" + UUID.randomUUID();
        servers.add(InProcessServerBuilder.forName(workerName)
                .addService(fakeWorker).build().start());
        ManagedChannel workerChannel = InProcessChannelBuilder.forName(workerName).build();
        channels.add(workerChannel);
        WorkerServiceGrpc.WorkerServiceBlockingStub workerStub =
                WorkerServiceGrpc.newBlockingStub(workerChannel);

        workers.register(RegisterRequest.newBuilder()
                .setWorkerId("worker-1").setHost("in-process").setPort(0)
                .setCores(4).setMemoryMb(512).setPoolSize(4).build());

        dispatcher = new Dispatcher(
                store, queue, workers, worker -> workerStub, retries, running,
                options.defaultTimeoutMs, options.outstandingPerWorkerFactor, 2, 20);
        timeouts = new TimeoutWatcher(running, workers, worker -> workerStub,
                options.timeoutCheckMs);

        String schedulerName = "sched-" + UUID.randomUUID();
        servers.add(InProcessServerBuilder.forName(schedulerName)
                .addService(new SchedulerServiceImpl(store, new TaskValidator(4096), queue, retries))
                .build().start());
        ManagedChannel schedulerChannel = InProcessChannelBuilder.forName(schedulerName).build();
        channels.add(schedulerChannel);
        client = SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
    }

    void start() {
        dispatcher.start();
        timeouts.start();
    }

    @Override
    public void close() {
        timeouts.close();
        dispatcher.close();
        retries.close();
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }
}
