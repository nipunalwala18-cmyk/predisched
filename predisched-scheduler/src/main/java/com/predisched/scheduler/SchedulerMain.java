package com.predisched.scheduler;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.NodeConfig;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.BerkeleyDaemon;
import com.predisched.common.time.ClockPeer;
import com.predisched.common.time.ClockServiceImpl;
import com.predisched.common.time.Clocks;
import com.predisched.common.time.LamportClock;
import com.predisched.common.time.PhysicalClock;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts a scheduler gRPC server with a single-worker dispatcher. */
public class SchedulerMain {

    private static final Logger log = LoggerFactory.getLogger(SchedulerMain.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String configPath = opts.getOrDefault("--config", "configs/local.yaml");
        NodeConfig config = NodeConfig.load(Paths.get(configPath));
        String id = opts.getOrDefault("--id", config.getScheduler().getId());
        int port = Integer.parseInt(opts.getOrDefault("--port",
                String.valueOf(config.getScheduler().getPort())));

        // Clocks and logging first: every line from here on carries node id and Lamport time.
        NodeConfig.ClockConfig clockConfig = config.getClock();
        long clockOffsetMs = Long.parseLong(opts.getOrDefault("--clock-offset",
                String.valueOf(clockConfig.getOffsetMs())));
        System.setProperty("predisched.node", id);
        PhysicalClock physicalClock = new PhysicalClock(clockOffsetMs, clockConfig.getDriftPpm());
        LamportClock lamportClock = new LamportClock();
        Clocks.install(id, physicalClock, lamportClock);
        LamportInterceptors.applyMdc(id, 0L, null);
        EventLog events = EventLog.install(id);

        TaskStore store = new InMemoryTaskStore();
        TaskValidator validator = new TaskValidator(config.getValidation().getMaxInputChars());

        NodeConfig.QueueConfig queueConfig = config.getQueue();
        // --queue-ageing makes the starvation demo runnable in seconds instead of minutes.
        double ageingPerSecond = Double.parseDouble(opts.getOrDefault("--queue-ageing",
                String.valueOf(queueConfig.getAgeingPerSecond())));
        TaskQueue queue = new AgeingPriorityQueue(
                ageingPerSecond, queueConfig.getMaxPriority());
        RetryPolicy retryPolicy = new RetryPolicy(
                queueConfig.getRetryBaseDelayMs(),
                queueConfig.getRetryMaxDelayMs(),
                queueConfig.getRetryJitter(),
                queueConfig.getMaxRetries(),
                queueConfig.getSeed());
        RetryCoordinator retries = new RetryCoordinator(
                store, queue, retryPolicy, new DeadLetterQueue());
        RunningTasks running = new RunningTasks();

        WorkerRegistry workers = new WorkerRegistry(config.getScheduler().getWorkerStaleAfterMs());
        WorkerClients clients = new WorkerClients();
        Dispatcher dispatcher = new Dispatcher(
                store, queue, workers, clients, retries, running,
                queueConfig.getDefaultTimeoutMs(),
                config.getScheduler().getOutstandingPerWorkerFactor(),
                config.getScheduler().getDispatchThreads(),
                config.getScheduler().getNoWorkerRetryMs());
        dispatcher.start();
        TimeoutWatcher timeouts = new TimeoutWatcher(
                running, workers, clients, queueConfig.getTimeoutCheckMs());
        timeouts.start();
        ClusterReporter reporter = new ClusterReporter(
                workers, config.getScheduler().getClusterReportIntervalMs());
        reporter.start();

        Server server = ServerBuilder.forPort(port)
                .addService(new SchedulerServiceImpl(store, validator, queue, retries))
                .addService(new RegistryServiceImpl(workers))
                .addService(new ClockServiceImpl(id, physicalClock))
                .intercept(LamportInterceptors.server(id, lamportClock))
                .build()
                .start();

        // Berkeley: this node is the time daemon for every worker that has registered.
        Map<String, ManagedChannel> clockChannels = new ConcurrentHashMap<>();
        BerkeleyDaemon berkeley = new BerkeleyDaemon(
                id,
                physicalClock,
                () -> clockPeers(workers, clockChannels),
                "berkeley".equalsIgnoreCase(clockConfig.getAlgorithm())
                        ? clockConfig.getSyncIntervalMs()
                        : 0,
                clockConfig.getOutlierMs());
        berkeley.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            timeouts.close();
            retries.close();
            berkeley.close();
            reporter.close();
            dispatcher.close();
            clients.close();
            clockChannels.values().forEach(ManagedChannel::shutdownNow);
            events.close();
        }));
        log.info("Scheduler {} listening on {} (dispatch threads {})",
                id, port, config.getScheduler().getDispatchThreads());
        System.out.println("Scheduler " + id + " listening on " + port
                + ", waiting for workers to register"
                + " (clock offset " + clockOffsetMs + " ms, sync "
                + clockConfig.getAlgorithm() + ")");
        server.awaitTermination();
    }

    /** A clock stub per registered worker, channels cached across rounds. */
    static List<ClockPeer> clockPeers(
            WorkerRegistry workers, Map<String, ManagedChannel> channels) {
        List<ClockPeer> peers = new ArrayList<>();
        for (WorkerInfo worker : workers.healthy()) {
            ManagedChannel channel = channels.computeIfAbsent(worker.address(), address ->
                    ManagedChannelBuilder.forAddress(worker.host(), worker.port())
                            .usePlaintext()
                            .build());
            peers.add(new ClockPeer(worker.id(), ClockServiceGrpc.newBlockingStub(channel)));
        }
        return peers;
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
                opts.put(args[i], args[i + 1]);
                i++;
            } else if (args[i].startsWith("--") && args[i].contains("=")) {
                String[] kv = args[i].split("=", 2);
                opts.put(kv[0], kv[1]);
            }
        }
        return opts;
    }
}
