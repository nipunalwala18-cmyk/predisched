package com.predisched.scheduler;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.NodeConfig;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.common.auth.NodeSecurity;
import com.predisched.common.db.Db;
import com.predisched.common.db.History;
import com.predisched.common.db.HistoryWriter;
import com.predisched.common.db.PostgresTaskStore;
import com.predisched.common.net.Transport;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.BerkeleyDaemon;
import com.predisched.common.time.ClockPeer;
import com.predisched.common.time.ClockServiceImpl;
import com.predisched.common.time.Clocks;
import com.predisched.common.time.LamportClock;
import com.predisched.common.time.PhysicalClock;
import com.predisched.fault.WorkerFailureDetector;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.scheduler.auth.ClientLimits;
import com.predisched.scheduler.auth.RateLimiter;
import com.predisched.scheduler.cache.ResultCache;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyRegistry;
import io.grpc.ManagedChannel;
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
        NodeConfig.ElectionConfig electionConfig = config.getElection();
        electionConfig.setAlgorithm(
                opts.getOrDefault("--election-algorithm", electionConfig.getAlgorithm()));
        // In a cluster the node's election id picks its own entry, and so its port, from peers.
        boolean clustered = !electionConfig.getPeers().isEmpty();
        int electionId = clustered ? SchedulerNode.electionId(opts.get("--node-id"), id) : 0;
        int configuredPort = config.getScheduler().getPort();
        for (NodeConfig.PeerConfig peer : electionConfig.getPeers()) {
            if (peer.getId() == electionId) {
                configuredPort = peer.getPort();
            }
        }
        int port = Integer.parseInt(opts.getOrDefault("--port", String.valueOf(configuredPort)));

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
        // Auth and TLS (F9): installs the transport every channel below uses. Null with auth off.
        NodeSecurity.Security security = NodeSecurity.install(config, true);
        ClientLimits limits = security == null
                ? null
                : new ClientLimits(security.clients(), new RateLimiter());

        // Storage (prompt 11): history and the tasks table go to PostgreSQL through one
        // asynchronous, batched writer, so nothing on the dispatch path waits for the database.
        NodeConfig.DbConfig dbConfig = config.getDb();
        Db db = null;
        HistoryWriter history = null;
        if (dbConfig.isEnabled()) {
            db = Db.open(dbConfig, dbConfig.getPoolSize(), id + "-db", true);
            history = new HistoryWriter(db.dataSource(), dbConfig.getHistoryQueueCapacity(),
                    dbConfig.getHistoryBatchSize(), dbConfig.getHistoryFlushMs());
            history.start();
            History.install(history);
        }

        config.getReplication().setMode(
                opts.getOrDefault("--replication-mode", config.getReplication().getMode()));
        SchedulerNode node = clustered
                ? SchedulerNode.fromConfig(electionId, id, config, lamportClock)
                : null;
        Leadership leadership = node == null ? Leadership.ALONE : node;
        // The rest of the scheduler sees only the TaskStore interface, replicated or not.
        TaskStore memoryOrReplicated = node == null
                ? new InMemoryTaskStore()
                : node.taskStore(new InMemoryTaskStore());
        TaskStore store = history == null
                ? memoryOrReplicated
                : new PostgresTaskStore(memoryOrReplicated, history);
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
        // An unknown strategy name stops the scheduler here, listing the valid ones.
        NodeConfig.SchedulingConfig scheduling = config.getScheduling();
        // Alerts (prompt 21): DRIFT, SLA_BREACH and NODE_FAILURE to the configured webhook.
        com.predisched.common.obs.AlertSink.install(new com.predisched.common.obs.AlertSink(id,
                config.getAlerts().getWebhookUrl(), config.getAlerts().getMinIntervalMs(),
                System::currentTimeMillis));
        StrategyRegistry.Settings strategySettings = StrategyRegistry.Settings.from(config);
        SchedulingStrategy strategy = StrategyRegistry.standard().create(
                opts.getOrDefault("--strategy", scheduling.getStrategy()), strategySettings);
        Dispatcher dispatcher = new Dispatcher(
                store, queue, workers, clients, retries, running,
                queueConfig.getDefaultTimeoutMs(),
                config.getScheduler().getOutstandingPerWorkerFactor(),
                config.getScheduler().getDispatchThreads(),
                config.getScheduler().getNoWorkerRetryMs(),
                strategy);
        SchedulerServiceImpl schedulerService = new SchedulerServiceImpl(
                store, validator, queue, retries, leadership, limits);
        dispatcher.useArrivalRate(schedulerService.arrivals());
        schedulerService.useAdmin(new DispatcherAdmin(dispatcher, StrategyRegistry.standard(),
                strategySettings, workers));
        ResultCache resultCache = null;
        if (config.getCache().isEnabled()) {
            resultCache = new ResultCache(config.getCache().getMaxEntries(),
                    db == null ? null : db.dataSource(), History.get());
            schedulerService.useResultCache(resultCache);
            dispatcher.useResultCache(resultCache);
        }
        // F3: every task with a deadline says whether it made it.
        retries.addTerminalListener(SchedulerMain::reportSla);
        // Prompt 10: what a promotion rebuilds, and what a dead worker's tasks go through.
        SchedulerFailover failover = new SchedulerFailover(
                store, queue, running, dispatcher, workers, clients, retries,
                schedulerService.workflows(), limits, electionConfig.getTimeoutMs());
        WorkerFailureDetector workerDeaths = new WorkerFailureDetector(
                failover, config.getWorker().getHeartbeatIntervalMs(),
                config.getWorker().getHeartbeatMisses(), System::currentTimeMillis);
        if (node == null) {
            // Alone, this scheduler is the primary from the start.
            dispatcher.start();
            workerDeaths.start();
        } else {
            node.enableFailover(failover, workerDeaths, store,
                    config.getReplication().getCatchUpIntervalMs());
        }
        TimeoutWatcher timeouts = new TimeoutWatcher(
                running, workers, clients, queueConfig.getTimeoutCheckMs());
        timeouts.start();
        // Auto-scaling (prompt 21, F12): off unless autoscale.mode (or --autoscale) says
        // reactive or predictive; only the primary acts.
        String autoscaleMode = opts.getOrDefault("--autoscale", config.getAutoscale().getMode());
        com.predisched.scheduler.autoscale.AutoScaler autoscaler = null;
        if (!"none".equalsIgnoreCase(autoscaleMode)) {
            com.predisched.scheduler.autoscale.Forecaster forecaster;
            if ("predictive".equalsIgnoreCase(autoscaleMode)) {
                com.predisched.scheduler.prediction.PredictionClient forecasts =
                        com.predisched.scheduler.prediction.PredictionClient.create(
                                config.getPrediction());
                forecasts.warmUp(2_000);
                forecaster = com.predisched.scheduler.autoscale.Forecaster.predictive(
                        forecasts::predict);
            } else {
                forecaster = com.predisched.scheduler.autoscale.Forecaster.reactive();
            }
            final TaskQueue scaleQueue = queue;
            autoscaler = new com.predisched.scheduler.autoscale.AutoScaler(config.getAutoscale(),
                    new com.predisched.scheduler.autoscale.ClusterView() {
                        @Override
                        public State snapshot() {
                            java.util.Map<String, Integer> inFlight = new java.util.HashMap<>();
                            java.util.List<WorkerInfo> live = new java.util.ArrayList<>();
                            for (WorkerInfo w : workers.healthy()) {
                                int n = running.countFor(w.id());
                                inFlight.put(w.id(), n);
                                if (!workers.isDraining(w.id())) {
                                    live.add(w.withLiveLoad(n));
                                }
                            }
                            return new State(live, scaleQueue.size(), inFlight);
                        }

                        @Override
                        public boolean isLeader() {
                            return leadership.isLeader();
                        }

                        @Override
                        public void drain(String workerId) {
                            workers.markDraining(workerId);
                        }

                        @Override
                        public void forget(String workerId) {
                            workers.remove(workerId);
                        }
                    }, forecaster,
                    com.predisched.scheduler.autoscale.WorkerLauncher.from(config.getAutoscale(),
                            configPath),
                    System::currentTimeMillis);
            autoscaler.start();
        }
        final com.predisched.scheduler.autoscale.AutoScaler scaler = autoscaler;
        // Speculative execution for stragglers (prompt 19, F11): off unless configured.
        StragglerDetector stragglers = config.getSpeculation().isEnabled()
                ? new StragglerDetector(dispatcher, running, store, config.getSpeculation())
                : null;
        if (stragglers != null) {
            stragglers.start();
        }
        ClusterReporter reporter = new ClusterReporter(
                workers, config.getScheduler().getClusterReportIntervalMs());
        reporter.start();

        // Chaos (prompt 19, F16): the ChaosService and its interceptor only when enabled.
        com.predisched.common.chaos.ChaosController chaos = config.getChaos().isEnabled()
                ? new com.predisched.common.chaos.ChaosController(id, null) : null;
        ServerBuilder<?> builder = Transport.get().server(port)
                .addService(schedulerService)
                .addService(new RegistryServiceImpl(workers, leadership::isLeader))
                .addService(new ClockServiceImpl(id, physicalClock));
        if (node != null) {
            builder.addService(node.service());
            if (node.replicationService() != null) {
                builder.addService(node.replicationService());
            }
        }
        if (chaos != null) {
            builder.addService(new com.predisched.common.chaos.ChaosServiceImpl(chaos));
            Transport.addOutgoing(chaos.outgoingInterceptor());
            log.warn("Chaos API enabled on scheduler {}", id);
        }
        Server server = builder
                .intercept(chaos == null ? LamportInterceptors.none()
                        : new com.predisched.common.chaos.ChaosInterceptor(chaos))
                .intercept(LamportInterceptors.server(id, lamportClock))
                // Interceptors run last-added first: authenticate, then rate-limit, then Lamport.
                .intercept(limits == null ? LamportInterceptors.none() : limits)
                .intercept(security == null
                        ? LamportInterceptors.none() : security.interceptor())
                .build()
                .start();
        if (node != null) {
            node.start();
        }

        // Berkeley: the leader is the time daemon for every worker that has registered.
        // Followers run the daemon too but give it no peers, so it does nothing until elected.
        Map<String, ManagedChannel> clockChannels = new ConcurrentHashMap<>();
        BerkeleyDaemon berkeley = new BerkeleyDaemon(
                id,
                physicalClock,
                () -> leadership.isLeader() ? clockPeers(workers, clockChannels) : List.of(),
                "berkeley".equalsIgnoreCase(clockConfig.getAlgorithm())
                        ? clockConfig.getSyncIntervalMs()
                        : 0,
                clockConfig.getOutlierMs());
        berkeley.start();
        final HistoryWriter historyWriter = history;
        final Db database = db;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (node != null) {
                node.close();
            }
            timeouts.close();
            if (stragglers != null) {
                stragglers.close();
            }
            if (scaler != null) {
                scaler.close();
            }
            workerDeaths.close();
            retries.close();
            berkeley.close();
            reporter.close();
            dispatcher.close();
            clients.close();
            clockChannels.values().forEach(ManagedChannel::shutdownNow);
            if (historyWriter != null) {
                historyWriter.close();
            }
            if (database != null) {
                database.close();
            }
            events.close();
        }));
        log.info("Scheduler {} listening on {} (dispatch threads {}, strategy {})",
                id, port, config.getScheduler().getDispatchThreads(), strategy.name());
        System.out.println("Scheduler " + id + " listening on " + port
                + (node == null ? "" : " as election node " + electionId + " ("
                        + electionConfig.getAlgorithm() + ")")
                + ", waiting for workers to register"
                + " (clock offset " + clockOffsetMs + " ms, sync "
                + clockConfig.getAlgorithm() + ")"
                + (db == null ? "" : "; history in " + dbConfig.getUrl())
                + (resultCache == null ? "" : "; result cache on"));
        server.awaitTermination();
    }

    /** Logs and records whether a finished task with a deadline made it (F3). */
    static void reportSla(com.predisched.common.TaskRecord task) {
        if (task.deadlineAt() <= 0) {
            return;
        }
        boolean met = task.slaMet();
        long overMs = (task.completedAt() > 0 ? task.completedAt() : Clocks.now())
                - task.deadlineAt();
        EventLog.get().event(met ? "SLA_MET" : "SLA_MISSED", task.id(), Map.of(
                "status", task.status().name(),
                "over_ms", String.valueOf(overMs)));
        if (!met) {
            log.info("Task {} missed its deadline: {} {} ms after it", task.id(), task.status(),
                    overMs);
        }
    }

    /** A clock stub per registered worker, channels cached across rounds. */
    static List<ClockPeer> clockPeers(
            WorkerRegistry workers, Map<String, ManagedChannel> channels) {
        List<ClockPeer> peers = new ArrayList<>();
        for (WorkerInfo worker : workers.healthy()) {
            ManagedChannel channel = channels.computeIfAbsent(worker.address(), address ->
                    Transport.get().channel(worker.host(), worker.port()));
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
