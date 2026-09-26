package com.predisched.worker;

import com.predisched.common.NodeConfig;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.ClockServiceImpl;
import com.predisched.common.time.Clocks;
import com.predisched.common.time.CristianClient;
import com.predisched.common.time.LamportClock;
import com.predisched.common.time.PhysicalClock;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.RegistryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts a worker gRPC server exposing {@code WorkerService}. */
public class WorkerMain {

    private static final Logger log = LoggerFactory.getLogger(WorkerMain.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parseArgs(args);
        String configPath = opts.getOrDefault("--config", "configs/local.yaml");
        NodeConfig config = NodeConfig.load(Paths.get(configPath));
        String id = opts.getOrDefault("--id", config.getWorker().getId());
        int port = Integer.parseInt(opts.getOrDefault("--port",
                String.valueOf(config.getWorker().getPort())));

        NodeConfig.WorkerConfig workerConfig = config.getWorker();
        int poolSize = Integer.parseInt(opts.getOrDefault("--pool-size",
                String.valueOf(workerConfig.getPoolSize())));

        NodeConfig.ClockConfig clockConfig = config.getClock();
        long clockOffsetMs = Long.parseLong(opts.getOrDefault("--clock-offset",
                String.valueOf(clockConfig.getOffsetMs())));
        System.setProperty("predisched.node", id);
        PhysicalClock physicalClock = new PhysicalClock(clockOffsetMs, clockConfig.getDriftPpm());
        LamportClock lamportClock = new LamportClock();
        Clocks.install(id, physicalClock, lamportClock);
        LamportInterceptors.applyMdc(id, 0L, null);
        EventLog events = EventLog.install(id);

        ExecutorRegistry registry = new ExecutorRegistry(resolveFileIoDir(id, workerConfig));
        WorkerMetrics metrics = new WorkerMetrics();
        ExecutionEngine engine = new ExecutionEngine(
                registry, metrics, id, poolSize, workerConfig.getQueueCapacity());

        Server server = ServerBuilder.forPort(port)
                .addService(new WorkerServiceImpl(engine, id))
                .addService(new ClockServiceImpl(id, physicalClock))
                .intercept(LamportInterceptors.server(id, lamportClock))
                .build()
                .start();

        ManagedChannel schedulerChannel = ManagedChannelBuilder
                .forAddress(config.getScheduler().getHost(), config.getScheduler().getPort())
                .usePlaintext()
                .intercept(LamportInterceptors.client(lamportClock))
                .build();

        // Cristian's algorithm is this worker pulling the scheduler's time; with Berkeley the
        // scheduler pushes corrections instead and this side only serves ClockService.
        String clockAlgorithm = opts.getOrDefault("--clock-algorithm", clockConfig.getAlgorithm());
        CristianClient cristian = null;
        if ("cristian".equalsIgnoreCase(clockAlgorithm)) {
            cristian = new CristianClient(
                    ClockServiceGrpc.newBlockingStub(schedulerChannel),
                    physicalClock,
                    id,
                    clockConfig.getSyncIntervalMs());
            cristian.start();
        }
        final CristianClient cristianClient = cristian;
        // In a scheduler cluster the worker registers with every scheduler, so whichever node
        // wins an election already knows it. Alone, it registers with the one configured.
        List<ManagedChannel> registryChannels = new ArrayList<>();
        List<NodeConfig.PeerConfig> schedulers = config.getElection().getPeers();
        if (schedulers.isEmpty()) {
            registryChannels.add(schedulerChannel);
        } else {
            for (NodeConfig.PeerConfig scheduler : schedulers) {
                registryChannels.add(ManagedChannelBuilder
                        .forAddress(scheduler.getHost(), scheduler.getPort())
                        .usePlaintext()
                        .intercept(LamportInterceptors.client(lamportClock))
                        .build());
            }
        }
        List<RegistrationClient> registrations = new ArrayList<>();
        for (ManagedChannel channel : registryChannels) {
            RegistrationClient registration = new RegistrationClient(
                    RegistryServiceGrpc.newBlockingStub(channel),
                    engine,
                    metrics,
                    id,
                    opts.getOrDefault("--host", workerConfig.getHost()),
                    port,
                    workerConfig.getHeartbeatIntervalMs());
            registration.start();
            registrations.add(registration);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (cristianClient != null) {
                cristianClient.close();
            }
            registrations.forEach(RegistrationClient::close);
            engine.close();
            registryChannels.forEach(ManagedChannel::shutdownNow);
            schedulerChannel.shutdownNow();
            events.close();
        }));

        log.info("Worker {} listening on {} (pool {}, queue {})",
                id, port, poolSize, workerConfig.getQueueCapacity());
        System.out.println("Worker " + id + " listening on " + port
                + " (pool " + poolSize + ", clock offset " + clockOffsetMs
                + " ms, clock sync " + clockAlgorithm + ", registering with "
                + (schedulers.isEmpty()
                        ? config.getScheduler().getHost() + ":" + config.getScheduler().getPort()
                        : schedulers.size() + " schedulers")
                + ")");
        Path done = Paths.get(opts.getOrDefault("--ready-file", ""));
        if (!done.toString().isEmpty()) {
            try {
                Files.write(done, ("ready " + port).getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                log.warn("Could not write ready file", e);
            }
        }
        server.awaitTermination();
    }

    /**
     * Temp dir for {@code FILE_IO_TASK} files: the configured dir, or a per-worker dir under the
     * JVM temp dir when nothing is configured. Created now so a bad path fails fast at startup.
     */
    static Path resolveFileIoDir(String id, NodeConfig.WorkerConfig workerConfig) throws Exception {
        String configured = workerConfig.getFileIoDir();
        Path dir = (configured == null || configured.isBlank())
                ? Paths.get(System.getProperty("java.io.tmpdir"), "predisched-fileio-" + id)
                : Paths.get(configured);
        Files.createDirectories(dir);
        return dir;
    }

    static Map<String, String> parseArgs(String[] args) {        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length && !args[i + 1].startsWith("--")) {
                opts.put(args[i], args[i + 1]);
                i++;
            } else if (args[i].contains("=") && args[i].startsWith("--")) {
                String[] kv = args[i].split("=", 2);
                opts.put(kv[0], kv[1]);
            }
        }
        return opts;
    }
}
