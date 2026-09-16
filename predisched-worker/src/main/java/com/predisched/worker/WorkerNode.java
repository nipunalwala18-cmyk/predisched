package com.predisched.worker;

import com.predisched.common.clock.ClockServiceImpl;
import com.predisched.common.clock.CristianSync;
import com.predisched.common.clock.EventLog;
import com.predisched.common.clock.LamportClock;
import com.predisched.common.clock.LamportServerInterceptor;
import com.predisched.common.clock.NodeClock;
import com.predisched.common.clock.NodeContext;
import com.predisched.common.config.NodeConfig;
import com.predisched.common.grpc.Channels;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.TimeRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Worker node: serves WorkerService on a bounded pool and heartbeats to the scheduler. */
public class WorkerNode {

  private static final Logger log = LoggerFactory.getLogger(WorkerNode.class);

  private final NodeConfig config;
  private final LamportClock lamport = new LamportClock();
  private final Channels channels;
  private NodeClock wall;
  private NodeContext ctx;
  private Server server;
  private WorkerServiceImpl service;
  private RegistrationClient registration;
  private ScheduledExecutorService cristianTimer;

  public WorkerNode(NodeConfig config) {
    this.config = config;
    this.channels = new Channels(config.nodeId(), lamport);
  }

  public void start() throws Exception {
    MDC.put("node", config.nodeId());
    Map<String, Object> settings = config.settings();
    wall =
        new NodeClock(
            longSetting(settings, "simulatedOffsetMs", 0),
            doubleSetting(settings, "driftPpm", 0));
    ctx = new NodeContext(config.nodeId(), lamport, wall, new EventLog());
    int poolSize = intSetting(settings, "poolSize", 4);
    int queueCapacity = intSetting(settings, "queueCapacity", 100);
    long heartbeatMs = longSetting(settings, "heartbeatMs", 1000);
    double cpuLimitFactor = doubleSetting(settings, "cpuLimitFactor", 1.0);
    int cores = intSetting(settings, "cores", Runtime.getRuntime().availableProcessors());
    long memoryMb = longSetting(settings, "memoryMb", 4096);

    WorkerMetrics metrics = new WorkerMetrics();
    service =
        new WorkerServiceImpl(config.nodeId(), poolSize, queueCapacity, cpuLimitFactor, metrics, ctx);
    server =
        ServerBuilder.forPort(config.port())
            .intercept(new LamportServerInterceptor(config.nodeId(), lamport))
            .addService(service)
            .addService(new ClockServiceImpl(wall))
            .build()
            .start();
    log.info(
        "worker {} listening on {}:{} (pool={}, queue={}, cpuLimitFactor={})",
        config.nodeId(), config.host(), config.port(), poolSize, queueCapacity, cpuLimitFactor);

    String[] scheduler = schedulerAddress(settings);
    registration =
        new RegistrationClient(
            config.nodeId(), config.host(), config.port(), cores, memoryMb, poolSize,
            scheduler[0], Integer.parseInt(scheduler[1]), heartbeatMs,
            metrics, () -> service.pool().getQueue().size(), channels);
    registration.start();

    Object mode = settings.get("clockSync");
    if (mode != null && mode.toString().equalsIgnoreCase("cristian")) {
      long syncIntervalMs = longSetting(settings, "syncIntervalMs", 5000);
      cristianTimer =
          Executors.newSingleThreadScheduledExecutor(
              task -> {
                Thread thread = new Thread(task, "cristian-sync");
                thread.setDaemon(true);
                return thread;
              });
      cristianTimer.scheduleAtFixedRate(
          () -> {
            MDC.put("node", config.nodeId());
            try {
              var stub =
                  ClockServiceGrpc.newBlockingStub(channels.get(scheduler[0], Integer.parseInt(scheduler[1])))
                      .withDeadlineAfter(2, TimeUnit.SECONDS);
              var result =
                  CristianSync.synchronize(
                      wall,
                      () ->
                          stub
                              .getTime(
                                  TimeRequest.newBuilder()
                                      .setRequesterTimeMs(wall.now())
                                      .build())
                              .getNodeTimeMs());
              log.info(
                  "cristian sync: rtt={} ms, offset applied={} ms",
                  result.rttMs(), result.offsetAppliedMs());
            } catch (Exception e) {
              log.warn("cristian sync failed: {}", e.toString());
            } finally {
              MDC.clear();
            }
          },
          syncIntervalMs,
          syncIntervalMs,
          TimeUnit.MILLISECONDS);
      log.info("clock sync mode: cristian against {}:{}", scheduler[0], scheduler[1]);
    } else {
      log.info("clock sync mode: berkeley (daemon-driven)");
    }
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "worker-shutdown"));
  }

  /** Scheduler address from {@code settings.scheduler} ("host:port"), else the first peer. */
  private String[] schedulerAddress(Map<String, Object> settings) {
    Object override = settings.get("scheduler");
    if (override != null) {
      return override.toString().split(":");
    }
    if (!config.peers().isEmpty()) {
      var peer = config.peers().get(0);
      return new String[] {peer.host(), String.valueOf(peer.port())};
    }
    return new String[] {"localhost", "50051"};
  }

  static int intSetting(Map<String, Object> settings, String name, int def) {
    Object value = settings.get(name);
    return value == null ? def : Integer.parseInt(String.valueOf(value));
  }

  static long longSetting(Map<String, Object> settings, String name, long def) {
    Object value = settings.get(name);
    return value == null ? def : Long.parseLong(String.valueOf(value));
  }

  static double doubleSetting(Map<String, Object> settings, String name, double def) {
    Object value = settings.get(name);
    return value == null ? def : Double.parseDouble(String.valueOf(value));
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public void stop() {
    if (cristianTimer != null) {
      cristianTimer.shutdownNow();
    }
    if (registration != null) {
      registration.close();
    }
    if (service != null) {
      service.shutdown();
    }
    if (server != null) {
      try {
        server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    channels.shutdown();
  }

  public static void main(String[] args) throws Exception {
    NodeConfig config = NodeConfig.load(args);
    WorkerNode node = new WorkerNode(config);
    node.start();
    node.blockUntilShutdown();
  }
}
