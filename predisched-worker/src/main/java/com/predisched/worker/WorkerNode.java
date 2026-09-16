package com.predisched.worker;

import com.predisched.common.config.NodeConfig;
import com.predisched.common.grpc.Channels;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Worker node: serves WorkerService on a bounded pool and heartbeats to the scheduler. */
public class WorkerNode {

  private static final Logger log = LoggerFactory.getLogger(WorkerNode.class);

  private final NodeConfig config;
  private final Channels channels = new Channels();
  private Server server;
  private WorkerServiceImpl service;
  private RegistrationClient registration;

  public WorkerNode(NodeConfig config) {
    this.config = config;
  }

  public void start() throws Exception {
    Map<String, Object> settings = config.settings();
    int poolSize = intSetting(settings, "poolSize", 4);
    int queueCapacity = intSetting(settings, "queueCapacity", 100);
    long heartbeatMs = longSetting(settings, "heartbeatMs", 1000);
    double cpuLimitFactor = doubleSetting(settings, "cpuLimitFactor", 1.0);
    int cores = intSetting(settings, "cores", Runtime.getRuntime().availableProcessors());
    long memoryMb = longSetting(settings, "memoryMb", 4096);

    WorkerMetrics metrics = new WorkerMetrics();
    service = new WorkerServiceImpl(config.nodeId(), poolSize, queueCapacity, cpuLimitFactor, metrics);
    server = ServerBuilder.forPort(config.port()).addService(service).build().start();
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
