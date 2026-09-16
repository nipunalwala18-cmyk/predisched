package com.predisched.scheduler;

import com.predisched.common.config.NodeConfig;
import com.predisched.common.grpc.Channels;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Scheduler node: serves SchedulerService and dispatches queued tasks to workers. */
public class SchedulerNode {

  private static final Logger log = LoggerFactory.getLogger(SchedulerNode.class);

  private final NodeConfig config;
  private final TaskStore store = new InMemoryTaskStore();
  private final TaskQueue queue = new TaskQueue();
  private final Channels channels = new Channels();
  private Server server;
  private Dispatcher dispatcher;

  public SchedulerNode(NodeConfig config) {
    this.config = config;
  }

  public void start() throws Exception {
    Map<String, Object> settings = config.settings();
    Object timeout = settings.get("workerTimeoutMs");
    long aliveTimeoutMs = timeout == null ? 5000 : Long.parseLong(String.valueOf(timeout));
    StrategyFactory factory =
        new StrategyFactory(
            config.seed(),
            doubleSetting(settings, "wCpu", 0.4),
            doubleSetting(settings, "wMem", 0.2),
            doubleSetting(settings, "wQueue", 0.4));
    Object strategyName = settings.get("strategy");
    SchedulingStrategy initial;
    try {
      initial = factory.create(strategyName == null ? "round-robin" : strategyName.toString());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("refusing to start: " + e.getMessage(), e);
    }
    WorkerRegistry registry = new WorkerRegistry(aliveTimeoutMs);
    ArrivalRate arrivals = new ArrivalRate();
    dispatcher = new Dispatcher(store, queue, initial, registry, channels);
    dispatcher.start();
    server =
        ServerBuilder.forPort(config.port())
            .addService(new SchedulerServiceImpl(store, queue, arrivals))
            .addService(new RegistryServiceImpl(registry))
            .addService(new AdminServiceImpl(dispatcher, factory))
            .build()
            .start();
    log.info(
        "scheduler {} listening on {}:{} (strategy={})",
        config.nodeId(), config.host(), config.port(), initial.name());
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "scheduler-shutdown"));
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
    try {
      if (dispatcher != null) {
        dispatcher.close();
      }
      if (server != null) {
        server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
      }
      channels.shutdown();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) throws Exception {
    NodeConfig config = NodeConfig.load(args);
    SchedulerNode node = new SchedulerNode(config);
    node.start();
    node.blockUntilShutdown();
  }
}
