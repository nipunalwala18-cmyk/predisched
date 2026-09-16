package com.predisched.scheduler;

import com.predisched.common.config.NodeConfig;
import com.predisched.common.grpc.Channels;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.scheduler.strategy.RotatingStrategy;
import io.grpc.Server;
import io.grpc.ServerBuilder;
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
    Object timeout = config.settings().get("workerTimeoutMs");
    long aliveTimeoutMs = timeout == null ? 5000 : Long.parseLong(String.valueOf(timeout));
    WorkerRegistry registry = new WorkerRegistry(aliveTimeoutMs);
    ArrivalRate arrivals = new ArrivalRate();
    dispatcher = new Dispatcher(store, queue, new RotatingStrategy(), registry, channels);
    dispatcher.start();
    server =
        ServerBuilder.forPort(config.port())
            .addService(new SchedulerServiceImpl(store, queue, arrivals))
            .addService(new RegistryServiceImpl(registry))
            .build()
            .start();
    log.info("scheduler {} listening on {}:{}", config.nodeId(), config.host(), config.port());
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "scheduler-shutdown"));
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
