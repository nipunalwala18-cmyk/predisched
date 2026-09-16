package com.predisched.scheduler;

import com.predisched.common.config.NodeConfig;
import com.predisched.common.grpc.Channels;
import com.predisched.common.model.WorkerInfo;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.scheduler.strategy.FirstWorkerStrategy;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.ArrayList;
import java.util.List;
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
    List<WorkerInfo> workers = workersFromConfig(config);
    dispatcher =
        new Dispatcher(store, queue, new FirstWorkerStrategy(), workers, channels);
    dispatcher.start();
    server =
        ServerBuilder.forPort(config.port())
            .addService(new SchedulerServiceImpl(store, queue))
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

  @SuppressWarnings("unchecked")
  static List<WorkerInfo> workersFromConfig(NodeConfig config) {
    Object raw = config.settings().get("workers");
    List<WorkerInfo> out = new ArrayList<>();
    if (raw instanceof List<?> list) {
      int n = 0;
      for (Object item : list) {
        n++;
        if (item instanceof java.util.Map m) {
          Object hostObj = m.get("host");
          Object portObj = m.get("port");
          Object idObj = m.get("id");
          String host = hostObj == null ? "localhost" : String.valueOf(hostObj);
          int port = portObj == null ? 50261 : Integer.parseInt(String.valueOf(portObj));
          String id = idObj == null ? "worker-" + n : String.valueOf(idObj);
          out.add(new WorkerInfo(id, host, port, 4, 4096, 4));
        } else {
          String[] parts = item.toString().split(":");
          out.add(
              new WorkerInfo(
                  "worker-" + n, parts[0], Integer.parseInt(parts[1]), 4, 4096, 4));
        }
      }
    }
    if (out.isEmpty()) {
      out.add(new WorkerInfo("worker-1", "localhost", 50261, 4, 4096, 4));
    }
    return out;
  }

  public static void main(String[] args) throws Exception {
    NodeConfig config = NodeConfig.load(args);
    SchedulerNode node = new SchedulerNode(config);
    node.start();
    node.blockUntilShutdown();
  }
}
