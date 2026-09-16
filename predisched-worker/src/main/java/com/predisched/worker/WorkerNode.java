package com.predisched.worker;

import com.predisched.common.config.NodeConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Worker node: serves WorkerService. */
public class WorkerNode {

  private static final Logger log = LoggerFactory.getLogger(WorkerNode.class);

  private final NodeConfig config;
  private Server server;

  public WorkerNode(NodeConfig config) {
    this.config = config;
  }

  public void start() throws Exception {
    server =
        ServerBuilder.forPort(config.port())
            .addService(new WorkerServiceImpl(config.nodeId()))
            .build()
            .start();
    log.info("worker {} listening on {}:{}", config.nodeId(), config.host(), config.port());
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "worker-shutdown"));
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public void stop() {
    if (server != null) {
      try {
        server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    NodeConfig config = NodeConfig.load(args);
    WorkerNode node = new WorkerNode(config);
    node.start();
    node.blockUntilShutdown();
  }
}
