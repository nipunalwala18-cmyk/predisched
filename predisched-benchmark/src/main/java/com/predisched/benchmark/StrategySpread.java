package com.predisched.benchmark;

import com.predisched.common.grpc.Channels;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.ArrivalRate;
import com.predisched.scheduler.Dispatcher;
import com.predisched.scheduler.RegistryServiceImpl;
import com.predisched.scheduler.SchedulerServiceImpl;
import com.predisched.scheduler.TaskQueue;
import com.predisched.scheduler.WorkerRegistry;
import com.predisched.scheduler.strategy.StrategyFactory;
import com.predisched.worker.WorkerMetrics;
import com.predisched.worker.WorkerServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Runs the same 300-task batch under each strategy (round-robin, random,
 * least-loaded, resource-aware) and prints tasks per worker, mean and P95 latency,
 * and load imbalance (std dev of tasks per worker) into
 * {@code results/strategy-spread.csv}.
 *
 * <p>Each run uses a fresh in-process cluster of 3 workers. Heartbeats do not run
 * in-process, so workers carry static snapshots (w1 idle, w2 warm, w3 hot) to show
 * that the load-aware strategies respond to reported load.
 */
public class StrategySpread {

  static final List<String> STRATEGIES =
      List.of("round-robin", "random", "least-loaded", "resource-aware");
  static final int TASKS = 300;

  record BatchTask(String id, TaskType type, String input) {}

  record Result(
      String strategy, long w1, long w2, long w3, double meanLatencyMs, double p95LatencyMs,
      double imbalance) {}

  /** The same batch for every strategy run. */
  static List<BatchTask> batch() {
    List<BatchTask> tasks = new ArrayList<>(TASKS);
    for (int i = 0; i < 100; i++) {
      tasks.add(new BatchTask("spread-sleep-" + i, TaskType.SLEEP_TASK, "100"));
      tasks.add(new BatchTask("spread-cpu-" + i, TaskType.CPU_TASK, "1000000"));
      tasks.add(new BatchTask("spread-matrix-" + i, TaskType.MATRIX_TASK, "20"));
    }
    return tasks;
  }

  public static void run(int tasks) throws Exception {
    List<BatchTask> batch = batch().subList(0, Math.min(tasks, batch().size()));
    List<Result> results = new ArrayList<>();
    for (String strategy : STRATEGIES) {
      System.out.printf(Locale.ROOT, "strategy-spread: strategy=%s tasks=%d%n", strategy, batch.size());
      results.add(runOnce(strategy, batch));
    }
    StringBuilder csv =
        new StringBuilder(
            "strategy,tasks_w1,tasks_w2,tasks_w3,mean_latency_ms,p95_latency_ms,imbalance\n");
    for (Result r : results) {
      csv.append(
          String.format(
              Locale.ROOT,
              "%s,%d,%d,%d,%.1f,%.1f,%.2f%n",
              r.strategy(), r.w1(), r.w2(), r.w3(), r.meanLatencyMs(), r.p95LatencyMs(),
              r.imbalance()));
    }
    Path out = Path.of("results", "strategy-spread.csv");
    Files.createDirectories(out.getParent());
    Files.writeString(out, csv.toString());
    System.out.println("wrote " + out.toAbsolutePath());
    System.out.println("strategy      | w1  w2  w3 | mean_ms | p95_ms | imbalance");
    for (Result r : results) {
      System.out.printf(
          Locale.ROOT,
          "%-13s | %3d %3d %3d | %7.0f | %6.0f | %.2f%n",
          r.strategy(), r.w1(), r.w2(), r.w3(), r.meanLatencyMs(), r.p95LatencyMs(), r.imbalance());
    }
  }

  private static Result runOnce(String strategyName, List<BatchTask> batch) throws Exception {
    List<WorkerServiceImpl> services = new ArrayList<>();
    List<Server> servers = new ArrayList<>();
    // Static snapshots: w1 idle, w2 warm, w3 hot (no heartbeats in-process).
    double[] cpu = {10, 50, 80};
    double[] mem = {20, 40, 70};
    WorkerRegistry registry = new WorkerRegistry(300_000);
    for (int w = 1; w <= 3; w++) {
      WorkerServiceImpl service =
          new WorkerServiceImpl("spread-w" + w, 4, 1000, 1.0, new WorkerMetrics());
      Server server = ServerBuilder.forPort(0).addService(service).build();
      server.start();
      services.add(service);
      servers.add(server);
      var req =
          RegisterRequest.newBuilder()
              .setWorkerId(service.workerId())
              .setHost("localhost")
              .setPort(server.getPort())
              .setCores(4)
              .setMemoryMb(4096)
              .setPoolSize(4)
              .build();
      registry.register(req);
      var info = registry.get(service.workerId()).orElseThrow();
      info.cpuPct(cpu[w - 1]);
      info.memPct(mem[w - 1]);
    }

    TaskStore store = new InMemoryTaskStore();
    TaskQueue queue = new TaskQueue();
    Channels channels = new Channels();
    StrategyFactory factory = new StrategyFactory(42, 0.4, 0.2, 0.4);
    Dispatcher dispatcher =
        new Dispatcher(store, queue, factory.create(strategyName), registry, channels);
    dispatcher.start();

    String schedName = "spread-sched-" + UUID.randomUUID();
    Server schedulerServer =
        InProcessServerBuilder.forName(schedName)
            .addService(new SchedulerServiceImpl(store, queue, new ArrivalRate()))
            .addService(new RegistryServiceImpl(registry))
            .directExecutor()
            .build()
            .start();
    ManagedChannel channel = InProcessChannelBuilder.forName(schedName).directExecutor().build();
    try {
      var stub = SchedulerServiceGrpc.newBlockingStub(channel);
      Map<String, Long> submittedAt = new HashMap<>();
      for (BatchTask task : batch) {
        long now = System.currentTimeMillis();
        var res =
            stub.submitTask(
                TaskRequest.newBuilder()
                    .setTaskId(task.id())
                    .setType(task.type())
                    .setInput(task.input())
                    .setPriority(5)
                    .build());
        if (!res.getAccepted()) {
          throw new IllegalStateException("not accepted: " + res.getMessage());
        }
        submittedAt.put(task.id(), now);
      }
      Map<String, Long> completedAt = new HashMap<>();
      Map<String, String> workerOf = new HashMap<>();
      long deadline = System.currentTimeMillis() + 300_000;
      while (completedAt.size() < batch.size()) {
        if (System.currentTimeMillis() > deadline) {
          throw new IllegalStateException("timed out waiting for batch completion");
        }
        for (BatchTask task : batch) {
          if (completedAt.containsKey(task.id())) {
            continue;
          }
          var status =
              stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(task.id()).build());
          if (status.getStatus() == TaskStatus.COMPLETED
              || status.getStatus() == TaskStatus.FAILED
              || status.getStatus() == TaskStatus.CANCELLED) {
            completedAt.put(task.id(), System.currentTimeMillis());
            workerOf.put(task.id(), status.getWorkerId());
          }
        }
        if (completedAt.size() < batch.size()) {
          Thread.sleep(25);
        }
      }
      List<Long> latencies = new ArrayList<>();
      Map<String, Long> perWorker = new HashMap<>();
      for (BatchTask task : batch) {
        latencies.add(completedAt.get(task.id()) - submittedAt.get(task.id()));
        perWorker.merge(workerOf.get(task.id()), 1L, Long::sum);
      }
      latencies.sort(Comparator.naturalOrder());
      double mean = latencies.stream().mapToLong(Long::longValue).average().orElseThrow();
      double p95 = latencies.get(Math.max(0, (int) Math.ceil(0.95 * latencies.size()) - 1));
      long w1 = perWorker.getOrDefault("spread-w1", 0L);
      long w2 = perWorker.getOrDefault("spread-w2", 0L);
      long w3 = perWorker.getOrDefault("spread-w3", 0L);
      double avg = (w1 + w2 + w3) / 3.0;
      double imbalance =
          Math.sqrt(
              ((w1 - avg) * (w1 - avg) + (w2 - avg) * (w2 - avg) + (w3 - avg) * (w3 - avg)) / 3);
      return new Result(strategyName, w1, w2, w3, mean, p95, imbalance);
    } finally {
      dispatcher.close();
      channel.shutdownNow();
      schedulerServer.shutdownNow();
      for (Server server : servers) {
        server.shutdownNow();
      }
      for (WorkerServiceImpl service : services) {
        service.shutdown();
      }
      channels.shutdown();
    }
  }
}
