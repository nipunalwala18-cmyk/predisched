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
import com.predisched.scheduler.strategy.RoundRobinStrategy;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Pool-scaling measurement: for pool sizes 1, 2, 4, 8, starts one in-process worker,
 * replays the same batch (16 × {@code SLEEP_TASK 500}, 16 × {@code CPU_TASK 5000000})
 * through the real scheduler, repeats 3 times, and writes
 * {@code results/pool-scaling.csv} with makespan, throughput, mean latency and speedup
 * (mean ± std dev across repetitions).
 */
public class PoolScaling {

  static final int[] POOL_SIZES = {1, 2, 4, 8};
  static final int REPS = 3;

  record Row(
      int pool,
      int rep,
      double makespanMs,
      double throughputTps,
      double meanLatencyMs,
      double meanSleepLatencyMs,
      double meanCpuLatencyMs) {}

  public static void run() throws Exception {
    List<Row> rows = new ArrayList<>();
    for (int pool : POOL_SIZES) {
      for (int rep = 1; rep <= REPS; rep++) {
        System.out.printf(Locale.ROOT, "pool-scaling: pool=%d rep=%d/%d%n", pool, rep, REPS);
        rows.add(runOnce(pool, rep));
      }
    }
    double p1Mean =
        rows.stream().filter(r -> r.pool() == 1).mapToDouble(Row::makespanMs).average().orElseThrow();

    StringBuilder csv =
        new StringBuilder(
            "pool_size,rep,makespan_ms,throughput_tps,mean_latency_ms,"
                + "mean_latency_sleep_ms,mean_latency_cpu_ms,speedup\n");
    for (Row r : rows) {
      csv.append(
          String.format(
              Locale.ROOT,
              "%d,%d,%.1f,%.2f,%.1f,%.1f,%.1f,%.2f%n",
              r.pool(), r.rep(), r.makespanMs(), r.throughputTps(), r.meanLatencyMs(),
              r.meanSleepLatencyMs(), r.meanCpuLatencyMs(), p1Mean / r.makespanMs()));
    }
    Path out = Path.of("results", "pool-scaling.csv");
    Files.createDirectories(out.getParent());
    Files.writeString(out, csv.toString());
    System.out.println("wrote " + out.toAbsolutePath());

    System.out.println(
        "pool | makespan_ms (mean±sd) | throughput_tps | mean_latency_ms | sleep_lat_ms | cpu_lat_ms | speedup");
    for (int pool : POOL_SIZES) {
      double[] makespans =
          rows.stream().filter(r -> r.pool() == pool).mapToDouble(Row::makespanMs).toArray();
      double[] throughputs =
          rows.stream().filter(r -> r.pool() == pool).mapToDouble(Row::throughputTps).toArray();
      double[] latencies =
          rows.stream().filter(r -> r.pool() == pool).mapToDouble(Row::meanLatencyMs).toArray();
      double[] sleepLat =
          rows.stream().filter(r -> r.pool() == pool).mapToDouble(Row::meanSleepLatencyMs).toArray();
      double[] cpuLat =
          rows.stream().filter(r -> r.pool() == pool).mapToDouble(Row::meanCpuLatencyMs).toArray();
      System.out.printf(
          Locale.ROOT,
          "%4d | %8.0f±%-7.0f | %13.1f±%-5.1f | %14.0f±%-6.0f | %11.0f | %9.0f | %.2f%n",
          pool, mean(makespans), sd(makespans), mean(throughputs), sd(throughputs),
          mean(latencies), sd(latencies), mean(sleepLat), mean(cpuLat),
          p1Mean / mean(makespans));
    }
  }

  private static double mean(double[] values) {
    double sum = 0;
    for (double v : values) {
      sum += v;
    }
    return sum / values.length;
  }

  private static double sd(double[] values) {
    double m = mean(values);
    double sum = 0;
    for (double v : values) {
      sum += (v - m) * (v - m);
    }
    return Math.sqrt(sum / values.length);
  }

  record Submitted(long atMs, TaskType type) {}

  private static Row runOnce(int poolSize, int rep) throws Exception {
    WorkerServiceImpl worker =
        new WorkerServiceImpl("bench-w", poolSize, 1000, 1.0, new WorkerMetrics());
    Server workerServer = ServerBuilder.forPort(0).addService(worker).build();
    workerServer.start();
    int workerPort = workerServer.getPort();

    WorkerRegistry registry = new WorkerRegistry(60_000);
    registry.register(
        RegisterRequest.newBuilder()
            .setWorkerId("bench-w")
            .setHost("localhost")
            .setPort(workerPort)
            .setCores(4)
            .setMemoryMb(4096)
            .setPoolSize(poolSize)
            .build());

    TaskStore store = new InMemoryTaskStore();
    TaskQueue queue = new TaskQueue();
    Channels channels = new Channels();
    Dispatcher dispatcher =
        new Dispatcher(store, queue, new RoundRobinStrategy(), registry, channels);
    dispatcher.start();

    String schedName = "bench-sched-" + UUID.randomUUID();
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
      Map<String, Submitted> submitted = new HashMap<>();
      for (int i = 0; i < 16; i++) {
        submitTimed(stub, TaskType.SLEEP_TASK, "500", submitted);
        submitTimed(stub, TaskType.CPU_TASK, "5000000", submitted);
      }
      Map<String, Long> completedAt = waitForAll(stub, submitted.keySet());
      long first =
          submitted.values().stream().mapToLong(Submitted::atMs).min().orElseThrow();
      long last = completedAt.values().stream().mapToLong(Long::longValue).max().orElseThrow();
      double makespanMs = last - first;
      double throughputTps = submitted.size() / (makespanMs / 1000.0);
      double meanLatencyMs =
          submitted.keySet().stream()
              .mapToLong(id -> completedAt.get(id) - submitted.get(id).atMs())
              .average()
              .orElseThrow();
      double meanSleepMs =
          submitted.keySet().stream()
              .filter(id -> submitted.get(id).type() == TaskType.SLEEP_TASK)
              .mapToLong(id -> completedAt.get(id) - submitted.get(id).atMs())
              .average()
              .orElseThrow();
      double meanCpuMs =
          submitted.keySet().stream()
              .filter(id -> submitted.get(id).type() == TaskType.CPU_TASK)
              .mapToLong(id -> completedAt.get(id) - submitted.get(id).atMs())
              .average()
              .orElseThrow();
      return new Row(poolSize, rep, makespanMs, throughputTps, meanLatencyMs, meanSleepMs, meanCpuMs);
    } finally {
      dispatcher.close();
      channel.shutdownNow();
      schedulerServer.shutdownNow();
      workerServer.shutdownNow();
      worker.shutdown();
      channels.shutdown();
    }
  }

  private static String submit(
      SchedulerServiceGrpc.SchedulerServiceBlockingStub stub, TaskType type, String input) {
    String id = "bench-" + UUID.randomUUID();
    var res =
        stub.submitTask(
            TaskRequest.newBuilder().setTaskId(id).setType(type).setInput(input).setPriority(5)
                .build());
    if (!res.getAccepted()) {
      throw new IllegalStateException("not accepted: " + res.getMessage());
    }
    return id;
  }

  private static void submitTimed(
      SchedulerServiceGrpc.SchedulerServiceBlockingStub stub,
      TaskType type,
      String input,
      Map<String, Submitted> submitted) {
    long now = System.currentTimeMillis();
    submitted.put(submit(stub, type, input), new Submitted(now, type));
  }

  private static Map<String, Long> waitForAll(
      SchedulerServiceGrpc.SchedulerServiceBlockingStub stub, java.util.Set<String> ids)
      throws InterruptedException {
    Map<String, Long> completedAt = new HashMap<>();
    long deadline = System.currentTimeMillis() + 300_000;
    while (completedAt.size() < ids.size()) {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("timed out waiting for batch completion");
      }
      for (String id : ids) {
        if (completedAt.containsKey(id)) {
          continue;
        }
        var status = stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build());
        if (status.getStatus() == TaskStatus.COMPLETED
            || status.getStatus() == TaskStatus.FAILED
            || status.getStatus() == TaskStatus.CANCELLED) {
          completedAt.put(id, System.currentTimeMillis());
        }
      }
      if (completedAt.size() < ids.size()) {
        Thread.sleep(25);
      }
    }
    return completedAt;
  }
}
