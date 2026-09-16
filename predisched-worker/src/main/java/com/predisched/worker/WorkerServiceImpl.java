package com.predisched.worker;

import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.common.clock.EventType;
import com.predisched.common.clock.NodeContext;
import io.grpc.stub.StreamObserver;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes tasks on a bounded pool, completing each gRPC call asynchronously when
 * the pool finishes, so gRPC threads never block on task work.
 *
 * <p>A saturated pool returns {@code success=false} with {@code "worker saturated"}
 * (the scheduler requeues the task). A thrown exception becomes {@code success=false}
 * with the message, never a gRPC error and never a hung call.
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

  private static final Logger log = LoggerFactory.getLogger(WorkerServiceImpl.class);

  /** Output marker telling the scheduler to requeue instead of failing the task. */
  public static final String SATURATED = "worker saturated";

  private final Map<TaskType, TaskExecutor> executors = new EnumMap<>(TaskType.class);
  private final String workerId;
  private final ThreadPoolExecutor pool;
  private final WorkerMetrics metrics;
  private final double cpuLimitFactor;
  private final NodeContext ctx;
  private final Set<String> executedIds = ConcurrentHashMap.newKeySet();

  public WorkerServiceImpl(
      String workerId,
      int poolSize,
      int queueCapacity,
      double cpuLimitFactor,
      WorkerMetrics metrics,
      NodeContext ctx) {
    this.workerId = workerId;
    this.cpuLimitFactor = cpuLimitFactor;
    this.metrics = metrics;
    this.ctx = ctx;
    AtomicInteger seq = new AtomicInteger(1);
    ThreadFactory factory =
        task -> {
          Thread thread = new Thread(task);
          thread.setName("worker-" + workerId + "-exec-" + seq.getAndIncrement());
          thread.setDaemon(true);
          return thread;
        };
    this.pool =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(queueCapacity),
            factory,
            new ThreadPoolExecutor.AbortPolicy());
    register(new CpuTaskExecutor());
    register(new MatrixTaskExecutor());
    register(new SleepTaskExecutor());
  }

  /** Defaults for tests: pool of 4, queue of 100, no slowdown. */
  public WorkerServiceImpl(String workerId, NodeContext ctx) {
    this(workerId, 4, 100, 1.0, new WorkerMetrics(), ctx);
  }

  void register(TaskExecutor executor) {
    executors.put(executor.type(), executor);
  }

  @Override
  public void executeTask(ExecuteRequest req, StreamObserver<ExecuteResult> obs) {
    String taskId = req.getTask().getTaskId();
    TaskExecutor executor = executors.get(req.getTask().getType());
    long acceptedAt = ctx.wall().now();
    if (executor == null) {
      obs.onNext(
          ExecuteResult.newBuilder()
              .setTaskId(taskId)
              .setSuccess(false)
              .setOutput("unsupported task type: " + req.getTask().getType())
              .setLamportTime(ctx.lamport().tick())
              .build());
      obs.onCompleted();
      return;
    }
    String input = req.getTask().getInput();
    try {
      pool.execute(() -> runTask(taskId, executor, input, acceptedAt, obs));
    } catch (RejectedExecutionException e) {
      log.warn("saturated, rejecting {}", taskId);
      obs.onNext(
          ExecuteResult.newBuilder()
              .setTaskId(taskId)
              .setSuccess(false)
              .setOutput(SATURATED)
              .setLamportTime(ctx.lamport().tick())
              .build());
      obs.onCompleted();
    }
  }

  private void runTask(
      String taskId, TaskExecutor executor, String input, long acceptedAt, StreamObserver<ExecuteResult> obs) {
    // Pool threads carry no RPC context: set ours (node + current tick), restore after.
    java.util.Map<String, String> previous = org.slf4j.MDC.getCopyOfContextMap();
    org.slf4j.MDC.put("node", workerId);
    org.slf4j.MDC.put("lamport", Long.toString(ctx.lamport().current()));
    long startedAt = ctx.wall().now();
    metrics.taskStarted();
    ctx.emit(EventType.START, taskId, "");
    try {
      long begin = System.nanoTime();
      String output;
      try {
        output = executor.execute(input);
      } catch (Exception e) {
        log.warn("task {} failed: {}", taskId, e.toString());
        ctx.emit(EventType.FAIL, taskId, e.toString());
        respond(obs, taskId, false, e.toString(), elapsedMs(begin), startedAt - acceptedAt);
        return;
      }
      long execMs = elapsedMs(begin);
      if (cpuLimitFactor > 0 && cpuLimitFactor != 1.0) {
        // Simulate slower hardware: stretch the genuine execution time.
        long extra = (long) (execMs * (1.0 / cpuLimitFactor - 1.0));
        if (extra > 0) {
          try {
            Thread.sleep(extra);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ctx.emit(EventType.FAIL, taskId, "interrupted");
            respond(obs, taskId, false, "interrupted: " + e, execMs, startedAt - acceptedAt);
            return;
          }
          execMs += extra;
        }
      }
      metrics.recordCompletion(execMs);
      executedIds.add(taskId);
      log.info("task {} done in {} ms on {}", taskId, execMs, Thread.currentThread().getName());
      ctx.emit(EventType.COMPLETE, taskId, "execMs=" + execMs);
      respond(obs, taskId, true, output, execMs, startedAt - acceptedAt);
    } catch (Throwable t) {
      // Guarantee the gRPC call always completes.
      try {
        respond(obs, taskId, false, t.toString(), 0, startedAt - acceptedAt);
      } catch (Throwable ignored) {
        log.warn("could not respond for task {}", taskId, t);
      }
    } finally {
      metrics.taskFinished();
      if (previous == null) {
        org.slf4j.MDC.clear();
      } else {
        org.slf4j.MDC.setContextMap(previous);
      }
    }
  }

  private static long elapsedMs(long beginNanos) {
    return (System.nanoTime() - beginNanos) / 1_000_000;
  }

  private void respond(
      StreamObserver<ExecuteResult> obs,
      String taskId,
      boolean success,
      String output,
      long execMs,
      long waitMs) {
    obs.onNext(
        ExecuteResult.newBuilder()
            .setTaskId(taskId)
            .setSuccess(success)
            .setOutput(output)
            .setExecTimeMs(execMs)
            .setWaitTimeMs(Math.max(0, waitMs))
            .setLamportTime(ctx.lamport().tick())
            .build());
    obs.onCompleted();
  }

  public String workerId() {
    return workerId;
  }

  public WorkerMetrics metrics() {
    return metrics;
  }

  public ThreadPoolExecutor pool() {
    return pool;
  }

  /** Ids this worker has executed; used by tests to detect duplicate execution. */
  public Set<String> executedIds() {
    return executedIds;
  }

  /** Shut the pool down; called when the node stops. */
  public void shutdown() {
    pool.shutdownNow();
  }
}
