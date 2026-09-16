package com.predisched.scheduler;

import com.predisched.common.grpc.Channels;
import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import com.predisched.common.store.TaskStore;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes tasks from the queue and sends each to the worker chosen by the strategy.
 *
 * <p>The queue thread only marks tasks RUNNING; the blocking worker call runs on a
 * sender pool so many tasks are in flight at once and worker thread pools actually
 * fill up. Saturation and transport failures requeue the task.
 */
public class Dispatcher implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

  /**
   * Matches the worker's saturation marker over the wire. Kept as a literal on purpose:
   * the scheduler never imports worker classes (module ownership), they talk gRPC only.
   */
  static final String WORKER_SATURATED = "worker saturated";

  private final TaskStore store;
  private final TaskQueue queue;
  private final SchedulingStrategy strategy;
  private final WorkerRegistry registry;
  private final Channels channels;
  private final Thread thread;
  private final ExecutorService inflight;
  private volatile boolean running = true;

  public Dispatcher(
      TaskStore store,
      TaskQueue queue,
      SchedulingStrategy strategy,
      WorkerRegistry registry,
      Channels channels) {
    this.store = store;
    this.queue = queue;
    this.strategy = strategy;
    this.registry = registry;
    this.channels = channels;
    this.thread = new Thread(this::loop, "dispatcher");
    this.thread.setDaemon(true);
    AtomicInteger seq = new AtomicInteger(1);
    this.inflight =
        Executors.newCachedThreadPool(
            task -> {
              Thread sender = new Thread(task, "dispatcher-send-" + seq.getAndIncrement());
              sender.setDaemon(true);
              return sender;
            });
  }

  public void start() {
    thread.start();
  }

  private void loop() {
    while (running) {
      try {
        TaskRecord record = queue.take();
        var chosen = strategy.select(record, registry.alive());
        if (chosen.isEmpty()) {
          log.warn("no workers available, requeueing {}", record.id());
          queue.offer(record);
          Thread.sleep(200);
          continue;
        }
        WorkerInfo w = chosen.get();
        if (markRunning(record, w)) {
          inflight.execute(() -> send(record, w));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  /** Transition QUEUED → RUNNING and stamp the worker; false if the task moved on. */
  private boolean markRunning(TaskRecord record, WorkerInfo worker) {
    synchronized (record) {
      if (record.status() != TaskStatus.QUEUED) {
        return false;
      }
      try {
        store.transition(record.id(), TaskStatus.RUNNING);
      } catch (IllegalStateException e) {
        log.warn("skip dispatch of {}: {}", record.id(), e.getMessage());
        return false;
      }
      record.workerId(worker.workerId());
      record.startedAt(System.currentTimeMillis());
      record.waitTimeMs(Math.max(0, record.startedAt() - record.submittedAt()));
      return true;
    }
  }

  private void send(TaskRecord record, WorkerInfo worker) {
    TaskRequest taskProto =
        TaskRequest.newBuilder()
            .setTaskId(record.id())
            .setType(record.type())
            .setInput(record.input())
            .setPriority(record.priority())
            .build();
    try {
      var channel = channels.get(worker.host(), worker.port());
      var stub =
          WorkerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(65, TimeUnit.SECONDS);
      ExecuteResult res =
          stub.executeTask(ExecuteRequest.newBuilder().setTask(taskProto).build());
      synchronized (record) {
        record.execTimeMs(res.getExecTimeMs());
        record.completedAt(System.currentTimeMillis());
        record.result(res.getOutput());
        if (!res.getSuccess() && res.getOutput().contains(WORKER_SATURATED)) {
          requeue(record, "worker saturated");
          return;
        }
        try {
          store.transition(
              record.id(), res.getSuccess() ? TaskStatus.COMPLETED : TaskStatus.FAILED);
        } catch (IllegalStateException e) {
          log.warn("cannot mark {} terminal: {}", record.id(), e.getMessage());
        }
      }
      strategy.onOutcome(record, worker, res.getExecTimeMs());
      log.info(
          "task {} {} on {} in {} ms",
          record.id(),
          res.getSuccess() ? "COMPLETED" : "FAILED",
          worker.workerId(),
          res.getExecTimeMs());
    } catch (Exception e) {
      log.warn("dispatch of {} to {} failed, requeueing: {}", record.id(), worker.workerId(), e);
      requeue(record, "dispatch failure");
    }
  }

  private void requeue(TaskRecord record, String reason) {
    synchronized (record) {
      try {
        store.transition(record.id(), TaskStatus.QUEUED);
      } catch (IllegalStateException ex) {
        log.warn("cannot requeue {}: {}", record.id(), ex.getMessage());
        return;
      }
      record.workerId("");
    }
    // Brief pause on the sender thread so a full worker is not hot-looped against.
    // (The queue thread is unaffected and keeps admitting other tasks.)
    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    log.info("requeued {} ({})", record.id(), reason);
    queue.offer(record);
  }

  @Override
  public void close() {
    running = false;
    thread.interrupt();
    inflight.shutdownNow();
  }
}
