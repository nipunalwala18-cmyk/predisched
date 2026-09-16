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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Takes tasks from the queue and sends each to the worker chosen by the strategy. */
public class Dispatcher implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

  private final TaskStore store;
  private final TaskQueue queue;
  private final SchedulingStrategy strategy;
  private final List<WorkerInfo> workers;
  private final Channels channels;
  private final Thread thread;
  private volatile boolean running = true;

  public Dispatcher(
      TaskStore store,
      TaskQueue queue,
      SchedulingStrategy strategy,
      List<WorkerInfo> workers,
      Channels channels) {
    this.store = store;
    this.queue = queue;
    this.strategy = strategy;
    this.workers = new CopyOnWriteArrayList<>(workers);
    this.channels = channels;
    this.thread = new Thread(this::loop, "dispatcher");
    this.thread.setDaemon(true);
  }

  public void start() {
    thread.start();
  }

  private void loop() {
    while (running) {
      try {
        TaskRecord record = queue.take();
        var chosen = strategy.select(record, List.copyOf(workers));
        if (chosen.isEmpty()) {
          log.warn("no workers available, requeueing {}", record.id());
          queue.offer(record);
          Thread.sleep(200);
          continue;
        }
        WorkerInfo w = chosen.get();
        dispatch(record, w);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void dispatch(TaskRecord record, WorkerInfo worker) {
    synchronized (record) {
      if (record.status() != TaskStatus.QUEUED) {
        return;
      }
      try {
        store.transition(record.id(), TaskStatus.RUNNING);
      } catch (IllegalStateException e) {
        log.warn("skip dispatch of {}: {}", record.id(), e.getMessage());
        return;
      }
      record.workerId(worker.workerId());
      record.startedAt(System.currentTimeMillis());
      record.waitTimeMs(Math.max(0, record.startedAt() - record.submittedAt()));
    }
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
      synchronized (record) {
        try {
          store.transition(record.id(), TaskStatus.QUEUED);
        } catch (IllegalStateException ex) {
          log.warn("cannot requeue {}: {}", record.id(), ex.getMessage());
          return;
        }
        record.workerId("");
      }
      queue.offer(record);
    }
  }

  @Override
  public void close() {
    running = false;
    thread.interrupt();
  }
}
