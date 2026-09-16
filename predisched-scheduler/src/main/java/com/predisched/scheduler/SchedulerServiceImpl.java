package com.predisched.scheduler;

import com.predisched.common.clock.EventType;
import com.predisched.common.clock.NodeContext;
import com.predisched.common.model.TaskRecord;
import com.predisched.common.store.TaskStore;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC front end: validates, stores QUEUED, enqueues, returns immediately.
 */
public class SchedulerServiceImpl extends SchedulerServiceGrpc.SchedulerServiceImplBase {

  private static final Logger log = LoggerFactory.getLogger(SchedulerServiceImpl.class);

  private final TaskStore store;
  private final TaskQueue queue;
  private final ArrivalRate arrivals;
  private final NodeContext ctx;

  public SchedulerServiceImpl(TaskStore store, TaskQueue queue, ArrivalRate arrivals, NodeContext ctx) {
    this.store = store;
    this.queue = queue;
    this.arrivals = arrivals;
    this.ctx = ctx;
  }

  private TaskResponse response(String taskId, boolean accepted, String message) {
    return TaskResponse.newBuilder()
        .setTaskId(taskId)
        .setAccepted(accepted)
        .setMessage(message)
        .setLamportTime(ctx.lamport().tick())
        .build();
  }

  @Override
  public void submitTask(TaskRequest req, StreamObserver<TaskResponse> obs) {
    List<String> violations = TaskValidator.validate(req, store);
    if (!violations.isEmpty()) {
      obs.onNext(response(req.getTaskId(), false, String.join("; ", violations)));
      obs.onCompleted();
      return;
    }
    TaskRecord record =
        new TaskRecord(
            req.getTaskId(), req.getType(), req.getInput(), req.getPriority(),
            ctx.wall().now());
    try {
      store.create(record);
    } catch (IllegalStateException e) {
      obs.onNext(response(req.getTaskId(), false, e.getMessage()));
      obs.onCompleted();
      return;
    }
    queue.offer(record);
    arrivals.mark();
    ctx.emit(EventType.SUBMIT, record.id(), "type=" + record.type() + " priority=" + record.priority());
    ctx.emit(EventType.ENQUEUE, record.id(), "queueSize=" + queue.size());
    log.info("accepted task {}", req.getTaskId());
    obs.onNext(response(req.getTaskId(), true, "QUEUED"));
    obs.onCompleted();
  }

  @Override
  public void getTaskStatus(TaskStatusRequest req, StreamObserver<TaskStatusResponse> obs) {
    var rec = store.get(req.getTaskId());
    if (rec.isEmpty()) {
      obs.onNext(
          TaskStatusResponse.newBuilder()
              .setTaskId(req.getTaskId())
              .setStatus(TaskStatus.QUEUED)
              .setResult("unknown task")
              .build());
    } else {
      TaskRecord r = rec.get();
      obs.onNext(
          TaskStatusResponse.newBuilder()
              .setTaskId(r.id())
              .setStatus(r.status())
              .setResult(r.result())
              .setWorkerId(r.workerId())
              .setExecTimeMs(r.execTimeMs())
              .build());
    }
    obs.onCompleted();
  }

  @Override
  public void cancelTask(TaskStatusRequest req, StreamObserver<com.predisched.proto.TaskResponse> obs) {
    var rec = store.get(req.getTaskId());
    if (rec.isEmpty()) {
      obs.onNext(response(req.getTaskId(), false, "unknown task"));
      obs.onCompleted();
      return;
    }
    TaskRecord r = rec.get();
    synchronized (r) {
      if (r.status() != TaskStatus.QUEUED) {
        obs.onNext(
            response(
                req.getTaskId(),
                false,
                "only QUEUED tasks can be cancelled (status=" + r.status() + ")"));
        obs.onCompleted();
        return;
      }
      try {
        store.transition(r.id(), TaskStatus.CANCELLED);
      } catch (IllegalStateException e) {
        obs.onNext(response(req.getTaskId(), false, e.getMessage()));
        obs.onCompleted();
        return;
      }
      queue.remove(r.id());
    }
    ctx.emit(EventType.CANCEL, r.id(), "");
    log.info("cancelled task {}", req.getTaskId());
    obs.onNext(response(req.getTaskId(), true, "CANCELLED"));
    obs.onCompleted();
  }
}
