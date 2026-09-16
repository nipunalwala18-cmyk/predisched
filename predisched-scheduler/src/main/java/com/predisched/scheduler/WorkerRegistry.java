package com.predisched.scheduler;

import com.predisched.common.clock.NodeClock;
import com.predisched.common.model.WorkerInfo;
import com.predisched.proto.Ack;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the changing set of workers: registration, heartbeat snapshots, liveness.
 *
 * <p>A heartbeat from an unregistered worker is rejected ({@code ok=false}) so the
 * worker re-registers. A worker is alive while its last heartbeat (or registration)
 * is within {@code aliveTimeoutMs}.
 */
public class WorkerRegistry {

  private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
  private final long aliveTimeoutMs;
  private final NodeClock wall;

  public WorkerRegistry(long aliveTimeoutMs, NodeClock wall) {
    this.aliveTimeoutMs = aliveTimeoutMs;
    this.wall = wall;
  }

  public Ack register(RegisterRequest req) {
    if (req.getWorkerId().isBlank()) {
      return Ack.newBuilder().setOk(false).setMessage("worker_id must be non-empty").build();
    }
    workers.compute(
        req.getWorkerId(),
        (id, existing) -> {
          WorkerInfo info =
              existing != null
                  ? existing
                  : new WorkerInfo(
                      id, req.getHost(), req.getPort(), req.getCores(), req.getMemoryMb(),
                      req.getPoolSize());
          info.lastSeen(wall.now());
          return info;
        });
    return Ack.newBuilder().setOk(true).setMessage("registered").build();
  }

  public Ack heartbeat(Heartbeat hb) {
    WorkerInfo info = workers.get(hb.getWorkerId());
    if (info == null) {
      return Ack.newBuilder().setOk(false).setMessage("unknown worker, please register").build();
    }
    info.cpuPct(hb.getCpuPct());
    info.memPct(hb.getMemPct());
    info.activeThreads(hb.getActiveThreads());
    info.queueLen(hb.getQueueLen());
    info.tasksCompleted(hb.getTasksCompleted());
    info.avgExecMs(hb.getAvgExecMs());
    info.lastSeen(wall.now());
    return Ack.newBuilder().setOk(true).setMessage("ok").build();
  }

  /** Snapshot of currently alive workers, sorted by id. */
  public List<WorkerInfo> alive() {
    long now = wall.now();
    return workers.values().stream()
        .filter(w -> now - w.lastSeen() <= aliveTimeoutMs)
        .sorted(Comparator.comparing(WorkerInfo::workerId))
        .toList();
  }

  public Optional<WorkerInfo> get(String workerId) {
    return Optional.ofNullable(workers.get(workerId));
  }

  public int size() {
    return workers.size();
  }
}
