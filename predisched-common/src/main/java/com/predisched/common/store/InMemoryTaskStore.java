package com.predisched.common.store;

import com.predisched.common.model.TaskRecord;
import com.predisched.proto.TaskStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory {@link TaskStore} backed by a {@link ConcurrentHashMap}. */
public class InMemoryTaskStore implements TaskStore {

  private final ConcurrentHashMap<String, TaskRecord> map = new ConcurrentHashMap<>();

  @Override
  public void create(TaskRecord record) {
    TaskRecord prev = map.putIfAbsent(record.id(), record);
    if (prev != null) {
      throw new IllegalStateException("duplicate task id: " + record.id());
    }
  }

  @Override
  public Optional<TaskRecord> get(String taskId) {
    return Optional.ofNullable(map.get(taskId));
  }

  @Override
  public void update(TaskRecord record) {
    map.compute(
        record.id(),
        (id, existing) -> {
          if (existing == null) {
            throw new IllegalStateException("unknown task id: " + id);
          }
          checkTransition(existing.status(), record.status());
          return record;
        });
  }

  @Override
  public void transition(String taskId, TaskStatus newStatus) {
    map.compute(
        taskId,
        (id, existing) -> {
          if (existing == null) {
            throw new IllegalStateException("unknown task id: " + id);
          }
          checkTransition(existing.status(), newStatus);
          existing.status(newStatus);
          return existing;
        });
  }

  @Override
  public List<TaskRecord> list() {
    return new ArrayList<>(map.values());
  }

  @Override
  public int size() {
    return map.size();
  }

  /** Spec §3.3 lifecycle. */
  public static void checkTransition(TaskStatus from, TaskStatus to) {
    if (from == to) {
      return;
    }
    boolean ok =
        switch (from) {
          case QUEUED -> to == TaskStatus.RUNNING || to == TaskStatus.CANCELLED;
          case RUNNING -> to == TaskStatus.COMPLETED
              || to == TaskStatus.FAILED
              || to == TaskStatus.QUEUED;
          case COMPLETED, FAILED, CANCELLED -> false;
          default -> false;
        };
    if (!ok) {
      throw new IllegalStateException("illegal transition " + from + " -> " + to);
    }
  }
}
