package com.predisched.scheduler;

import com.predisched.common.model.TaskRecord;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

/** Priority queue ordered by priority (high first), then submit time. */
public class TaskQueue {

  private final PriorityBlockingQueue<TaskRecord> queue =
      new PriorityBlockingQueue<>(
          1024,
          Comparator.comparingInt(TaskRecord::priority)
              .reversed()
              .thenComparingLong(TaskRecord::submittedAt)
              .thenComparing(TaskRecord::id));

  public void offer(TaskRecord record) {
    queue.offer(record);
  }

  public TaskRecord take() throws InterruptedException {
    return queue.take();
  }

  public boolean remove(String taskId) {
    return queue.removeIf(r -> r.id().equals(taskId));
  }

  public int size() {
    return queue.size();
  }
}
