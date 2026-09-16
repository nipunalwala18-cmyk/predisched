package com.predisched.common.store;

import com.predisched.common.model.TaskRecord;
import com.predisched.proto.TaskStatus;
import java.util.List;
import java.util.Optional;

/**
 * Stores task records with strict lifecycle transitions (spec §3.3).
 *
 * <p>Allowed: QUEUED→RUNNING, QUEUED→CANCELLED, RUNNING→COMPLETED, RUNNING→FAILED,
 * RUNNING→QUEUED (worker died, reassign). Anything else throws
 * {@link IllegalStateException}. Prompt 06 adds a replicated implementation behind
 * this same interface.
 */
public interface TaskStore {

  /** Create a QUEUED record; throws if the id already exists. */
  void create(TaskRecord record);

  Optional<TaskRecord> get(String taskId);

  /**
   * Replace the stored record after checking the status transition is legal.
   * The passed record's status is the desired new status.
   */
  void update(TaskRecord record);

  /** Transition a record's status, applying the change atomically. */
  void transition(String taskId, TaskStatus newStatus);

  List<TaskRecord> list();

  int size();
}
