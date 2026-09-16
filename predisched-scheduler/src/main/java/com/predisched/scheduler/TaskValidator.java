package com.predisched.scheduler;

import com.predisched.common.store.TaskStore;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates task submissions (FR2), returning every violation found.
 */
public final class TaskValidator {

  static final int MAX_INPUT_BYTES = 1024;
  static final int MAX_CPU_N = 10_000_000;
  static final int MAX_MATRIX_N = 1000;
  static final int MAX_SLEEP_MS = 60_000;

  private TaskValidator() {}

  public static List<String> validate(TaskRequest req, TaskStore store) {
    List<String> errors = new ArrayList<>();
    String id = req.getTaskId();
    if (id == null || id.isBlank()) {
      errors.add("task_id must be non-empty");
    } else if (store.get(id).isPresent()) {
      errors.add("task_id already exists: " + id);
    }
    TaskType type = req.getType();
    if (type == TaskType.MAPREDUCE_TASK || type == TaskType.UNRECOGNIZED) {
      errors.add("unsupported task type: " + type);
    }
    String input = req.getInput();
    if (input != null && input.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_INPUT_BYTES) {
      errors.add("input exceeds 1 KB");
    }
    if (type == TaskType.CPU_TASK || type == TaskType.MATRIX_TASK || type == TaskType.SLEEP_TASK) {
      validateInputForType(type, input == null ? "" : input.trim(), errors);
    }
    int priority = req.getPriority();
    if (priority < 1 || priority > 10) {
      errors.add("priority must be 1-10, got " + priority);
    }
    return errors;
  }

  private static void validateInputForType(TaskType type, String input, List<String> errors) {
    switch (type) {
      case CPU_TASK -> {
        try {
          long n = Long.parseLong(input);
          if (n <= 0 || n > MAX_CPU_N) {
            errors.add("CPU_TASK input must be an integer 1.." + MAX_CPU_N);
          }
        } catch (NumberFormatException e) {
          errors.add("CPU_TASK input must be an integer 1.." + MAX_CPU_N);
        }
      }
      case MATRIX_TASK -> {
        try {
          long n = Long.parseLong(input);
          if (n < 1 || n > MAX_MATRIX_N) {
            errors.add("MATRIX_TASK input must be an integer 1.." + MAX_MATRIX_N);
          }
        } catch (NumberFormatException e) {
          errors.add("MATRIX_TASK input must be an integer 1.." + MAX_MATRIX_N);
        }
      }
      case SLEEP_TASK -> {
        try {
          long n = Long.parseLong(input);
          if (n < 0 || n > MAX_SLEEP_MS) {
            errors.add("SLEEP_TASK input must be an integer 0.." + MAX_SLEEP_MS);
          }
        } catch (NumberFormatException e) {
          errors.add("SLEEP_TASK input must be an integer 0.." + MAX_SLEEP_MS);
        }
      }
      default -> {}
    }
  }
}
