package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskType;
import java.util.List;
import org.junit.jupiter.api.Test;

class TaskValidatorTest {

  private TaskRequest req(String id, TaskType type, String input, int priority) {
    return TaskRequest.newBuilder()
        .setTaskId(id)
        .setType(type)
        .setInput(input)
        .setPriority(priority)
        .build();
  }

  @Test
  void acceptsValidTasks() {
    var store = new InMemoryTaskStore();
    assertTrue(TaskValidator.validate(req("a", TaskType.CPU_TASK, "100", 5), store).isEmpty());
    assertTrue(TaskValidator.validate(req("b", TaskType.MATRIX_TASK, "200", 1), store).isEmpty());
    assertTrue(TaskValidator.validate(req("c", TaskType.SLEEP_TASK, "0", 10), store).isEmpty());
  }

  @Test
  void rejectsEmptyAndDuplicateId() {
    var store = new InMemoryTaskStore();
    var violations = TaskValidator.validate(req("", TaskType.CPU_TASK, "100", 5), store);
    assertTrue(violations.stream().anyMatch(m -> m.contains("task_id")));

    var ok = req("dup", TaskType.CPU_TASK, "100", 5);
    assertTrue(TaskValidator.validate(ok, store).isEmpty());
    store.create(
        new com.predisched.common.model.TaskRecord("dup", TaskType.CPU_TASK, "100", 5, 0));
    violations = TaskValidator.validate(ok, store);
    assertTrue(violations.stream().anyMatch(m -> m.contains("already exists")));
  }

  @Test
  void rejectsUnsupportedType() {
    var store = new InMemoryTaskStore();
    var violations =
        TaskValidator.validate(req("x", TaskType.MAPREDUCE_TASK, "100", 5), store);
    assertTrue(violations.stream().anyMatch(m -> m.contains("unsupported")));
  }

  @Test
  void rejectsBadInputsPerType() {
    var store = new InMemoryTaskStore();
    assertTrue(
        TaskValidator.validate(req("c1", TaskType.CPU_TASK, "0", 5), store).stream()
            .anyMatch(m -> m.contains("CPU_TASK")));
    assertTrue(
        TaskValidator.validate(req("c2", TaskType.CPU_TASK, "10000001", 5), store).stream()
            .anyMatch(m -> m.contains("CPU_TASK")));
    assertTrue(
        TaskValidator.validate(req("c3", TaskType.CPU_TASK, "abc", 5), store).stream()
            .anyMatch(m -> m.contains("CPU_TASK")));
    assertTrue(
        TaskValidator.validate(req("m1", TaskType.MATRIX_TASK, "0", 5), store).stream()
            .anyMatch(m -> m.contains("MATRIX_TASK")));
    assertTrue(
        TaskValidator.validate(req("m2", TaskType.MATRIX_TASK, "1001", 5), store).stream()
            .anyMatch(m -> m.contains("MATRIX_TASK")));
    assertTrue(
        TaskValidator.validate(req("s1", TaskType.SLEEP_TASK, "-1", 5), store).stream()
            .anyMatch(m -> m.contains("SLEEP_TASK")));
    assertTrue(
        TaskValidator.validate(req("s2", TaskType.SLEEP_TASK, "60001", 5), store).stream()
            .anyMatch(m -> m.contains("SLEEP_TASK")));
  }

  @Test
  void rejectsOversizeInputAndBadPriority() {
    var store = new InMemoryTaskStore();
    String big = "1".repeat(2000);
    List<String> v = TaskValidator.validate(req("big", TaskType.CPU_TASK, big, 5), store);
    assertTrue(v.stream().anyMatch(m -> m.contains("1 KB")));
    assertTrue(
        TaskValidator.validate(req("p0", TaskType.CPU_TASK, "10", 0), store).stream()
            .anyMatch(m -> m.contains("priority")));
    assertTrue(
        TaskValidator.validate(req("p11", TaskType.CPU_TASK, "10", 11), store).stream()
            .anyMatch(m -> m.contains("priority")));
  }

  @Test
  void returnsEveryViolationAtOnce() {
    var store = new InMemoryTaskStore();
    List<String> v = TaskValidator.validate(req("", TaskType.CPU_TASK, "abc", 99), store);
    // id, input, priority = at least 3 violations
    assertTrue(v.size() >= 3, "expected all violations, got: " + v);
  }
}
