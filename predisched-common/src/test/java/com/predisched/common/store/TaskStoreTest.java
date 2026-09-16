package com.predisched.common.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.model.TaskRecord;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import org.junit.jupiter.api.Test;

class TaskStoreTest {

  private TaskRecord rec(String id) {
    return new TaskRecord(id, TaskType.CPU_TASK, "100", 5, System.currentTimeMillis());
  }

  @Test
  void legalTransitions() {
    InMemoryTaskStore store = new InMemoryTaskStore();
    TaskRecord r = rec("t1");
    store.create(r);
    store.transition("t1", TaskStatus.RUNNING);
    assertEquals(TaskStatus.RUNNING, store.get("t1").orElseThrow().status());
    store.transition("t1", TaskStatus.COMPLETED);
    assertEquals(TaskStatus.COMPLETED, store.get("t1").orElseThrow().status());

    TaskRecord r2 = rec("t2");
    store.create(r2);
    store.transition("t2", TaskStatus.RUNNING);
    store.transition("t2", TaskStatus.FAILED);
    assertEquals(TaskStatus.FAILED, store.get("t2").orElseThrow().status());

    TaskRecord r3 = rec("t3");
    store.create(r3);
    store.transition("t3", TaskStatus.CANCELLED);
    assertEquals(TaskStatus.CANCELLED, store.get("t3").orElseThrow().status());

    // RUNNING -> QUEUED (worker died, reassign)
    TaskRecord r4 = rec("t4");
    store.create(r4);
    store.transition("t4", TaskStatus.RUNNING);
    store.transition("t4", TaskStatus.QUEUED);
    assertEquals(TaskStatus.QUEUED, store.get("t4").orElseThrow().status());
  }

  @Test
  void illegalTransitionsThrow() {
    InMemoryTaskStore store = new InMemoryTaskStore();
    TaskRecord r = rec("t1");
    store.create(r);
    // QUEUED -> COMPLETED is illegal
    assertThrows(IllegalStateException.class, () -> store.transition("t1", TaskStatus.COMPLETED));
    assertThrows(IllegalStateException.class, () -> store.transition("t1", TaskStatus.FAILED));
    store.transition("t1", TaskStatus.RUNNING);
    // RUNNING -> CANCELLED is illegal
    assertThrows(IllegalStateException.class, () -> store.transition("t1", TaskStatus.CANCELLED));
    store.transition("t1", TaskStatus.COMPLETED);
    // terminal states allow nothing
    assertThrows(IllegalStateException.class, () -> store.transition("t1", TaskStatus.QUEUED));
    assertThrows(IllegalStateException.class, () -> store.transition("t1", TaskStatus.RUNNING));
  }

  @Test
  void duplicateCreateThrows() {
    InMemoryTaskStore store = new InMemoryTaskStore();
    store.create(rec("t1"));
    assertThrows(IllegalStateException.class, () -> store.create(rec("t1")));
  }

  @Test
  void unknownTaskThrows() {
    InMemoryTaskStore store = new InMemoryTaskStore();
    assertThrows(IllegalStateException.class, () -> store.transition("nope", TaskStatus.RUNNING));
    assertTrue(store.get("nope").isEmpty());
  }
}
