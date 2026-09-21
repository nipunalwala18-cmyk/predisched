package com.predisched.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskType;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TaskValidatorTest {

    private final TaskValidator validator = new TaskValidator(4096);
    private final TaskStore store = new InMemoryTaskStore();

    private TaskRequest valid(String id) {
        return TaskRequest.newBuilder()
                .setTaskId(id)
                .setType(TaskType.CPU_TASK)
                .setInput("n=100")
                .setPriority(5)
                .build();
    }

    @Test
    public void validRequestHasNoErrors() {
        assertTrue(validator.validate(valid("t-1"), store).isEmpty());
    }

    @Test
    public void emptyIdIsRejected() {
        TaskRequest req = valid("t-1").toBuilder().setTaskId("  ").build();
        List<String> errors = validator.validate(req, store);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("task_id"));
    }

    @Test
    public void duplicateIdIsRejected() {
        store.put(TaskRecord.createQueued("dup", TaskType.CPU_TASK, "n=100", 5));
        List<String> errors = validator.validate(valid("dup"), store);
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("already exists"));
    }

    @Test
    public void unrecognizedTypeIsRejected() {
        TaskRequest req = valid("t-1").toBuilder().setTypeValue(999).build();
        List<String> errors = validator.validate(req, store);
        assertFalse(errors.isEmpty());
        assertTrue(errors.stream().anyMatch(e -> e.contains("type")));
    }

    @Test
    public void emptyInputIsRejected() {
        TaskRequest req = valid("t-1").toBuilder().setInput("").build();
        List<String> errors = validator.validate(req, store);
        assertTrue(errors.stream().anyMatch(e -> e.contains("input")));
    }

    @Test
    public void malformedInputIsRejected() {
        TaskRequest req = valid("t-1").toBuilder().setInput("not-a-kv").build();
        List<String> errors = validator.validate(req, store);
        assertTrue(errors.stream().anyMatch(e -> e.contains("well-formed")));
    }

    @Test
    public void oversizedInputIsRejected() {
        TaskValidator small = new TaskValidator(8);
        TaskRequest req = valid("t-1").toBuilder().setInput("n=123456789").build();
        List<String> errors = small.validate(req, store);
        assertTrue(errors.stream().anyMatch(e -> e.contains("size limit")));
    }

    @Test
    public void priorityOutOfRangeIsRejected() {
        for (int bad : new int[] {0, -1, 11, 100}) {
            TaskRequest req = valid("t-1").toBuilder().setPriority(bad).build();
            List<String> errors = validator.validate(req, store);
            assertTrue(errors.stream().anyMatch(e -> e.contains("priority")),
                    "priority " + bad + " should be rejected");
        }
    }

    @Test
    public void priorityBoundariesAreAccepted() {
        assertTrue(validator.validate(valid("a").toBuilder().setPriority(1).build(), store).isEmpty());
        assertTrue(validator.validate(valid("b").toBuilder().setPriority(10).build(), store).isEmpty());
    }

    @Test
    public void severalErrorsAreReturnedAtOnce() {
        TaskRequest req = TaskRequest.newBuilder()
                .setTaskId("")
                .setType(TaskType.SLEEP_TASK)
                .setInput("")
                .setPriority(11)
                .build();
        List<String> errors = validator.validate(req, store);
        assertTrue(errors.size() >= 3,
                "expected several errors at once, got: " + errors);
        assertTrue(errors.stream().anyMatch(e -> e.contains("task_id")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("input")));
        assertTrue(errors.stream().anyMatch(e -> e.contains("priority")));
    }
}
