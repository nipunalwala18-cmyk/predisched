package com.predisched.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.TaskType;
import java.util.List;
import org.junit.jupiter.api.Test;

class TaskInputSpecTest {

    @Test
    void acceptsWellFormedInputForEveryKnownType() {
        assertTrue(TaskInputSpec.validate(TaskType.CPU_TASK, "n=1000").isEmpty());
        assertTrue(TaskInputSpec.validate(TaskType.SLEEP_TASK, "ms=0").isEmpty());
        assertTrue(TaskInputSpec.validate(TaskType.MATRIX_TASK, "size=200").isEmpty());
    }

    @Test
    void rejectsNonNumericValue() {
        List<String> errors = TaskInputSpec.validate(TaskType.SLEEP_TASK, "ms=abc");
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("ms must be a number"), errors.get(0));
    }

    @Test
    void rejectsMissingKey() {
        List<String> errors = TaskInputSpec.validate(TaskType.CPU_TASK, "rounds=10");
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("requires 'n=<number>'"), errors.get(0));
    }

    @Test
    void rejectsValueOutsideRange() {
        assertTrue(TaskInputSpec.validate(TaskType.SLEEP_TASK, "ms=60001").get(0)
                .contains("between 0 and 60000"));
        assertTrue(TaskInputSpec.validate(TaskType.MATRIX_TASK, "size=0").get(0)
                .contains("between 1 and 1000"));
    }

    @Test
    void rejectsMalformedInputBeforeCheckingKeys() {
        List<String> errors = TaskInputSpec.validate(TaskType.CPU_TASK, "n");
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("key=value"), errors.get(0));
    }

    @Test
    void knowsWhichTypesHaveExecutors() {
        assertTrue(TaskInputSpec.isKnown(TaskType.CPU_TASK));
        assertTrue(TaskInputSpec.validate(TaskType.GRAPH_TASK, "nodes=10").isEmpty(),
                "a type with no rules yet only has to parse");
        assertEquals(List.of(TaskType.CPU_TASK, TaskType.SLEEP_TASK, TaskType.MATRIX_TASK),
                TaskInputSpec.knownTypes());
    }

    @Test
    void firstErrorJoinsEveryProblemAndIsNullWhenValid() {
        assertNull(TaskInputSpec.firstError(TaskType.CPU_TASK, "n=10"));
        assertNotNull(TaskInputSpec.firstError(TaskType.CPU_TASK, "n=1"));
    }
}
