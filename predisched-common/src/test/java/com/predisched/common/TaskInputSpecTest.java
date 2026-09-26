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
        assertTrue(TaskInputSpec.validate(TaskType.HASH_TASK, "rounds=200000").isEmpty());
        assertTrue(TaskInputSpec.validate(
                TaskType.MONTE_CARLO_TASK, "samples=1000000, seed=7").isEmpty());
        assertTrue(TaskInputSpec.validate(TaskType.SORT_TASK, "n=5000, type=random").isEmpty());
        assertTrue(TaskInputSpec.validate(TaskType.SORT_TASK, "n=5000").isEmpty());
        assertTrue(TaskInputSpec.validate(
                TaskType.COMPRESS_TASK, "size_mb=50, level=6").isEmpty());
        assertTrue(TaskInputSpec.validate(
                TaskType.GRAPH_TASK, "nodes=100000, algo=bfs").isEmpty());
        assertTrue(TaskInputSpec.validate(TaskType.FILE_IO_TASK, "size_mb=100, mode=write").isEmpty());
        assertTrue(TaskInputSpec.validate(
                TaskType.HTTP_TASK, "url=http://localhost:8100/delay?ms=50, timeout=2000")
                .isEmpty());
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
        assertTrue(TaskInputSpec.isKnown(TaskType.GRAPH_TASK));
        assertTrue(TaskInputSpec.isKnown(TaskType.HTTP_TASK));
        assertEquals(
                List.of(
                        TaskType.CPU_TASK,
                        TaskType.SLEEP_TASK,
                        TaskType.MATRIX_TASK,
                        TaskType.HASH_TASK,
                        TaskType.MONTE_CARLO_TASK,
                        TaskType.SORT_TASK,
                        TaskType.COMPRESS_TASK,
                        TaskType.GRAPH_TASK,
                        TaskType.FILE_IO_TASK,
                        TaskType.HTTP_TASK,
                        TaskType.WORKFLOW_TASK),
                TaskInputSpec.knownTypes());
    }

    @Test
    void firstErrorJoinsEveryProblemAndIsNullWhenValid() {
        assertNull(TaskInputSpec.firstError(TaskType.CPU_TASK, "n=10"));
        assertNotNull(TaskInputSpec.firstError(TaskType.CPU_TASK, "n=1"));
    }

    @Test
    void rejectsUnknownEnumValues() {
        assertTrue(TaskInputSpec.validate(TaskType.SORT_TASK, "n=100, type=bogus").get(0)
                .contains("must be one of"));
        assertTrue(TaskInputSpec.validate(TaskType.GRAPH_TASK, "nodes=100, algo=dfs").get(0)
                .contains("must be one of"));
        assertTrue(TaskInputSpec.validate(TaskType.FILE_IO_TASK, "size_mb=1, mode=sideways").get(0)
                .contains("must be one of"));
    }

    @Test
    void rejectsMissingEnumKeyOnlyWhenRequired() {
        // All enum keys are optional today: absent is fine, present-but-wrong is not.
        assertTrue(TaskInputSpec.validate(TaskType.SORT_TASK, "n=100").isEmpty());
        assertTrue(TaskInputSpec.validate(TaskType.COMPRESS_TASK, "size_mb=1, level=10").get(0)
                .contains("between 1 and 9"));
    }

    @Test
    void rejectsNonHttpUrls() {
        assertTrue(TaskInputSpec.validate(TaskType.HTTP_TASK, "url=ftp://x/y, timeout=1000").get(0)
                .contains("must be an http(s) URL"));
        assertTrue(TaskInputSpec.validate(TaskType.HTTP_TASK, "timeout=1000").get(0)
                .contains("requires 'url="));
        assertTrue(TaskInputSpec.validate(
                        TaskType.HTTP_TASK, "url=http://localhost:8100/delay?ms=10, timeout=0")
                .get(0).contains("between 1 and 120000"));
    }

    @Test
    void reservedTypesSayWhereTheyArrive() {
        // WORKFLOW_TASK arrived in prompt 09: known, and it needs a dag file.
        assertTrue(TaskInputSpec.isKnown(TaskType.WORKFLOW_TASK));
        assertTrue(TaskInputSpec.validate(TaskType.WORKFLOW_TASK, "dag=x.json").isEmpty());
        assertTrue(TaskInputSpec.validate(TaskType.WORKFLOW_TASK, "x=1").get(0)
                .contains("requires 'dag="));
        assertTrue(TaskInputSpec.reservedReason(TaskType.DB_QUERY_TASK).contains("prompt 11"));
        assertTrue(TaskInputSpec.reservedReason(TaskType.MAPREDUCE_TASK).contains("prompt 12"));
        assertTrue(TaskInputSpec.reservedReason(TaskType.ML_INFER_TASK).contains("prompt 16"));
        assertTrue(TaskInputSpec.reservedReason(TaskType.IMAGE_TASK).contains("reserved"));
        assertNull(TaskInputSpec.reservedReason(TaskType.CPU_TASK));
    }
}
