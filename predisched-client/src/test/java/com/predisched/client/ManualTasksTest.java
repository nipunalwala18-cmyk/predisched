package com.predisched.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class ManualTasksTest {

    @Test
    void parsesTypePriorityAndInput() {
        ManualTasks.Line line = ManualTasks.parse("CPU_TASK 7 n=2000000");
        assertTrue(line.ok(), line.error());
        assertEquals(TaskType.CPU_TASK, line.type());
        assertEquals(7, line.priority());
        assertEquals("n=2000000", line.input());
    }

    @Test
    void priorityDefaultsAndShortCaseInsensitiveTypes() {
        ManualTasks.Line line = ManualTasks.parse("  sort n=1000, type=reversed ");
        assertTrue(line.ok(), line.error());
        assertEquals(TaskType.SORT_TASK, line.type());
        assertEquals(ManualTasks.DEFAULT_PRIORITY, line.priority());
        assertEquals("n=1000, type=reversed", line.input());
    }

    @Test
    void mistakesAreReportedBeforeSubmitting() {
        assertTrue(ManualTasks.parse("NOPE_TASK 5 n=1").error().contains("unknown task type"));
        assertTrue(ManualTasks.parse("unrecognized 5 n=1").error().contains("unknown task type"));
        assertTrue(ManualTasks.parse("CPU_TASK 11 n=100").error().contains("priority must be 1-10"));
        assertTrue(ManualTasks.parse("CPU_TASK 5").error().contains("e.g. n=2000000"));
        assertFalse(ManualTasks.parse("CPU_TASK 5 n=1").ok(), "n below its minimum");
        assertTrue(ManualTasks.skip("# comment"));
        assertTrue(ManualTasks.skip("   "));
    }

    @Test
    void everyExampleIsValidAndEveryAvailableTypeHasOne() {
        for (TaskType type : TaskInputSpec.knownTypes()) {
            String example = ManualTasks.EXAMPLES.get(type);
            assertTrue(example != null, "no example for " + type);
            assertTrue(TaskInputSpec.validate(type, example).isEmpty(), type + ": " + example);
        }
    }

    @Test
    void theSampleTasksFileParses() throws Exception {
        long tasks = 0;
        for (String text : Files.readAllLines(Paths.get("..", "workloads", "manual-tasks.txt"))) {
            if (!ManualTasks.skip(text)) {
                assertTrue(ManualTasks.parse(text).ok(), text);
                tasks++;
            }
        }
        assertEquals(6, tasks);
    }

    @Test
    void describeListsRequiredThenOptionalKeys() {
        assertEquals("size=<1..1000>, [threads=<1..64>], [procs=<1..64>],"
                + " [seed=<0..9223372036854775807>], [mode=local|threads|mpi]",
                TaskInputSpec.describe(TaskType.MATRIX_TASK));
        assertEquals("job=avg_exec|wordcount, dataset=<text>",
                TaskInputSpec.describe(TaskType.MAPREDUCE_TASK));
    }
}
