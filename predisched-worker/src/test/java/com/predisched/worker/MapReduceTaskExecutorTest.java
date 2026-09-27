package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.ExecutionResult;
import com.predisched.common.ResourceProfile;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

class MapReduceTaskExecutorTest {

    private final MapReduceTaskExecutor.Settings settings = new MapReduceTaskExecutor.Settings(
            Paths.get("opt", "spark").toString(), "python3", Paths.get("spark", "predisched_spark"),
            Paths.get("spark", "out", "tasks"), 60_000);

    @Test
    void buildsTheSparkSubmitCommandForEachJob() {
        MapReduceTaskExecutor executor = new MapReduceTaskExecutor(settings);
        List<String> command = executor.command("spark/data/execution_history.csv", "avg_exec",
                Paths.get("spark", "out", "tasks", "run1"));
        assertTrue(command.get(0).startsWith(Paths.get("opt", "spark", "bin", "spark-submit")
                .toString()), command.get(0));
        assertEquals(List.of("--master", "local[*]",
                Paths.get("spark", "predisched_spark", "exec_stats.py").toString(),
                "--input", "spark/data/execution_history.csv",
                "--output", Paths.get("spark", "out", "tasks", "run1").toString()),
                command.subList(1, command.size()));
        assertTrue(executor.command("spark/data/events.jsonl", "wordcount", Paths.get("o"))
                .get(3).endsWith("wordcount.py"));
        assertEquals(ResourceProfile.PARALLEL, executor.profile());
        assertFalse(executor.deterministic());
    }

    @Test
    void isDisabledWithoutSparkHome() throws Exception {
        MapReduceTaskExecutor executor =
                new MapReduceTaskExecutor(MapReduceTaskExecutor.Settings.DISABLED);
        assertFalse(executor.enabled());
        ExecutionResult result = executor.execute("dataset=x.csv, job=avg_exec");
        assertFalse(result.success());
        assertTrue(result.errorMessage().contains("spark.home is not configured"),
                result.errorMessage());
        assertTrue(executor.execute("dataset=x.csv, job=sort").errorMessage()
                .contains("job must be one of"), "input is validated first");
    }
}
