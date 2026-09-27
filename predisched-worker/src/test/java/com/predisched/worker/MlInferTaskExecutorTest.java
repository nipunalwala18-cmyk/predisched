package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.predisched.common.ExecutionResult;
import com.predisched.common.ResourceProfile;
import com.predisched.proto.TaskType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ML_INFER_TASK (prompt 16): command, validation, disabled state, and a live run when possible. */
class MlInferTaskExecutorTest {

    @Test
    void buildsTheInferCommand() {
        MlInferTaskExecutor executor = new MlInferTaskExecutor(
                new MlInferTaskExecutor.Settings("python3", Paths.get("ml"), 60_000));
        assertEquals(List.of("python3", "-m", "predisched_ml.infer", "--model", "overload",
                "--batch", "500", "--seed", "7", "--json"), executor.command("overload", 500, 7));
        assertEquals(TaskType.ML_INFER_TASK, executor.type());
        assertEquals(ResourceProfile.CPU_BOUND, executor.profile());
        assertFalse(executor.deterministic());
    }

    @Test
    void rejectsBadInputAndIsDisabledByDefault() throws Exception {
        ExecutorRegistry registry = new ExecutorRegistry();
        ExecutionResult bad = registry.execute(TaskType.ML_INFER_TASK, "model=nope, batch=10");
        assertFalse(bad.success());
        assertTrue(bad.errorMessage().contains("model"), bad.errorMessage());
        ExecutionResult disabled = registry.execute(TaskType.ML_INFER_TASK,
                "model=exec_time, batch=10");
        assertFalse(disabled.success());
        assertTrue(disabled.errorMessage().contains("ml.python is not configured"),
                disabled.errorMessage());
    }

    @Test
    void runsABatchWithTheLiveModelWhenTheVenvAndModelsExist() throws Exception {
        Path root = Paths.get("..").toAbsolutePath().normalize();
        Path python = root.resolve(Paths.get(".venv", "Scripts", "python.exe"));
        if (!Files.exists(python)) {
            python = root.resolve(Paths.get(".venv", "bin", "python"));
        }
        Path ml = root.resolve("ml");
        assumeTrue(Files.exists(python), "no repo venv");
        assumeTrue(Files.exists(ml.resolve(Paths.get("models", "registry.json"))),
                "no trained models: run python -m predisched_ml.train");
        MlInferTaskExecutor executor = new MlInferTaskExecutor(
                new MlInferTaskExecutor.Settings(python.toString(), ml, 120_000));
        ExecutionResult first = executor.execute("model=exec_time, batch=200, seed=3");
        assertTrue(first.success(), first.errorMessage());
        assertTrue(first.output().matches("model=exec_time version=\\d+ batch=200 seed=3 .*"
                + "checksum=[0-9.]+"), first.output());
        ExecutionResult again = executor.execute("model=exec_time, batch=200, seed=3");
        assertEquals(checksum(first.output()), checksum(again.output()), "same seed, same batch");
    }

    private static String checksum(String output) {
        return output.substring(output.indexOf("checksum="));
    }
}
