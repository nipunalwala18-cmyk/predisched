package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.ExecutionResult;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** MATRIX_TASK mode=mpi (prompt 14): command line, disabled state, JSON parsing, new keys. */
class MatrixTaskExecutorMpiTest {

    private final MatrixTaskExecutor.MpiSettings settings = new MatrixTaskExecutor.MpiSettings(
            "mpiexec", "python3", Paths.get("mpi"), 4, 60_000);

    @Test
    void buildsTheMpiexecCommand() {
        List<String> command = new MatrixTaskExecutor(settings).mpiCommand(400, 42, 4);
        assertEquals(List.of("mpiexec", "-n", "4", "python3", "-m", "predisched_mpi.matmul",
                "--size", "400", "--seed", "42", "--json"), command);
    }

    @Test
    void mpiModeIsDisabledWithoutMpiExecAndCappedByMaxProcs() throws Exception {
        ExecutionResult disabled = new MatrixTaskExecutor().execute("size=50, mode=mpi, procs=2");
        assertFalse(disabled.success());
        assertTrue(disabled.errorMessage().contains("mpi.exec is not configured"),
                disabled.errorMessage());
        ExecutionResult tooMany = new MatrixTaskExecutor(settings)
                .execute("size=50, mode=mpi, procs=8");
        assertFalse(tooMany.success());
        assertTrue(tooMany.errorMessage().contains("exceeds mpi.maxProcs=4"),
                tooMany.errorMessage());
    }

    @Test
    void localAndThreadModesKeepTheirChecksumAndSeedChangesIt() throws Exception {
        MatrixTaskExecutor executor = new MatrixTaskExecutor();
        String local = MatrixTaskExecutor.checksumOf(executor.execute("size=30").output());
        assertEquals(local, MatrixTaskExecutor.checksumOf(
                executor.execute("size=30, mode=local").output()));
        assertEquals(local, MatrixTaskExecutor.checksumOf(
                executor.execute("size=30, mode=threads, threads=3").output()));
        assertEquals(local, MatrixTaskExecutor.checksumOf(
                executor.execute("size=30, seed=42").output()), "42 is the default seed");
        assertFalse(local.equals(MatrixTaskExecutor.checksumOf(
                executor.execute("size=30, seed=1").output())));
    }

    @Test
    void parsesMatmulJsonLine() {
        Map<String, String> fields = MatrixTaskExecutor.parseJson("{\"size\": 400, \"procs\": 4,"
                + " \"seed\": 42, \"rows_per_rank\": [100, 100, 100, 100],"
                + " \"scatter_bcast_ms\": 3.43, \"compute_ms\": 2.293, \"gather_ms\": 2.003,"
                + " \"total_ms\": 6.027, \"checksum\": 16009981.32837095, \"verified\": true,"
                + " \"max_abs_diff\": 0.0}");
        assertEquals("16009981.32837095", fields.get("checksum"));
        assertEquals("true", fields.get("verified"));
        assertEquals("2.293", fields.get("compute_ms"));
    }

    @Test
    void inputRulesAcceptModeProcsAndSeed() {
        assertTrue(TaskInputSpec.validate(TaskType.MATRIX_TASK, "size=400, mode=mpi, procs=4")
                .isEmpty());
        assertFalse(TaskInputSpec.validate(TaskType.MATRIX_TASK, "size=400, mode=gpu").isEmpty());
        assertFalse(TaskInputSpec.validate(TaskType.MATRIX_TASK, "size=400, procs=0").isEmpty());
    }
}
