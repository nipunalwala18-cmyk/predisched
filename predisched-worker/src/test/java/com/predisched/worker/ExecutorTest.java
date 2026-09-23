package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.ExecutionResult;
import com.predisched.proto.TaskType;
import org.junit.jupiter.api.Test;

public class ExecutorTest {

    private final ExecutorRegistry registry = new ExecutorRegistry();

    @Test
    public void cpuExecutorCountsPrimesDeterministically() {
        CpuTaskExecutor executor = new CpuTaskExecutor();
        ExecutionResult first = executor.execute("n=100");
        ExecutionResult second = executor.execute("n=100");
        assertTrue(first.success());
        assertEquals(first.output(), second.output());
        assertEquals("primes_below_100=25", first.output());
    }

    @Test
    public void sleepExecutorSleepsAndReports() {
        SleepTaskExecutor executor = new SleepTaskExecutor();
        ExecutionResult result = executor.execute("ms=5");
        assertTrue(result.success());
        assertEquals("slept_ms=5", result.output());
    }

    @Test
    public void matrixExecutorIsDeterministic() {
        MatrixTaskExecutor executor = new MatrixTaskExecutor();
        ExecutionResult first = executor.execute("size=5");
        ExecutionResult second = executor.execute("size=5");
        assertTrue(first.success());
        assertEquals(first.output(), second.output());
        assertTrue(first.output().startsWith("size=5 threads=1 checksum="));
    }

    @Test
    public void matrixChecksumIsTheSameHoweverManyThreadsRun() {
        String sequential = MatrixTaskExecutor.checksumOf(
                new MatrixTaskExecutor().execute("size=40").output());
        String parallel = MatrixTaskExecutor.checksumOf(
                new MatrixTaskExecutor().execute("size=40, threads=4").output());
        assertEquals(sequential, parallel);
    }

    @Test
    public void matrixRejectsAnUnusableThreadCount() {
        assertFalse(new MatrixTaskExecutor().execute("size=10, threads=0").success());
        assertFalse(new MatrixTaskExecutor().execute("size=10, threads=abc").success());
    }

    @Test
    public void badInputsBecomeFailedResults() {
        assertFalse(registry.execute(TaskType.CPU_TASK, "n=abc").success());
        assertFalse(registry.execute(TaskType.CPU_TASK, "garbage").success());
        assertFalse(registry.execute(TaskType.CPU_TASK, "").success());
        assertFalse(registry.execute(TaskType.SLEEP_TASK, "ms=abc").success());
        assertFalse(registry.execute(TaskType.MATRIX_TASK, "size=0").success());
        assertFalse(registry.execute(TaskType.MATRIX_TASK, "oops").success());
    }

    @Test
    public void unknownTypeBecomesFailedResult() {
        ExecutionResult result = registry.execute(TaskType.WORKFLOW_TASK, "dag=x.json");
        assertFalse(result.success());
    }

    @Test
    public void reservedTypesHaveNoExecutor() {
        for (TaskType type : new TaskType[] {
                TaskType.DB_QUERY_TASK, TaskType.MAPREDUCE_TASK, TaskType.ML_INFER_TASK,
                TaskType.IMAGE_TASK}) {
            assertFalse(registry.execute(type, "n=1").success(), type + " should be unsupported");
            assertTrue(registry.execute(type, "n=1").errorMessage().contains("unsupported"));
        }
    }

    @Test
    public void hashExecutorIsDeterministic() {
        HashTaskExecutor executor = new HashTaskExecutor();
        ExecutionResult first = executor.execute("rounds=5000");
        ExecutionResult second = executor.execute("rounds=5000");
        assertTrue(first.success());
        assertEquals(first.output(), second.output());
        assertTrue(first.output().matches("rounds=5000 digest=[0-9a-f]{64}"), first.output());
    }

    @Test
    public void hashExecutorRejectsBadInput() {
        assertFalse(registry.execute(TaskType.HASH_TASK, "rounds=0").success());
        assertFalse(registry.execute(TaskType.HASH_TASK, "rounds=abc").success());
        assertFalse(registry.execute(TaskType.HASH_TASK, "n=10").success());
    }

    @Test
    public void monteCarloEstimatesPiDeterministically() {
        MonteCarloTaskExecutor executor = new MonteCarloTaskExecutor();
        ExecutionResult first = executor.execute("samples=20000, seed=7");
        ExecutionResult second = executor.execute("samples=20000, seed=7");
        assertTrue(first.success());
        assertEquals(first.output(), second.output());
        assertTrue(first.output().contains("pi_estimate="), first.output());
        double estimate = Double.parseDouble(
                first.output().replaceAll(".*pi_estimate=([0-9.]+).*", "$1"));
        assertTrue(Math.abs(estimate - Math.PI) < 0.05, "estimate " + estimate + " is far from pi");
    }

    @Test
    public void monteCarloRejectsBadInput() {
        assertFalse(registry.execute(TaskType.MONTE_CARLO_TASK, "samples=0").success());
        assertFalse(registry.execute(TaskType.MONTE_CARLO_TASK, "samples=many").success());
    }

    @Test
    public void sortExecutorSortsEveryOrder() {
        SortTaskExecutor executor = new SortTaskExecutor();
        for (String order : new String[] {"random", "sorted", "reversed"}) {
            ExecutionResult result = executor.execute("n=2000, type=" + order);
            assertTrue(result.success(), order + ": " + result.errorMessage());
            assertTrue(result.output().contains("sorted=true"), result.output());
        }
        // The default order is random and deterministic for the same input.
        assertEquals(executor.execute("n=2000").output(), executor.execute("n=2000").output());
    }

    @Test
    public void sortExecutorRejectsBadInput() {
        assertFalse(registry.execute(TaskType.SORT_TASK, "n=1").success());
        assertFalse(registry.execute(TaskType.SORT_TASK, "n=100, type=bogus").success());
        assertFalse(registry.execute(TaskType.SORT_TASK, "oops").success());
    }

    @Test
    public void compressExecutorRoundTripsDeterministically() {
        CompressTaskExecutor executor = new CompressTaskExecutor();
        ExecutionResult first = executor.execute("size_mb=1, level=6");
        ExecutionResult second = executor.execute("size_mb=1, level=6");
        assertTrue(first.success(), first.errorMessage());
        assertEquals(first.output(), second.output());
        assertTrue(first.output().contains("raw_bytes=1048576"), first.output());
        assertTrue(first.output().contains("ratio="), first.output());
    }

    @Test
    public void compressLevelChangesTheCostCurve() {
        CompressTaskExecutor executor = new CompressTaskExecutor();
        String fast = executor.execute("size_mb=1, level=1").output();
        String best = executor.execute("size_mb=1, level=9").output();
        assertTrue(fast.startsWith("size_mb=1 level=1 "), fast);
        assertTrue(best.startsWith("size_mb=1 level=9 "), best);
    }

    @Test
    public void compressExecutorRejectsBadInput() {
        assertFalse(registry.execute(TaskType.COMPRESS_TASK, "size_mb=0").success());
        assertFalse(registry.execute(TaskType.COMPRESS_TASK, "size_mb=1, level=10").success());
        assertFalse(registry.execute(TaskType.COMPRESS_TASK, "size_mb=big").success());
    }

    @Test
    public void graphBfsVisitsTheWholeGraph() {
        GraphTaskExecutor executor = new GraphTaskExecutor();
        ExecutionResult first = executor.execute("nodes=500, algo=bfs, seed=3");
        ExecutionResult second = executor.execute("nodes=500, algo=bfs, seed=3");
        assertTrue(first.success(), first.errorMessage());
        assertEquals(first.output(), second.output());
        assertTrue(first.output().contains("visited=500"), first.output());
    }

    @Test
    public void graphPageRankSumsToOne() {
        ExecutionResult result = new GraphTaskExecutor().execute("nodes=500, algo=pagerank");
        assertTrue(result.success(), result.errorMessage());
        assertTrue(result.output().contains("rank_sum=1.000000"), result.output());
        assertEquals(result.output(),
                new GraphTaskExecutor().execute("nodes=500, algo=pagerank").output());
    }

    @Test
    public void graphExecutorRejectsBadInput() {
        assertFalse(registry.execute(TaskType.GRAPH_TASK, "nodes=1").success());
        assertFalse(registry.execute(TaskType.GRAPH_TASK, "nodes=100, algo=dfs").success());
    }

    @Test
    public void fileIoWritesAndReadsBack() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("predisched-test-");
        try {
            FileIoTaskExecutor executor = new FileIoTaskExecutor(dir);
            ExecutionResult result = executor.execute("size_mb=1, mode=both");
            assertTrue(result.success(), result.errorMessage());
            assertTrue(result.output().contains("written_bytes=1048576"), result.output());
            assertTrue(result.output().contains("sha256="), result.output());
            assertEquals(result.output(), executor.execute("size_mb=1, mode=both").output());
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    public void fileIoRejectsBadInput() {
        assertFalse(registry.execute(TaskType.FILE_IO_TASK, "size_mb=0").success());
        assertFalse(registry.execute(TaskType.FILE_IO_TASK, "size_mb=1, mode=sideways").success());
    }

    @Test
    public void fileIoLeavesNoTempFilesBehind() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("predisched-test-");
        try {
            FileIoTaskExecutor executor = new FileIoTaskExecutor(dir);
            assertTrue(executor.execute("size_mb=1, mode=write").success());
            assertTrue(executor.execute("size_mb=1, mode=read").success());
            try (var files = java.nio.file.Files.list(dir)) {
                assertEquals(0, files.count(), "temp files were left behind");
            }
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    public void fileIoLeavesNoTempFilesWhenCancelled() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("predisched-test-");
        try {
            FileIoTaskExecutor executor = new FileIoTaskExecutor(dir);
            Thread worker = new Thread(() -> executor.execute("size_mb=100, mode=both"));
            worker.start();
            // Wait until the temp file actually exists, so the interrupt lands mid-write.
            long deadline = System.currentTimeMillis() + 10_000;
            boolean sawFile = false;
            while (System.currentTimeMillis() < deadline && worker.isAlive()) {
                try (var files = java.nio.file.Files.list(dir)) {
                    if (files.findAny().isPresent()) {
                        sawFile = true;
                        break;
                    }
                }
                Thread.sleep(10);
            }
            assertTrue(sawFile, "the task never created its temp file");
            worker.interrupt();
            worker.join(10_000);
            assertFalse(worker.isAlive(), "interrupted FILE_IO_TASK did not stop");
            try (var files = java.nio.file.Files.list(dir)) {
                assertEquals(0, files.count(), "cancelled task left its temp file behind");
            }
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    public void httpExecutorFetchesFromTheMockService() throws Exception {
        com.sun.net.httpserver.HttpServer server =
                com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(0), 0);
        byte[] body = "predisched-ok".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        server.createContext("/delay", exchange -> {
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.createContext("/fail", exchange -> {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        try {
            int port = server.getAddress().getPort();
            HttpTaskExecutor executor = new HttpTaskExecutor();
            ExecutionResult ok =
                    executor.execute("url=http://localhost:" + port + "/delay?ms=5, timeout=5000");
            assertTrue(ok.success(), ok.errorMessage());
            assertTrue(ok.output().contains("status=200"), ok.output());
            assertTrue(ok.output().contains("bytes=" + body.length), ok.output());
            assertEquals(ok.output(),
                    executor.execute("url=http://localhost:" + port + "/delay?ms=5, timeout=5000")
                            .output());
            ExecutionResult failed =
                    executor.execute("url=http://localhost:" + port + "/fail?rate=1.0, timeout=5000");
            assertFalse(failed.success());
            assertTrue(failed.errorMessage().contains("500"), failed.errorMessage());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void httpExecutorRejectsBadInput() {
        assertFalse(registry.execute(TaskType.HTTP_TASK, "url=ftp://x/y, timeout=1000").success());
        assertFalse(registry.execute(TaskType.HTTP_TASK, "timeout=1000").success());
        assertFalse(registry.execute(
                TaskType.HTTP_TASK, "url=http://localhost:9/x, timeout=0").success());
    }

    private static void deleteRecursively(java.nio.file.Path dir) throws Exception {
        if (!java.nio.file.Files.exists(dir)) {
            return;
        }
        try (var walk = java.nio.file.Files.walk(dir)) {
            for (java.nio.file.Path path :
                    walk.sorted(java.util.Comparator.reverseOrder()).toList()) {
                java.nio.file.Files.deleteIfExists(path);
            }
        }
    }
}
