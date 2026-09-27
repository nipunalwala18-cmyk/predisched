package com.predisched.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.common.db.Db;
import com.predisched.dashboard.cluster.GrpcCluster;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.AdminServiceImpl;
import com.predisched.scheduler.Dispatcher;
import com.predisched.scheduler.DispatcherAdmin;
import com.predisched.scheduler.Leadership;
import com.predisched.scheduler.SchedulerServiceImpl;
import com.predisched.scheduler.WorkerRegistry;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.scheduler.strategy.RoundRobinStrategy;
import com.predisched.scheduler.strategy.StrategyRegistry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The API against a live in-process scheduler (real AdminService and SchedulerService over
 * gRPC) and a real, migrated PostgreSQL database: live numbers in the overview, and a strategy
 * switch that the scheduler really makes. Needs {@code PREDISCHED_TEST_DB_URL} (skipped without).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"dashboard.schedulers=inprocess-scheduler", "dashboard.admin-key=it-key"})
class IntegrationTest {

    static String dbUrl;
    static String adminUrl;
    static String dbName;
    static Server scheduler;
    static Server worker;
    static ManagedChannel workerChannel;
    static Dispatcher dispatcher;
    static RetryCoordinator retries;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) throws Exception {
        setUp();
        registry.add("spring.datasource.url", () -> dbUrl);
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "predisched");
    }

    @TestConfiguration
    static class InProcess {
        @Bean
        GrpcCluster.Channels channels() {
            return address -> InProcessChannelBuilder.forName(address).build();
        }
    }

    static void setUp() throws Exception {
        if (dbUrl != null) {
            return;
        }
        adminUrl = System.getenv("PREDISCHED_TEST_DB_URL");
        assumeTrue(adminUrl != null && !adminUrl.isBlank(),
                "no database: set PREDISCHED_TEST_DB_URL");
        dbName = "predisched_dash_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        try (Connection admin = DriverManager.getConnection(adminUrl, "postgres", "predisched");
                Statement s = admin.createStatement()) {
            s.execute("CREATE DATABASE " + dbName);
        }
        dbUrl = adminUrl.substring(0, adminUrl.lastIndexOf('/') + 1) + dbName;
        Db.open(dbUrl, "postgres", "predisched", 2, "it-migrate", true).close();

        // One fake worker that completes everything at once.
        String workerName = "it-worker-" + UUID.randomUUID();
        worker = InProcessServerBuilder.forName(workerName)
                .addService(new WorkerServiceGrpc.WorkerServiceImplBase() {
                    @Override
                    public void executeTask(ExecuteRequest request,
                            StreamObserver<ExecuteResult> observer) {
                        observer.onNext(ExecuteResult.newBuilder()
                                .setTaskId(request.getTask().getTaskId()).setSuccess(true)
                                .setOutput("ok").setExecTimeMs(2).build());
                        observer.onCompleted();
                    }
                }).build().start();
        workerChannel = InProcessChannelBuilder.forName(workerName).build();
        WorkerServiceGrpc.WorkerServiceBlockingStub stub =
                WorkerServiceGrpc.newBlockingStub(workerChannel);

        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
        retries = new RetryCoordinator(store, queue, new RetryPolicy(10, 100, 0, 3, 1),
                new DeadLetterQueue());
        WorkerRegistry registry = new WorkerRegistry(60_000);
        registry.register(RegisterRequest.newBuilder().setWorkerId("worker-1")
                .setHost("in-process").setPort(0).setCores(4).setMemoryMb(512).setPoolSize(4)
                .build());
        RunningTasks running = new RunningTasks();
        dispatcher = new Dispatcher(store, queue, registry, w -> stub, retries, running, 0L, 2.0,
                2, 20, new RoundRobinStrategy());
        SchedulerServiceImpl service = new SchedulerServiceImpl(store, new TaskValidator(4096),
                queue, retries);
        dispatcher.useArrivalRate(service.arrivals());
        StrategyRegistry.Settings settings = StrategyRegistry.Settings.defaults();
        DispatcherAdmin admin = new DispatcherAdmin(dispatcher, StrategyRegistry.standard(),
                settings, registry);
        service.useAdmin(admin);
        scheduler = InProcessServerBuilder.forName("inprocess-scheduler")
                .addService(service)
                .addService(new AdminServiceImpl(new AdminServiceImpl.Parts("scheduler-1",
                        dispatcher, admin, registry, running, queue, store, Leadership.ALONE,
                        null, List.of(), () -> service.arrivals().perSecond(), "none")))
                .build().start();
        dispatcher.start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (scheduler != null) {
            scheduler.shutdownNow();
            worker.shutdownNow();
            workerChannel.shutdownNow();
            dispatcher.close();
            retries.close();
        }
        if (dbName != null) {
            try (Connection admin = DriverManager.getConnection(adminUrl, "postgres", "predisched");
                    Statement s = admin.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)");
            } catch (Exception ignored) {
                // a leftover test database is harmless
            }
        }
    }

    @Autowired
    TestRestTemplate http;

    @BeforeAll
    static void needDatabase() {
        assumeTrue(System.getenv("PREDISCHED_TEST_DB_URL") != null);
    }

    @Test
    @SuppressWarnings("unchecked")
    void overviewShowsTheLiveSchedulerAndAStrategySwitchReachesIt() throws Exception {
        // Tasks through the REST API, into the real scheduler.
        for (int i = 0; i < 3; i++) {
            ResponseEntity<Map> submitted = http.postForEntity("/api/tasks",
                    Map.of("type", "CPU_TASK", "input", "n=1000", "taskId", "it-" + i), Map.class);
            assertEquals(202, submitted.getStatusCode().value(), String.valueOf(submitted.getBody()));
        }
        long deadline = System.currentTimeMillis() + 10_000;
        Map<String, Object> cluster;
        do {
            Map<String, Object> overview = http.getForObject("/api/overview", Map.class);
            cluster = (Map<String, Object>) overview.get("cluster");
            if (cluster != null && ((Number) cluster.get("tasksCompleted")).intValue() >= 3) {
                break;
            }
            Thread.sleep(50);
        } while (System.currentTimeMillis() < deadline);
        assertEquals("round_robin", cluster.get("strategy"));
        assertEquals(3, ((Number) cluster.get("tasksCompleted")).intValue());
        assertEquals(1, ((List<?>) cluster.get("workers")).size());
        assertEquals("scheduler-1", cluster.get("answeredBy"));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Admin-Key", "it-key");
        ResponseEntity<Map> switched = http.postForEntity("/api/admin/strategy",
                new HttpEntity<>("{\"strategy\":\"least_loaded\"}", headers), Map.class);
        assertEquals(200, switched.getStatusCode().value());
        assertEquals("least_loaded", switched.getBody().get("current"));
        assertEquals("least_loaded", dispatcher.strategy().name(), "the scheduler switched");
        Map<String, Object> after = (Map<String, Object>) http
                .getForObject("/api/overview", Map.class).get("cluster");
        assertEquals("least_loaded", after.get("strategy"));

        ResponseEntity<Map> paused = http.postForEntity("/api/admin/pause",
                new HttpEntity<>("{\"paused\":true}", headers), Map.class);
        assertEquals(200, paused.getStatusCode().value());
        assertTrue(dispatcher.isPaused());
        http.postForEntity("/api/admin/pause", new HttpEntity<>("{\"paused\":false}", headers),
                Map.class);
        assertTrue(!dispatcher.isPaused());
        // Stored reads work against the migrated schema (empty here: no history writer).
        ResponseEntity<Map> tasks = http.getForEntity("/api/tasks?limit=5", Map.class);
        assertEquals(200, tasks.getStatusCode().value());
        assertEquals(200, http.getForEntity("/api/queue/forecast", Map.class).getStatusCode()
                .value());
        assertEquals(200, http.getForEntity("/actuator/health", Map.class).getStatusCode().value());
    }
}
