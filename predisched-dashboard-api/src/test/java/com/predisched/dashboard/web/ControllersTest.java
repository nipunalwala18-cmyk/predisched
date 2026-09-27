package com.predisched.dashboard.web;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.predisched.dashboard.DashboardProperties;
import com.predisched.dashboard.cluster.Cluster;
import com.predisched.dashboard.cluster.ClusterUnavailableException;
import com.predisched.dashboard.repo.Repositories;
import com.predisched.proto.AdminReply;
import com.predisched.proto.ClusterState;
import com.predisched.proto.SetStrategyResponse;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.WorkerEntry;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

/** Every controller against mocked cluster, repositories and jobs: shapes, codes, the admin key. */
@WebMvcTest
@Import({AdminGuard.class, ControllersTest.Config.class})
@TestPropertySource(properties = {"dashboard.admin-key=test-key",
        "dashboard.admin-rate-per-second=2"})
class ControllersTest {

    @TestConfiguration
    @EnableConfigurationProperties(DashboardProperties.class)
    static class Config {}

    @Autowired
    MockMvc mvc;

    @MockBean
    Cluster cluster;

    @MockBean
    Repositories repo;

    @MockBean
    Jobs jobs;

    @Autowired
    AdminGuard guard;

    @org.junit.jupiter.api.BeforeEach
    void fullBucket() {
        guard.reset();
    }

    private static ClusterState state() {
        return ClusterState.newBuilder().setNodeId("scheduler-1").setLeader(true).setLeaderId(1)
                .setStrategy("predictive").setModelVersions("m1=v1,m2=v1,m3=v1")
                .setQueueDepth(7).setRunning(3).setLiveMaeMs(Double.NaN)
                .addWorkers(WorkerEntry.newBuilder().setWorkerId("worker-1").setHealthy(true)
                        .setPoolSize(4).setHost("localhost").setPort(51061))
                .addWorkers(WorkerEntry.newBuilder().setWorkerId("worker-2").setHealthy(false))
                .build();
    }

    @Test
    void overviewCombinesStoredKpisWithTheLiveCluster() throws Exception {
        when(repo.kpis(60)).thenReturn(Map.of("completed", 120L, "mean_latency_ms", 150.0,
                "p95_latency_ms", 400.0, "with_deadline", 10L, "sla_met", 9L));
        when(cluster.state()).thenReturn(state());
        mvc.perform(get("/api/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.kpis.tasksPerSecond").value(2.0))
                .andExpect(jsonPath("$.kpis.slaCompliancePct").value(90.0))
                .andExpect(jsonPath("$.kpis.queueDepth").value(7))
                .andExpect(jsonPath("$.kpis.activeWorkers").value(1))
                .andExpect(jsonPath("$.cluster.strategy").value("predictive"))
                .andExpect(jsonPath("$.cluster.liveMaeMs").value(nullValue()))
                .andExpect(jsonPath("$.cluster.workers", hasSize(2)));
    }

    @Test
    void overviewStillAnswersWhenTheClusterIsDown() throws Exception {
        when(repo.kpis(60)).thenReturn(Map.of("completed", 0L, "with_deadline", 0L,
                "sla_met", 0L));
        when(cluster.state()).thenThrow(new ClusterUnavailableException("no primary"));
        mvc.perform(get("/api/overview"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cluster").value(nullValue()))
                .andExpect(jsonPath("$.clusterError").value("no primary"));
    }

    @Test
    void adminCallsNeedTheKey() throws Exception {
        when(cluster.setStrategy("least_loaded")).thenReturn(SetStrategyResponse.newBuilder()
                .setOk(true).setPrevious("round_robin").setCurrent("least_loaded").build());
        mvc.perform(post("/api/admin/strategy").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"strategy\":\"least_loaded\"}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/admin/strategy").header(AdminGuard.HEADER, "wrong")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"strategy\":\"least_loaded\"}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/admin/strategy").header(AdminGuard.HEADER, "test-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"strategy\":\"least_loaded\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.previous").value("round_robin"))
                .andExpect(jsonPath("$.current").value("least_loaded"));
        mvc.perform(post("/api/chaos/kill-primary"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void adminCallsAreRateLimited() throws Exception {
        when(cluster.pause(true)).thenReturn(AdminReply.newBuilder().setOk(true)
                .setMessage("paused").build());
        int limited = 0;
        for (int i = 0; i < 10; i++) {
            int code = mvc.perform(post("/api/admin/pause").header(AdminGuard.HEADER, "test-key"))
                    .andReturn().getResponse().getStatus();
            if (code == 429) {
                limited++;
            }
        }
        // 2 per second, bursts of 4: most of 10 immediate calls are refused.
        org.junit.jupiter.api.Assertions.assertTrue(limited >= 5, "refused " + limited);
    }

    @Test
    void refusedAdminCallsAreConflicts() throws Exception {
        when(cluster.drain("worker-9")).thenReturn(AdminReply.newBuilder().setOk(false)
                .setMessage("unknown worker: worker-9").build());
        mvc.perform(post("/api/admin/drain/worker-9").header(AdminGuard.HEADER, "test-key"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value("unknown worker: worker-9"));
    }

    @Test
    void chaosActionsCheckTheirInput() throws Exception {
        mvc.perform(post("/api/chaos/latency").header(AdminGuard.HEADER, "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"ms\": 100}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error", containsString("node")));
        mvc.perform(post("/api/chaos/meteor").header(AdminGuard.HEADER, "test-key"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void tasksListDetailAndSubmission() throws Exception {
        when(repo.tasks(eq("COMPLETED"), eq(null), anyInt())).thenReturn(List.of(
                Map.of("task_id", "t1", "status", "COMPLETED")));
        mvc.perform(get("/api/tasks?status=COMPLETED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tasks[0].task_id").value("t1"));
        when(repo.task("nope")).thenReturn(null);
        mvc.perform(get("/api/tasks/nope")).andExpect(status().isNotFound());
        when(cluster.submit(any(), eq("ApiKey k1"))).thenReturn(TaskResponse.newBuilder()
                .setTaskId("api-1").setAccepted(true).setMessage("queued").build());
        mvc.perform(post("/api/tasks").header("Authorization", "ApiKey k1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"taskId\":\"api-1\",\"type\":\"cpu_task\",\"input\":\"n=1000\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted").value(true));
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"TELEPORT_TASK\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void promotionRefusalIsAConflict() throws Exception {
        when(jobs.root()).thenReturn(Paths.get(".."));
        when(jobs.python()).thenReturn("python");
        when(jobs.run(any(), any(), anyLong())).thenReturn(new Jobs.Result(1,
                "refused: only 12 compared tasks, 200 needed (--force to override)"));
        mvc.perform(post("/api/models/2/promote").header(AdminGuard.HEADER, "test-key"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.promoted").value(false))
                .andExpect(jsonPath("$.output", containsString("200 needed")));
    }

    @Test
    void leaderListsEveryScheduler() throws Exception {
        when(cluster.schedulers()).thenReturn(List.of("a:1", "b:2"));
        when(cluster.allStates()).thenReturn(Map.of("a:1", state()));
        when(repo.events(any(), anyInt())).thenReturn(List.of());
        mvc.perform(get("/api/cluster/leader"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.leader").value(1))
                .andExpect(jsonPath("$.leaderAddress").value("a:1"))
                .andExpect(jsonPath("$.nodes[1].reachable").value(false));
    }

    @Test
    void clusterTroubleIs503() throws Exception {
        when(cluster.state()).thenThrow(new ClusterUnavailableException("no primary among [x]"));
        when(repo.workersLatest()).thenReturn(List.of());
        mvc.perform(get("/api/workers")).andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterError").value("no primary among [x]"));
        when(cluster.setStrategy(anyString()))
                .thenThrow(new ClusterUnavailableException("no primary among [x]"));
        mvc.perform(post("/api/admin/strategy").header(AdminGuard.HEADER, "test-key")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"strategy\":\"random\"}"))
                .andExpect(status().isServiceUnavailable());
    }
}
