package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.grpc.Channels;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.proto.AdminServiceGrpc;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.SetStrategyRequest;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.strategy.StrategyFactory;
import com.predisched.worker.WorkerMetrics;
import com.predisched.worker.WorkerServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Switch strategy at runtime via AdminService; subsequent decisions carry the new name. */
class StrategySwitchIT {

  private Server workerServer;
  private WorkerServiceImpl workerService;
  private Server schedulerServer;
  private ManagedChannel schedulerChannel;
  private SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;
  private AdminServiceGrpc.AdminServiceBlockingStub admin;
  private Dispatcher dispatcher;
  private Channels channels;

  @BeforeEach
  void setUp() throws Exception {
    workerService = new WorkerServiceImpl("switch-w", 4, 100, 1.0, new WorkerMetrics());
    workerServer = ServerBuilder.forPort(0).addService(workerService).build();
    workerServer.start();

    WorkerRegistry registry = new WorkerRegistry(60_000);
    registry.register(
        RegisterRequest.newBuilder()
            .setWorkerId("switch-w")
            .setHost("localhost")
            .setPort(workerServer.getPort())
            .setCores(4)
            .setMemoryMb(4096)
            .setPoolSize(4)
            .build());

    TaskStore store = new InMemoryTaskStore();
    TaskQueue queue = new TaskQueue();
    channels = new Channels();
    StrategyFactory factory = new StrategyFactory(42, 0.4, 0.2, 0.4);
    dispatcher = new Dispatcher(store, queue, factory.create("round-robin"), registry, channels);
    dispatcher.start();

    String name = "switch-sched-" + UUID.randomUUID();
    schedulerServer =
        InProcessServerBuilder.forName(name)
            .addService(new SchedulerServiceImpl(store, queue, new ArrivalRate()))
            .addService(new AdminServiceImpl(dispatcher, factory))
            .directExecutor()
            .build()
            .start();
    schedulerChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    stub = SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
    admin = AdminServiceGrpc.newBlockingStub(schedulerChannel);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (dispatcher != null) {
      dispatcher.close();
    }
    if (schedulerChannel != null) {
      schedulerChannel.shutdownNow();
    }
    if (schedulerServer != null) {
      schedulerServer.shutdownNow();
    }
    if (workerServer != null) {
      workerServer.shutdownNow();
    }
    if (workerService != null) {
      workerService.shutdown();
    }
    if (channels != null) {
      channels.shutdown();
    }
  }

  @Test
  void switchChangesSubsequentDecisions() {
    String first = submitAndWait();
    assertEquals(TaskStatus.COMPLETED, statusOf(first));
    assertTrue(
        dispatcher.decisions().stream().allMatch(d -> d.strategy().equals("round-robin")));

    var ack =
        admin.setStrategy(SetStrategyRequest.newBuilder().setName("random").build());
    assertTrue(ack.getOk(), ack.getMessage());

    String second = submitAndWait();
    assertEquals(TaskStatus.COMPLETED, statusOf(second));
    var decisions = dispatcher.decisions();
    assertTrue(decisions.size() >= 2);
    assertEquals("random", decisions.get(decisions.size() - 1).strategy());

    var bad = admin.setStrategy(SetStrategyRequest.newBuilder().setName("magic").build());
    assertFalse(bad.getOk());
  }

  private String submitAndWait() {
    String id = "sw-" + UUID.randomUUID();
    var res =
        stub.submitTask(
            TaskRequest.newBuilder()
                .setTaskId(id)
                .setType(TaskType.CPU_TASK)
                .setInput("100")
                .setPriority(5)
                .build());
    assertTrue(res.getAccepted());
    long deadline = System.currentTimeMillis() + 15_000;
    while (System.currentTimeMillis() < deadline) {
      if (statusOf(id) == TaskStatus.COMPLETED) {
        return id;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new IllegalStateException("task did not complete: " + id);
  }

  private TaskStatus statusOf(String id) {
    return stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build()).getStatus();
  }
}
