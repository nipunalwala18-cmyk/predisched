package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.grpc.Channels;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.strategy.RotatingStrategy;
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

class TaskFlowIT {

  private Server workerServer;
  private Server schedulerServer;
  private ManagedChannel schedulerChannel;
  private SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;
  private Dispatcher dispatcher;
  private Channels channels;
  private TaskStore store;
  private TaskQueue queue;

  @BeforeEach
  void setUp() throws Exception {
    // Real worker on an ephemeral port.
    WorkerServiceImpl workerService =
        new WorkerServiceImpl("worker-it", 4, 100, 1.0, new WorkerMetrics());
    workerServer = ServerBuilder.forPort(0).addService(workerService).build().start();
    int workerPort = workerServer.getPort();

    store = new InMemoryTaskStore();
    queue = new TaskQueue();
    channels = new Channels();
    WorkerRegistry registry = new WorkerRegistry(60_000);
    registry.register(
        RegisterRequest.newBuilder()
            .setWorkerId("worker-it")
            .setHost("localhost")
            .setPort(workerPort)
            .setCores(4)
            .setMemoryMb(4096)
            .setPoolSize(4)
            .build());
    dispatcher = new Dispatcher(store, queue, new RotatingStrategy(), registry, channels);
    dispatcher.start();

    String name = "sched-" + UUID.randomUUID();
    schedulerServer =
        InProcessServerBuilder.forName(name)
            .addService(new SchedulerServiceImpl(store, queue, new ArrivalRate()))
            .directExecutor()
            .build()
            .start();
    schedulerChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    stub = SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
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
    if (channels != null) {
      channels.shutdown();
    }
  }

  private TaskStatusResponse waitFor(String id) {
    long deadline = System.currentTimeMillis() + 15_000;
    while (System.currentTimeMillis() < deadline) {
      TaskStatusResponse res =
          stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build());
      if (res.getStatus() == TaskStatus.COMPLETED
          || res.getStatus() == TaskStatus.FAILED
          || res.getStatus() == TaskStatus.CANCELLED) {
        return res;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build());
  }

  @Test
  void eachTypeReachesCompleted() {
    var cpu =
        stub.submitTask(
            TaskRequest.newBuilder()
                .setTaskId("cpu-" + UUID.randomUUID())
                .setType(TaskType.CPU_TASK)
                .setInput("100")
                .setPriority(5)
                .build());
    assertTrue(cpu.getAccepted());
    assertEquals(TaskStatus.COMPLETED, waitFor(cpu.getTaskId()).getStatus());
    assertEquals("5050", waitFor(cpu.getTaskId()).getResult());

    var matrix =
        stub.submitTask(
            TaskRequest.newBuilder()
                .setTaskId("mat-" + UUID.randomUUID())
                .setType(TaskType.MATRIX_TASK)
                .setInput("10")
                .setPriority(5)
                .build());
    assertTrue(matrix.getAccepted());
    TaskStatusResponse mres = waitFor(matrix.getTaskId());
    assertEquals(TaskStatus.COMPLETED, mres.getStatus());
    assertTrue(mres.getResult().startsWith("checksum="));

    var sleep =
        stub.submitTask(
            TaskRequest.newBuilder()
                .setTaskId("slp-" + UUID.randomUUID())
                .setType(TaskType.SLEEP_TASK)
                .setInput("10")
                .setPriority(5)
                .build());
    assertTrue(sleep.getAccepted());
    assertEquals(TaskStatus.COMPLETED, waitFor(sleep.getTaskId()).getStatus());
  }

  @Test
  void invalidTasksReturnEveryViolation() {
    var res =
        stub.submitTask(
            TaskRequest.newBuilder()
                .setTaskId("")
                .setType(TaskType.CPU_TASK)
                .setInput("abc")
                .setPriority(99)
                .build());
    assertFalse(res.getAccepted());
    assertTrue(res.getMessage().contains("task_id"));
    assertTrue(res.getMessage().contains("CPU_TASK"));
    assertTrue(res.getMessage().contains("priority"));
  }

  @Test
  void cancelQueuedTask() throws Exception {
    // Scheduler with no workers: tasks stay QUEUED so cancel succeeds.
    TaskStore s2 = new InMemoryTaskStore();
    TaskQueue q2 = new TaskQueue();
    Channels ch2 = new Channels();
    Dispatcher d2 =
        new Dispatcher(s2, q2, new RotatingStrategy(), new WorkerRegistry(60_000), ch2);
    d2.start();
    String name = "sched-cancel-" + UUID.randomUUID();
    Server srv =
        InProcessServerBuilder.forName(name)
            .addService(new SchedulerServiceImpl(s2, q2, new ArrivalRate()))
            .directExecutor()
            .build()
            .start();
    ManagedChannel ch =
        InProcessChannelBuilder.forName(name).directExecutor().build();
    try {
      var s = SchedulerServiceGrpc.newBlockingStub(ch);
      String id = "cancel-" + UUID.randomUUID();
      var sub =
          s.submitTask(
              TaskRequest.newBuilder()
                  .setTaskId(id)
                  .setType(TaskType.SLEEP_TASK)
                  .setInput("5000")
                  .setPriority(5)
                  .build());
      assertTrue(sub.getAccepted());
      var cancelled = s.cancelTask(TaskStatusRequest.newBuilder().setTaskId(id).build());
      assertTrue(cancelled.getAccepted(), cancelled.getMessage());
      var status = s.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build());
      assertEquals(TaskStatus.CANCELLED, status.getStatus());
    } finally {
      d2.close();
      ch.shutdownNow();
      srv.shutdownNow();
      ch2.shutdown();
    }
  }
}
