package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.clock.EventLog;
import com.predisched.common.clock.LamportClientInterceptor;
import com.predisched.common.clock.LamportClock;
import com.predisched.common.clock.LamportServerInterceptor;
import com.predisched.common.clock.NodeClock;
import com.predisched.common.clock.NodeContext;
import com.predisched.common.grpc.Channels;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.strategy.RoundRobinStrategy;
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

/**
 * Client → scheduler → worker → scheduler over real and in-process channels with
 * interceptors installed everywhere: the Lamport time strictly increases along the
 * causal chain for one task.
 */
class InterceptorIT {

  private final LamportClock clientClock = new LamportClock();
  private final LamportClock schedClock = new LamportClock();
  private final LamportClock workerClock = new LamportClock();
  private final LamportClientInterceptor clientInterceptor =
      new LamportClientInterceptor("client-it", clientClock);
  private final LamportServerInterceptor schedInterceptor =
      new LamportServerInterceptor("sched-it", schedClock);
  private final LamportServerInterceptor workerInterceptor =
      new LamportServerInterceptor("worker-it", workerClock);

  private Server workerServer;
  private WorkerServiceImpl workerService;
  private Server schedulerServer;
  private ManagedChannel clientChannel;
  private ManagedChannel pollChannel;
  private Dispatcher dispatcher;
  private Channels channels;

  @BeforeEach
  void setUp() throws Exception {
    NodeContext wctx =
        new NodeContext("worker-it", workerClock, new NodeClock(0, 0), new EventLog());
    workerService = new WorkerServiceImpl("worker-it", 4, 100, 1.0, new WorkerMetrics(), wctx);
    workerServer =
        ServerBuilder.forPort(0)
            .intercept(workerInterceptor)
            .addService(workerService)
            .build()
            .start();

    NodeContext sctx =
        new NodeContext("sched-it", schedClock, new NodeClock(0, 0), new EventLog());
    WorkerRegistry registry = new WorkerRegistry(300_000, sctx.wall());
    registry.register(
        RegisterRequest.newBuilder()
            .setWorkerId("worker-it")
            .setHost("localhost")
            .setPort(workerServer.getPort())
            .setCores(4)
            .setMemoryMb(4096)
            .setPoolSize(4)
            .build());
    TaskStore store = new InMemoryTaskStore();
    TaskQueue queue = new TaskQueue();
    channels = new Channels("sched-it", schedClock);
    dispatcher = new Dispatcher(store, queue, new RoundRobinStrategy(), registry, channels, sctx);
    dispatcher.start();

    String name = "intercept-sched-" + UUID.randomUUID();
    schedulerServer =
        InProcessServerBuilder.forName(name)
            .intercept(schedInterceptor)
            .addService(new SchedulerServiceImpl(store, queue, new ArrivalRate(sctx.wall()), sctx))
            .directExecutor()
            .build()
            .start();
    clientChannel =
        InProcessChannelBuilder.forName(name)
            .intercept(clientInterceptor)
            .directExecutor()
            .build();
    // Unintercepted channel for polling, so polls do not move the clocks under test.
    pollChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (dispatcher != null) {
      dispatcher.close();
    }
    if (clientChannel != null) {
      clientChannel.shutdownNow();
    }
    if (pollChannel != null) {
      pollChannel.shutdownNow();
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
  void lamportTimeIncreasesAlongCausalChain() {
    var submit = SchedulerServiceGrpc.newBlockingStub(clientChannel);
    var poll = SchedulerServiceGrpc.newBlockingStub(pollChannel);
    String id = "chain-" + UUID.randomUUID();
    var res =
        submit.submitTask(
            TaskRequest.newBuilder()
                .setTaskId(id)
                .setType(TaskType.CPU_TASK)
                .setInput("100")
                .setPriority(5)
                .build());
    assertTrue(res.getAccepted());

    long deadline = System.currentTimeMillis() + 15_000;
    while (System.currentTimeMillis() < deadline) {
      var status = poll.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build());
      if (status.getStatus() == TaskStatus.COMPLETED) {
        break;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    var done = poll.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build());
    assertEquals(TaskStatus.COMPLETED, done.getStatus());

    // One intercepted send from the client, then the chain scheduler → worker.
    assertEquals(1, clientInterceptor.lastSent());
    assertEquals(1, schedInterceptor.lastReceived());
    assertTrue(
        workerInterceptor.lastReceived() > schedInterceptor.lastReceived(),
        "worker received " + workerInterceptor.lastReceived());
    assertTrue(
        clientClock.current() < schedClock.current()
            && schedClock.current() < workerClock.current(),
        "clocks: client=" + clientClock.current() + " sched=" + schedClock.current()
            + " worker=" + workerClock.current());
  }
}
