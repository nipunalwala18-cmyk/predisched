package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 10 client threads × 100 submits against 3 in-process workers: exactly 1,000
 * COMPLETED records and no duplicate execution.
 */
class ConcurrentSubmitIT {

  private static final int CLIENTS = 10;
  private static final int PER_CLIENT = 100;

  private final List<Server> workerServers = new ArrayList<>();
  private final List<WorkerServiceImpl> workerServices = new ArrayList<>();
  private Server schedulerServer;
  private ManagedChannel schedulerChannel;
  private Dispatcher dispatcher;
  private Channels channels;
  private TaskStore store;
  private TaskQueue queue;

  @BeforeEach
  void setUp() throws Exception {
    WorkerRegistry registry = new WorkerRegistry(300_000);
    for (int w = 1; w <= 3; w++) {
      WorkerServiceImpl service =
          new WorkerServiceImpl("concurrent-w" + w, 8, 2000, 1.0, new WorkerMetrics());
      Server server = ServerBuilder.forPort(0).addService(service).build();
      server.start();
      workerServers.add(server);
      workerServices.add(service);
      registry.register(
          RegisterRequest.newBuilder()
              .setWorkerId(service.workerId())
              .setHost("localhost")
              .setPort(server.getPort())
              .setCores(4)
              .setMemoryMb(4096)
              .setPoolSize(8)
              .build());
    }

    store = new InMemoryTaskStore();
    queue = new TaskQueue();
    channels = new Channels();
    dispatcher = new Dispatcher(store, queue, new RoundRobinStrategy(), registry, channels);
    dispatcher.start();

    String name = "concurrent-sched-" + UUID.randomUUID();
    schedulerServer =
        InProcessServerBuilder.forName(name)
            .addService(new SchedulerServiceImpl(store, queue, new ArrivalRate()))
            .directExecutor()
            .build()
            .start();
    schedulerChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
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
    for (Server server : workerServers) {
      server.shutdownNow();
    }
    for (WorkerServiceImpl service : workerServices) {
      service.shutdown();
    }
    if (channels != null) {
      channels.shutdown();
    }
  }

  @Test
  void thousandConcurrentSubmitsCompleteExactlyOnce() throws Exception {
    var stub = SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
    ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<>();
    ExecutorService clients = Executors.newFixedThreadPool(CLIENTS);
    CountDownLatch ready = new CountDownLatch(CLIENTS);
    CountDownLatch done = new CountDownLatch(CLIENTS);
    for (int c = 0; c < CLIENTS; c++) {
      clients.execute(
          () -> {
            ready.countDown();
            try {
              ready.await();
              for (int i = 0; i < PER_CLIENT; i++) {
                String id = "cc-" + UUID.randomUUID();
                var res =
                    stub.submitTask(
                        TaskRequest.newBuilder()
                            .setTaskId(id)
                            .setType(TaskType.CPU_TASK)
                            .setInput("1000")
                            .setPriority(5)
                            .build());
                assertTrue(res.getAccepted(), res.getMessage());
                ids.add(id);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              done.countDown();
            }
          });
    }
    assertTrue(done.await(60, TimeUnit.SECONDS));
    clients.shutdownNow();
    assertEquals(CLIENTS * PER_CLIENT, ids.size());
    assertEquals(CLIENTS * PER_CLIENT, new HashSet<>(ids).size());

    // Every task reaches COMPLETED.
    long deadline = System.currentTimeMillis() + 120_000;
    Set<String> pending = new HashSet<>(ids);
    while (!pending.isEmpty()) {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("timed out, still pending: " + pending.size());
      }
      pending.removeIf(
          id -> {
            var status =
                stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(id).build());
            return status.getStatus() == TaskStatus.COMPLETED;
          });
      if (!pending.isEmpty()) {
        Thread.sleep(50);
      }
    }
    assertEquals(CLIENTS * PER_CLIENT, store.size());

    // No duplicate execution: each worker logs its task ids; the union has no repeats
    // and covers every submitted id exactly once.
    Set<String> union = new HashSet<>();
    for (WorkerServiceImpl service : workerServices) {
      for (String id : service.executedIds()) {
        assertTrue(union.add(id), "duplicate execution of " + id);
      }
    }
    assertEquals(new HashSet<>(ids), union);
  }
}
