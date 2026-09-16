package com.predisched.client;

import com.predisched.common.clock.LamportClientInterceptor;
import com.predisched.common.clock.LamportClock;
import com.predisched.common.clock.NodeClock;
import com.predisched.proto.AdminServiceGrpc;
import com.predisched.proto.Ack;
import com.predisched.proto.ClockOffsets;
import com.predisched.proto.EmptyMsg;
import com.predisched.proto.EventsRequest;
import com.predisched.proto.EventsResponse;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.SetStrategyRequest;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Library class used by the CLI, the workload generator and the benchmark.
 */
public class SchedulerClient implements AutoCloseable {

  private final ManagedChannel channel;
  private final SchedulerServiceGrpc.SchedulerServiceBlockingStub stub;
  private final LamportClock lamport = new LamportClock();
  private final NodeClock wall = new NodeClock(0, 0);

  public SchedulerClient(String host, int port) {
    this.channel =
        ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .intercept(new LamportClientInterceptor("client", lamport))
            .build();
    this.stub = SchedulerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
  }

  SchedulerClient(SchedulerServiceGrpc.SchedulerServiceBlockingStub stub, ManagedChannel channel) {
    this.stub = stub;
    this.channel = channel;
  }

  public static String newId() {
    return UUID.randomUUID().toString();
  }

  public TaskResponse submit(String taskId, TaskType type, String input, int priority) {
    return stub.submitTask(
        TaskRequest.newBuilder()
            .setTaskId(taskId)
            .setType(type)
            .setInput(input)
            .setPriority(priority)
            .setLamportTime(lamport.tick())
            .build());
  }

  public TaskStatusResponse status(String taskId) {
    return stub.getTaskStatus(TaskStatusRequest.newBuilder().setTaskId(taskId).build());
  }

  public TaskResponse cancel(String taskId) {
    return stub.cancelTask(TaskStatusRequest.newBuilder().setTaskId(taskId).build());
  }

  /** Switch the scheduler's placement strategy without a restart. */
  public Ack setStrategy(String name) {
    return admin().setStrategy(SetStrategyRequest.newBuilder().setName(name).build());
  }

  /** Each node's current offset from the Berkeley daemon (empty before the first round). */
  public ClockOffsets clocks() {
    return admin().getClocks(EmptyMsg.newBuilder().build());
  }

  /** Cluster events, optionally for one task, Lamport-ordered. */
  public EventsResponse events(String taskId) {
    return admin()
        .getEvents(EventsRequest.newBuilder().setTaskId(taskId == null ? "" : taskId).build());
  }

  private AdminServiceGrpc.AdminServiceBlockingStub admin() {
    return AdminServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
  }

  /** Polls until the task reaches a terminal state. */
  public TaskStatusResponse watch(String taskId, long pollMs, long timeoutMs)
      throws InterruptedException {
    long deadline = wall.now() + timeoutMs;
    while (true) {
      TaskStatusResponse res = status(taskId);
      TaskStatus s = res.getStatus();
      if (s == TaskStatus.COMPLETED || s == TaskStatus.FAILED || s == TaskStatus.CANCELLED) {
        return res;
      }
      if (wall.now() > deadline) {
        return res;
      }
      Thread.sleep(pollMs);
    }
  }

  public static String[] splitAddress(String address) {
    return address.split(":");
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }
}
