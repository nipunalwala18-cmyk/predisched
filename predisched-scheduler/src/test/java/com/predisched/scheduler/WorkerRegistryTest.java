package com.predisched.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.Heartbeat;
import com.predisched.common.clock.NodeClock;
import com.predisched.proto.RegisterRequest;
import org.junit.jupiter.api.Test;

class WorkerRegistryTest {

  private static final NodeClock WALL = new NodeClock(0, 0);

  private static RegisterRequest register(String id) {
    return RegisterRequest.newBuilder()
        .setWorkerId(id)
        .setHost("localhost")
        .setPort(50261)
        .setCores(4)
        .setMemoryMb(4096)
        .setPoolSize(4)
        .build();
  }

  private static Heartbeat heartbeat(String id) {
    return Heartbeat.newBuilder()
        .setWorkerId(id)
        .setCpuPct(12.5)
        .setMemPct(33.0)
        .setActiveThreads(2)
        .setQueueLen(1)
        .setTasksCompleted(10)
        .setAvgExecMs(4.5)
        .build();
  }

  @Test
  void registerThenHeartbeatUpdatesSnapshot() {
    WorkerRegistry registry = new WorkerRegistry(5000, WALL);
    var ack = registry.register(register("w-1"));
    assertTrue(ack.getOk());
    assertEquals(1, registry.alive().size());

    var hbAck = registry.heartbeat(heartbeat("w-1"));
    assertTrue(hbAck.getOk());
    var info = registry.get("w-1").orElseThrow();
    assertEquals(12.5, info.cpuPct(), 1e-9);
    assertEquals(33.0, info.memPct(), 1e-9);
    assertEquals(2, info.activeThreads());
    assertEquals(1, info.queueLen());
    assertEquals(10, info.tasksCompleted());
    assertEquals(4.5, info.avgExecMs(), 1e-9);
  }

  @Test
  void unknownWorkerHeartbeatIsRejected() {
    WorkerRegistry registry = new WorkerRegistry(5000, WALL);
    var ack = registry.heartbeat(heartbeat("ghost"));
    assertFalse(ack.getOk());
    assertTrue(registry.alive().isEmpty());
  }

  @Test
  void blankIdRegistrationIsRejected() {
    WorkerRegistry registry = new WorkerRegistry(5000, WALL);
    var ack = registry.register(RegisterRequest.newBuilder().setWorkerId("").build());
    assertFalse(ack.getOk());
    assertEquals(0, registry.size());
  }

  @Test
  void staleWorkersLeaveAlive() throws Exception {
    WorkerRegistry registry = new WorkerRegistry(50, WALL);
    registry.register(register("w-1"));
    assertEquals(1, registry.alive().size());
    Thread.sleep(150);
    assertTrue(registry.alive().isEmpty());
    // A fresh heartbeat brings it back.
    assertTrue(registry.heartbeat(heartbeat("w-1")).getOk());
    assertEquals(1, registry.alive().size());
  }
}
