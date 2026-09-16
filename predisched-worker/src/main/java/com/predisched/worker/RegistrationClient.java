package com.predisched.worker;

import com.predisched.common.grpc.Channels;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.RegistryServiceGrpc;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers this worker with the scheduler on start (retrying with backoff until a
 * scheduler answers) and then sends a heartbeat every {@code heartbeatMs}.
 *
 * <p>A heartbeat answered with {@code ok=false} means the scheduler does not know
 * this worker, so the next tick re-registers first.
 */
public class RegistrationClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(RegistrationClient.class);

  private final String workerId;
  private final String host;
  private final int port;
  private final int cores;
  private final long memoryMb;
  private final int poolSize;
  private final String schedulerHost;
  private final int schedulerPort;
  private final long heartbeatMs;
  private final WorkerMetrics metrics;
  private final IntSupplier queueLen;
  private final Channels channels;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
      task -> {
        Thread thread = new Thread(task, "registration-heartbeat");
        thread.setDaemon(true);
        return thread;
      });
  private volatile boolean running = true;
  private volatile boolean registered;

  public RegistrationClient(
      String workerId,
      String host,
      int port,
      int cores,
      long memoryMb,
      int poolSize,
      String schedulerHost,
      int schedulerPort,
      long heartbeatMs,
      WorkerMetrics metrics,
      IntSupplier queueLen,
      Channels channels) {
    this.workerId = workerId;
    this.host = host;
    this.port = port;
    this.cores = cores;
    this.memoryMb = memoryMb;
    this.poolSize = poolSize;
    this.schedulerHost = schedulerHost;
    this.schedulerPort = schedulerPort;
    this.heartbeatMs = heartbeatMs;
    this.metrics = metrics;
    this.queueLen = queueLen;
    this.channels = channels;
  }

  /** Start registration in the background; heartbeats begin after the first success. */
  public void start() {
    Thread registerThread = new Thread(this::registerUntilSuccess, "registration");
    registerThread.setDaemon(true);
    registerThread.start();
  }

  private RegistryServiceGrpc.RegistryServiceBlockingStub stub() {
    return RegistryServiceGrpc.newBlockingStub(channels.get(schedulerHost, schedulerPort))
        .withDeadlineAfter(Math.max(heartbeatMs, 2000), TimeUnit.MILLISECONDS);
  }

  private void registerUntilSuccess() {
    long backoffMs = 500;
    while (running && !registered) {
      try {
        var ack =
            stub()
                .register(
                    RegisterRequest.newBuilder()
                        .setWorkerId(workerId)
                        .setHost(host)
                        .setPort(port)
                        .setCores(cores)
                        .setMemoryMb(memoryMb)
                        .setPoolSize(poolSize)
                        .build());
        if (ack.getOk()) {
          registered = true;
          log.info("registered with scheduler {}:{}", schedulerHost, schedulerPort);
          scheduler.scheduleAtFixedRate(
              this::sendHeartbeat, heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
          return;
        }
        log.warn("registration refused, retrying: {}", ack.getMessage());
      } catch (Exception e) {
        log.warn("registration failed, retrying in {} ms: {}", backoffMs, e);
      }
      try {
        Thread.sleep(backoffMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      backoffMs = Math.min(backoffMs * 2, 5000);
    }
  }

  private void sendHeartbeat() {
    if (!running) {
      return;
    }
    try {
      var ack =
          stub()
              .sendHeartbeat(
                  Heartbeat.newBuilder()
                      .setWorkerId(workerId)
                      .setCpuPct(metrics.cpuPct())
                      .setMemPct(metrics.memPct())
                      .setActiveThreads(metrics.activeThreads())
                      .setQueueLen(queueLen.getAsInt())
                      .setTasksCompleted(metrics.completedCount())
                      .setAvgExecMs(metrics.meanExecMs())
                      .build());
      if (!ack.getOk()) {
        log.warn("heartbeat rejected ({}), re-registering", ack.getMessage());
        registered = false;
        scheduler.shutdown();
        registerUntilSuccess();
      }
    } catch (Exception e) {
      log.warn("heartbeat failed: {}", e);
    }
  }

  public boolean isRegistered() {
    return registered;
  }

  @Override
  public void close() {
    running = false;
    scheduler.shutdownNow();
  }
}
