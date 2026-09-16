package com.predisched.scheduler;

import com.predisched.common.clock.BerkeleyTimeDaemon;
import com.predisched.common.clock.ClockServiceImpl;
import com.predisched.common.clock.ClockSyncRecord;
import com.predisched.common.clock.EventLog;
import com.predisched.common.clock.LamportClock;
import com.predisched.common.clock.LamportServerInterceptor;
import com.predisched.common.clock.NodeClock;
import com.predisched.common.clock.NodeContext;
import com.predisched.common.config.NodeConfig;
import com.predisched.common.grpc.Channels;
import com.predisched.common.store.InMemoryTaskStore;
import com.predisched.common.store.TaskStore;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.TimeRequest;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Scheduler node: serves SchedulerService and dispatches queued tasks to workers. */
public class SchedulerNode {

  private static final Logger log = LoggerFactory.getLogger(SchedulerNode.class);

  private final NodeConfig config;
  private final TaskStore store = new InMemoryTaskStore();
  private final TaskQueue queue = new TaskQueue();
  private final LamportClock lamport = new LamportClock();
  private final Channels channels;
  private NodeClock wall;
  private NodeContext ctx;
  private Server server;
  private Dispatcher dispatcher;
  private BerkeleyTimeDaemon daemon;
  private ScheduledExecutorService daemonTimer;

  public SchedulerNode(NodeConfig config) {
    this.config = config;
    this.channels = new Channels(config.nodeId(), lamport);
  }

  public void start() throws Exception {
    MDC.put("node", config.nodeId());
    Map<String, Object> settings = config.settings();
    wall =
        new NodeClock(
            longSetting(settings, "simulatedOffsetMs", 0),
            doubleSetting(settings, "driftPpm", 0));
    ctx = new NodeContext(config.nodeId(), lamport, wall, new EventLog());
    Object timeout = settings.get("workerTimeoutMs");
    long aliveTimeoutMs = timeout == null ? 5000 : Long.parseLong(String.valueOf(timeout));
    StrategyFactory factory =
        new StrategyFactory(
            config.seed(),
            doubleSetting(settings, "wCpu", 0.4),
            doubleSetting(settings, "wMem", 0.2),
            doubleSetting(settings, "wQueue", 0.4));
    Object strategyName = settings.get("strategy");
    SchedulingStrategy initial;
    try {
      initial = factory.create(strategyName == null ? "round-robin" : strategyName.toString());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("refusing to start: " + e.getMessage(), e);
    }
    WorkerRegistry registry = new WorkerRegistry(aliveTimeoutMs, wall);
    ArrivalRate arrivals = new ArrivalRate(wall);
    dispatcher = new Dispatcher(store, queue, initial, registry, channels, ctx);
    dispatcher.start();
    daemon = new BerkeleyTimeDaemon(wall, config.nodeId(), grpcPeers(settings), longSetting(settings, "outlierMs", 1000));
    server =
        ServerBuilder.forPort(config.port())
            .intercept(new LamportServerInterceptor(config.nodeId(), lamport))
            .addService(new SchedulerServiceImpl(store, queue, arrivals, ctx))
            .addService(new RegistryServiceImpl(registry, ctx))
            .addService(
                new AdminServiceImpl(dispatcher, factory, () -> daemon.lastRound(), ctx))
            .addService(new ClockServiceImpl(wall))
            .build()
            .start();
    long syncIntervalMs = longSetting(settings, "syncIntervalMs", 5000);
    daemonTimer = Executors.newSingleThreadScheduledExecutor(
        task -> {
          Thread thread = new Thread(task, "berkeley-daemon");
          thread.setDaemon(true);
          return thread;
        });
    daemonTimer.scheduleAtFixedRate(
        () -> {
          MDC.put("node", config.nodeId());
          try {
            var round = daemon.synchronizeOnce();
            log.info(
                "berkeley round: avg offset applied, {} nodes, {} excluded",
                round.records().size(), round.excluded().size());
          } catch (Exception e) {
            log.warn("berkeley round failed: {}", e.toString());
          } finally {
            MDC.clear();
          }
        },
        syncIntervalMs,
        syncIntervalMs,
        TimeUnit.MILLISECONDS);
    log.info(
        "scheduler {} listening on {}:{} (strategy={})",
        config.nodeId(), config.host(), config.port(), initial.name());
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "scheduler-shutdown"));
  }

  /** gRPC peer clocks for the Berkeley daemon from {@code settings.timePeers}. */
  private List<BerkeleyTimeDaemon.PeerClock> grpcPeers(Map<String, Object> settings) {
    List<BerkeleyTimeDaemon.PeerClock> peers = new ArrayList<>();
    Object raw = settings.get("timePeers");
    if (raw instanceof List<?> list) {
      for (Object item : list) {
        if (item instanceof Map<?, ?> m) {
          Object hostObj = m.get("host");
          Object portObj = m.get("port");
          Object idObj = m.get("id");
          String host = hostObj == null ? "localhost" : String.valueOf(hostObj);
          int port = portObj == null ? 0 : Integer.parseInt(String.valueOf(portObj));
          String id = idObj == null ? host + ":" + port : String.valueOf(idObj);
          peers.add(new GrpcPeerClock(id, host, port));
        }
      }
    }
    return peers;
  }

  private class GrpcPeerClock implements BerkeleyTimeDaemon.PeerClock {
    private final String id;
    private final String host;
    private final int port;

    GrpcPeerClock(String id, String host, int port) {
      this.id = id;
      this.host = host;
      this.port = port;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public long read() {
      long before = wall.now();
      var stub =
          ClockServiceGrpc.newBlockingStub(channels.get(host, port))
              .withDeadlineAfter(2, TimeUnit.SECONDS);
      long serverTime =
          stub.getTime(TimeRequest.newBuilder().setRequesterTimeMs(before).build()).getNodeTimeMs();
      long after = wall.now();
      return serverTime + (after - before) / 2;
    }

    @Override
    public void adjust(long offsetMs) {
      var stub =
          ClockServiceGrpc.newBlockingStub(channels.get(host, port))
              .withDeadlineAfter(2, TimeUnit.SECONDS);
      stub.adjust(com.predisched.proto.ClockAdjust.newBuilder().setOffsetMs(offsetMs).build());
    }
  }

  static double doubleSetting(Map<String, Object> settings, String name, double def) {
    Object value = settings.get(name);
    return value == null ? def : Double.parseDouble(String.valueOf(value));
  }

  static long longSetting(Map<String, Object> settings, String name, long def) {
    Object value = settings.get(name);
    return value == null ? def : Long.parseLong(String.valueOf(value));
  }

  public List<ClockSyncRecord> lastClockRound() {
    return daemon == null ? List.of() : daemon.lastRound();
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public void stop() {
    try {
      if (daemonTimer != null) {
        daemonTimer.shutdownNow();
      }
      if (dispatcher != null) {
        dispatcher.close();
      }
      if (server != null) {
        server.shutdownNow().awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
      }
      channels.shutdown();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static void main(String[] args) throws Exception {
    NodeConfig config = NodeConfig.load(args);
    SchedulerNode node = new SchedulerNode(config);
    node.start();
    node.blockUntilShutdown();
  }
}
