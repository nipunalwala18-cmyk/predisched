package com.predisched.scheduler;

import com.predisched.common.clock.ClockSyncRecord;
import com.predisched.common.clock.Event;
import com.predisched.common.clock.NodeContext;
import com.predisched.proto.Ack;
import com.predisched.proto.AdminServiceGrpc;
import com.predisched.proto.ClockOffset;
import com.predisched.proto.ClockOffsets;
import com.predisched.proto.EmptyMsg;
import com.predisched.proto.EventProto;
import com.predisched.proto.EventsRequest;
import com.predisched.proto.EventsResponse;
import com.predisched.proto.SetStrategyRequest;
import com.predisched.scheduler.strategy.StrategyFactory;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operator RPCs: strategy switching, clock offsets, and the ordered event log. */
public class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {

  private static final Logger log = LoggerFactory.getLogger(AdminServiceImpl.class);

  private final Dispatcher dispatcher;
  private final StrategyFactory factory;
  private final Supplier<List<ClockSyncRecord>> clockRecords;
  private final NodeContext ctx;

  public AdminServiceImpl(
      Dispatcher dispatcher,
      StrategyFactory factory,
      Supplier<List<ClockSyncRecord>> clockRecords,
      NodeContext ctx) {
    this.dispatcher = dispatcher;
    this.factory = factory;
    this.clockRecords = clockRecords;
    this.ctx = ctx;
  }

  @Override
  public void setStrategy(SetStrategyRequest req, StreamObserver<Ack> obs) {
    try {
      var strategy = factory.create(req.getName());
      dispatcher.setStrategy(strategy);
      log.info("strategy switched to {}", strategy.name());
      obs.onNext(Ack.newBuilder().setOk(true).setMessage(strategy.name()).build());
    } catch (IllegalArgumentException e) {
      obs.onNext(Ack.newBuilder().setOk(false).setMessage(e.getMessage()).build());
    }
    obs.onCompleted();
  }

  @Override
  public void getClocks(EmptyMsg req, StreamObserver<ClockOffsets> obs) {
    var builder = ClockOffsets.newBuilder();
    for (ClockSyncRecord record : clockRecords.get()) {
      builder.addOffsets(
          ClockOffset.newBuilder()
              .setNodeId(record.nodeId())
              .setOffsetMs(record.offsetBeforeMs())
              .setRoundTs(record.ts())
              .build());
    }
    obs.onNext(builder.build());
    obs.onCompleted();
  }

  @Override
  public void getEvents(EventsRequest req, StreamObserver<EventsResponse> obs) {
    List<Event> events =
        req.getTaskId().isBlank() ? ctx.events().snapshot() : ctx.events().forTask(req.getTaskId());
    var builder = EventsResponse.newBuilder();
    events.stream()
        .sorted(Event.ORDER)
        .forEach(
            e ->
                builder.addEvents(
                    EventProto.newBuilder()
                        .setNodeId(e.nodeId())
                        .setLamport(e.lamport())
                        .setWallTimeMs(e.wallTimeMs())
                        .setType(e.type().name())
                        .setTaskId(e.taskId())
                        .setDetails(e.details())
                        .build()));
    obs.onNext(builder.build());
    obs.onCompleted();
  }
}
