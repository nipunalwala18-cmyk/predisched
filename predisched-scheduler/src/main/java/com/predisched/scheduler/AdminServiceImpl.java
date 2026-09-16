package com.predisched.scheduler;

import com.predisched.proto.Ack;
import com.predisched.proto.AdminServiceGrpc;
import com.predisched.proto.SetStrategyRequest;
import com.predisched.scheduler.strategy.StrategyFactory;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operator RPCs: switch the placement strategy without a restart. */
public class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {

  private static final Logger log = LoggerFactory.getLogger(AdminServiceImpl.class);

  private final Dispatcher dispatcher;
  private final StrategyFactory factory;

  public AdminServiceImpl(Dispatcher dispatcher, StrategyFactory factory) {
    this.dispatcher = dispatcher;
    this.factory = factory;
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
}
