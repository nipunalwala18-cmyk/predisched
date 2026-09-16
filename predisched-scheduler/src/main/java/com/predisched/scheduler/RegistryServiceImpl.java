package com.predisched.scheduler;

import com.predisched.common.clock.EventType;
import com.predisched.common.clock.NodeContext;
import com.predisched.proto.Ack;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.RegistryServiceGrpc;
import io.grpc.stub.StreamObserver;

/** gRPC front end for worker self-registration and heartbeats. */
public class RegistryServiceImpl extends RegistryServiceGrpc.RegistryServiceImplBase {

  private final WorkerRegistry registry;
  private final NodeContext ctx;

  public RegistryServiceImpl(WorkerRegistry registry, NodeContext ctx) {
    this.registry = registry;
    this.ctx = ctx;
  }

  @Override
  public void register(RegisterRequest req, StreamObserver<Ack> obs) {
    Ack ack = registry.register(req);
    if (ack.getOk()) {
      ctx.emit(EventType.REGISTER, "", "worker=" + req.getWorkerId());
    }
    obs.onNext(ack);
    obs.onCompleted();
  }

  @Override
  public void sendHeartbeat(Heartbeat req, StreamObserver<Ack> obs) {
    obs.onNext(registry.heartbeat(req));
    obs.onCompleted();
  }
}
