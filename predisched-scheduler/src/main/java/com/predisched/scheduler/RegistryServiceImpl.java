package com.predisched.scheduler;

import com.predisched.proto.Ack;
import com.predisched.proto.Heartbeat;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.RegistryServiceGrpc;
import io.grpc.stub.StreamObserver;

/** gRPC front end for worker self-registration and heartbeats. */
public class RegistryServiceImpl extends RegistryServiceGrpc.RegistryServiceImplBase {

  private final WorkerRegistry registry;

  public RegistryServiceImpl(WorkerRegistry registry) {
    this.registry = registry;
  }

  @Override
  public void register(RegisterRequest req, StreamObserver<Ack> obs) {
    obs.onNext(registry.register(req));
    obs.onCompleted();
  }

  @Override
  public void sendHeartbeat(Heartbeat req, StreamObserver<Ack> obs) {
    obs.onNext(registry.heartbeat(req));
    obs.onCompleted();
  }
}
