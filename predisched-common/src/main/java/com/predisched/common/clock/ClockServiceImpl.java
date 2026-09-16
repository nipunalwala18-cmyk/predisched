package com.predisched.common.clock;

import com.predisched.proto.Ack;
import com.predisched.proto.ClockAdjust;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.TimeRequest;
import com.predisched.proto.TimeResponse;
import io.grpc.stub.StreamObserver;

/** Serves {@code ClockService} from a {@link NodeClock}. Runs on every node. */
public class ClockServiceImpl extends ClockServiceGrpc.ClockServiceImplBase {

  private final NodeClock clock;

  public ClockServiceImpl(NodeClock clock) {
    this.clock = clock;
  }

  @Override
  public void getTime(TimeRequest req, StreamObserver<TimeResponse> obs) {
    obs.onNext(TimeResponse.newBuilder().setNodeTimeMs(clock.now()).build());
    obs.onCompleted();
  }

  @Override
  public void adjust(ClockAdjust req, StreamObserver<Ack> obs) {
    clock.adjust(req.getOffsetMs());
    obs.onNext(Ack.newBuilder().setOk(true).setMessage("adjusted").build());
    obs.onCompleted();
  }
}
