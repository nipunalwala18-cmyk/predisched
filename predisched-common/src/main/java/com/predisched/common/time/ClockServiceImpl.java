package com.predisched.common.time;

import com.predisched.proto.Ack;
import com.predisched.proto.ClockAdjust;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.TimeRequest;
import com.predisched.proto.TimeResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Every node serves its own clock: the Berkeley daemon polls it with {@code GetTime} and corrects
 * it with {@code Adjust}, and a Cristian client asks a time server the same way (lab Exp 3).
 */
public class ClockServiceImpl extends ClockServiceGrpc.ClockServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(ClockServiceImpl.class);

    private final String nodeId;
    private final PhysicalClock clock;

    public ClockServiceImpl(String nodeId, PhysicalClock clock) {
        this.nodeId = nodeId;
        this.clock = clock;
    }

    @Override
    public void getTime(TimeRequest request, StreamObserver<TimeResponse> observer) {
        observer.onNext(TimeResponse.newBuilder()
                .setNodeTimeMs(clock.now())
                .setNodeId(nodeId)
                .build());
        observer.onCompleted();
    }

    @Override
    public void adjust(ClockAdjust request, StreamObserver<Ack> observer) {
        long before = clock.now();
        clock.adjust(request.getOffsetMs());
        log.info("Clock adjusted by {} ms ({} -> {})",
                request.getOffsetMs(), before, clock.now());
        observer.onNext(Ack.newBuilder()
                .setOk(true)
                .setMessage("adjusted " + request.getOffsetMs() + " ms")
                .build());
        observer.onCompleted();
    }
}
