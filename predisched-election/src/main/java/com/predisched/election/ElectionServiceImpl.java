package com.predisched.election;

import com.predisched.proto.Ack;
import com.predisched.proto.CoordinatorMsg;
import com.predisched.proto.ElectionMsg;
import com.predisched.proto.ElectionServiceGrpc;
import io.grpc.stub.StreamObserver;

/** gRPC face of a node's election: hands each message to the algorithm and returns its Ack. */
public class ElectionServiceImpl extends ElectionServiceGrpc.ElectionServiceImplBase {

    private final ElectionAlgorithm algorithm;

    public ElectionServiceImpl(ElectionAlgorithm algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public void election(ElectionMsg request, StreamObserver<Ack> observer) {
        reply(observer, algorithm.onElection(request));
    }

    @Override
    public void ringPass(ElectionMsg request, StreamObserver<Ack> observer) {
        reply(observer, algorithm.onRingPass(request));
    }

    @Override
    public void coordinator(CoordinatorMsg request, StreamObserver<Ack> observer) {
        reply(observer, algorithm.onCoordinator(request));
    }

    /** Liveness check; the reply also says who this node thinks the leader is. */
    @Override
    public void ping(Ack request, StreamObserver<Ack> observer) {
        reply(observer, Ack.newBuilder()
                .setOk(true)
                .setMessage(describe(algorithm))
                .build());
    }

    /** {@code node=3 leader=5 algorithm=bully}, or {@code leader=none} mid-election. */
    public static String describe(ElectionAlgorithm algorithm) {
        return "node=" + algorithm.cluster().selfId()
                + " leader=" + algorithm.leader().map(String::valueOf).orElse("none")
                + " algorithm=" + algorithm.name();
    }

    private static void reply(StreamObserver<Ack> observer, Ack ack) {
        observer.onNext(ack);
        observer.onCompleted();
    }
}
