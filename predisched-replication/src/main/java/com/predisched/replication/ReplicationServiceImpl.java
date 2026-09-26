package com.predisched.replication;

import com.predisched.proto.Ack;
import com.predisched.proto.JoinRequest;
import com.predisched.proto.JoinResponse;
import com.predisched.proto.ReadRequest;
import com.predisched.proto.ReadResponse;
import com.predisched.proto.ReplicateRequest;
import com.predisched.proto.ReplicationServiceGrpc;
import com.predisched.proto.SyncRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;

/** gRPC face of one replica: take a change, answer a read, stream the log for catch-up. */
public class ReplicationServiceImpl extends ReplicationServiceGrpc.ReplicationServiceImplBase {

    private final ReplicatedTaskStore store;

    public ReplicationServiceImpl(ReplicatedTaskStore store) {
        this.store = store;
    }

    /**
     * Primary-backup answers only once the change is applied in sequence, which may wait for
     * earlier ones still in flight; the other modes apply by last-writer-wins and ack at once.
     */
    @Override
    public void replicate(ReplicateRequest request, StreamObserver<Ack> observer) {
        CompletableFuture<Ack> sequenced = store.mode().receive(request);
        if (sequenced != null) {
            sequenced.whenComplete((ack, error) -> {
                if (error != null) {
                    observer.onError(Status.UNAVAILABLE.withDescription(error.getMessage())
                            .asRuntimeException());
                } else {
                    observer.onNext(ack);
                    observer.onCompleted();
                }
            });
            return;
        }
        boolean applied = store.local().apply(VersionedRecord.from(request));
        observer.onNext(Ack.newBuilder()
                .setOk(true)
                .setMessage(applied ? "applied" : "already had a newer copy")
                .build());
        observer.onCompleted();
    }

    /** {@code local_only} returns this replica's copy; otherwise the mode's read (quorum if strong). */
    @Override
    public void read(ReadRequest request, StreamObserver<ReadResponse> observer) {
        try {
            VersionedRecord copy = request.getLocalOnly()
                    ? store.local().get(request.getTaskId())
                    : store.mode().read(request.getTaskId());
            observer.onNext(copy == null
                    ? ReadResponse.newBuilder().setTaskId(request.getTaskId()).setFound(false).build()
                    : copy.toResponse());
            observer.onCompleted();
        } catch (ReplicationException e) {
            observer.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void join(JoinRequest request, StreamObserver<JoinResponse> observer) {
        observer.onNext(store.mode().join(request));
        observer.onCompleted();
    }

    @Override
    public void syncFrom(SyncRequest request, StreamObserver<ReplicateRequest> observer) {
        for (ReplicationLog.Entry entry : store.local().log().from(request.getFromSeq())) {
            observer.onNext(entry.record().toRequest(entry.seqNo()));
        }
        observer.onCompleted();
    }
}
