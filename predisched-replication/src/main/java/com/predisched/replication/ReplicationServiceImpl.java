package com.predisched.replication;

import com.predisched.proto.Ack;
import com.predisched.proto.ReadRequest;
import com.predisched.proto.ReadResponse;
import com.predisched.proto.ReplicateRequest;
import com.predisched.proto.ReplicationServiceGrpc;
import com.predisched.proto.SyncRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/** gRPC face of one replica: take a change, answer a read, stream the log for catch-up. */
public class ReplicationServiceImpl extends ReplicationServiceGrpc.ReplicationServiceImplBase {

    private final ReplicatedTaskStore store;

    public ReplicationServiceImpl(ReplicatedTaskStore store) {
        this.store = store;
    }

    @Override
    public void replicate(ReplicateRequest request, StreamObserver<Ack> observer) {
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
    public void syncFrom(SyncRequest request, StreamObserver<ReplicateRequest> observer) {
        for (ReplicationLog.Entry entry : store.local().log().from(request.getFromSeq())) {
            observer.onNext(entry.record().toRequest(entry.seqNo()));
        }
        observer.onCompleted();
    }
}
