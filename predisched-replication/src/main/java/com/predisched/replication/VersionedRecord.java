package com.predisched.replication;

import com.google.protobuf.ByteString;
import com.predisched.common.TaskRecord;
import com.predisched.proto.ReadResponse;
import com.predisched.proto.ReplicateRequest;

/**
 * One replica's copy of a task: the encoded record, its per-task version, the Lamport time of
 * the write that produced it, and the node that made that write.
 *
 * <p>Last-writer-wins: of two copies, the one with the higher Lamport time is newer; equal
 * times (concurrent writes on two nodes) are broken by the origin node id. Lamport clocks are
 * merged on every message, so a write made after seeing another always has the higher time,
 * and every replica applying this rule ends up with the same copy.
 */
public record VersionedRecord(
        String taskId, ByteString payload, long version, long lamportTime, String originNode,
        String op) {

    public static final String ENQUEUE = "ENQUEUE";
    public static final String UPDATE_STATUS = "UPDATE_STATUS";

    public boolean isNewerThan(VersionedRecord other) {
        if (other == null) {
            return true;
        }
        if (lamportTime != other.lamportTime) {
            return lamportTime > other.lamportTime;
        }
        return originNode.compareTo(other.originNode) > 0;
    }

    public TaskRecord task() {
        return TaskCodec.decode(payload);
    }

    public ReplicateRequest toRequest(long seqNo) {
        return ReplicateRequest.newBuilder()
                .setSeqNo(seqNo)
                .setOp(op)
                .setTaskId(taskId)
                .setPayload(payload)
                .setVersion(version)
                .setLamportTime(lamportTime)
                .setOriginNode(originNode)
                .build();
    }

    public static VersionedRecord from(ReplicateRequest request) {
        return new VersionedRecord(request.getTaskId(), request.getPayload(), request.getVersion(),
                request.getLamportTime(), request.getOriginNode(), request.getOp());
    }

    /** Null when the replica has no copy. */
    public static VersionedRecord from(ReadResponse response) {
        if (!response.getFound()) {
            return null;
        }
        return new VersionedRecord(response.getTaskId(), response.getPayload(),
                response.getVersion(), response.getLamportTime(), response.getOriginNode(),
                UPDATE_STATUS);
    }

    public ReadResponse toResponse() {
        return ReadResponse.newBuilder()
                .setTaskId(taskId)
                .setPayload(payload)
                .setVersion(version)
                .setLamportTime(lamportTime)
                .setOriginNode(originNode)
                .setFound(true)
                .build();
    }
}
