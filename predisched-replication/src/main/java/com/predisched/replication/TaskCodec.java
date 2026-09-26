package com.predisched.replication;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskAttemptProto;
import com.predisched.proto.TaskRecordProto;
import java.util.ArrayList;
import java.util.List;

/** {@link TaskRecord} to bytes and back, through {@code TaskRecordProto}. Lossless. */
public final class TaskCodec {

    private TaskCodec() {}

    public static ByteString encode(TaskRecord record) {
        TaskRecordProto.Builder proto = TaskRecordProto.newBuilder()
                .setId(record.id())
                .setType(record.type())
                .setInput(record.input())
                .setPriority(record.priority())
                .setStatus(record.status())
                .setWorkerId(record.workerId())
                .setResult(record.result())
                .setSubmittedAt(record.submittedAt())
                .setStartedAt(record.startedAt())
                .setCompletedAt(record.completedAt())
                .setExecTimeMs(record.execTimeMs())
                .setTraceId(record.traceId())
                .setTimeoutMs(record.timeoutMs())
                .setMaxRetries(record.maxRetries())
                .setClientId(record.clientId())
                .setWorkflowId(record.workflowId());
        for (TaskAttempt attempt : record.attempts()) {
            proto.addAttempts(TaskAttemptProto.newBuilder()
                    .setAttempt(attempt.attempt())
                    .setWorkerId(attempt.workerId())
                    .setOutcome(attempt.outcome().name())
                    .setReason(attempt.reason())
                    .setStartedAtMs(attempt.startedAtMs())
                    .setEndedAtMs(attempt.endedAtMs())
                    .setExecTimeMs(attempt.execTimeMs()));
        }
        return proto.build().toByteString();
    }

    public static TaskRecord decode(ByteString bytes) {
        TaskRecordProto proto;
        try {
            proto = TaskRecordProto.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("not a TaskRecordProto: " + e.getMessage(), e);
        }
        List<TaskAttempt> attempts = new ArrayList<>(proto.getAttemptsCount());
        for (TaskAttemptProto attempt : proto.getAttemptsList()) {
            attempts.add(new TaskAttempt(
                    attempt.getAttempt(),
                    attempt.getWorkerId(),
                    TaskAttempt.Outcome.valueOf(attempt.getOutcome()),
                    attempt.getReason(),
                    attempt.getStartedAtMs(),
                    attempt.getEndedAtMs(),
                    attempt.getExecTimeMs()));
        }
        return TaskRecord.restore(
                proto.getId(), proto.getType(), proto.getInput(), proto.getPriority(),
                proto.getStatus(), proto.getWorkerId(), proto.getResult(), proto.getSubmittedAt(),
                proto.getStartedAt(), proto.getCompletedAt(), proto.getExecTimeMs(),
                proto.getTraceId(), proto.getTimeoutMs(), proto.getMaxRetries(), attempts)
                .withClientId(proto.getClientId())
                .withWorkflowId(proto.getWorkflowId());
    }
}
