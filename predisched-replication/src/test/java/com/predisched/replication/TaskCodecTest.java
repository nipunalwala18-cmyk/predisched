package com.predisched.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.predisched.common.TaskAttempt;
import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import org.junit.jupiter.api.Test;

class TaskCodecTest {

    @Test
    void everyFieldSurvivesTheRoundTrip() {
        TaskRecord record = TaskRecord.createQueued(
                        "codec-1", TaskType.MATRIX_TASK, "size=50", 7, "trace-9", 1500L, 2)
                .withStatus(TaskStatus.RUNNING)
                .withWorkerId("worker-2")
                .withAttempt(new TaskAttempt(1, "worker-2", TaskAttempt.Outcome.TIMED_OUT,
                        "exceeded 1500 ms", 10L, 1510L, 1500L))
                .withResult("checksum=abc")
                .withExecTimeMs(42L);

        assertEquals(record, TaskCodec.decode(TaskCodec.encode(record)));
    }
}
