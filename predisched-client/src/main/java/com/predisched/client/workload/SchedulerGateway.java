package com.predisched.client.workload;

import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;

/**
 * The scheduler calls a replayer makes, so tests can replay against an in-process fake while the
 * CLI replays against a real cluster. Implemented by {@code SchedulerClient}.
 */
public interface SchedulerGateway {

    TaskResponse submit(
            String taskId,
            TaskType type,
            String input,
            int priority,
            String traceId,
            long timeoutMs,
            int maxRetries);

    TaskStatusResponse status(String taskId);
}
