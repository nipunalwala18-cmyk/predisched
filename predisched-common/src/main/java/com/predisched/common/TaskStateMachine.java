package com.predisched.common;

import com.predisched.proto.TaskStatus;

/**
 * The only place that knows legal task transitions (spec section 3.3).
 *
 * <pre>
 * QUEUED -&gt; RUNNING | CANCELLED
 * RUNNING -&gt; COMPLETED | FAILED | QUEUED (worker died, reassign)
 * COMPLETED, FAILED, CANCELLED are terminal.
 * </pre>
 */
public final class TaskStateMachine {

    private TaskStateMachine() {}

    public static boolean isLegal(TaskStatus from, TaskStatus to) {
        if (from == null || to == null) {
            return false;
        }
        if (from == to) {
            return false;
        }
        switch (from) {
            case QUEUED:
                return to == TaskStatus.RUNNING || to == TaskStatus.CANCELLED;
            case RUNNING:
                return to == TaskStatus.COMPLETED
                        || to == TaskStatus.FAILED
                        || to == TaskStatus.QUEUED;
            case COMPLETED:
            case FAILED:
            case CANCELLED:
            case UNRECOGNIZED:
            default:
                return false;
        }
    }

    public static void checkTransition(TaskStatus from, TaskStatus to) {
        if (!isLegal(from, to)) {
            throw new IllegalStateException("Illegal task transition: " + from + " -> " + to);
        }
    }
}
