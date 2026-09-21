package com.predisched.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.TaskStatus;
import org.junit.jupiter.api.Test;

public class TaskStateMachineTest {

    @Test
    public void legalTransitionsAreAccepted() {
        assertTrue(TaskStateMachine.isLegal(TaskStatus.QUEUED, TaskStatus.RUNNING));
        assertTrue(TaskStateMachine.isLegal(TaskStatus.QUEUED, TaskStatus.CANCELLED));
        assertTrue(TaskStateMachine.isLegal(TaskStatus.RUNNING, TaskStatus.COMPLETED));
        assertTrue(TaskStateMachine.isLegal(TaskStatus.RUNNING, TaskStatus.FAILED));
        assertTrue(TaskStateMachine.isLegal(TaskStatus.RUNNING, TaskStatus.QUEUED));
    }

    @Test
    public void illegalTransitionsAreRejected() {
        // Wrong target from QUEUED.
        assertFalse(TaskStateMachine.isLegal(TaskStatus.QUEUED, TaskStatus.QUEUED));
        assertFalse(TaskStateMachine.isLegal(TaskStatus.QUEUED, TaskStatus.COMPLETED));
        assertFalse(TaskStateMachine.isLegal(TaskStatus.QUEUED, TaskStatus.FAILED));
        // No self loop and no cancel from RUNNING.
        assertFalse(TaskStateMachine.isLegal(TaskStatus.RUNNING, TaskStatus.RUNNING));
        assertFalse(TaskStateMachine.isLegal(TaskStatus.RUNNING, TaskStatus.CANCELLED));
        // Terminal states have no outgoing transitions.
        for (TaskStatus terminal
                : new TaskStatus[] {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED}) {
            for (TaskStatus target : TaskStatus.values()) {
                if (target == TaskStatus.UNRECOGNIZED) {
                    continue;
                }
                assertFalse(TaskStateMachine.isLegal(terminal, target),
                        terminal + " -> " + target + " should be illegal");
            }
        }
    }

    @Test
    public void checkTransitionThrowsOnIllegalMove() {
        assertThrows(IllegalStateException.class,
                () -> TaskStateMachine.checkTransition(TaskStatus.QUEUED, TaskStatus.COMPLETED));
        assertThrows(IllegalStateException.class,
                () -> TaskStateMachine.checkTransition(TaskStatus.COMPLETED, TaskStatus.QUEUED));
    }

    @Test
    public void recordEnforcesTransitions() {
        TaskRecord queued = TaskRecord.createQueued("t", com.predisched.proto.TaskType.CPU_TASK, "n=10", 5);
        // Legal: QUEUED -> RUNNING -> COMPLETED.
        TaskRecord running = queued.withStatus(TaskStatus.RUNNING);
        running.withStatus(TaskStatus.COMPLETED);
        // Illegal: QUEUED -> COMPLETED directly.
        assertThrows(IllegalStateException.class, () -> queued.withStatus(TaskStatus.COMPLETED));
    }
}
