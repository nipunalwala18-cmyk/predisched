package com.predisched.clock;

/**
 * ================================================================
 *  PrediSched — Experiment 3: Clock Synchronization
 *  ClockEvent.java — Distributed Event Representation
 * ================================================================
 *
 *  Represents a logical event occurring on a node in the distributed system.
 */
public class ClockEvent {

    public enum EventType {
        LOCAL,
        SEND,
        RECEIVE,
        TASK_SUBMIT,
        TASK_DISPATCH,
        TASK_START,
        TASK_COMPLETE
    }

    private final String nodeId;
    private final EventType eventType;
    private final String taskId;
    private final long timestamp;
    private final Long receivedTimestamp; // null if not a RECEIVE event
    private final String description;

    public ClockEvent(String nodeId, EventType eventType, String taskId, long timestamp, String description) {
        this(nodeId, eventType, taskId, timestamp, null, description);
    }

    public ClockEvent(String nodeId, EventType eventType, String taskId, long timestamp, Long receivedTimestamp, String description) {
        this.nodeId = nodeId;
        this.eventType = eventType;
        this.taskId = taskId;
        this.timestamp = timestamp;
        this.receivedTimestamp = receivedTimestamp;
        this.description = description;
    }

    public String getNodeId() { return nodeId; }
    public EventType getEventType() { return eventType; }
    public String getTaskId() { return taskId; }
    public long getTimestamp() { return timestamp; }
    public Long getReceivedTimestamp() { return receivedTimestamp; }
    public String getDescription() { return description; }

    public void printFormatted() {
        System.out.println("[" + nodeId + "]");
        System.out.println("EVENT     : " + eventType);
        System.out.println("TASK ID   : " + taskId);
        if (receivedTimestamp != null) {
            System.out.println("RECEIVED  : " + receivedTimestamp);
        }
        System.out.println("CLOCK     : " + timestamp);
        if (description != null && !description.isEmpty()) {
            System.out.println("DETAILS   : " + description);
        }
        System.out.println();
    }

    @Override
    public String toString() {
        if (receivedTimestamp != null) {
            return String.format("[%s] EVENT: %-13s | TASK: %-6s | RECV_L: %-4d | CLOCK: L=%d",
                    nodeId, eventType, taskId, receivedTimestamp, timestamp);
        } else {
            return String.format("[%s] EVENT: %-13s | TASK: %-6s | CLOCK: L=%d",
                    nodeId, eventType, taskId, timestamp);
        }
    }
}
