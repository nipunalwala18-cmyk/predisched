package com.predisched.clock;

import java.util.ArrayList;
import java.util.List;

/**
 * ================================================================
 *  PrediSched — Experiment 3: Clock Synchronization
 *  ClockDemo.java — Lamport Logical Clock Demonstration
 * ================================================================
 *
 *  Demonstrates distributed event ordering using independent Lamport
 *  Logical Clocks across four distributed nodes:
 *   - Client
 *   - Scheduler
 *   - Worker-1
 *   - Worker-2
 */
public class ClockDemo {

    public static void main(String[] args) {
        System.out.println();
        System.out.println("========================================");
        System.out.println("     PREDISCHED EXPERIMENT 3            ");
        System.out.println("     LAMPORT LOGICAL CLOCK              ");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Nodes:");
        System.out.println("  Client      (LogicalClock = 0)");
        System.out.println("  Scheduler   (LogicalClock = 0)");
        System.out.println("  Worker-1    (LogicalClock = 0)");
        System.out.println("  Worker-2    (LogicalClock = 0)");
        System.out.println();

        // Initialize independent node clocks
        LogicalClock clientClock    = new LogicalClock("Client");
        LogicalClock schedulerClock = new LogicalClock("Scheduler");
        LogicalClock worker1Clock   = new LogicalClock("Worker-1");
        LogicalClock worker2Clock   = new LogicalClock("Worker-2");

        List<ClockEvent> eventHistory = new ArrayList<>();

        // ════════════════════════════════════════════════════════
        //  DEMONSTRATION 1: Single Task Distributed Sequence (T001)
        // ════════════════════════════════════════════════════════
        System.out.println("── 1. Distributed Event Sequence for Task T001 ─────────────────────────");
        System.out.println();

        // 1. Client TASK_SUBMIT (Local Event)
        long c1 = clientClock.localEvent();
        ClockEvent e1 = new ClockEvent("Client", ClockEvent.EventType.TASK_SUBMIT, "T001", c1, "Task created by client");
        e1.printFormatted();
        eventHistory.add(e1);

        // 2. Client SEND T001 to Scheduler
        long msgTimeClientToSend = clientClock.sendEvent();
        ClockEvent e2 = new ClockEvent("Client", ClockEvent.EventType.SEND, "T001", msgTimeClientToSend, "Sending T001 request over gRPC");
        e2.printFormatted();
        eventHistory.add(e2);

        // 3. Scheduler RECEIVE T001 from Client
        long s1 = schedulerClock.receiveEvent(msgTimeClientToSend);
        ClockEvent e3 = new ClockEvent("Scheduler", ClockEvent.EventType.RECEIVE, "T001", s1, msgTimeClientToSend, "Received T001 request from Client");
        e3.printFormatted();
        eventHistory.add(e3);

        // 4. Scheduler TASK_DISPATCH to Worker-1
        long msgTimeDispatch = schedulerClock.sendEvent();
        ClockEvent e4 = new ClockEvent("Scheduler", ClockEvent.EventType.TASK_DISPATCH, "T001", msgTimeDispatch, "Dispatching T001 to Worker-1");
        e4.printFormatted();
        eventHistory.add(e4);

        // 5. Worker-1 RECEIVE T001 from Scheduler
        long w1_1 = worker1Clock.receiveEvent(msgTimeDispatch);
        ClockEvent e5 = new ClockEvent("Worker-1", ClockEvent.EventType.RECEIVE, "T001", w1_1, msgTimeDispatch, "Received T001 from Scheduler");
        e5.printFormatted();
        eventHistory.add(e5);

        // 6. Worker-1 TASK_START
        long w1_2 = worker1Clock.localEvent();
        ClockEvent e6 = new ClockEvent("Worker-1", ClockEvent.EventType.TASK_START, "T001", w1_2, "Execution started on Thread-1");
        e6.printFormatted();
        eventHistory.add(e6);

        // 7. Worker-1 TASK_COMPLETE
        long w1_3 = worker1Clock.localEvent();
        ClockEvent e7 = new ClockEvent("Worker-1", ClockEvent.EventType.TASK_COMPLETE, "T001", w1_3, "Execution finished on Thread-1");
        e7.printFormatted();
        eventHistory.add(e7);

        // 8. Worker-1 SEND result to Scheduler
        long msgTimeWorkerResult = worker1Clock.sendEvent();
        ClockEvent e8 = new ClockEvent("Worker-1", ClockEvent.EventType.SEND, "T001", msgTimeWorkerResult, "Sending completion result to Scheduler");
        e8.printFormatted();
        eventHistory.add(e8);

        // 9. Scheduler RECEIVE result from Worker-1
        long s2 = schedulerClock.receiveEvent(msgTimeWorkerResult);
        ClockEvent e9 = new ClockEvent("Scheduler", ClockEvent.EventType.RECEIVE, "T001", s2, msgTimeWorkerResult, "Received completion result from Worker-1");
        e9.printFormatted();
        eventHistory.add(e9);

        // ════════════════════════════════════════════════════════
        //  DEMONSTRATION 2: Multiple Workers (T002 -> Worker-2)
        // ════════════════════════════════════════════════════════
        System.out.println("── 2. Concurrent Event Sequence for Task T002 on Worker-2 ──────────────");
        System.out.println();

        long c2 = clientClock.sendEvent();
        long s3 = schedulerClock.receiveEvent(c2);
        long s4 = schedulerClock.sendEvent();

        long w2_1 = worker2Clock.receiveEvent(s4);
        ClockEvent ew1 = new ClockEvent("Worker-2", ClockEvent.EventType.RECEIVE, "T002", w2_1, s4, "Received T002 from Scheduler");
        ew1.printFormatted();

        long w2_2 = worker2Clock.localEvent();
        ClockEvent ew2 = new ClockEvent("Worker-2", ClockEvent.EventType.TASK_START, "T002", w2_2, "Execution started on Worker-2");
        ew2.printFormatted();

        long w2_3 = worker2Clock.localEvent();
        ClockEvent ew3 = new ClockEvent("Worker-2", ClockEvent.EventType.TASK_COMPLETE, "T002", w2_3, "Execution completed on Worker-2");
        ew3.printFormatted();

        // ════════════════════════════════════════════════════════
        //  DEMONSTRATION 3: Causal Order & Clock Advancement Rule
        // ════════════════════════════════════════════════════════
        System.out.println("── 3. Causal Ordering Demonstration ───────────────────────────────────");
        System.out.println();

        // Simulate Worker-1 sending a message at L=10 when Scheduler is at L=6
        LogicalClock testWorker = new LogicalClock("Worker-1", 9);
        LogicalClock testScheduler = new LogicalClock("Scheduler", 6);

        long workerSendTime = testWorker.sendEvent(); // Worker-1 clock becomes 10
        System.out.println("Worker-1 SEND       L=" + workerSendTime);

        long schedPrevTime = testScheduler.getTime();
        long schedRecvTime = testScheduler.receiveEvent(workerSendTime); // Scheduler clock becomes max(6,10)+1 = 11

        System.out.println("Scheduler RECEIVE  L=" + schedRecvTime + "  (Previous Clock = " + schedPrevTime + ", Received = " + workerSendTime + ")");
        System.out.println();
        System.out.println("Calculation: max(" + schedPrevTime + ", " + workerSendTime + ") + 1 = " + schedRecvTime);
        System.out.println("Result     : Logical ordering preserved.");
        System.out.println();

        // ════════════════════════════════════════════════════════
        //  DEMONSTRATION 4: Final Logical Clocks Table
        // ════════════════════════════════════════════════════════
        System.out.println("========================================");
        System.out.println("       FINAL LOGICAL CLOCKS             ");
        System.out.println("========================================");
        System.out.printf("%-18s %-15s%n", "Node", "Logical Clock");
        System.out.println("----------------------------------------");
        System.out.printf("%-18s L=%-13d%n", clientClock.getNodeId(), clientClock.getTime());
        System.out.printf("%-18s L=%-13d%n", schedulerClock.getNodeId(), schedulerClock.getTime());
        System.out.printf("%-18s L=%-13d%n", worker1Clock.getNodeId(), worker1Clock.getTime());
        System.out.printf("%-18s L=%-13d%n", worker2Clock.getNodeId(), worker2Clock.getTime());
        System.out.println("========================================");
        System.out.println();

        // ════════════════════════════════════════════════════════
        //  Phase 11 — Important Distinction Note
        // ════════════════════════════════════════════════════════
        System.out.println("Note on Logical vs Physical Time:");
        System.out.println("  \"Lamport clocks provide logical timestamps for ordering distributed");
        System.out.println("   events; they do not represent physical time.\"");
        System.out.println("========================================");
        System.out.println();
    }
}
