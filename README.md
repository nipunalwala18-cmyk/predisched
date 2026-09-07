# PrediSched — Predictive Distributed Task Scheduler

A distributed task scheduling platform built with Java 17, gRPC, Protocol Buffers, Java Concurrency Utilities, and Lamport Logical Clocks.

---

## EXPERIMENT 1 — Client-Server Communication using RPC

### Aim
To implement and demonstrate client-server communication using **gRPC Remote Procedure Call (RPC)** in a distributed computing environment.

### Objective
1. Understand the concept of Remote Procedure Call (RPC) in distributed systems.
2. Implement a gRPC-based client-server architecture where the client submits computational tasks to a remote server.
3. Demonstrate that the server processes the task and returns a meaningful result to the client over the network.
4. Handle different task types (`CPU_TASK`, `MATRIX_TASK`, `SLEEP_TASK`) and error conditions gracefully.

### Architecture
```
Client (TaskClient.java)
   │
   │  gRPC RPC (Protocol Buffers)
   │  Port 50051
   ▼
Server (TaskServer.java)
   │
   ▼
Task Processing
   ├── CPU_TASK    → Sum of 1 to N
   ├── MATRIX_TASK → NxN Matrix Multiplication
   └── SLEEP_TASK  → Simulated I/O Delay
   │
   ▼
Response sent back to Client
```

### Execution Commands for Experiment 1

**Terminal 1 — Server:**
```bash
mvn exec:java@server
```

**Terminal 2 — Client:**
```bash
mvn exec:java@client
```

---

## EXPERIMENT 2 — Multithreading in a Distributed System

### Aim
To extend the PrediSched distributed architecture with a multithreaded worker node, demonstrating concurrent task execution, thread pool management, and performance scaling.

### Objective
1. Implement a `WorkerNode` component containing a configurable thread pool (`ExecutorService`).
2. Offload task execution from single-threaded handlers to a pool of worker threads.
3. Demonstrate that independent computational tasks run concurrently across separate worker threads.
4. Measure and compare execution time, throughput, and speedup across **1 Thread**, **2 Threads**, and **4 Threads**.

### Architecture
```
Client (TaskClient.java)
   │
   │ gRPC RPC
   ▼
Task Server (TaskServer.java)
   │
   │ Dispatches tasks
   ▼
Worker Node (WorkerNode.java)
   │
   ├── Thread-1 → Task 1 (e.g. SLEEP_TASK 2000ms)
   ├── Thread-2 → Task 2 (e.g. SLEEP_TASK 2000ms)
   ├── Thread-3 → Task 3 (e.g. SLEEP_TASK 2000ms)
   └── Thread-4 → Task 4 (e.g. SLEEP_TASK 2000ms)
```

### Execution Commands for Experiment 2

**Run Multithreading Performance Benchmark (1 vs 2 vs 4 Threads):**
```bash
mvn exec:java@benchmark
```

**Run Standalone Multithreaded Worker Node:**
```bash
mvn exec:java@worker
```

---

## EXPERIMENT 3 — Clock Synchronization / Lamport Logical Clock

### Aim
To implement and demonstrate distributed event ordering using **Lamport Logical Clocks** across multiple independent distributed nodes (`Client`, `Scheduler`, `Worker-1`, `Worker-2`).

### Objective
1. Understand the problem of event ordering in distributed systems without shared physical clocks.
2. Implement Leslie Lamport's Logical Clock algorithm rules:
   - **Rule 1 (Local Event)**: $L_i = L_i + 1$
   - **Rule 2 (Send Message)**: $L_i = L_i + 1$; attach timestamp $L_i$ to message.
   - **Rule 3 (Receive Message)**: $L_j = \max(L_j, T_{\text{recv}}) + 1$
3. Assign logical timestamps to all distributed lifecycle events (`TASK_SUBMIT`, `SEND`, `RECEIVE`, `TASK_DISPATCH`, `TASK_START`, `TASK_COMPLETE`).
4. Demonstrate causal ordering preservation across multiple independent workers.

### Architecture & Integrated Event Flow
```
Client (LogicalClock = 0)
   │
   │ [TASK_SUBMIT: L=1, SEND: L=2]
   ▼
Scheduler (LogicalClock = 0 → L=3 on RECEIVE)
   │
   │ [TASK_DISPATCH / SEND: L=4]
   ▼
Worker-1 (LogicalClock = 0 → L=5 on RECEIVE)
   │
   ├── [TASK_START: L=6]
   ├── [TASK_COMPLETE: L=7]
   └── [SEND result: L=8]
   │
   ▼
Scheduler (LogicalClock L=4 → L=9 on RECEIVE L=8)
```

---

### Comprehensive Project Structure

```
PrediSched/
│
├── pom.xml                                     ← Maven build configuration
├── README.md                                   ← Lab documentation
│
├── src/main/proto/
│   └── task.proto                              ← Protocol Buffers service definition
│
├── src/main/java/com/predisched/
│   ├── TaskServer.java                         ← gRPC Server with integrated WorkerNode
│   ├── TaskClient.java                         ← Interactive gRPC Client
│   │
│   ├── worker/
│   │   ├── WorkerNode.java                     ← Multithreaded Worker Node component
│   │   ├── WorkerTaskExecutor.java             ← Task execution engine & metrics collector
│   │   └── MultithreadingBenchmark.java        ← Benchmark comparing 1 vs 2 vs 4 threads
│   │
│   └── clock/
│       ├── LogicalClock.java                   ← Lamport Logical Clock implementation
│       ├── ClockEvent.java                     ← Distributed event model
│       └── ClockDemo.java                      ← Executable Lamport clock demonstration
│
└── src/test/java/com/predisched/
    ├── worker/WorkerNodeTest.java              ← JUnit 5 tests for multithreading (7 tests)
    └── clock/LogicalClockTest.java             ← JUnit 5 tests for Lamport clock (8 tests)
```

---

### Execution Commands for Experiment 3

#### 1. Run Lamport Logical Clock Demonstration
```bash
mvn exec:java@clock
```

#### 2. Run All Unit Tests (JUnit 5 Suite — 15 Tests)
```bash
mvn test
```

---

### Sample Output — Lamport Logical Clock Demonstration

Command: `mvn exec:java@clock`

```
========================================
     PREDISCHED EXPERIMENT 3            
     LAMPORT LOGICAL CLOCK              
========================================

Nodes:
  Client      (LogicalClock = 0)
  Scheduler   (LogicalClock = 0)
  Worker-1    (LogicalClock = 0)
  Worker-2    (LogicalClock = 0)

── 1. Distributed Event Sequence for Task T001 ─────────────────────────

[Client]
EVENT     : TASK_SUBMIT
TASK ID   : T001
CLOCK     : 1

[Client]
EVENT     : SEND
TASK ID   : T001
CLOCK     : 2

[Scheduler]
EVENT     : RECEIVE
TASK ID   : T001
RECEIVED  : 2
CLOCK     : 3

[Scheduler]
EVENT     : TASK_DISPATCH
TASK ID   : T001
CLOCK     : 4

[Worker-1]
EVENT     : RECEIVE
TASK ID   : T001
RECEIVED  : 4
CLOCK     : 5

[Worker-1]
EVENT     : TASK_START
TASK ID   : T001
CLOCK     : 6

[Worker-1]
EVENT     : TASK_COMPLETE
TASK ID   : T001
CLOCK     : 7

[Worker-1]
EVENT     : SEND
TASK ID   : T001
CLOCK     : 8

[Scheduler]
EVENT     : RECEIVE
TASK ID   : T001
RECEIVED  : 8
CLOCK     : 9

── 3. Causal Ordering Demonstration ───────────────────────────────────

Worker-1 SEND       L=10
Scheduler RECEIVE  L=11  (Previous Clock = 6, Received = 10)

Calculation: max(6, 10) + 1 = 11
Result     : Logical ordering preserved.

========================================
       FINAL LOGICAL CLOCKS             
========================================
Node               Logical Clock  
----------------------------------------
Client             L=2            
Scheduler          L=9            
Worker-1           L=8            
Worker-2           L=3            
========================================

Note on Logical vs Physical Time:
  "Lamport clocks provide logical timestamps for ordering distributed
   events; they do not represent physical time."
```

---

### Result & Conclusion Summary

| Experiment | Distributed Concept | Technology / Pattern | Key Finding / Metric |
|------------|---------------------|----------------------|----------------------|
| **Exp 1** | Client-Server Communication | gRPC / Protocol Buffers | Synchronous RPC task execution & validation |
| **Exp 2** | Multithreading & Concurrency | `ExecutorService` Thread Pool | ~4x throughput speedup (8000ms → 2000ms across 4 threads) |
| **Exp 3** | Clock Synchronization | Lamport Logical Clocks | Causal event ordering ($L = \max(L_{\text{local}}, T_{\text{recv}}) + 1$) without physical clocks |

> **Important Distinction Note**: Lamport clocks provide logical timestamps for ordering distributed events based on causality; they do not represent physical wall-clock time, milliseconds, or CPU duration.
#   p r e d i s c h e d  
 