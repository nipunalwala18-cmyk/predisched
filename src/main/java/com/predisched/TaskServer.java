package com.predisched;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskServiceGrpc;
import com.predisched.worker.WorkerNode;
import com.predisched.worker.WorkerTaskExecutor;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * ================================================================
 *  PrediSched — Experiment 1 & 2 Integrated Server
 *  TaskServer.java — gRPC Server with Multithreaded Worker Node
 * ================================================================
 *
 *  Listens on port 50051 for gRPC RPC requests from clients.
 *  Dispatches incoming tasks to an integrated multithreaded WorkerNode
 *  thread pool for concurrent execution.
 */
public class TaskServer {

    private static final int PORT = 50051;
    private static WorkerNode workerNode;

    public static void main(String[] args) throws IOException, InterruptedException {
        int threads = 4;
        if (args.length > 0) {
            try {
                threads = Integer.parseInt(args[0]);
            } catch (NumberFormatException ignored) {}
        }

        // Initialize integrated WorkerNode with configurable thread pool
        workerNode = new WorkerNode("Worker-Server", threads);

        Server server = ServerBuilder.forPort(PORT)
                .addService(new TaskServiceImpl())
                .build();

        server.start();

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║       PrediSched Task Server (Exp 1 & 2)     ║");
        System.out.println("╠══════════════════════════════════════════════╣");
        System.out.println("║  Protocol    : gRPC / Protocol Buffers       ║");
        System.out.println("║  Port        : " + PORT + "                          ║");
        System.out.println("║  Worker Pool : " + String.format("%-29s", threads + " threads") + "║");
        System.out.println("║  Status      : RUNNING                       ║");
        System.out.println("╚══════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Waiting for client requests...");
        System.out.println();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("Server shutting down...");
            server.shutdown();
            workerNode.shutdown();
            System.out.println("Server stopped.");
        }));

        server.awaitTermination();
    }

    static class TaskServiceImpl extends TaskServiceGrpc.TaskServiceImplBase {

        @Override
        public void submitTask(TaskRequest request, StreamObserver<TaskResponse> responseObserver) {

            String taskId   = request.getTaskId();
            String taskType = request.getTaskType();
            String input    = request.getInput();
            int    priority = request.getPriority();

            System.out.println("────────────────────────────────────────────");
            System.out.println("  Task received:");
            System.out.println("  Task ID      : " + taskId);
            System.out.println("  Task Type    : " + taskType);
            System.out.println("  Input        : " + input);
            System.out.println("  Priority     : " + priority);
            System.out.println("────────────────────────────────────────────");

            // Validate task ID
            if (taskId == null || taskId.trim().isEmpty()) {
                TaskResponse errorResponse = TaskResponse.newBuilder()
                        .setTaskId("")
                        .setStatus("FAILED")
                        .setResult("")
                        .setMessage("Invalid task ID")
                        .build();

                System.out.println("  ✗ Rejected: Invalid task ID\n");
                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
                return;
            }

            // Offload task execution to WorkerNode thread pool
            try {
                Future<WorkerTaskExecutor.TaskExecutionResult> future = workerNode.submitTask(request);
                WorkerTaskExecutor.TaskExecutionResult executionResult = future.get();

                TaskResponse response = executionResult.toProtoResponse();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (Exception e) {
                TaskResponse errResponse = TaskResponse.newBuilder()
                        .setTaskId(taskId)
                        .setStatus("FAILED")
                        .setResult("")
                        .setMessage("Execution error: " + e.getMessage())
                        .build();
                responseObserver.onNext(errResponse);
                responseObserver.onCompleted();
            }
        }
    }
}
