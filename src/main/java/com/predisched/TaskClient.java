package com.predisched;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskServiceGrpc;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * ================================================================
 *  PrediSched — Experiment 1: RPC Client-Server Communication
 *  TaskClient.java — gRPC Client
 * ================================================================
 *
 *  This client connects to the PrediSched Task Server on port 50051
 *  and allows the user to submit computational tasks via gRPC RPC.
 *
 *  The user provides a Task ID, Task Type, Input, and Priority.
 *  The client sends the request to the server and displays the
 *  server's response.
 */
public class TaskClient {

    // ── Server Connection Configuration ───────────────────
    private static final String SERVER_HOST = "localhost";
    private static final int    SERVER_PORT = 50051;

    // ── Main Entry Point ──────────────────────────────────
    public static void main(String[] args) {

        // ── Create the gRPC channel ──────────────────────
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(SERVER_HOST, SERVER_PORT)
                .usePlaintext()         // No TLS for this experiment
                .build();

        // ── Create the blocking stub for synchronous RPC ─
        TaskServiceGrpc.TaskServiceBlockingStub stub =
                TaskServiceGrpc.newBlockingStub(channel);

        // ── Display the client banner ────────────────────
        Scanner scanner = new Scanner(System.in);

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║         PrediSched RPC Client (Exp 1)       ║");
        System.out.println("╠══════════════════════════════════════════════╣");
        System.out.println("║  Server     : " + SERVER_HOST + ":" + SERVER_PORT + "                  ║");
        System.out.println("║  Protocol   : gRPC / Protocol Buffers       ║");
        System.out.println("╚══════════════════════════════════════════════╝");
        System.out.println();

        // ── Interactive Menu Loop ────────────────────────
        boolean running = true;

        while (running) {
            System.out.println("┌──────────────────────────────────┐");
            System.out.println("│           MAIN MENU              │");
            System.out.println("├──────────────────────────────────┤");
            System.out.println("│  1. Submit Task                  │");
            System.out.println("│  2. Exit                         │");
            System.out.println("└──────────────────────────────────┘");
            System.out.print("  Enter choice: ");

            String choice = scanner.nextLine().trim();
            System.out.println();

            switch (choice) {
                case "1":
                    submitTask(scanner, stub);
                    break;

                case "2":
                    running = false;
                    System.out.println("  Disconnecting from server...");
                    break;

                default:
                    System.out.println("  ✗ Invalid choice. Please enter 1 or 2.");
                    System.out.println();
            }
        }

        // ── Shutdown ─────────────────────────────────────
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("  Client stopped.");
        System.out.println();
        scanner.close();
    }

    // ══════════════════════════════════════════════════════
    //  Submit Task — Collect input and make RPC call
    // ══════════════════════════════════════════════════════

    /**
     * Prompts the user for task details, builds a TaskRequest,
     * sends it to the server via submitTask() RPC, and displays
     * the server's response.
     */
    private static void submitTask(Scanner scanner, TaskServiceGrpc.TaskServiceBlockingStub stub) {

        System.out.println("  ── Submit a New Task ──────────────────────");
        System.out.println();
        System.out.println("  Available Task Types:");
        System.out.println("    CPU_TASK    — Compute sum of 1 to N");
        System.out.println("    MATRIX_TASK — Multiply two NxN matrices");
        System.out.println("    SLEEP_TASK  — Simulate work (sleep N ms)");
        System.out.println();

        // ── Collect task details ─────────────────────────
        System.out.print("  Enter Task ID    : ");
        String taskId = scanner.nextLine().trim();

        System.out.print("  Enter Task Type  : ");
        String taskType = scanner.nextLine().trim();

        System.out.print("  Enter Input      : ");
        String input = scanner.nextLine().trim();

        System.out.print("  Enter Priority   : ");
        String priorityStr = scanner.nextLine().trim();

        int priority;
        try {
            priority = Integer.parseInt(priorityStr);
        } catch (NumberFormatException e) {
            priority = 1; // Default priority
        }

        System.out.println();
        System.out.println("  Sending RPC request to server...");
        System.out.println();

        // ── Build the RPC request ────────────────────────
        TaskRequest request = TaskRequest.newBuilder()
                .setTaskId(taskId)
                .setTaskType(taskType)
                .setInput(input)
                .setPriority(priority)
                .build();

        // ── Make the RPC call ────────────────────────────
        try {
            TaskResponse response = stub.submitTask(request);

            // ── Display the response ─────────────────────
            System.out.println("  ┌──────────────────────────────────────────┐");
            System.out.println("  │           SERVER RESPONSE                │");
            System.out.println("  ├──────────────────────────────────────────┤");
            System.out.println("  │  Task ID  : " + padRight(response.getTaskId(), 28) + "│");
            System.out.println("  │  Status   : " + padRight(response.getStatus(), 28) + "│");
            System.out.println("  │  Result   : " + padRight(truncate(response.getResult(), 28), 28) + "│");
            System.out.println("  │  Message  : " + padRight(truncate(response.getMessage(), 28), 28) + "│");
            System.out.println("  └──────────────────────────────────────────┘");
            System.out.println();

            // If the result is long, print it in full below the box
            if (response.getResult().length() > 28) {
                System.out.println("  Full Result  : " + response.getResult());
                System.out.println();
            }
            if (response.getMessage().length() > 28) {
                System.out.println("  Full Message : " + response.getMessage());
                System.out.println();
            }

        } catch (StatusRuntimeException e) {
            System.out.println("  ✗ RPC FAILED: " + e.getStatus());
            System.out.println("    Make sure the server is running on " + SERVER_HOST + ":" + SERVER_PORT);
            System.out.println();
        }
    }

    // ── Utility Methods ──────────────────────────────────

    /** Pads a string to the right with spaces to a given width. */
    private static String padRight(String text, int width) {
        if (text.length() >= width) {
            return text.substring(0, width);
        }
        return text + " ".repeat(width - text.length());
    }

    /** Truncates a string and adds "..." if it exceeds maxLen. */
    private static String truncate(String text, int maxLen) {
        if (text.length() <= maxLen) {
            return text;
        }
        return text.substring(0, maxLen - 3) + "...";
    }
}
