package com.predisched.client;

import com.predisched.common.NodeConfig;
import com.predisched.proto.DeadLetterEntry;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "predisched",
        mixinStandardHelpOptions = true,
        description = "PrediSched client: submit, track and cancel tasks over gRPC.",
        subcommands = {
            PredischedCli.Submit.class,
            PredischedCli.Status.class,
            PredischedCli.Cancel.class,
            PredischedCli.Watch.class,
            PredischedCli.Dlq.class
        })
public class PredischedCli implements Runnable {

    @Option(names = "--host", description = "Scheduler host (default: ${DEFAULT-VALUE})")
    String host = "localhost";

    @Option(names = "--port", description = "Scheduler port (default: ${DEFAULT-VALUE})")
    int port = 51051;

    @Option(names = "--config", description = "Config YAML (default: ${DEFAULT-VALUE})")
    String config = "configs/local.yaml";

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }

    SchedulerClient newClient() {
        String resolvedHost = host;
        int resolvedPort = port;
        try {
            if (Files.exists(Paths.get(config))) {
                NodeConfig loaded = NodeConfig.load(Paths.get(config));
                if ("localhost".equals(host) && port == 51051) {
                    resolvedHost = loaded.getScheduler().getHost();
                    resolvedPort = loaded.getScheduler().getPort();
                }
            }
        } catch (Exception ignored) {
            // Fall back to CLI-provided host/port.
        }
        return new SchedulerClient(resolvedHost, resolvedPort);
    }

    public static void main(String[] args) {
        int exit = new CommandLine(new PredischedCli()).execute(args);
        System.exit(exit);
    }

    @Command(name = "submit", description = "Submit a task.")
    static class Submit implements Callable<Integer> {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Option(names = "--type", required = true, description = "Task type, e.g. CPU_TASK")
        String type;

        @Option(names = "--input", required = true, description = "Task input, e.g. n=2000000")
        String input;

        @Option(names = "--priority", required = true, description = "Priority 1-10")
        int priority;

        @Option(names = "--id", description = "Task id (default: generated)")
        String id;

        @Option(names = "--trace",
                description = "Trace id to follow this task across nodes (default: generated)")
        String trace;

        @Option(names = "--timeout",
                description = "Cancel the task after this many ms (default: scheduler setting)")
        long timeoutMs = 0;

        @Option(names = "--max-retries",
                description = "Retries after a failure (default: scheduler setting)")
        int maxRetries = -1;

        @Override
        public Integer call() {
            TaskType taskType;
            try {
                taskType = TaskType.valueOf(type);
            } catch (IllegalArgumentException e) {
                System.out.println("accepted=false message='unknown task type: " + type + "'");
                return 2;
            }
            String taskId = (id == null || id.isEmpty())
                    ? "task-" + UUID.randomUUID().toString().substring(0, 8)
                    : id;
            String traceId = (trace == null || trace.isEmpty())
                    ? com.predisched.common.obs.TraceContext.newTraceId()
                    : trace;
            try (SchedulerClient client = parent.newClient()) {
                TaskResponse response = client.submitTask(
                        taskId, taskType, input, priority, traceId, timeoutMs, maxRetries);
                System.out.println("accepted=" + response.getAccepted()
                        + " task_id=" + response.getTaskId()
                        + " trace=" + traceId
                        + " lamport=" + response.getLamportTime()
                        + " message='" + response.getMessage() + "'");
                return response.getAccepted() ? 0 : 1;
            } catch (Exception e) {
                System.out.println("submit failed: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "status", description = "Query a task status.")
    static class Status implements Callable<Integer> {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Parameters(index = "0", description = "Task id")
        String id;

        @Override
        public Integer call() {
            try (SchedulerClient client = parent.newClient()) {
                TaskStatusResponse response = client.getStatus(id);
                System.out.println("task_id=" + response.getTaskId()
                        + " status=" + response.getStatus()
                        + " worker=" + response.getWorkerId()
                        + " exec_ms=" + response.getExecTimeMs()
                        + " trace=" + response.getTraceId()
                        + " attempts=" + response.getAttempt()
                        + " result='" + response.getResult() + "'");
                for (String attempt : response.getAttemptHistoryList()) {
                    System.out.println("  attempt " + attempt);
                }
                return 0;
            } catch (Exception e) {
                System.out.println("status failed: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "dlq", description = "Inspect and retry tasks that exhausted their retries.",
            subcommands = {Dlq.ListEntries.class, Dlq.Retry.class})
    static class Dlq implements Runnable {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Override
        public void run() {
            new CommandLine(this).usage(System.out);
        }

        @Command(name = "list", description = "Show parked tasks, newest first.")
        static class ListEntries implements Callable<Integer> {
            @CommandLine.ParentCommand
            Dlq parent;

            @Option(names = "--limit", description = "Rows to show (default: ${DEFAULT-VALUE})")
            int limit = 20;

            @Override
            public Integer call() {
                try (SchedulerClient client = parent.parent.newClient()) {
                    DeadLetterList list = client.listDeadLetters(limit);
                    if (list.getEntriesCount() == 0) {
                        System.out.println("dead-letter queue is empty");
                        return 0;
                    }
                    System.out.println("task_id                type          attempts  last_error");
                    for (DeadLetterEntry entry : list.getEntriesList()) {
                        System.out.printf(Locale.ROOT, "%-22s %-13s %-9d %s%n",
                                entry.getTaskId(), entry.getType(), entry.getAttempts(),
                                entry.getLastError());
                    }
                    return 0;
                } catch (Exception e) {
                    System.out.println("dlq list failed: " + e.getMessage());
                    return 1;
                }
            }
        }

        @Command(name = "retry", description = "Put a parked task back in the queue.")
        static class Retry implements Callable<Integer> {
            @CommandLine.ParentCommand
            Dlq parent;

            @Parameters(index = "0", description = "Task id")
            String id;

            @Override
            public Integer call() {
                try (SchedulerClient client = parent.parent.newClient()) {
                    TaskResponse response = client.retryDeadLetter(id);
                    System.out.println("accepted=" + response.getAccepted()
                            + " task_id=" + response.getTaskId()
                            + " message='" + response.getMessage() + "'");
                    return response.getAccepted() ? 0 : 1;
                } catch (Exception e) {
                    System.out.println("dlq retry failed: " + e.getMessage());
                    return 1;
                }
            }
        }
    }

    @Command(name = "cancel", description = "Cancel a queued task.")
    static class Cancel implements Callable<Integer> {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Parameters(index = "0", description = "Task id")
        String id;

        @Override
        public Integer call() {
            try (SchedulerClient client = parent.newClient()) {
                TaskResponse response = client.cancelTask(id);
                System.out.println("accepted=" + response.getAccepted()
                        + " task_id=" + response.getTaskId()
                        + " message='" + response.getMessage() + "'");
                return response.getAccepted() ? 0 : 1;
            } catch (Exception e) {
                System.out.println("cancel failed: " + e.getMessage());
                return 1;
            }
        }
    }

    @Command(name = "watch", description = "Poll until a task reaches a terminal state.")
    static class Watch implements Callable<Integer> {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Parameters(index = "0", description = "Task id")
        String id;

        @Override
        public Integer call() throws Exception {
            try (SchedulerClient client = parent.newClient()) {
                long deadline = System.currentTimeMillis() + 120_000L;
                while (true) {
                    TaskStatusResponse response;
                    try {
                        response = client.getStatus(id);
                    } catch (Exception e) {
                        System.out.println("watch failed: " + e.getMessage());
                        return 1;
                    }
                    TaskStatus status = response.getStatus();
                    if (status == TaskStatus.COMPLETED
                            || status == TaskStatus.FAILED
                            || status == TaskStatus.CANCELLED) {
                        System.out.println("task_id=" + response.getTaskId()
                                + " status=" + status
                                + " worker=" + response.getWorkerId()
                                + " exec_ms=" + response.getExecTimeMs()
                                + " trace=" + response.getTraceId()
                                + " result='" + response.getResult() + "'");
                        return 0;
                    }
                    if (System.currentTimeMillis() > deadline) {
                        System.out.println("watch timed out for " + id);
                        return 1;
                    }
                    Thread.sleep(200L);
                }
            }
        }
    }
}
