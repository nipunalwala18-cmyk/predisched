package com.predisched.client;

import com.predisched.common.NodeConfig;
import com.predisched.common.WorkflowDag;
import com.predisched.common.net.Transport;
import com.predisched.proto.Ack;
import com.predisched.proto.DeadLetterEntry;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.ElectionServiceGrpc;
import com.predisched.proto.ReadRequest;
import com.predisched.proto.ReadResponse;
import com.predisched.proto.ReplicationServiceGrpc;
import com.predisched.proto.TaskRecordProto;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkflowStatusResponse;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
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
            PredischedCli.SubmitFile.class,
            PredischedCli.Shell.class,
            PredischedCli.Tasks.class,
            PredischedCli.Status.class,
            PredischedCli.Cancel.class,
            PredischedCli.Watch.class,
            PredischedCli.Dlq.class,
            PredischedCli.Workload.class,
            PredischedCli.Cluster.class,
            PredischedCli.Replica.class,
            PredischedCli.Workflow.class,
            PredischedCli.Report.class
        })
public class PredischedCli implements Runnable {

    @Option(names = "--host", description = "Scheduler host (default: ${DEFAULT-VALUE})")
    String host = "localhost";

    @Option(names = "--port", description = "Scheduler port (default: ${DEFAULT-VALUE})")
    int port = 51051;

    @Option(names = "--config", description = "Config YAML (default: ${DEFAULT-VALUE})")
    String config = "configs/local.yaml";

    @Option(names = "--api-key", defaultValue = "${env:PREDISCHED_API_KEY}",
            description = "API key sent as 'authorization: ApiKey <key>' (env PREDISCHED_API_KEY)")
    String apiKey;

    @Option(names = "--token", defaultValue = "${env:PREDISCHED_TOKEN}",
            description = "JWT sent as 'authorization: Bearer <jwt>' (env PREDISCHED_TOKEN)")
    String token;

    @Option(names = "--tls-trust",
            description = "PEM CA certificate: connect over TLS and trust this CA")
    String tlsTrust;

    /** Every channel this CLI opens carries the credential and TLS setting given (F9). */
    void installTransport() {
        String authorization = apiKey != null && !apiKey.isBlank()
                ? "ApiKey " + apiKey
                : token != null && !token.isBlank() ? "Bearer " + token : null;
        NodeConfig.TlsConfig tls = new NodeConfig.TlsConfig();
        if (tlsTrust != null) {
            tls.setEnabled(true);
            tls.setTrustCert(tlsTrust);
        }
        Transport.install(new Transport(tls, authorization));
    }

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }

    /**
     * A client for the scheduler at --host/--port, or, when the config lists a scheduler cluster
     * and no address was given, a failover client for the whole cluster (prompt 10).
     */
    SchedulerClient newClient() {
        String resolvedHost = host;
        int resolvedPort = port;
        try {
            if (Files.exists(Paths.get(config))) {
                NodeConfig loaded = NodeConfig.load(Paths.get(config));
                boolean addressGiven = !"localhost".equals(host) || port != 51051;
                if (!addressGiven && !loaded.getElection().getPeers().isEmpty()) {
                    return SchedulerClient.forCluster(loaded.getElection().getPeers(),
                            SchedulerClient.Failover.DEFAULT);
                }
                if (!addressGiven) {
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
        PredischedCli cli = new PredischedCli();
        int exit = new CommandLine(cli)
                .setExecutionStrategy(parsed -> {
                    cli.installTransport();
                    return new CommandLine.RunLast().execute(parsed);
                })
                .execute(args);
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

        @Option(names = "--repeat",
                description = "Submit this many copies back to back and print a summary"
                        + " (load and rate-limit demos)")
        int repeat = 1;

        @Option(names = "--deadline-ms",
                description = "Should complete within this many ms of submit (SLA, F3)")
        long deadlineMs = 0;

        @Option(names = "--no-cache", description = "Run it even if the result is cached (F6)")
        boolean noCache;

        @Override
        public Integer call() {
            if (repeat > 1) {
                return burst();
            }
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
                TaskResponse response = client.submitTask(taskId, taskType, input, priority,
                        traceId, timeoutMs, maxRetries, deadlineMs, noCache);
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

        /** {@code repeat} submits as fast as one connection allows; counts outcomes by reason. */
        private int burst() {
            TaskType taskType = TaskType.valueOf(type);
            java.util.Map<String, Integer> outcomes = new java.util.TreeMap<>();
            long start = System.nanoTime();
            try (SchedulerClient client = parent.newClient()) {
                for (int i = 0; i < repeat; i++) {
                    String taskId = "task-" + UUID.randomUUID().toString().substring(0, 8);
                    String outcome;
                    try {
                        TaskResponse response = client.submitTask(taskId, taskType, input,
                                priority, com.predisched.common.obs.TraceContext.newTraceId(),
                                timeoutMs, maxRetries, deadlineMs, noCache);
                        outcome = response.getAccepted() ? "accepted" : "refused: "
                                + response.getMessage();
                    } catch (io.grpc.StatusRuntimeException e) {
                        outcome = e.getStatus().getCode().toString();
                    }
                    outcomes.merge(outcome, 1, Integer::sum);
                }
            }
            long ms = (System.nanoTime() - start) / 1_000_000;
            System.out.println(repeat + " submits in " + ms + " ms:");
            outcomes.forEach((outcome, count) -> System.out.println("  " + count + " " + outcome));
            return 0;
        }
    }

    @Command(name = "tasks", description = "List the task types, their inputs and an example of each.")
    static class Tasks implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("Task types (line format for submit-file and shell:"
                    + " TYPE [priority] input):");
            for (TaskType type : TaskType.values()) {
                if (type == TaskType.UNRECOGNIZED) {
                    continue;
                }
                String reserved = com.predisched.common.TaskInputSpec.reservedReason(type);
                if (reserved != null) {
                    System.out.printf("  %-17s not available: %s%n", type, reserved);
                    continue;
                }
                System.out.printf("  %-17s %s%n", type,
                        com.predisched.common.TaskInputSpec.describe(type));
                String example = ManualTasks.EXAMPLES.get(type);
                if (example != null) {
                    System.out.printf("  %-17s   e.g. %s %d %s%n", "", type,
                            ManualTasks.DEFAULT_PRIORITY, example);
                }
            }
            return 0;
        }
    }

    /** Submits one parsed line; prints the outcome and returns the task id, or null if refused. */
    static String submitLine(SchedulerClient client, ManualTasks.Line line, boolean noCache) {
        String taskId = "task-" + UUID.randomUUID().toString().substring(0, 8);
        String traceId = com.predisched.common.obs.TraceContext.newTraceId();
        TaskResponse response = client.submitTask(taskId, line.type(), line.input(),
                line.priority(), traceId, 0, -1, 0, noCache);
        System.out.println("accepted=" + response.getAccepted()
                + " task_id=" + response.getTaskId()
                + " type=" + line.type()
                + " priority=" + line.priority()
                + " message='" + response.getMessage() + "'");
        return response.getAccepted() ? response.getTaskId() : null;
    }

    /** Polls until every task is terminal or the timeout passes; returns how many completed. */
    static int waitFor(SchedulerClient client, List<String> ids, long timeoutMs)
            throws InterruptedException {
        java.util.Set<String> pending = new java.util.LinkedHashSet<>(ids);
        int completed = 0;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!pending.isEmpty() && System.currentTimeMillis() < deadline) {
            for (java.util.Iterator<String> it = pending.iterator(); it.hasNext(); ) {
                TaskStatusResponse response = client.getStatus(it.next());
                TaskStatus status = response.getStatus();
                if (status == TaskStatus.COMPLETED || status == TaskStatus.FAILED
                        || status == TaskStatus.CANCELLED) {
                    System.out.println("  " + response.getTaskId() + " " + status
                            + " worker=" + response.getWorkerId()
                            + " exec_ms=" + response.getExecTimeMs()
                            + " result='" + response.getResult() + "'");
                    completed += status == TaskStatus.COMPLETED ? 1 : 0;
                    it.remove();
                }
            }
            Thread.sleep(200L);
        }
        pending.forEach(id -> System.out.println("  " + id + " still running after "
                + timeoutMs + " ms"));
        return completed;
    }

    @Command(name = "submit-file",
            description = "Submit the tasks listed in a text file, one per line:"
                    + " TYPE [priority] input (e.g. 'CPU_TASK 5 n=2000000'); # starts a comment.")
    static class SubmitFile implements Callable<Integer> {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Parameters(index = "0", description = "Tasks file")
        String file;

        @Option(names = "--watch", description = "Wait for every task and print its result")
        boolean watch;

        @Option(names = "--watch-timeout",
                description = "Stop waiting after this many ms (default: ${DEFAULT-VALUE})")
        long watchTimeoutMs = 300_000;

        @Option(names = "--no-cache", description = "Run tasks even if their result is cached")
        boolean noCache;

        @Override
        public Integer call() throws Exception {
            List<String> lines = Files.readAllLines(Paths.get(file));
            List<ManualTasks.Line> tasks = new java.util.ArrayList<>();
            boolean bad = false;
            for (int i = 0; i < lines.size(); i++) {
                if (ManualTasks.skip(lines.get(i))) {
                    continue;
                }
                ManualTasks.Line line = ManualTasks.parse(lines.get(i));
                if (!line.ok()) {
                    System.out.println(file + ":" + (i + 1) + ": " + line.error());
                    bad = true;
                }
                tasks.add(line);
            }
            if (bad) {
                System.out.println("nothing submitted: fix the lines above first");
                return 2;
            }
            List<String> ids = new java.util.ArrayList<>();
            try (SchedulerClient client = parent.newClient()) {
                for (ManualTasks.Line line : tasks) {
                    String id = submitLine(client, line, noCache);
                    if (id != null) {
                        ids.add(id);
                    }
                }
                System.out.println(ids.size() + " of " + tasks.size() + " tasks accepted");
                if (watch && !ids.isEmpty()) {
                    int completed = waitFor(client, ids, watchTimeoutMs);
                    System.out.println(completed + " of " + ids.size() + " completed");
                    return completed == tasks.size() ? 0 : 1;
                }
            }
            return ids.size() == tasks.size() ? 0 : 1;
        }
    }

    @Command(name = "shell",
            description = "Type tasks interactively: TYPE [priority] input, status <id>,"
                    + " watch <id>, tasks, quit.")
    static class Shell implements Callable<Integer> {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Option(names = "--watch", description = "Wait for each task and print its result")
        boolean watch;

        @Override
        public Integer call() throws Exception {
            java.io.BufferedReader in = new java.io.BufferedReader(new java.io.InputStreamReader(
                    System.in, java.nio.charset.StandardCharsets.UTF_8));
            System.out.println("PrediSched shell. Type a task (e.g. 'cpu 5 n=2000000'),"
                    + " 'status <id>', 'watch <id>', 'tasks' or 'quit'.");
            try (SchedulerClient client = parent.newClient()) {
                while (true) {
                    System.out.print("predisched> ");
                    System.out.flush();
                    String text = in.readLine();
                    if (text == null) {
                        return 0;
                    }
                    if (ManualTasks.skip(text)) {
                        continue;
                    }
                    String[] words = text.strip().split("\\s+", 2);
                    String command = words[0].toLowerCase(Locale.ROOT);
                    try {
                        if (command.equals("quit") || command.equals("exit")) {
                            return 0;
                        } else if (command.equals("tasks") || command.equals("help")) {
                            new Tasks().call();
                        } else if (command.equals("status") || command.equals("watch")) {
                            if (words.length < 2) {
                                System.out.println("  " + command + " needs a task id");
                            } else if (command.equals("watch")) {
                                waitFor(client, List.of(words[1].strip()), 120_000);
                            } else {
                                TaskStatusResponse r = client.getStatus(words[1].strip());
                                System.out.println("  " + r.getTaskId() + " " + r.getStatus()
                                        + " worker=" + r.getWorkerId()
                                        + " exec_ms=" + r.getExecTimeMs()
                                        + " result='" + r.getResult() + "'");
                            }
                        } else {
                            ManualTasks.Line line = ManualTasks.parse(text);
                            if (!line.ok()) {
                                System.out.println("  " + line.error());
                                continue;
                            }
                            String id = submitLine(client, line, false);
                            if (id != null && watch) {
                                waitFor(client, List.of(id), 120_000);
                            }
                        }
                    } catch (StatusRuntimeException e) {
                        System.out.println("  " + e.getStatus().getCode() + ": "
                                + e.getStatus().getDescription());
                    }
                }
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
                        + (response.getDeadlineAt() > 0
                                ? " sla=" + (response.getSlaMet() ? "met" : "not met") : "")
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

    @Command(name = "workload", description = "Generate and replay reproducible workloads.",
            subcommands = {Workload.Generate.class, Workload.Replay.class})
    static class Workload implements Runnable {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Override
        public void run() {
            new CommandLine(this).usage(System.out);
        }

        @Command(name = "generate", description = "Write a seeded trace file.")
        static class Generate implements Callable<Integer> {
            @CommandLine.ParentCommand
            Workload parent;

            @Option(names = "--profile", required = true,
                    description = "Profile from configs/workloads.yaml, e.g. mixed")
            String profile;

            @Option(names = "--pattern", required = true,
                    description = "Arrival process: steady|bursty|periodic")
            String pattern;

            @Option(names = "--tasks", required = true, description = "Task count")
            int tasks;

            @Option(names = "--seed", required = true, description = "Random seed")
            long seed;

            @Option(names = "--rate", description = "Base arrival rate/s (default: ${DEFAULT-VALUE})")
            double rate = 5.0;

            @Option(names = "--profiles", description = "Workload profiles YAML (default: ${DEFAULT-VALUE})")
            String profiles = "configs/workloads.yaml";

            @Option(names = "--output", description = "Trace file (default: workloads/<profile>-<pattern>-<seed>.jsonl)")
            String output;

            @Option(names = "--http-base-url",
                    description = "Mock HTTP base URL for HTTP_TASK (default: from the profiles file)")
            String httpBaseUrl;

            @Override
            public Integer call() throws Exception {
                com.predisched.client.workload.ArrivalPattern arrival;
                try {
                    arrival = com.predisched.client.workload.ArrivalPattern.parse(pattern);
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                    return 2;
                }
                java.util.Map<String, com.predisched.client.workload.WorkloadProfile> all;
                try {
                    all = com.predisched.client.workload.WorkloadProfile.load(
                            java.nio.file.Paths.get(profiles), httpBaseUrl);
                } catch (Exception e) {
                    System.out.println("cannot load profiles: " + e.getMessage());
                    return 1;
                }
                com.predisched.client.workload.WorkloadProfile selected = all.get(profile);
                if (selected == null) {
                    System.out.println("unknown profile '" + profile + "', have: " + all.keySet());
                    return 2;
                }
                java.util.List<com.predisched.client.workload.TraceEntry> entries;
                try {
                    entries = com.predisched.client.workload.WorkloadGenerator.generate(
                            selected, arrival, tasks, seed, rate);
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                    return 2;
                }
                String out = output != null ? output
                        : "workloads/" + profile + "-" + pattern.toLowerCase(Locale.ROOT)
                                + "-" + seed + ".jsonl";
                java.nio.file.Path path = java.nio.file.Paths.get(out);
                if (path.getParent() != null) {
                    java.nio.file.Files.createDirectories(path.getParent());
                }
                com.predisched.client.workload.TraceIo.writeTrace(path, entries);
                java.util.Map<String, Integer> counts = new java.util.TreeMap<>();
                for (com.predisched.client.workload.TraceEntry entry : entries) {
                    counts.merge(entry.type().name(), 1, Integer::sum);
                }
                System.out.println("wrote " + entries.size() + " tasks to " + path
                        + " (profile=" + profile + " pattern=" + arrival.name().toLowerCase(Locale.ROOT)
                        + " seed=" + seed + " rate=" + rate + "/s)");
                for (java.util.Map.Entry<String, Integer> count : counts.entrySet()) {
                    System.out.println("  " + count.getKey() + ": " + count.getValue());
                }
                return 0;
            }
        }

        @Command(name = "replay", description = "Submit a trace with its original timing.")
        static class Replay implements Callable<Integer> {
            @CommandLine.ParentCommand
            Workload parent;

            @Parameters(index = "0", description = "Trace file")
            String trace;

            @Option(names = "--speed", description = "Timing scale factor (default: ${DEFAULT-VALUE})")
            double speed = 1.0;

            @Option(names = "--output", description = "Results CSV (default: results/<trace>-replay.csv)")
            String output;

            @Option(names = "--poll-ms", description = "Status poll interval (default: ${DEFAULT-VALUE})")
            long pollMs = 100;

            @Option(names = "--timeout-ms", description = "Give up waiting after this long (default: ${DEFAULT-VALUE})")
            long timeoutMs = 600_000;

            @Option(names = "--deadline-ms",
                    description = "Give every task a deadline this many ms after its submit (F3)")
            long deadlineMs = 0;

            @Option(names = "--id-suffix",
                    description = "Append this to every task id, to replay a trace again as new"
                            + " tasks (a repeated id is the same task)")
            String idSuffix = "";

            @Override
            public Integer call() throws Exception {
                java.util.List<com.predisched.client.workload.TraceEntry> entries;
                try {
                    entries = com.predisched.client.workload.TraceIo.readTrace(
                            java.nio.file.Paths.get(trace));
                } catch (Exception e) {
                    System.out.println("cannot read trace: " + e.getMessage());
                    return 1;
                }
                if (!idSuffix.isEmpty()) {
                    entries = entries.stream().map(e -> new com.predisched.client.workload
                            .TraceEntry(e.offsetMs(), e.taskId() + idSuffix, e.type(), e.input(),
                                    e.priority(), e.timeoutMs())).toList();
                }
                String base = java.nio.file.Paths.get(trace).getFileName().toString()
                        .replaceFirst("\\.jsonl$", "");
                String out = output != null ? output : "results/" + base + "-replay.csv";
                try (SchedulerClient client = parent.parent.newClient()) {
                    com.predisched.client.workload.Replayer.Summary summary =
                            com.predisched.client.workload.Replayer.replay(
                                    entries, speed, client, java.nio.file.Paths.get(out),
                                    pollMs, timeoutMs, deadlineMs, System.out);
                    return summary.totalFailed() == 0 ? 0 : 1;
                } catch (Exception e) {
                    System.out.println("replay failed: " + e.getMessage());
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

    @Command(name = "cluster", description = "Inspect the scheduler cluster.",
            subcommands = {Cluster.Leader.class})
    static class Cluster implements Runnable {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Override
        public void run() {
            new CommandLine(this).usage(System.out);
        }

        @Command(name = "leader",
                description = "Ask every scheduler in the cluster who it thinks the leader is.")
        static class Leader implements Callable<Integer> {
            @CommandLine.ParentCommand
            Cluster parent;

            @Option(names = "--timeout-ms", description = "Per-node deadline (default: ${DEFAULT-VALUE})")
            long timeoutMs = 1000;

            @Override
            public Integer call() throws Exception {
                List<NodeConfig.PeerConfig> peers = peers(parent.parent.config);
                if (peers.isEmpty()) {
                    System.out.println("no election peers in " + parent.parent.config
                            + " or configs/cluster.yaml");
                    return 1;
                }
                int answered = 0;
                for (NodeConfig.PeerConfig peer : peers) {
                    String address = peer.getHost() + ":" + peer.getPort();
                    ManagedChannel channel = Transport.get().channel(peer.getHost(), peer.getPort());
                    try {
                        Ack reply = ElectionServiceGrpc.newBlockingStub(channel)
                                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                                .ping(Ack.getDefaultInstance());
                        System.out.printf(Locale.ROOT, "scheduler %d (%s): %s%n",
                                peer.getId(), address, reply.getMessage());
                        answered++;
                    } catch (StatusRuntimeException e) {
                        System.out.printf(Locale.ROOT, "scheduler %d (%s): unreachable (%s)%n",
                                peer.getId(), address, e.getStatus().getCode());
                    } finally {
                        channel.shutdownNow();
                    }
                }
                return answered > 0 ? 0 : 1;
            }

            /** The peers in the given config, else in configs/cluster.yaml. */
            static List<NodeConfig.PeerConfig> peers(String config) throws Exception {
                for (String path : List.of(config, "configs/cluster.yaml")) {
                    if (Files.exists(Paths.get(path))) {
                        List<NodeConfig.PeerConfig> peers =
                                NodeConfig.load(Paths.get(path)).getElection().getPeers();
                        if (!peers.isEmpty()) {
                            return peers;
                        }
                    }
                }
                return List.of();
            }
        }
    }

    @Command(name = "replica", description = "Inspect replicated task state (Exp 5).",
            subcommands = {Replica.Read.class})
    static class Replica implements Runnable {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Override
        public void run() {
            new CommandLine(this).usage(System.out);
        }

        @Command(name = "read",
                description = "Read a task from one specific replica, for the stale-read demo.")
        static class Read implements Callable<Integer> {
            @CommandLine.ParentCommand
            Replica parent;

            @Option(names = "--node", required = true, description = "Scheduler node id to ask")
            int node;

            @Option(names = "--local",
                    description = "That replica's own copy, not its consistency mode's read")
            boolean localOnly;

            @Parameters(index = "0", description = "Task id")
            String taskId;

            @Override
            public Integer call() throws Exception {
                NodeConfig.PeerConfig peer = Cluster.Leader.peers(parent.parent.config).stream()
                        .filter(candidate -> candidate.getId() == node)
                        .findFirst()
                        .orElse(null);
                if (peer == null) {
                    System.out.println("no scheduler " + node + " in the config");
                    return 1;
                }
                ManagedChannel channel = Transport.get().channel(peer.getHost(), peer.getPort());
                try {
                    ReadResponse reply = ReplicationServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5, TimeUnit.SECONDS)
                            .read(ReadRequest.newBuilder()
                                    .setTaskId(taskId)
                                    .setLocalOnly(localOnly)
                                    .build());
                    String how = localOnly ? "own copy" : "read";
                    if (!reply.getFound()) {
                        System.out.printf(Locale.ROOT, "node %d %s: %s not found%n",
                                node, how, taskId);
                        return 0;
                    }
                    TaskRecordProto task = TaskRecordProto.parseFrom(reply.getPayload());
                    System.out.printf(Locale.ROOT,
                            "node %d %s: %s status=%s worker=%s version=%d lamport=%d origin=%s%n",
                            node, how, taskId, task.getStatus(),
                            task.getWorkerId().isEmpty() ? "-" : task.getWorkerId(),
                            reply.getVersion(), reply.getLamportTime(), reply.getOriginNode());
                    return 0;
                } catch (StatusRuntimeException e) {
                    System.out.printf(Locale.ROOT, "node %d: %s (%s)%n", node,
                            e.getStatus().getCode(), e.getStatus().getDescription());
                    return 1;
                } finally {
                    channel.shutdownNow();
                }
            }
        }
    }

    @Command(name = "workflow", description = "Submit and track task DAGs (F1).",
            subcommands = {Workflow.Submit.class, Workflow.StatusOf.class})
    static class Workflow implements Runnable {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Override
        public void run() {
            new CommandLine(this).usage(System.out);
        }

        @Command(name = "submit", description = "Submit a workflow JSON file.")
        static class Submit implements Callable<Integer> {
            @CommandLine.ParentCommand
            Workflow parent;

            @Parameters(index = "0", description = "Workflow file, e.g. workloads/dags/map-reduce.json")
            String file;

            @Option(names = "--id", description = "Workflow id (default: wf-<random>)")
            String workflowId;

            @Override
            public Integer call() {
                String id = workflowId != null
                        ? workflowId
                        : "wf-" + UUID.randomUUID().toString().substring(0, 8);
                try (SchedulerClient client = parent.parent.newClient()) {
                    WorkflowDag dag = WorkflowDag.load(Paths.get(file));
                    TaskResponse response = client.submitWorkflow(
                            dag.toRequest(id, UUID.randomUUID().toString().substring(0, 8)));
                    System.out.println("accepted=" + response.getAccepted()
                            + " workflow_id=" + response.getTaskId()
                            + " message='" + response.getMessage() + "'");
                    return response.getAccepted() ? 0 : 1;
                } catch (StatusRuntimeException e) {
                    System.out.println("workflow submit failed: " + e.getStatus().getCode()
                            + " (" + e.getStatus().getDescription() + ")");
                    return 1;
                } catch (Exception e) {
                    System.out.println("workflow submit failed: " + e.getMessage());
                    return 1;
                }
            }
        }

        @Command(name = "status", description = "Show every task of a workflow, in order.")
        static class StatusOf implements Callable<Integer> {
            @CommandLine.ParentCommand
            Workflow parent;

            @Parameters(index = "0", description = "Workflow id")
            String workflowId;

            @Override
            public Integer call() {
                try (SchedulerClient client = parent.parent.newClient()) {
                    WorkflowStatusResponse status = client.workflowStatus(workflowId);
                    if (!status.getFound()) {
                        System.out.println("unknown workflow: " + workflowId);
                        return 1;
                    }
                    System.out.println("workflow " + workflowId + ":");
                    for (TaskStatusResponse task : status.getTasksList()) {
                        String result = task.getResult();
                        System.out.printf(Locale.ROOT, "  %-28s %-9s %-9s %s%n", task.getTaskId(),
                                task.getStatus(),
                                task.getWorkerId().isEmpty() ? "-" : task.getWorkerId(),
                                result.length() > 60 ? result.substring(0, 60) + "..." : result);
                    }
                    return 0;
                } catch (StatusRuntimeException e) {
                    System.out.println("workflow status failed: " + e.getStatus().getCode()
                            + " (" + e.getStatus().getDescription() + ")");
                    return 1;
                }
            }
        }
    }
    @Command(name = "report", description = "Reports computed from the database (prompt 11).",
            subcommands = {Report.Sla.class})
    static class Report implements Runnable {
        @CommandLine.ParentCommand
        PredischedCli parent;

        @Override
        public void run() {
            new CommandLine(this).usage(System.out);
        }

        @Command(name = "sla",
                description = "On-time % of tasks with a deadline, per strategy and task type.")
        static class Sla implements Callable<Integer> {
            @CommandLine.ParentCommand
            Report parent;

            @Option(names = "--since",
                    description = "Only tasks submitted in this last period: 30m, 2h, 1d"
                            + " (default: all)")
            String since;

            @Override
            public Integer call() {
                NodeConfig.DbConfig db;
                try {
                    db = Files.exists(Paths.get(parent.parent.config))
                            ? NodeConfig.load(Paths.get(parent.parent.config)).getDb()
                            : new NodeConfig.DbConfig();
                } catch (Exception e) {
                    System.out.println("cannot read " + parent.parent.config + ": "
                            + e.getMessage());
                    return 1;
                }
                java.time.Instant from;
                try {
                    from = since == null ? java.time.Instant.EPOCH
                            : java.time.Instant.now().minus(period(since));
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                    return 2;
                }
                try (com.predisched.common.db.Db database =
                        com.predisched.common.db.Db.open(db, 1, "report", false)) {
                    System.out.println("SLA report from " + db.getUrl()
                            + (since == null ? "" : ", tasks submitted in the last " + since));
                    System.out.print(com.predisched.common.db.SlaReport.format(
                            com.predisched.common.db.SlaReport.byStrategy(
                                    database.dataSource(), from),
                            com.predisched.common.db.SlaReport.byTaskType(
                                    database.dataSource(), from)));
                    return 0;
                } catch (Exception e) {
                    System.out.println("report failed: " + e.getMessage());
                    return 1;
                }
            }

            static java.time.Duration period(String text) {
                java.util.regex.Matcher m =
                        java.util.regex.Pattern.compile("(\\d+)([smhd])").matcher(text.trim());
                if (!m.matches()) {
                    throw new IllegalArgumentException("--since must look like 30m, 2h or 1d");
                }
                long n = Long.parseLong(m.group(1));
                return switch (m.group(2)) {
                    case "s" -> java.time.Duration.ofSeconds(n);
                    case "m" -> java.time.Duration.ofMinutes(n);
                    case "h" -> java.time.Duration.ofHours(n);
                    default -> java.time.Duration.ofDays(n);
                };
            }
        }
    }
}
