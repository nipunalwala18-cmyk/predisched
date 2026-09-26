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
            PredischedCli.Status.class,
            PredischedCli.Cancel.class,
            PredischedCli.Watch.class,
            PredischedCli.Dlq.class,
            PredischedCli.Workload.class,
            PredischedCli.Cluster.class,
            PredischedCli.Replica.class,
            PredischedCli.Workflow.class
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
                                timeoutMs, maxRetries);
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
                String base = java.nio.file.Paths.get(trace).getFileName().toString()
                        .replaceFirst("\\.jsonl$", "");
                String out = output != null ? output : "results/" + base + "-replay.csv";
                try (SchedulerClient client = parent.parent.newClient()) {
                    com.predisched.client.workload.Replayer.Summary summary =
                            com.predisched.client.workload.Replayer.replay(
                                    entries, speed, client, java.nio.file.Paths.get(out),
                                    pollMs, timeoutMs, System.out);
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
}
