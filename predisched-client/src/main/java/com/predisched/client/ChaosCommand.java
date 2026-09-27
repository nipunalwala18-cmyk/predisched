package com.predisched.client;

import com.predisched.common.NodeConfig;
import com.predisched.common.net.Transport;
import com.predisched.common.obs.EventLog;
import com.predisched.proto.ChaosReply;
import com.predisched.proto.ChaosServiceGrpc;
import com.predisched.proto.CpuRequest;
import com.predisched.proto.CrashRequest;
import com.predisched.proto.DrainRequest;
import com.predisched.proto.IsolateRequest;
import com.predisched.proto.LatencyRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.WorkerEntry;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * {@code predisched chaos ...} (prompt 19, F16, spec 15.7): faults on demand through the
 * ChaosService every node serves when {@code chaos.enabled} is true. A node is a worker id
 * (resolved through the scheduler's {@code ListWorkers}) or a scheduler, {@code scheduler-N}
 * (resolved through the election peers in the config). Every action writes a {@code CHAOS} event
 * on the node it hits; {@code burst} writes one to the client's own event log.
 */
@Command(name = "chaos", description = "Inject faults: kill, latency, CPU, partition, drain, burst.",
        subcommands = {
            ChaosCommand.KillWorker.class,
            ChaosCommand.KillPrimary.class,
            ChaosCommand.Latency.class,
            ChaosCommand.Cpu.class,
            ChaosCommand.Partition.class,
            ChaosCommand.Drain.class,
            ChaosCommand.Burst.class
        })
public class ChaosCommand implements Runnable {

    private static final Pattern SCHEDULER = Pattern.compile("(?:scheduler-?|s)?(\\d+)");

    @CommandLine.ParentCommand
    PredischedCli parent;

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }

    /** Where a node's ChaosService listens. */
    record Target(String name, String host, int port) {
        String address() {
            return host + ":" + port;
        }
    }

    Target resolve(String node) throws Exception {
        Matcher m = SCHEDULER.matcher(node);
        if (m.matches()) {
            int id = Integer.parseInt(m.group(1));
            for (NodeConfig.PeerConfig peer : peers()) {
                if (peer.getId() == id) {
                    return new Target("scheduler-" + id, peer.getHost(), peer.getPort());
                }
            }
            if (node.startsWith("scheduler") && peers().isEmpty()) {
                NodeConfig.SchedulerConfig s = config().getScheduler();
                return new Target(node, s.getHost(), s.getPort());
            }
        }
        try (SchedulerClient client = parent.newClient()) {
            for (WorkerEntry w : client.listWorkers().getWorkersList()) {
                if (w.getWorkerId().equals(node)) {
                    return new Target(w.getWorkerId(), w.getHost(), w.getPort());
                }
            }
        }
        throw new IllegalArgumentException("no node called '" + node + "': not a registered"
                + " worker, and not scheduler-N from the election peers of " + parent.config);
    }

    NodeConfig config() throws Exception {
        return Files.exists(Paths.get(parent.config))
                ? NodeConfig.load(Paths.get(parent.config)) : new NodeConfig();
    }

    List<NodeConfig.PeerConfig> peers() throws Exception {
        return config().getElection().getPeers();
    }

    int call(String node, Function<ChaosServiceGrpc.ChaosServiceBlockingStub, ChaosReply> action) {
        Target target;
        try {
            target = resolve(node);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return 2;
        }
        return call(target, action);
    }

    static int call(Target target,
            Function<ChaosServiceGrpc.ChaosServiceBlockingStub, ChaosReply> action) {
        ManagedChannel channel = Transport.get().channel(target.host(), target.port());
        try {
            ChaosReply reply = action.apply(ChaosServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS));
            System.out.printf(Locale.ROOT, "%s (%s): %s%s%n", reply.getNodeId(),
                    target.address(), reply.getMessage(), reply.getUntilMs() > 0
                            ? ", until " + Instant.ofEpochMilli(reply.getUntilMs()) : "");
            return reply.getOk() ? 0 : 1;
        } catch (StatusRuntimeException e) {
            System.out.printf(Locale.ROOT, "%s (%s): %s%s%n", target.name(), target.address(),
                    e.getStatus().getCode(), e.getStatus().getCode()
                            == io.grpc.Status.Code.UNIMPLEMENTED
                            ? " (is chaos.enabled true in its config?)"
                            : e.getStatus().getDescription() == null ? ""
                                    : ": " + e.getStatus().getDescription());
            return 1;
        } finally {
            channel.shutdownNow();
        }
    }

    @Command(name = "kill-worker", description = "Crash a worker process.")
    static class KillWorker implements Callable<Integer> {
        @CommandLine.ParentCommand
        ChaosCommand parent;

        @Parameters(index = "0", description = "Worker id, e.g. worker-2")
        String worker;

        @Override
        public Integer call() {
            return parent.call(worker, stub -> stub.crash(
                    CrashRequest.newBuilder().setReason("chaos kill-worker").build()));
        }
    }

    @Command(name = "kill-primary",
            description = "Crash the scheduler the cluster names as leader (election follows).")
    static class KillPrimary implements Callable<Integer> {
        @CommandLine.ParentCommand
        ChaosCommand parent;

        @Override
        public Integer call() throws Exception {
            List<NodeConfig.PeerConfig> peers = parent.peers();
            Target target;
            if (peers.isEmpty()) {
                NodeConfig.SchedulerConfig s = parent.config().getScheduler();
                target = new Target(s.getId(), s.getHost(), s.getPort());
            } else {
                Optional<Integer> leader;
                try (SchedulerClient client = parent.parent.newClient()) {
                    leader = client.locateLeader();
                }
                if (leader.isEmpty()) {
                    System.out.println("no leader agreed on by the schedulers in "
                            + parent.parent.config);
                    return 1;
                }
                NodeConfig.PeerConfig peer = peers.stream()
                        .filter(p -> p.getId() == leader.get()).findFirst().orElseThrow();
                target = new Target("scheduler-" + peer.getId(), peer.getHost(), peer.getPort());
            }
            System.out.println("primary is " + target.name() + " (" + target.address() + ")");
            return ChaosCommand.call(target, stub -> stub.crash(
                    CrashRequest.newBuilder().setReason("chaos kill-primary").build()));
        }
    }

    @Command(name = "latency", description = "Delay every call a node receives.")
    static class Latency implements Callable<Integer> {
        @CommandLine.ParentCommand
        ChaosCommand parent;

        @Parameters(index = "0", description = "Node: worker id or scheduler-N")
        String node;

        @Parameters(index = "1", description = "Added delay per call, ms")
        long ms;

        @Parameters(index = "2", description = "For how long, seconds")
        long seconds;

        @Override
        public Integer call() {
            return parent.call(node, stub -> stub.injectLatency(
                    LatencyRequest.newBuilder().setMs(ms).setDurationS(seconds).build()));
        }
    }

    @Command(name = "cpu", description = "Busy-loop threads on a node.")
    static class Cpu implements Callable<Integer> {
        @CommandLine.ParentCommand
        ChaosCommand parent;

        @Parameters(index = "0", description = "Node: worker id or scheduler-N")
        String node;

        @Parameters(index = "1", description = "For how long, seconds")
        long seconds;

        @Option(names = "--threads", description = "Threads (default: one per core)")
        int threads = 0;

        @Override
        public Integer call() {
            return parent.call(node, stub -> stub.spikeCpu(
                    CpuRequest.newBuilder().setThreads(threads).setDurationS(seconds).build()));
        }
    }

    @Command(name = "partition",
            description = "Cut a node off from replication traffic (Exp 5: eventual consistency).")
    static class Partition implements Callable<Integer> {
        @CommandLine.ParentCommand
        ChaosCommand parent;

        @Parameters(index = "0", description = "Node, e.g. scheduler-3")
        String node;

        @Parameters(index = "1", description = "For how long, seconds")
        long seconds;

        @Override
        public Integer call() {
            return parent.call(node, stub -> stub.isolate(
                    IsolateRequest.newBuilder().setDurationS(seconds).build()));
        }
    }

    @Command(name = "drain", description = "A worker finishes its work and accepts nothing new.")
    static class Drain implements Callable<Integer> {
        @CommandLine.ParentCommand
        ChaosCommand parent;

        @Parameters(index = "0", description = "Worker id")
        String worker;

        @Override
        public Integer call() {
            return parent.call(worker, stub -> stub.drain(DrainRequest.getDefaultInstance()));
        }
    }

    @Command(name = "burst", description = "Submit n tasks from a profile all at once.")
    static class Burst implements Callable<Integer> {
        @CommandLine.ParentCommand
        ChaosCommand parent;

        @Parameters(index = "0", description = "How many tasks")
        int tasks;

        @Option(names = "--profile", description = "Profile (default: ${DEFAULT-VALUE})")
        String profile = "bursty";

        @Option(names = "--profiles", description = "Profiles YAML (default: ${DEFAULT-VALUE})")
        String profiles = "configs/workloads.yaml";

        @Option(names = "--seed", description = "Seed (default: the current time)")
        Long seed;

        @Override
        public Integer call() throws Exception {
            if (tasks < 1 || tasks > 100_000) {
                System.out.println("tasks must be 1..100000");
                return 2;
            }
            var all = com.predisched.client.workload.WorkloadProfile.load(
                    Paths.get(profiles), null);
            var selected = all.get(profile);
            if (selected == null) {
                System.out.println("unknown profile '" + profile + "', have: " + all.keySet());
                return 2;
            }
            long s = seed != null ? seed : System.currentTimeMillis();
            var entries = com.predisched.client.workload.WorkloadGenerator.generate(selected,
                    com.predisched.client.workload.ArrivalPattern.parse("steady"), tasks, s, 1.0);
            EventLog events = EventLog.install("client-chaos");
            events.event(com.predisched.common.chaos.ChaosController.EVENT, "", Map.of(
                    "action", "burst", "node", "client", "tasks", String.valueOf(tasks),
                    "profile", profile, "seed", String.valueOf(s)));
            int accepted = 0;
            long started = System.nanoTime();
            try (SchedulerClient client = parent.parent.newClient()) {
                for (var e : entries) {
                    TaskResponse r = client.submitTask("burst-" + e.taskId(), e.type(), e.input(),
                            e.priority());
                    if (r.getAccepted()) {
                        accepted++;
                    }
                }
            } finally {
                events.close();
            }
            System.out.printf(Locale.ROOT, "burst: %d of %d %s tasks accepted in %d ms (seed %d)%n",
                    accepted, tasks, profile, (System.nanoTime() - started) / 1_000_000, s);
            return accepted == tasks ? 0 : 1;
        }
    }
}
