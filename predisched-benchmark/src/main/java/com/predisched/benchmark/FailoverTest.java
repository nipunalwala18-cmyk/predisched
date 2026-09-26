package com.predisched.benchmark;

import com.predisched.client.SchedulerClient;
import com.predisched.common.NodeConfig;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import io.grpc.StatusRuntimeException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Exp 8 measurement (prompt 10) against a live cluster started with the cluster scripts: submit
 * {@code --tasks} tasks through the failover client, kill the primary process right after task
 * {@code --kill-at} with {@code scripts/kill-primary} (a real process kill, not a flag), wait for
 * every task to finish, then check each one COMPLETED exactly once.
 *
 * <p>"Exactly once" is checked two ways: the task's attempt history holds exactly one SUCCEEDED
 * attempt, and the workers' event logs show how many times each task actually started. Timings
 * come from the new primary's event log: when it learnt it was leader, how long its promotion
 * took, when it first dispatched after the kill (a new dispatch, or a re-attach to a task the old
 * primary left running), and when it first dispatched a task it had not seen running.
 */
public final class FailoverTest {

    private static final Pattern TYPE = Pattern.compile("\"type\":\"([A-Z_]+)\"");
    private static final Pattern WALL = Pattern.compile("\"wall_ms\":(\\d+)");
    private static final Pattern TASK = Pattern.compile("\"task_id\":\"([^\"]*)\"");
    private static final Pattern LEADER = Pattern.compile("\"leader\":\"(\\d+)\"");
    private static final Pattern TOOK = Pattern.compile("\"took_ms\":\"(\\d+)\"");

    private FailoverTest() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = options(args);
        int tasks = Integer.parseInt(opts.getOrDefault("--tasks", "100"));
        int killAt = Integer.parseInt(opts.getOrDefault("--kill-at", "50"));
        String config = opts.getOrDefault("--config", "configs/cluster.yaml");
        TaskType type = TaskType.valueOf(opts.getOrDefault("--type", "SLEEP_TASK"));
        // Long enough that the kill lands while tasks are queued and running on workers, so the
        // new primary has in-doubt dispatches to settle, not just an empty queue.
        String input = opts.getOrDefault("--input", "ms=2000");
        long intervalMs = Long.parseLong(opts.getOrDefault("--submit-interval-ms", "20"));
        long timeoutS = Long.parseLong(opts.getOrDefault("--timeout-s", "300"));
        Path logs = Paths.get(opts.getOrDefault("--logs", "logs"));
        Path out = Paths.get(opts.getOrDefault("--out", "results/exp8-failover.csv"));
        List<String> killCommand = opts.containsKey("--kill-cmd")
                ? Arrays.asList(opts.get("--kill-cmd").split(" "))
                : defaultKillCommand(config);

        List<NodeConfig.PeerConfig> peers = NodeConfig.load(Paths.get(config)).getElection().getPeers();
        if (peers.isEmpty()) {
            throw new IllegalArgumentException(config + " lists no scheduler cluster");
        }
        String runId = "fo-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMdd-HHmmss"));
        long started = System.currentTimeMillis();
        try (SchedulerClient client = SchedulerClient.forCluster(peers,
                new SchedulerClient.Failover(80, 100, 2_000, 10_000))) {
            int primaryBefore = client.locateLeader().orElseThrow(() ->
                    new IllegalStateException("no leader: is the cluster running?"));
            System.out.printf(Locale.ROOT, "failover-test %s: %d x %s(%s), primary is scheduler %d,"
                    + " kill after task %d%n", runId, tasks, type, input, primaryBefore, killAt);

            List<String> ids = new ArrayList<>();
            int accepted = 0;
            long killedAt = 0;
            for (int i = 1; i <= tasks; i++) {
                String id = String.format(Locale.ROOT, "%s-%03d", runId, i);
                ids.add(id);
                TaskResponse reply = client.submitTask(id, type, input, 5);
                if (reply.getAccepted()) {
                    accepted++;
                } else {
                    System.out.println("  not accepted: " + id + ": " + reply.getMessage());
                }
                if (i == killAt) {
                    System.out.println("  task " + i + " submitted; killing the primary:");
                    runKill(killCommand);
                    killedAt = System.currentTimeMillis();
                }
                Thread.sleep(intervalMs);
            }
            System.out.printf(Locale.ROOT, "  %d/%d accepted; waiting for them to finish%n",
                    accepted, tasks);

            Map<String, TaskStatusResponse> finals = awaitAll(client, ids, timeoutS);
            int primaryAfter = client.locateLeader().orElse(-1);
            Map<String, Integer> starts = workerStarts(logs, runId);

            int completed = 0;
            int lost = 0;
            int duplicated = 0;
            for (String id : ids) {
                TaskStatusResponse status = finals.get(id);
                if (status == null || status.getStatus() != TaskStatus.COMPLETED) {
                    lost++;
                    System.out.println("  LOST " + id + ": " + (status == null
                            ? "unknown to the cluster" : status.getStatus()
                                    + " " + status.getAttemptHistoryList()));
                    continue;
                }
                completed++;
                long successes = status.getAttemptHistoryList().stream()
                        .filter(line -> line.contains("SUCCEEDED")).count();
                if (successes != 1) {
                    duplicated++;
                    System.out.println("  DUPLICATED " + id + ": " + status.getAttemptHistoryList());
                }
            }
            long ranTwice = starts.values().stream().filter(count -> count > 1).count();
            Path newLog = logs.resolve("scheduler-" + primaryAfter + ".jsonl");
            OptionalLong electedAt = firstEvent(newLog, "LEADER_ELECTED", killedAt,
                    line -> String.valueOf(primaryAfter).equals(group(LEADER, line)));
            long promotionMs = lastPromotion(newLog, killedAt);
            OptionalLong firstDispatch = firstEvent(newLog, "DISPATCH", killedAt, line -> true);
            OptionalLong firstReattach = firstEvent(newLog, "REATTACH", killedAt, line -> true);
            long electionMs = electedAt.isPresent() ? electedAt.getAsLong() - killedAt : -1;
            long newDispatchMs = firstDispatch.isPresent() ? firstDispatch.getAsLong() - killedAt : -1;
            long reattachMs = firstReattach.isPresent() ? firstReattach.getAsLong() - killedAt : -1;
            long failoverMs = newDispatchMs < 0 ? reattachMs
                    : reattachMs < 0 ? newDispatchMs : Math.min(newDispatchMs, reattachMs);
            double totalS = (System.currentTimeMillis() - started) / 1000.0;

            System.out.println();
            System.out.printf(Locale.ROOT, "Failover summary (%s)%n", runId);
            System.out.printf(Locale.ROOT, "  primary killed:     scheduler %d, after task %d of %d%n",
                    primaryBefore, killAt, tasks);
            System.out.printf(Locale.ROOT, "  new primary:        scheduler %d (leader %d ms after the"
                    + " kill, promotion %d ms)%n", primaryAfter, electionMs, promotionMs);
            System.out.printf(Locale.ROOT, "  failover time:      %d ms from the kill to the new"
                    + " primary's first dispatch or re-attach%n", failoverMs);
            System.out.printf(Locale.ROOT, "  first new dispatch: %d ms after the kill%n",
                    newDispatchMs);
            System.out.printf(Locale.ROOT, "  completed:          %d/%d%n", completed, tasks);
            System.out.printf(Locale.ROOT, "  lost:               %d%n", lost);
            System.out.printf(Locale.ROOT, "  duplicated:         %d (tasks started more than once on"
                    + " workers: %d; seen starting: %d)%n", duplicated, ranTwice, starts.size());
            System.out.printf(Locale.ROOT, "  wall time:          %.1f s%n", totalS);

            appendCsv(out, String.format(Locale.ROOT,
                    "%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.1f",
                    runId, input, tasks, killAt, primaryBefore, primaryAfter, accepted, completed,
                    lost, duplicated, ranTwice, electionMs, promotionMs, failoverMs, newDispatchMs,
                    totalS));
            System.out.println("  appended to " + out);
            if (lost > 0 || duplicated > 0) {
                System.exit(1);
            }
        }
    }

    /** scripts/kill-primary for this platform. */
    private static List<String> defaultKillCommand(String config) {
        boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
        return windows
                ? List.of("powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                        "scripts/kill-primary.ps1", "-Config", config)
                : List.of("bash", "scripts/kill-primary.sh", config);
    }

    private static void runKill(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        try (BufferedReader output = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = output.readLine()) != null) {
                System.out.println("    | " + line);
            }
        }
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("kill command failed with exit " + exit + ": " + command);
        }
    }

    /** Polls until every task is terminal or the timeout passes; the last status of each. */
    private static Map<String, TaskStatusResponse> awaitAll(SchedulerClient client, List<String> ids,
            long timeoutS) throws InterruptedException {
        Map<String, TaskStatusResponse> finals = new LinkedHashMap<>();
        long deadline = System.currentTimeMillis() + timeoutS * 1000;
        while (System.currentTimeMillis() < deadline) {
            boolean allDone = true;
            for (String id : ids) {
                TaskStatusResponse known = finals.get(id);
                if (known != null && isTerminal(known.getStatus())) {
                    continue;
                }
                try {
                    TaskStatusResponse status = client.getStatus(id);
                    finals.put(id, status);
                    allDone &= isTerminal(status.getStatus());
                } catch (StatusRuntimeException e) {
                    allDone = false;
                }
            }
            if (allDone) {
                break;
            }
            Thread.sleep(500);
        }
        return finals;
    }

    private static boolean isTerminal(TaskStatus status) {
        return status == TaskStatus.COMPLETED || status == TaskStatus.FAILED
                || status == TaskStatus.CANCELLED;
    }

    /** EXECUTE_START events per task of this run, over every worker's event log. */
    private static Map<String, Integer> workerStarts(Path logs, String runId) throws IOException {
        Map<String, Integer> starts = new HashMap<>();
        if (!Files.isDirectory(logs)) {
            return starts;
        }
        try (Stream<Path> files = Files.list(logs)) {
            for (Path file : files.filter(path -> path.getFileName().toString()
                    .matches("worker-.*\\.jsonl")).toList()) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    String task = group(TASK, line);
                    if ("EXECUTE_START".equals(group(TYPE, line)) && task != null
                            && task.startsWith(runId)) {
                        starts.merge(task, 1, Integer::sum);
                    }
                }
            }
        }
        return starts;
    }

    private static OptionalLong firstEvent(Path file, String type, long after,
            java.util.function.Predicate<String> matches) throws IOException {
        if (!Files.exists(file)) {
            return OptionalLong.empty();
        }
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if (type.equals(group(TYPE, line)) && matches.test(line)) {
                long wall = Long.parseLong(group(WALL, line));
                if (wall >= after) {
                    return OptionalLong.of(wall);
                }
            }
        }
        return OptionalLong.empty();
    }

    private static long lastPromotion(Path file, long after) throws IOException {
        if (!Files.exists(file)) {
            return -1;
        }
        long took = -1;
        for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            if ("PROMOTED".equals(group(TYPE, line))
                    && Long.parseLong(group(WALL, line)) >= after) {
                took = Long.parseLong(group(TOOK, line));
            }
        }
        return took;
    }

    private static String group(Pattern pattern, String line) {
        Matcher matcher = pattern.matcher(line);
        return matcher.find() ? matcher.group(1) : null;
    }

    private static void appendCsv(Path out, String row) throws IOException {
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        if (!Files.exists(out)) {
            Files.writeString(out, "run_id,input,tasks,kill_at,killed_primary,new_primary,"
                    + "accepted,completed,lost,duplicated,ran_twice_on_workers,election_ms,"
                    + "promotion_ms,failover_ms,first_new_dispatch_ms,wall_s\n",
                    StandardCharsets.UTF_8);
        }
        Files.writeString(out, row + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        return opts;
    }
}
