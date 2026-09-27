package com.predisched.client;

import com.predisched.client.workload.SchedulerGateway;
import com.predisched.common.NodeConfig;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.obs.TraceContext;
import com.predisched.common.time.Clocks;
import com.predisched.common.time.LamportClock;
import com.predisched.proto.Ack;
import com.predisched.proto.DeadLetterList;
import com.predisched.proto.DeadLetterRequest;
import com.predisched.proto.ElectionServiceGrpc;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkflowRequest;
import com.predisched.proto.WorkflowStatusRequest;
import com.predisched.proto.WorkflowStatusResponse;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The scheduler API for the CLI, the replayer and the benchmarks.
 *
 * <p>Given one address it talks to that scheduler and nothing else. Given the whole cluster
 * (prompt 10) it fails over: it sends every call to the scheduler it believes is the primary,
 * follows the {@code leader=<id>} hint in a follower's refusal, and when the primary stops
 * answering it asks the others who leads now and retries with exponential backoff. Submits carry
 * the client-chosen task id, so a submit retried after its reply was lost is idempotent: the
 * scheduler answers "already accepted" instead of storing it twice.
 */
public class SchedulerClient implements AutoCloseable, SchedulerGateway {

    private static final Logger log = LoggerFactory.getLogger(SchedulerClient.class);
    private static final Pattern LEADER_HINT = Pattern.compile("leader=(\\d+)");

    /** How a cluster client retries: attempts per call, backoff from first to max, per-call deadline. */
    public record Failover(int maxAttempts, long firstBackoffMs, long maxBackoffMs,
            long callTimeoutMs) {
        public static final Failover DEFAULT = new Failover(40, 100, 2_000, 10_000);
        /** One address, no failover: a call is made once, as before prompt 10. */
        public static final Failover NONE = new Failover(1, 0, 0, 0);
    }

    private final Map<Integer, ManagedChannel> channels;
    private final boolean ownsChannels;
    private final Failover failover;
    private final AtomicInteger target;
    private final LamportClock clock = Clocks.lamport();

    public SchedulerClient(String host, int port) {
        this(Map.of(0, com.predisched.common.net.Transport.get().channel(
                host, port, LamportInterceptors.client(Clocks.lamport()))), true, Failover.NONE);
    }

    /**
     * @param channels a channel per scheduler election id; the caller keeps ownership
     */
    public SchedulerClient(Map<Integer, ManagedChannel> channels, Failover failover) {
        this(channels, false, failover);
    }

    private SchedulerClient(Map<Integer, ManagedChannel> channels, boolean ownsChannels,
            Failover failover) {
        if (channels.isEmpty()) {
            throw new IllegalArgumentException("at least one scheduler address is needed");
        }
        this.channels = new TreeMap<>(channels);
        this.ownsChannels = ownsChannels;
        this.failover = failover;
        this.target = new AtomicInteger(this.channels.keySet().iterator().next());
    }

    /** A failover client for every scheduler in a cluster config's {@code election.peers}. */
    public static SchedulerClient forCluster(List<NodeConfig.PeerConfig> peers, Failover failover) {
        Map<Integer, ManagedChannel> channels = new HashMap<>();
        for (NodeConfig.PeerConfig peer : peers) {
            channels.put(peer.getId(), com.predisched.common.net.Transport.get().channel(
                    peer.getHost(), peer.getPort(), LamportInterceptors.client(Clocks.lamport())));
        }
        SchedulerClient client = new SchedulerClient(channels, true, failover);
        client.locateLeader().ifPresent(client.target::set);
        return client;
    }

    /** The scheduler id calls go to now. */
    public int target() {
        return target.get();
    }

    public TaskResponse submitTask(String taskId, TaskType type, String input, int priority) {
        return submitTask(taskId, type, input, priority, TraceContext.newTraceId());
    }

    /** Replayer entry point: same submit through the {@link SchedulerGateway} interface. */
    @Override
    public TaskResponse submit(
            String taskId,
            TaskType type,
            String input,
            int priority,
            String traceId,
            long timeoutMs,
            int maxRetries) {
        return submitTask(taskId, type, input, priority, traceId, timeoutMs, maxRetries);
    }

    /** Submits with an explicit trace id, which follows the task to the worker (F10). */
    public TaskResponse submitTask(
            String taskId, TaskType type, String input, int priority, String traceId) {
        return submitTask(taskId, type, input, priority, traceId, 0L, -1);
    }

    /**
     * Full submit.
     *
     * @param timeoutMs  0 leaves the scheduler's default in place (F5)
     * @param maxRetries negative leaves the scheduler's default in place (F4)
     */
    public TaskResponse submitTask(
            String taskId,
            TaskType type,
            String input,
            int priority,
            String traceId,
            long timeoutMs,
            int maxRetries) {
        return submitTask(taskId, type, input, priority, traceId, timeoutMs, maxRetries, 0L, false);
    }

    /** Replayer entry point with a deadline (F3). */
    @Override
    public TaskResponse submit(String taskId, TaskType type, String input, int priority,
            String traceId, long timeoutMs, int maxRetries, long deadlineMs) {
        return submitTask(taskId, type, input, priority, traceId, timeoutMs, maxRetries,
                deadlineMs, false);
    }

    /**
     * Full submit.
     *
     * @param deadlineMs ms after submit by which it should complete (F3); 0 for none
     * @param noCache    run it even if an identical result is cached (F6)
     */
    public TaskResponse submitTask(
            String taskId,
            TaskType type,
            String input,
            int priority,
            String traceId,
            long timeoutMs,
            int maxRetries,
            long deadlineMs,
            boolean noCache) {
        TraceContext.set(traceId);
        try {
            return call("submit " + taskId, stub -> stub.submitTask(TaskRequest.newBuilder()
                    .setTaskId(taskId)
                    .setType(type)
                    .setInput(input)
                    .setPriority(priority)
                    .setLamportTime(clock.current())
                    .setTraceId(traceId)
                    .setTimeoutMs(timeoutMs)
                    .setMaxRetries(maxRetries)
                    .setDeadlineMs(deadlineMs)
                    .setNoCache(noCache)
                    .build()), SchedulerClient::refusal);
        } finally {
            TraceContext.clear();
        }
    }

    public DeadLetterList listDeadLetters(int limit) {
        return call("dlq list", stub -> stub.listDeadLetters(
                DeadLetterRequest.newBuilder().setLimit(limit).build()), reply -> null);
    }

    public TaskResponse retryDeadLetter(String taskId) {
        return call("dlq retry " + taskId, stub -> stub.retryDeadLetter(
                TaskStatusRequest.newBuilder().setTaskId(taskId).build()), SchedulerClient::refusal);
    }

    public TaskStatusResponse getStatus(String taskId) {
        return call("status " + taskId, stub -> stub.getTaskStatus(
                TaskStatusRequest.newBuilder().setTaskId(taskId).build()), reply -> null);
    }

    /** Replayer entry point: same status query through the {@link SchedulerGateway} interface. */
    @Override
    public TaskStatusResponse status(String taskId) {
        return getStatus(taskId);
    }

    public TaskResponse submitWorkflow(WorkflowRequest request) {
        return call("workflow " + request.getWorkflowId(), stub -> stub.submitWorkflow(request),
                SchedulerClient::refusal);
    }

    public WorkflowStatusResponse workflowStatus(String workflowId) {
        return call("workflow status " + workflowId, stub -> stub.getWorkflowStatus(
                WorkflowStatusRequest.newBuilder().setWorkflowId(workflowId).build()),
                reply -> null);
    }

    /** Switches the scheduler's strategy at runtime (prompt 18). */
    public com.predisched.proto.SetStrategyResponse setStrategy(String strategy) {
        return call("strategy " + strategy, stub -> stub.setStrategy(
                com.predisched.proto.SetStrategyRequest.newBuilder().setStrategy(strategy)
                        .build()),
                reply -> !reply.getOk() && reply.getMessage().startsWith("not the leader")
                        ? reply.getMessage() : null);
    }

    /** The registered workers and their addresses (prompt 19: chaos CLI). */
    public com.predisched.proto.WorkerList listWorkers() {
        return call("list workers", stub -> stub.listWorkers(
                com.predisched.proto.ListWorkersRequest.getDefaultInstance()), reply -> null);
    }

    /** The per-worker breakdown of one placement (F13, prompt 18). */
    public com.predisched.proto.DecisionExplanation explain(String taskId) {
        return call("explain " + taskId, stub -> stub.explainDecision(
                TaskStatusRequest.newBuilder().setTaskId(taskId).build()), reply -> null);
    }

    public TaskResponse cancelTask(String taskId) {
        return call("cancel " + taskId, stub -> stub.cancelTask(
                TaskStatusRequest.newBuilder().setTaskId(taskId).build()), SchedulerClient::refusal);
    }

    /**
     * Makes one call, failing over as the class comment describes.
     *
     * @param refusal the {@code leader=} message when a reply is a "not the leader" refusal, else
     *     null: a refusal is retried against the leader it names, or after a backoff
     */
    private <T> T call(String what, Function<SchedulerServiceGrpc.SchedulerServiceBlockingStub, T> rpc,
            Function<T, String> refusal) {
        long backoffMs = failover.firstBackoffMs();
        StatusRuntimeException lastError = null;
        T lastReply = null;
        for (int attempt = 1; attempt <= failover.maxAttempts(); attempt++) {
            int id = target.get();
            try {
                T reply = rpc.apply(stub(id));
                String notLeader = refusal.apply(reply);
                if (notLeader == null) {
                    return reply;
                }
                lastReply = reply;
                Optional<Integer> hinted = leaderIn(notLeader);
                if (hinted.isPresent() && hinted.get() != id && channels.containsKey(hinted.get())) {
                    log.info("Scheduler {} is not the primary; {} goes to scheduler {}",
                            id, what, hinted.get());
                    target.set(hinted.get());
                    continue;
                }
                // No leader yet, or the leader is still promoting itself: wait and ask again.
            } catch (StatusRuntimeException e) {
                if (!retryable(e) || failover.maxAttempts() == 1) {
                    throw e;
                }
                lastError = e;
                Optional<Integer> leader = locateLeader();
                log.info("Scheduler {} did not answer {} ({}); {}", id, what,
                        e.getStatus().getCode(), leader.map(l -> "leader is now " + l)
                                .orElse("no leader known yet, retrying"));
                leader.ifPresent(target::set);
                if (leader.isPresent() && leader.get() != id) {
                    continue;   // a new leader answered the vote: try it now, not after a wait
                }
            }
            if (attempt < failover.maxAttempts()) {
                sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, failover.maxBackoffMs());
            }
        }
        if (lastReply != null) {
            return lastReply;
        }
        throw lastError != null
                ? lastError
                : Status.UNAVAILABLE.withDescription("no scheduler took " + what)
                        .asRuntimeException();
    }

    /**
     * Asks every scheduler who leads (the election {@code Ping}) and returns the id most of them
     * name, if that one is reachable.
     */
    public Optional<Integer> locateLeader() {
        if (channels.size() == 1) {
            return Optional.empty();
        }
        Map<Integer, Integer> votes = new HashMap<>();
        for (Map.Entry<Integer, ManagedChannel> scheduler : channels.entrySet()) {
            try {
                Ack reply = ElectionServiceGrpc.newBlockingStub(
                                com.predisched.common.net.Transport.reconnecting(
                                        scheduler.getValue()))
                        .withDeadlineAfter(pingTimeoutMs(), TimeUnit.MILLISECONDS)
                        .ping(Ack.getDefaultInstance());
                leaderIn(reply.getMessage()).ifPresent(leader -> votes.merge(leader, 1, Integer::sum));
            } catch (StatusRuntimeException e) {
                // Down: no vote.
            }
        }
        return votes.entrySet().stream()
                .filter(vote -> channels.containsKey(vote.getKey()))
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    private SchedulerServiceGrpc.SchedulerServiceBlockingStub stub(int id) {
        SchedulerServiceGrpc.SchedulerServiceBlockingStub stub = SchedulerServiceGrpc.newBlockingStub(
                com.predisched.common.net.Transport.reconnecting(channels.get(id)));
        return failover.callTimeoutMs() > 0
                ? stub.withDeadlineAfter(failover.callTimeoutMs(), TimeUnit.MILLISECONDS)
                : stub;
    }

    private long pingTimeoutMs() {
        return Math.max(200, Math.min(1_000, failover.callTimeoutMs()));
    }

    /** The message of a "not the leader" refusal, else null. */
    private static String refusal(TaskResponse reply) {
        return !reply.getAccepted() && reply.getMessage().startsWith("not the leader")
                ? reply.getMessage()
                : null;
    }

    static Optional<Integer> leaderIn(String message) {
        Matcher matcher = LEADER_HINT.matcher(message);
        return matcher.find() ? Optional.of(Integer.parseInt(matcher.group(1))) : Optional.empty();
    }

    private static boolean retryable(StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();
        return code == Status.Code.UNAVAILABLE
                || code == Status.Code.DEADLINE_EXCEEDED
                || code == Status.Code.CANCELLED;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription("interrupted").asRuntimeException();
        }
    }

    @Override
    public void close() {
        if (ownsChannels) {
            channels.values().forEach(ManagedChannel::shutdownNow);
        }
    }
}
