package com.predisched.scheduler;

import com.predisched.common.NodeConfig;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.obs.EventLog;
import com.predisched.proto.AdminReply;
import com.predisched.proto.AdminServiceGrpc;
import com.predisched.proto.ClusterState;
import com.predisched.proto.ClusterStateRequest;
import com.predisched.proto.DrainWorkerRequest;
import com.predisched.proto.ElectionRequest;
import com.predisched.proto.EventMessage;
import com.predisched.proto.NodeState;
import com.predisched.proto.PauseRequest;
import com.predisched.proto.SetStrategyRequest;
import com.predisched.proto.SetStrategyResponse;
import com.predisched.proto.StreamRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.scheduler.strategy.PredictiveStrategy;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code AdminService} (prompt 22, F7): the dashboard API's controls and live view of one
 * scheduler. Mutating calls (strategy, pause, drain, election) only on the primary; the others
 * answer {@code ok=false} with a {@code leader=<id>} hint. Every admin action is an
 * {@code ADMIN} event.
 */
public final class AdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(AdminServiceImpl.class);
    private static final Pattern EVENT_TYPE = Pattern.compile("\"type\":\"([A-Z_]+)\"");

    /** The pieces of a scheduler the admin service reads and drives. */
    public record Parts(String nodeId, Dispatcher dispatcher, DispatcherAdmin admin,
            WorkerRegistry workers, RunningTasks running, TaskQueue queue, TaskStore store,
            Leadership leadership, SchedulerNode node, List<NodeConfig.PeerConfig> peers,
            Supplier<Double> arrivalRate, String autoscaleMode) {}

    private final Parts p;

    public AdminServiceImpl(Parts parts) {
        this.p = parts;
    }

    private String refusal() {
        if (p.leadership().isLeader()) {
            return null;
        }
        return "not the leader; leader=" + p.leadership().leader().map(String::valueOf)
                .orElse("unknown");
    }

    private static void reply(StreamObserver<AdminReply> observer, boolean ok, String message) {
        observer.onNext(AdminReply.newBuilder().setOk(ok).setMessage(message).build());
        observer.onCompleted();
    }

    private void audit(String action, Map<String, String> details) {
        java.util.Map<String, String> all = new java.util.LinkedHashMap<>(details);
        all.put("action", action);
        EventLog.get().event("ADMIN", "", all);
        log.info("Admin {} {}", action, details);
    }

    @Override
    public void setStrategy(SetStrategyRequest request,
            StreamObserver<SetStrategyResponse> observer) {
        String refusal = refusal();
        if (refusal != null) {
            observer.onNext(SetStrategyResponse.newBuilder().setOk(false).setMessage(refusal)
                    .build());
            observer.onCompleted();
            return;
        }
        SetStrategyResponse response = p.admin().setStrategy(request.getStrategy().trim());
        audit("set_strategy", Map.of("requested", request.getStrategy(),
                "previous", response.getPrevious(), "current", response.getCurrent(),
                "ok", String.valueOf(response.getOk())));
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void pauseQueue(PauseRequest request, StreamObserver<AdminReply> observer) {
        String refusal = refusal();
        if (refusal != null) {
            reply(observer, false, refusal);
            return;
        }
        p.dispatcher().pause();
        audit("pause", Map.of("queue_depth", String.valueOf(p.queue().size())));
        reply(observer, true, "paused: " + p.queue().size() + " queued");
    }

    @Override
    public void resumeQueue(PauseRequest request, StreamObserver<AdminReply> observer) {
        String refusal = refusal();
        if (refusal != null) {
            reply(observer, false, refusal);
            return;
        }
        p.dispatcher().resume();
        audit("resume", Map.of("queue_depth", String.valueOf(p.queue().size())));
        reply(observer, true, "resumed");
    }

    @Override
    public void drainWorker(DrainWorkerRequest request, StreamObserver<AdminReply> observer) {
        String refusal = refusal();
        if (refusal != null) {
            reply(observer, false, refusal);
            return;
        }
        String id = request.getWorkerId();
        if (p.workers().get(id).isEmpty()) {
            reply(observer, false, "unknown worker: " + id);
            return;
        }
        p.workers().markDraining(id);
        audit("drain", Map.of("worker", id,
                "in_flight", String.valueOf(p.running().countFor(id))));
        reply(observer, true, id + " draining: " + p.running().countFor(id)
                + " in flight finish, no new dispatches");
    }

    @Override
    public void triggerElection(ElectionRequest request, StreamObserver<AdminReply> observer) {
        if (p.node() == null) {
            reply(observer, false, "not clustered: this scheduler is always the leader");
            return;
        }
        audit("election", Map.of("leader_before",
                p.leadership().leader().map(String::valueOf).orElse("unknown")));
        p.node().election().startElection();
        reply(observer, true, "election started by " + p.nodeId());
    }

    @Override
    public void getClusterState(ClusterStateRequest request, StreamObserver<ClusterState> observer) {
        observer.onNext(state());
        observer.onCompleted();
    }

    ClusterState state() {
        Dispatcher d = p.dispatcher();
        SchedulingStrategy strategy = d.strategy();
        double[] decisions = d.decisionPercentiles(50, 95);
        long total = 0;
        long completed = 0;
        long failed = 0;
        for (TaskRecord t : p.store().list()) {
            total++;
            if (t.status() == TaskStatus.COMPLETED) {
                completed++;
            } else if (t.status() == TaskStatus.FAILED) {
                failed++;
            }
        }
        Optional<Integer> leader = p.leadership().leader();
        ClusterState.Builder out = ClusterState.newBuilder()
                .setNodeId(p.nodeId())
                .setLeader(p.leadership().isLeader())
                .setLeaderId(leader.orElse(0))
                .addAllWorkers(p.admin().listWorkers().getWorkersList())
                .setQueueDepth(p.queue().size())
                .setRunning(p.running().size())
                .setStrategy(strategy.name())
                .setModelVersions(strategy instanceof PredictiveStrategy ps
                        ? ps.modelVersions() : "")
                .setPaused(d.isPaused())
                .setArrivalRate(p.arrivalRate().get())
                .setDecisionP50Ms(decisions[0])
                .setDecisionP95Ms(decisions[1])
                .setLiveMaeMs(strategy instanceof PredictiveStrategy ps
                        ? ps.accuracy().mae() : Double.NaN)
                .setSpeculations(d.speculation().launched())
                .setAutoscaleMode(p.autoscaleMode() == null ? "none" : p.autoscaleMode())
                .setNowMs(System.currentTimeMillis())
                .setTasksTotal(total)
                .setTasksCompleted(completed)
                .setTasksFailed(failed);
        for (NodeConfig.PeerConfig peer : p.peers()) {
            out.addNodes(NodeState.newBuilder().setId(peer.getId())
                    .setAddress(peer.getHost() + ":" + peer.getPort())
                    .setLeader(leader.isPresent() && leader.get() == peer.getId())
                    .setReachable(true));
        }
        return out.build();
    }

    @Override
    public void streamEvents(StreamRequest request, StreamObserver<EventMessage> observer) {
        Set<String> types = Set.copyOf(request.getTypesList());
        ServerCallStreamObserver<EventMessage> call =
                (ServerCallStreamObserver<EventMessage>) observer;
        Object lock = new Object();
        Consumer<String>[] self = new Consumer[1];
        self[0] = line -> {
            if (!types.isEmpty()) {
                Matcher m = EVENT_TYPE.matcher(line);
                if (!m.find() || !types.contains(m.group(1))) {
                    return;
                }
            }
            synchronized (lock) {
                if (call.isCancelled()) {
                    EventLog.removeListener(self[0]);
                    return;
                }
                call.onNext(EventMessage.newBuilder().setJson(line).build());
            }
        };
        call.setOnCancelHandler(() -> EventLog.removeListener(self[0]));
        EventLog.addListener(self[0]);
    }
}
