package com.predisched.dashboard.cluster;

import com.predisched.common.NodeConfig;
import com.predisched.common.net.Transport;
import com.predisched.dashboard.DashboardProperties;
import com.predisched.proto.AdminReply;
import com.predisched.proto.AdminServiceGrpc;
import com.predisched.proto.ChaosReply;
import com.predisched.proto.ChaosServiceGrpc;
import com.predisched.proto.ClusterState;
import com.predisched.proto.ClusterStateRequest;
import com.predisched.proto.DecisionExplanation;
import com.predisched.proto.DrainWorkerRequest;
import com.predisched.proto.ElectionRequest;
import com.predisched.proto.EventMessage;
import com.predisched.proto.PauseRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.SetStrategyRequest;
import com.predisched.proto.SetStrategyResponse;
import com.predisched.proto.StreamRequest;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.WorkerEntry;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Cluster} over gRPC. The schedulers come from {@code dashboard.clusterConfig}'s election
 * peers, else {@code dashboard.schedulers}; the primary is whichever answers
 * {@code GetClusterState} with {@code leader=true} (cached until a call to it fails).
 */
public class GrpcCluster implements Cluster {

    private static final Logger log = LoggerFactory.getLogger(GrpcCluster.class);

    /** How to reach an address; in-process channels in the integration test. */
    public interface Channels {
        ManagedChannel open(String address);
    }

    private final List<String> schedulers;
    private final Map<Integer, String> byElectionId = new LinkedHashMap<>();
    private final long timeoutMs;
    private final Channels channels;
    private final Map<String, ManagedChannel> open = new ConcurrentHashMap<>();
    private volatile String primary;

    public GrpcCluster(DashboardProperties props) {
        this(props, address -> {
            String[] hp = address.split(":");
            return Transport.get().channel(hp[0], Integer.parseInt(hp[1]));
        });
    }

    public GrpcCluster(DashboardProperties props, Channels channels) {
        this.timeoutMs = props.getGrpcTimeoutMs();
        this.channels = channels;
        List<String> list = new ArrayList<>(props.getSchedulers());
        if (props.getClusterConfig() != null && !props.getClusterConfig().isBlank()) {
            try {
                Path path = Paths.get(props.getClusterConfig());
                if (!path.isAbsolute()) {
                    path = Paths.get(props.getRepoRoot()).resolve(path);
                }
                if (Files.exists(path)) {
                    List<NodeConfig.PeerConfig> peers = NodeConfig.load(path).getElection()
                            .getPeers();
                    if (!peers.isEmpty()) {
                        list.clear();
                        for (NodeConfig.PeerConfig p : peers) {
                            String address = p.getHost() + ":" + p.getPort();
                            list.add(address);
                            byElectionId.put(p.getId(), address);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Cannot read {}: {}; using {}", props.getClusterConfig(), e.toString(),
                        list);
            }
        }
        this.schedulers = List.copyOf(list);
        log.info("Dashboard API watches schedulers {}", schedulers);
    }

    private ManagedChannel channel(String address) {
        return open.computeIfAbsent(address, channels::open);
    }

    private AdminServiceGrpc.AdminServiceBlockingStub admin(String address) {
        return AdminServiceGrpc.newBlockingStub(channel(address))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<String> schedulers() {
        return schedulers;
    }

    /** The primary's address: the cached one while it still says so, else a fresh search. */
    String primary() {
        String cached = primary;
        if (cached != null) {
            try {
                if (admin(cached).getClusterState(ClusterStateRequest.getDefaultInstance())
                        .getLeader()) {
                    return cached;
                }
            } catch (StatusRuntimeException e) {
                // search again
            }
        }
        for (String address : schedulers) {
            try {
                if (admin(address).getClusterState(ClusterStateRequest.getDefaultInstance())
                        .getLeader()) {
                    primary = address;
                    return address;
                }
            } catch (StatusRuntimeException e) {
                // try the next one
            }
        }
        primary = null;
        throw new ClusterUnavailableException("no primary among " + schedulers);
    }

    private <T> T onPrimary(Function<String, T> call) {
        String address = primary();
        try {
            return call.apply(address);
        } catch (StatusRuntimeException e) {
            primary = null;
            throw new ClusterUnavailableException("primary " + address + ": "
                    + e.getStatus().getCode());
        }
    }

    @Override
    public ClusterState state() {
        return onPrimary(a -> admin(a).getClusterState(ClusterStateRequest.getDefaultInstance()));
    }

    @Override
    public Map<String, ClusterState> allStates() {
        Map<String, ClusterState> out = new LinkedHashMap<>();
        for (String address : schedulers) {
            try {
                out.put(address, admin(address)
                        .getClusterState(ClusterStateRequest.getDefaultInstance()));
            } catch (StatusRuntimeException e) {
                // unreachable: left out
            }
        }
        return out;
    }

    @Override
    public SetStrategyResponse setStrategy(String strategy) {
        return onPrimary(a -> admin(a).setStrategy(SetStrategyRequest.newBuilder()
                .setStrategy(strategy).build()));
    }

    @Override
    public AdminReply pause(boolean paused) {
        return onPrimary(a -> paused
                ? admin(a).pauseQueue(PauseRequest.getDefaultInstance())
                : admin(a).resumeQueue(PauseRequest.getDefaultInstance()));
    }

    @Override
    public AdminReply drain(String workerId) {
        return onPrimary(a -> admin(a).drainWorker(DrainWorkerRequest.newBuilder()
                .setWorkerId(workerId).build()));
    }

    @Override
    public AdminReply election() {
        return onPrimary(a -> admin(a).triggerElection(ElectionRequest.getDefaultInstance()));
    }

    @Override
    public TaskResponse submit(TaskRequest request, String authorization) {
        return onPrimary(a -> {
            SchedulerServiceGrpc.SchedulerServiceBlockingStub stub =
                    SchedulerServiceGrpc.newBlockingStub(channel(a))
                            .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
            if (authorization != null && !authorization.isBlank()) {
                Metadata headers = new Metadata();
                headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                        authorization);
                stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
            }
            return stub.submitTask(request);
        });
    }

    @Override
    public DecisionExplanation explain(String taskId) {
        return onPrimary(a -> SchedulerServiceGrpc.newBlockingStub(channel(a))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .explainDecision(TaskStatusRequest.newBuilder().setTaskId(taskId).build()));
    }

    @Override
    public ChaosReply chaos(String node,
            Function<ChaosServiceGrpc.ChaosServiceBlockingStub, ChaosReply> call) {
        String address = resolve(node);
        return call.apply(ChaosServiceGrpc.newBlockingStub(channel(address))
                .withDeadlineAfter(timeoutMs * 5, TimeUnit.MILLISECONDS));
    }

    @Override
    public ChaosReply chaosOnPrimary(
            Function<ChaosServiceGrpc.ChaosServiceBlockingStub, ChaosReply> call) {
        String address = primary();
        ChaosReply reply = call.apply(ChaosServiceGrpc.newBlockingStub(channel(address))
                .withDeadlineAfter(timeoutMs * 5, TimeUnit.MILLISECONDS));
        primary = null;
        return reply;
    }

    /** A worker id through the primary's worker list, or scheduler-N through the peers. */
    String resolve(String node) {
        if (node.startsWith("scheduler-")) {
            try {
                String address = byElectionId.get(Integer.parseInt(node.substring(10)));
                if (address != null) {
                    return address;
                }
            } catch (NumberFormatException ignored) {
                // fall through
            }
        }
        for (WorkerEntry w : state().getWorkersList()) {
            if (w.getWorkerId().equals(node)) {
                return w.getHost() + ":" + w.getPort();
            }
        }
        throw new IllegalArgumentException("unknown node " + node
                + ": not a registered worker nor scheduler-N");
    }

    @Override
    public Iterator<EventMessage> streamEvents() {
        String address = primary();
        return AdminServiceGrpc.newBlockingStub(channel(address))
                .streamEvents(StreamRequest.getDefaultInstance());
    }

    public void close() {
        open.values().forEach(ManagedChannel::shutdownNow);
    }
}
