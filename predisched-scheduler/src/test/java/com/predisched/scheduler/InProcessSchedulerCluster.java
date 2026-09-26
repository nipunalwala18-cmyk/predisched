package com.predisched.scheduler;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.NodeConfig;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.common.time.LamportClock;
import com.predisched.election.ClusterView;
import com.predisched.fault.WorkerFailureDetector;
import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.ExecutionState;
import com.predisched.proto.ExecutionStatus;
import com.predisched.proto.QueryExecutionRequest;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.replication.PrimaryBackupReplication;
import com.predisched.replication.ReplicatedTaskStore;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import io.grpc.ForwardingServerCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * N scheduler nodes in one JVM, each a real {@link SchedulerNode} (Bully election, primary-backup
 * replication, failover coordinator) with its own queue and dispatcher, behind an in-process gRPC
 * server, sharing one fake worker. {@link #crashBeforeNextSubmitReply} kills the primary after a
 * submit is stored and replicated but before its reply leaves: the window prompt 10 asks about.
 */
final class InProcessSchedulerCluster implements AutoCloseable {

    /**
     * A worker test double that honours dispatch ids like the real engine (rule 3 keeps worker
     * classes out of scheduler tests): one run per dispatch id, counted per task.
     */
    static final class FakeWorker extends WorkerServiceGrpc.WorkerServiceImplBase {
        final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        private final Map<String, CompletableFuture<ExecuteResult>> dispatches =
                new ConcurrentHashMap<>();
        private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(4);
        private final long workMs;

        FakeWorker(long workMs) {
            this.workMs = workMs;
        }

        @Override
        public void executeTask(ExecuteRequest request, StreamObserver<ExecuteResult> observer) {
            String taskId = request.getTask().getTaskId();
            CompletableFuture<ExecuteResult> mine = new CompletableFuture<>();
            CompletableFuture<ExecuteResult> running =
                    dispatches.putIfAbsent(request.getDispatchId(), mine);
            if (running == null) {
                runs.computeIfAbsent(taskId, id -> new AtomicInteger()).incrementAndGet();
                timer.schedule(() -> mine.complete(ExecuteResult.newBuilder()
                        .setTaskId(taskId).setSuccess(true).setOutput("done " + taskId)
                        .setExecTimeMs(workMs).build()), workMs, TimeUnit.MILLISECONDS);
                running = mine;
            }
            running.thenAccept(result -> {
                observer.onNext(result);
                observer.onCompleted();
            });
        }

        @Override
        public void queryExecution(QueryExecutionRequest request,
                StreamObserver<ExecutionStatus> observer) {
            CompletableFuture<ExecuteResult> dispatch = dispatches.get(request.getDispatchId());
            ExecutionStatus.Builder reply = ExecutionStatus.newBuilder();
            if (dispatch == null) {
                reply.setState(ExecutionState.EXECUTION_UNKNOWN);
            } else if (dispatch.isDone()) {
                reply.setState(ExecutionState.EXECUTION_FINISHED).setResult(dispatch.join());
            } else {
                reply.setState(ExecutionState.EXECUTION_RUNNING);
            }
            observer.onNext(reply.build());
            observer.onCompleted();
        }

        int runsOf(String taskId) {
            AtomicInteger count = runs.get(taskId);
            return count == null ? 0 : count.get();
        }

        void close() {
            timer.shutdownNow();
        }
    }

    static final class Member {
        final int id;
        final SchedulerNode node;
        final TaskStore store;
        final Dispatcher dispatcher;
        final RetryCoordinator retries;
        final AtomicBoolean crashBeforeReply = new AtomicBoolean();
        volatile Server server;
        volatile boolean crashed;

        Member(int id, SchedulerNode node, TaskStore store, Dispatcher dispatcher,
                RetryCoordinator retries) {
            this.id = id;
            this.node = node;
            this.store = store;
            this.dispatcher = dispatcher;
            this.retries = retries;
        }

        PrimaryBackupReplication replication() {
            return (PrimaryBackupReplication) ((ReplicatedTaskStore) store).mode();
        }
    }

    private final String prefix = "scheduler-" + UUID.randomUUID() + "-";
    private final String workerName = "worker-" + UUID.randomUUID();
    private final Map<Integer, Member> members = new TreeMap<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private final Server workerServer;
    private final ManagedChannel workerChannel;

    private InProcessSchedulerCluster(FakeWorker worker) throws Exception {
        workerServer = InProcessServerBuilder.forName(workerName).addService(worker).build().start();
        workerChannel = InProcessChannelBuilder.forName(workerName).build();
    }

    static InProcessSchedulerCluster start(List<Integer> ids, FakeWorker worker) throws Exception {
        InProcessSchedulerCluster cluster = new InProcessSchedulerCluster(worker);
        NodeConfig config = new NodeConfig();
        config.getElection().setAlgorithm("bully");
        config.getElection().setTimeoutMs(300);
        config.getElection().setPingIntervalMs(100);
        config.getElection().setPingMisses(3);
        config.getReplication().setEnabled(true);
        config.getReplication().setMode("primary-backup");
        config.getReplication().setWriteTimeoutMs(1_000);
        for (int id : ids) {
            cluster.members.put(id, cluster.newMember(id, ids, config));
        }
        for (Member member : cluster.members.values()) {
            member.node.start();
        }
        return cluster;
    }

    private Member newMember(int id, List<Integer> ids, NodeConfig config) throws Exception {
        ClusterView view = new ClusterView(id, ids,
                peer -> InProcessChannelBuilder.forName(prefix + peer).build());
        SchedulerNode node = SchedulerNode.inCluster(id, "scheduler-" + id, config, view,
                new LamportClock());
        TaskStore store = node.taskStore(new InMemoryTaskStore());
        TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
        RetryCoordinator retries = new RetryCoordinator(store, queue,
                new RetryPolicy(20, 200, 0, 3, 42), new DeadLetterQueue());
        RunningTasks running = new RunningTasks();
        WorkerRegistry workers = new WorkerRegistry(60_000);
        workers.register(RegisterRequest.newBuilder()
                .setWorkerId("worker-1").setHost("in-process").setPort(0)
                .setCores(4).setMemoryMb(512).setPoolSize(8).build());
        WorkerServiceGrpc.WorkerServiceBlockingStub workerStub =
                WorkerServiceGrpc.newBlockingStub(workerChannel);
        WorkerStubs stubs = worker -> workerStub;
        Dispatcher dispatcher = new Dispatcher(
                store, queue, workers, stubs, retries, running, 0L, 2.0, 4, 20);
        SchedulerServiceImpl service = new SchedulerServiceImpl(
                store, new TaskValidator(4096), queue, retries, node, null);
        SchedulerFailover failover = new SchedulerFailover(store, queue, running, dispatcher,
                workers, stubs, retries, service.workflows(), null, 300);
        // The fake worker sends no heartbeats; the detector has its own tests.
        WorkerFailureDetector detector =
                new WorkerFailureDetector(failover, 60_000, 3, System::currentTimeMillis);
        node.enableFailover(failover, detector, store, 100);
        Member member = new Member(id, node, store, dispatcher, retries);
        member.server = InProcessServerBuilder.forName(prefix + id)
                .addService(service)
                .addService(node.service())
                .addService(node.replicationService())
                .intercept(crashSwitch(member))
                .build()
                .start();
        return member;
    }

    /** On a SubmitTask armed by {@link #crashBeforeNextSubmitReply}: crash instead of replying. */
    private ServerInterceptor crashSwitch(Member member) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                if (!call.getMethodDescriptor().getFullMethodName().endsWith("/SubmitTask")) {
                    return next.startCall(call, headers);
                }
                AtomicBoolean crashedHere = new AtomicBoolean();
                return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    @Override
                    public void sendMessage(RespT message) {
                        if (member.crashBeforeReply.compareAndSet(true, false)) {
                            crashedHere.set(true);
                            crash(member.id);
                            return;
                        }
                        super.sendMessage(message);
                    }

                    @Override
                    public void close(Status status, Metadata trailers) {
                        super.close(crashedHere.get()
                                ? Status.UNAVAILABLE.withDescription("scheduler crashed")
                                : status, trailers);
                    }
                }, headers);
            }
        };
    }

    /** Arms the primary to crash when it is about to answer the next submit. */
    void crashBeforeNextSubmitReply(int id) {
        members.get(id).crashBeforeReply.set(true);
    }

    /** Kills a node as a crash would: its server and its peer channels go first. */
    void crash(int id) {
        Member member = members.get(id);
        member.crashed = true;
        member.server.shutdownNow();
        member.node.close();
        member.dispatcher.close();
        member.retries.close();
    }

    Member member(int id) {
        return members.get(id);
    }

    /** The node that has finished promoting itself, once there is exactly one. */
    Optional<Integer> awaitPrimary(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<Integer> primaries = members.values().stream()
                    .filter(member -> !member.crashed && member.node.coordinator().isPrimary())
                    .map(member -> member.id)
                    .toList();
            if (primaries.size() == 1) {
                return Optional.of(primaries.get(0));
            }
            Thread.sleep(10);
        }
        return Optional.empty();
    }

    /** Waits until the primary's live set holds every other node still running. */
    boolean awaitAllBackupsLive(int primary, long timeoutMs) throws InterruptedException {
        long expected = members.values().stream()
                .filter(member -> !member.crashed && member.id != primary)
                .count();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (members.get(primary).replication().liveBackups().size() == expected) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
    }

    /** A channel per node, for a failover client; closed with the cluster. */
    Map<Integer, ManagedChannel> clientChannels() {
        Map<Integer, ManagedChannel> byId = new TreeMap<>();
        for (int id : members.keySet()) {
            ManagedChannel channel = InProcessChannelBuilder.forName(prefix + id).build();
            channels.add(channel);
            byId.put(id, channel);
        }
        return byId;
    }

    @Override
    public void close() {
        channels.forEach(ManagedChannel::shutdownNow);
        for (Member member : members.values()) {
            if (!member.crashed) {
                crash(member.id);
            }
        }
        workerChannel.shutdownNow();
        workerServer.shutdownNow();
    }
}
