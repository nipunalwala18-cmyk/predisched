package com.predisched.dashboard.cluster;

import com.predisched.proto.AdminReply;
import com.predisched.proto.ChaosReply;
import com.predisched.proto.ChaosServiceGrpc;
import com.predisched.proto.ClusterState;
import com.predisched.proto.DecisionExplanation;
import com.predisched.proto.EventMessage;
import com.predisched.proto.SetStrategyResponse;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** The live cluster as the dashboard API sees it (prompt 22); mocked in the controller tests. */
public interface Cluster {

    /** The primary's view: leader, nodes, workers, queue, strategy, model versions. */
    ClusterState state();

    /** Every scheduler asked for its view, keyed by address (unreachable ones missing). */
    Map<String, ClusterState> allStates();

    List<String> schedulers();

    SetStrategyResponse setStrategy(String strategy);

    AdminReply pause(boolean paused);

    AdminReply drain(String workerId);

    AdminReply election();

    TaskResponse submit(TaskRequest request, String authorization);

    DecisionExplanation explain(String taskId);

    /** A chaos call on a node: a worker id or {@code scheduler-N} (N from the peer list). */
    ChaosReply chaos(String node, Function<ChaosServiceGrpc.ChaosServiceBlockingStub, ChaosReply> call);

    /** The primary's chaos service (kill-primary). */
    ChaosReply chaosOnPrimary(Function<ChaosServiceGrpc.ChaosServiceBlockingStub, ChaosReply> call);

    /** A blocking stream of the primary's events, until the stream breaks. */
    Iterator<EventMessage> streamEvents();
}
