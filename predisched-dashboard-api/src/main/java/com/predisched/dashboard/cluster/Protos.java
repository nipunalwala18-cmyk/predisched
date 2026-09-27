package com.predisched.dashboard.cluster;

import com.predisched.proto.CandidateBreakdown;
import com.predisched.proto.ClusterState;
import com.predisched.proto.DecisionExplanation;
import com.predisched.proto.NodeState;
import com.predisched.proto.WorkerEntry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Protobuf messages as plain maps for JSON (Jackson does not serialise protobuf). */
public final class Protos {

    private Protos() {}

    private static Object num(double v) {
        return Double.isFinite(v) ? v : null;
    }

    public static Map<String, Object> worker(WorkerEntry w) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("workerId", w.getWorkerId());
        m.put("host", w.getHost());
        m.put("port", w.getPort());
        m.put("poolSize", w.getPoolSize());
        m.put("healthy", w.getHealthy());
        m.put("draining", w.getDraining());
        m.put("activeThreads", w.getActiveThreads());
        m.put("queueLen", w.getQueueLen());
        return m;
    }

    public static Map<String, Object> node(NodeState n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", n.getId());
        m.put("address", n.getAddress());
        m.put("leader", n.getLeader());
        m.put("reachable", n.getReachable());
        return m;
    }

    public static Map<String, Object> state(ClusterState s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("answeredBy", s.getNodeId());
        m.put("leaderId", s.getLeaderId() == 0 ? null : s.getLeaderId());
        m.put("strategy", s.getStrategy());
        m.put("modelVersions", s.getModelVersions().isEmpty() ? null : s.getModelVersions());
        m.put("paused", s.getPaused());
        m.put("queueDepth", s.getQueueDepth());
        m.put("running", s.getRunning());
        m.put("arrivalRatePerSec", s.getArrivalRate());
        m.put("decisionP50Ms", s.getDecisionP50Ms());
        m.put("decisionP95Ms", s.getDecisionP95Ms());
        m.put("liveMaeMs", num(s.getLiveMaeMs()));
        m.put("speculations", s.getSpeculations());
        m.put("autoscale", s.getAutoscaleMode());
        m.put("tasksTotal", s.getTasksTotal());
        m.put("tasksCompleted", s.getTasksCompleted());
        m.put("tasksFailed", s.getTasksFailed());
        m.put("nodes", s.getNodesList().stream().map(Protos::node).toList());
        m.put("workers", s.getWorkersList().stream().map(Protos::worker).toList());
        m.put("atMs", s.getNowMs());
        return m;
    }

    public static Map<String, Object> explanation(DecisionExplanation e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("found", e.getFound());
        if (!e.getFound()) {
            m.put("message", e.getMessage());
            return m;
        }
        m.put("strategy", e.getStrategy());
        m.put("chosenWorker", e.getChosenWorker());
        m.put("fallback", e.getFallback());
        m.put("fallbackReason", e.getFallbackReason());
        m.put("decisionUs", e.getDecisionUs());
        m.put("modelVersions", e.getModelVersions());
        List<Map<String, Object>> candidates = e.getCandidatesList().stream().map(Protos::candidate)
                .toList();
        m.put("candidates", candidates);
        return m;
    }

    static Map<String, Object> candidate(CandidateBreakdown c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("workerId", c.getWorkerId());
        m.put("chosen", c.getChosen());
        m.put("score", num(c.getScore()));
        m.put("predQueueLen", num(c.getPredQueueLen()));
        m.put("predictedWaitMs", num(c.getPredictedWaitMs()));
        m.put("predictedExecMs", num(c.getPredictedExecMs()));
        m.put("overloadProb", num(c.getOverloadProb()));
        m.put("overloadPenaltyMs", num(c.getOverloadPenaltyMs()));
        m.put("cost", num(c.getCost()));
        m.put("skipped", c.getSkipped());
        m.put("skipReason", c.getSkipReason());
        m.put("coldStart", c.getColdStart());
        return m;
    }
}
