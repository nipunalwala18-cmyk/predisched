package com.predisched.dashboard.web;

import com.predisched.dashboard.cluster.Cluster;
import com.predisched.dashboard.cluster.Protos;
import com.predisched.dashboard.repo.Repositories;
import com.predisched.proto.ClusterState;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Workers, leader, replication and clocks (spec §15.3, §15.10). */
@RestController
public class ClusterController {

    private final Cluster cluster;
    private final Repositories repo;

    public ClusterController(Cluster cluster, Repositories repo) {
        this.cluster = cluster;
        this.repo = repo;
    }

    /** Live state from the primary where it has it, the latest stored metrics otherwise. */
    @GetMapping("/api/workers")
    public Map<String, Object> workers() {
        Map<String, Object> live = new LinkedHashMap<>();
        String error = null;
        try {
            for (Map<String, Object> w : Protos.state(cluster.state()).get("workers") instanceof
                    List<?> list ? (List<Map<String, Object>>) list : List.<Map<String, Object>>of()) {
                live.put((String) w.get("workerId"), w);
            }
        } catch (RuntimeException e) {
            error = e.getMessage();
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> row : repo.workersLatest()) {
            Map<String, Object> w = new LinkedHashMap<>(row);
            Object now = live.remove(String.valueOf(row.get("worker_id")));
            w.put("live", now);
            out.add(w);
        }
        live.values().forEach(w -> out.add(Map.of("worker_id", ((Map<?, ?>) w).get("workerId"),
                "live", w)));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("workers", out);
        result.put("clusterError", error);
        return result;
    }

    @GetMapping("/api/workers/{id}/history")
    public Map<String, Object> history(@PathVariable String id,
            @RequestParam(defaultValue = "5") int minutes) {
        return Map.of("workerId", id, "minutes", minutes,
                "samples", repo.workerHistory(id, Math.max(1, Math.min(minutes, 240))));
    }

    @GetMapping("/api/cluster/leader")
    public Map<String, Object> leader() {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, ClusterState> states = cluster.allStates();
        List<Map<String, Object>> nodes = new ArrayList<>();
        Integer leader = null;
        String leaderAddress = null;
        for (String address : cluster.schedulers()) {
            ClusterState s = states.get(address);
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("address", address);
            n.put("reachable", s != null);
            if (s != null) {
                n.put("nodeId", s.getNodeId());
                n.put("isLeader", s.getLeader());
                n.put("believesLeader", s.getLeaderId() == 0 ? null : s.getLeaderId());
                if (s.getLeader()) {
                    leader = s.getLeaderId() == 0 ? null : s.getLeaderId();
                    leaderAddress = address;
                }
            }
            nodes.add(n);
        }
        out.put("leader", leader);
        out.put("leaderAddress", leaderAddress);
        out.put("nodes", nodes);
        out.put("elections", repo.events(List.of("ELECTION_START", "LEADER_ELECTED"), 20));
        return out;
    }

    @GetMapping("/api/replication/status")
    public Map<String, Object> replication() {
        List<Map<String, Object>> replicas = repo.replication();
        long max = replicas.stream().mapToLong(r -> ((Number) r.get("max_seq")).longValue())
                .max().orElse(0);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : replicas) {
            Map<String, Object> m = new LinkedHashMap<>(r);
            m.put("lag", max - ((Number) r.get("max_seq")).longValue());
            out.add(m);
        }
        return Map.of("headSeq", max, "replicas", out);
    }

    @GetMapping("/api/clock/offsets")
    public Map<String, Object> clocks() {
        return Map.of("nodes", repo.clockOffsets());
    }
}
