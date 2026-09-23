package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskExecutor;
import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CancellationException;

/**
 * BFS or PageRank on a generated graph. Irregular memory access makes execution time harder to
 * predict than the dense kernels, which is exactly the point (spec §7.2).
 *
 * <p>Every node links to its next two neighbours and one seeded random target (average
 * out-degree 3), so the graph is connected and both algorithms are deterministic for a seed.
 */
public class GraphTaskExecutor implements TaskExecutor {

    static final int PAGERANK_ITERATIONS = 20;
    static final double DAMPING = 0.85;

    @Override
    public TaskType type() {
        return TaskType.GRAPH_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.MEMORY_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        Map<String, String> params = InputParser.parse(input);
        int nodes = Integer.parseInt(params.get("nodes"));
        String algo = params.getOrDefault("algo", "bfs");
        long seed = params.containsKey("seed") ? Long.parseLong(params.get("seed")) : 42L;
        int[][] edges = buildGraph(nodes, seed);
        return switch (algo) {
            case "pagerank" -> runPageRank(nodes, edges);
            default -> runBfs(nodes, edges);
        };
    }

    private int[][] buildGraph(int nodes, long seed) {
        Random random = new Random(seed);
        int[][] edges = new int[nodes][3];
        for (int i = 0; i < nodes; i++) {
            if ((i & 0xFFFF) == 0 && Thread.currentThread().isInterrupted()) {
                throw new CancellationException("interrupted while building the graph");
            }
            edges[i][0] = (i + 1) % nodes;
            edges[i][1] = (i + 2) % nodes;
            edges[i][2] = random.nextInt(nodes);
        }
        return edges;
    }

    private ExecutionResult runBfs(int nodes, int[][] edges) {
        int[] distance = new int[nodes];
        for (int i = 0; i < nodes; i++) {
            distance[i] = -1;
        }
        ArrayDeque<Integer> queue = new ArrayDeque<>();
        distance[0] = 0;
        queue.add(0);
        int visited = 0;
        long totalDistance = 0;
        int maxDistance = 0;
        while (!queue.isEmpty()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("interrupted during BFS");
            }
            int node = queue.removeFirst();
            visited++;
            for (int next : edges[node]) {
                if (distance[next] == -1) {
                    distance[next] = distance[node] + 1;
                    totalDistance += distance[next];
                    maxDistance = Math.max(maxDistance, distance[next]);
                    queue.add(next);
                }
            }
        }
        return ExecutionResult.success("nodes=" + nodes + " algo=bfs visited=" + visited
                + " total_distance=" + totalDistance + " max_distance=" + maxDistance);
    }

    private ExecutionResult runPageRank(int nodes, int[][] edges) {
        double[] rank = new double[nodes];
        double[] next = new double[nodes];
        for (int i = 0; i < nodes; i++) {
            rank[i] = 1.0 / nodes;
        }
        double teleport = (1.0 - DAMPING) / nodes;
        for (int iter = 0; iter < PAGERANK_ITERATIONS; iter++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("interrupted during PageRank");
            }
            for (int i = 0; i < nodes; i++) {
                next[i] = teleport;
            }
            for (int i = 0; i < nodes; i++) {
                double share = DAMPING * rank[i] / edges[i].length;
                for (int target : edges[i]) {
                    next[target] += share;
                }
            }
            double[] swap = rank;
            rank = next;
            next = swap;
        }
        int top = 0;
        double sum = 0;
        for (int i = 0; i < nodes; i++) {
            sum += rank[i];
            if (rank[i] > rank[top]) {
                top = i;
            }
        }
        return ExecutionResult.success(String.format(Locale.ROOT,
                "nodes=%d algo=pagerank iterations=%d rank_sum=%.6f top_node=%d top_rank=%.6f",
                nodes, PAGERANK_ITERATIONS, sum, top, rank[top]));
    }
}
