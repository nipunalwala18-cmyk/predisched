package com.predisched.election;

import com.predisched.proto.Ack;
import com.predisched.proto.CoordinatorMsg;
import com.predisched.proto.ElectionMsg;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bully election (spec section 11, Exp 4). A node that needs a leader sends {@code Election} to
 * every higher id. Any OK means a higher node is alive and will take over, so it waits for that
 * node's {@code Coordinator}; no OK within the timeout means it is the highest node alive, so it
 * declares itself leader and tells everyone. A node that receives {@code Election} from a lower id
 * answers OK and runs its own election. Worst case, started by the lowest id, is O(n^2) messages.
 */
public class BullyElection extends AbstractElection {

    private static final Logger log = LoggerFactory.getLogger(BullyElection.class);

    /** Counts Coordinator messages from higher nodes; guarded by its own monitor. */
    private final Object coordinatorSignal = new Object();
    private long coordinatorsSeen;

    public BullyElection(ClusterView cluster, long timeoutMs) {
        super(cluster, timeoutMs);
    }

    @Override
    public String name() {
        return "bully";
    }

    @Override
    public void startElection() {
        if (isClosed() || !running.compareAndSet(false, true)) {
            return;
        }
        long seenAtStart = coordinatorsSeen();
        onElectionThread(() -> runElection(seenAtStart));
    }

    private void runElection(long seenAtStart) {
        try {
            electionStarted();
            long seen = seenAtStart;
            while (!isClosed()) {
                if (!anyHigherNodeAnswers()) {
                    becomeLeader();
                    return;
                }
                if (awaitCoordinator(seen)) {
                    return;
                }
                log.info("A higher node answered OK but sent no Coordinator within {} ms;"
                        + " node {} tries again", timeoutMs, cluster.selfId());
                seen = coordinatorsSeen();
            }
        } finally {
            running.set(false);
        }
    }

    /** Sends Election to every higher id in parallel; true if at least one answered OK. */
    private boolean anyHigherNodeAnswers() {
        ElectionMsg message = ElectionMsg.newBuilder().setSenderId(cluster.selfId()).build();
        List<Future<Boolean>> replies = new ArrayList<>();
        for (int peer : cluster.higher()) {
            replies.add(rpc.submit(() -> sendElection(peer, message)));
        }
        boolean answered = false;
        for (Future<Boolean> reply : replies) {
            try {
                answered |= reply.get();
            } catch (ExecutionException e) {
                // A failed call is the same as no answer.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return true;
            }
        }
        return answered;
    }

    private boolean sendElection(int peer, ElectionMsg message) {
        stats.sent(ElectionStats.Message.ELECTION);
        try {
            return cluster.stub(peer, timeoutMs).election(message).getOk();
        } catch (StatusRuntimeException e) {
            log.debug("Node {} did not answer Election: {}", peer, e.getStatus().getCode());
            return false;
        }
    }

    /** Waits up to the timeout for a Coordinator that arrived after {@code seen}. */
    private boolean awaitCoordinator(long seen) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (coordinatorSignal) {
            while (coordinatorsSeen == seen) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    coordinatorSignal.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return true;
                }
            }
            return true;
        }
    }

    private void becomeLeader() {
        setLeader(cluster.selfId());
        CoordinatorMsg announce = CoordinatorMsg.newBuilder()
                .setLeaderId(cluster.selfId())
                .setOriginId(cluster.selfId())
                .build();
        for (int peer : cluster.others()) {
            rpc.execute(() -> {
                stats.sent(ElectionStats.Message.COORDINATOR);
                try {
                    cluster.stub(peer, timeoutMs).coordinator(announce);
                } catch (StatusRuntimeException e) {
                    log.debug("Node {} missed the Coordinator: {}", peer, e.getStatus().getCode());
                }
            });
        }
    }

    @Override
    public Ack onElection(ElectionMsg message) {
        // Answering is the OK message; then this node, being higher, bullies the sender.
        stats.sent(ElectionStats.Message.OK);
        startElection();
        return Ack.newBuilder().setOk(true).setMessage("OK from " + cluster.selfId()).build();
    }

    @Override
    public Ack onRingPass(ElectionMsg message) {
        return Ack.newBuilder().setOk(false).setMessage("this cluster runs bully").build();
    }

    @Override
    public Ack onCoordinator(CoordinatorMsg message) {
        int leader = message.getLeaderId();
        if (leader < cluster.selfId()) {
            // A lower node claims leadership while this one is alive: take it over.
            startElection();
        } else {
            setLeader(leader);
            synchronized (coordinatorSignal) {
                coordinatorsSeen++;
                coordinatorSignal.notifyAll();
            }
        }
        return Ack.newBuilder().setOk(true).build();
    }

    private long coordinatorsSeen() {
        synchronized (coordinatorSignal) {
            return coordinatorsSeen;
        }
    }
}
