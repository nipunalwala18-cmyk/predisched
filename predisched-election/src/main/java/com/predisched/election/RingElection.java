package com.predisched.election;

import com.predisched.proto.Ack;
import com.predisched.proto.CoordinatorMsg;
import com.predisched.proto.ElectionMsg;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ring election (spec section 11, Exp 4). Nodes form a ring in id order. The initiator sends
 * {@code RingPass} carrying its id to its successor; every node appends its own id and forwards,
 * skipping a successor that does not answer. When the message comes back to the initiator, the
 * highest id in it is the leader, and a {@code Coordinator} message goes once around the ring.
 * One round costs about 2n messages: O(n).
 *
 * <p>Handlers acknowledge at once and forward on the election thread, so a round is a chain of
 * short calls, not n nested ones. If a round is lost (a node died holding it), the initiator
 * starts another after {@code timeoutMs} per node.
 */
public class RingElection extends AbstractElection {

    private static final Logger log = LoggerFactory.getLogger(RingElection.class);

    private final AtomicLong rounds = new AtomicLong();

    public RingElection(ClusterView cluster, long timeoutMs) {
        super(cluster, timeoutMs);
    }

    @Override
    public String name() {
        return "ring";
    }

    @Override
    public void startElection() {
        if (isClosed() || !running.compareAndSet(false, true)) {
            return;
        }
        long round = rounds.incrementAndGet();
        onElectionThread(() -> {
            electionStarted();
            election.schedule(() -> retryIfLost(round),
                    timeoutMs * cluster.size(), TimeUnit.MILLISECONDS);
            ElectionMsg pass = ElectionMsg.newBuilder()
                    .setSenderId(cluster.selfId())
                    .addRingIds(cluster.selfId())
                    .build();
            if (!forwardRingPass(pass)) {
                // No other node answers: this one is the whole ring.
                setLeader(cluster.selfId());
                running.set(false);
            }
        });
    }

    private void retryIfLost(long round) {
        if (running.get() && rounds.get() == round && !isClosed()) {
            log.info("Ring round from node {} never came back; starting another", cluster.selfId());
            running.set(false);
            startElection();
        }
    }

    @Override
    public Ack onRingPass(ElectionMsg message) {
        onElectionThread(() -> handleRingPass(message));
        return Ack.newBuilder().setOk(true).build();
    }

    private void handleRingPass(ElectionMsg message) {
        List<Integer> ids = message.getRingIdsList();
        int self = cluster.selfId();
        if (ids.get(0) == self) {
            int leader = Collections.max(ids);
            log.info("Ring pass came back to node {} through {}: leader is {}", self, ids, leader);
            setLeader(leader);
            running.set(false);
            forwardCoordinator(CoordinatorMsg.newBuilder()
                    .setLeaderId(leader)
                    .setOriginId(self)
                    .build());
            return;
        }
        if (ids.contains(self)) {
            // Already passed through here and its initiator is gone: let it die.
            log.debug("Dropping a stale ring pass {} at node {}", ids, self);
            return;
        }
        ElectionMsg next = message.toBuilder().addRingIds(self).build();
        if (!forwardRingPass(next)) {
            startElection();
        }
    }

    /** Sends to the first successor that answers; false if none does. */
    private boolean forwardRingPass(ElectionMsg message) {
        for (int successor : cluster.successors()) {
            stats.sent(ElectionStats.Message.RING_PASS);
            try {
                if (cluster.stub(successor, timeoutMs).ringPass(message).getOk()) {
                    return true;
                }
            } catch (StatusRuntimeException e) {
                log.info("Ring successor {} of node {} is unreachable ({}); skipping it",
                        successor, cluster.selfId(), e.getStatus().getCode());
            }
        }
        return false;
    }

    @Override
    public Ack onCoordinator(CoordinatorMsg message) {
        onElectionThread(() -> {
            setLeader(message.getLeaderId());
            running.set(false);
            forwardCoordinator(message);
        });
        return Ack.newBuilder().setOk(true).build();
    }

    /** Passes the announcement on until the next live hop would be the node that started it. */
    private void forwardCoordinator(CoordinatorMsg message) {
        for (int successor : cluster.successors()) {
            if (successor == message.getOriginId()) {
                return;
            }
            stats.sent(ElectionStats.Message.COORDINATOR);
            try {
                cluster.stub(successor, timeoutMs).coordinator(message);
                return;
            } catch (StatusRuntimeException e) {
                log.debug("Coordinator hop {} unreachable; skipping", successor);
            }
        }
    }

    @Override
    public Ack onElection(ElectionMsg message) {
        return Ack.newBuilder().setOk(false).setMessage("this cluster runs ring").build();
    }
}
