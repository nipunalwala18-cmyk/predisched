package com.predisched.replication;

import com.predisched.proto.ReadRequest;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Quorum replication: N replicas, a write needs W acknowledgements and a read asks R replicas, and
 * with W + R > N every read quorum overlaps every write quorum, so a read always sees the latest
 * successful write. N = 3, W = 2, R = 2 by default.
 *
 * <p>A write goes to the peers first and is applied here only once W - 1 of them have
 * acknowledged, this node being the W-th. If the quorum is not reached within the timeout the
 * write fails with {@link ReplicationException} and this node never applied it, so a failed write
 * leaves nothing readable behind. (A peer that acked before the failure keeps its copy; with
 * N = 3 and W = 2 a failure means no peer acked, so that cannot happen here.)
 *
 * <p>A read asks this replica and the first R - 1 peers to answer, returns the newest copy, and
 * pushes it to any of those replicas that held an older one (read repair).
 */
public class StrongConsistency implements ConsistencyMode {

    private static final Logger log = LoggerFactory.getLogger(StrongConsistency.class);

    private final LocalReplica local;
    private final ReplicaSet replicas;
    private final int writeQuorum;
    private final int readQuorum;
    private final long timeoutMs;
    private final ExecutorService rpc;

    public StrongConsistency(
            LocalReplica local, ReplicaSet replicas, int writeQuorum, int readQuorum,
            long timeoutMs) {
        int n = replicas.size();
        if (writeQuorum < 1 || writeQuorum > n || readQuorum < 1 || readQuorum > n) {
            throw new IllegalArgumentException(
                    "quorums must be within 1.." + n + ", got W=" + writeQuorum + " R=" + readQuorum);
        }
        if (writeQuorum + readQuorum <= n) {
            log.warn("W + R = {} is not above N = {}: reads may be stale", writeQuorum + readQuorum, n);
        }
        this.local = local;
        this.replicas = replicas;
        this.writeQuorum = writeQuorum;
        this.readQuorum = readQuorum;
        this.timeoutMs = timeoutMs;
        this.rpc = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "replication-" + local.nodeName());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public String name() {
        return "strong";
    }

    @Override
    public void write(VersionedRecord record) {
        int peerAcksNeeded = writeQuorum - 1;
        List<Integer> peers = replicas.peers();
        CompletionService<Boolean> acks = new ExecutorCompletionService<>(rpc);
        for (int peer : peers) {
            acks.submit(() -> push(peer, record));
        }
        int acked = 0;
        int answered = 0;
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (acked < peerAcksNeeded && answered < peers.size()) {
            Future<Boolean> reply = poll(acks, deadline);
            if (reply == null) {
                break;
            }
            answered++;
            if (resultOf(reply)) {
                acked++;
            }
        }
        if (acked < peerAcksNeeded) {
            throw new ReplicationException(String.format(
                    "write of %s v%d got %d of the %d peer acks W=%d needs within %d ms;"
                            + " not applied",
                    record.taskId(), record.version(), acked, peerAcksNeeded, writeQuorum,
                    timeoutMs));
        }
        local.apply(record);
    }

    @Override
    public VersionedRecord read(String taskId) {
        List<VersionedRecord> copies = new ArrayList<>();
        List<Integer> answeredBy = new ArrayList<>();
        VersionedRecord mine = local.get(taskId);
        int peerReadsNeeded = readQuorum - 1;
        if (peerReadsNeeded > 0) {
            CompletionService<PeerCopy> replies = new ExecutorCompletionService<>(rpc);
            for (int peer : replicas.peers()) {
                replies.submit(() -> new PeerCopy(peer, VersionedRecord.from(replicas.stub(peer)
                        .read(ReadRequest.newBuilder()
                                .setTaskId(taskId)
                                .setLocalOnly(true)
                                .build()))));
            }
            long deadline = System.currentTimeMillis() + timeoutMs;
            int outstanding = replicas.peers().size();
            while (answeredBy.size() < peerReadsNeeded && outstanding > 0) {
                Future<PeerCopy> reply = poll(replies, deadline);
                if (reply == null) {
                    break;
                }
                outstanding--;
                try {
                    PeerCopy copy = reply.get();
                    answeredBy.add(copy.peer());
                    copies.add(copy.record());
                } catch (ExecutionException e) {
                    // That peer is down or cut off; another may still answer.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (answeredBy.size() < peerReadsNeeded) {
                throw new ReplicationException(String.format(
                        "read of %s reached %d of R=%d replicas within %d ms",
                        taskId, answeredBy.size() + 1, readQuorum, timeoutMs));
            }
        }
        VersionedRecord newest = mine;
        for (VersionedRecord copy : copies) {
            if (copy != null && copy.isNewerThan(newest)) {
                newest = copy;
            }
        }
        if (newest != null) {
            repair(newest, mine, answeredBy, copies);
        }
        return newest;
    }

    /** Pushes the newest copy to every replica in this read that returned an older one. */
    private void repair(VersionedRecord newest, VersionedRecord mine, List<Integer> peers,
            List<VersionedRecord> copies) {
        if (newest.isNewerThan(mine)) {
            local.apply(newest);
        }
        for (int i = 0; i < peers.size(); i++) {
            if (newest.isNewerThan(copies.get(i))) {
                int stale = peers.get(i);
                log.info("Read repair: replica {} held an older copy of {}; sending v{}",
                        stale, newest.taskId(), newest.version());
                rpc.execute(() -> push(stale, newest));
            }
        }
    }

    private boolean push(int peer, VersionedRecord record) {
        try {
            long delay = replicas.delay(peer);
            if (delay > 0) {
                Thread.sleep(delay);
            }
            return replicas.stub(peer).replicate(record.toRequest(0)).getOk();
        } catch (StatusRuntimeException e) {
            log.debug("Replica {} did not take {}: {}", peer, record.taskId(),
                    e.getStatus().getCode());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static <T> Future<T> poll(CompletionService<T> service, long deadline) {
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            return null;
        }
        try {
            return service.poll(remaining, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static boolean resultOf(Future<Boolean> reply) {
        try {
            return reply.get();
        } catch (ExecutionException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private record PeerCopy(int peer, VersionedRecord record) {}

    @Override
    public void close() {
        rpc.shutdownNow();
    }
}
