package com.predisched.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.proto.Ack;
import com.predisched.proto.ElectionServiceGrpc;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskType;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The cluster client follows leader hints and survives the primary going away (prompt 10). */
class SchedulerClientFailoverTest {

    /** 0 means no leader yet. */
    private final AtomicInteger leader = new AtomicInteger();
    private final Map<Integer, List<String>> accepted = new TreeMap<>();
    private final Map<Integer, Server> servers = new TreeMap<>();
    private final Map<Integer, ManagedChannel> channels = new TreeMap<>();
    private final String prefix = "fake-scheduler-" + UUID.randomUUID() + "-";

    @AfterEach
    void stop() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        servers.values().forEach(Server::shutdownNow);
    }

    @Test
    void aFollowersRefusalSendsTheSubmitToTheLeaderItNames() throws Exception {
        startSchedulers(3);
        leader.set(2);
        try (SchedulerClient client = new SchedulerClient(channels, fast())) {
            assertEquals(1, client.target(), "starts at the first address");
            TaskResponse reply = client.submitTask("a", TaskType.SLEEP_TASK, "ms=1", 5);
            assertTrue(reply.getAccepted());
            assertEquals(List.of("a"), accepted.get(2));
            assertEquals(2, client.target());
        }
    }

    @Test
    void whenThePrimaryDiesTheClientFindsTheNewOneAndRetries() throws Exception {
        startSchedulers(3);
        leader.set(3);
        try (SchedulerClient client = new SchedulerClient(channels, fast())) {
            assertTrue(client.submitTask("a", TaskType.SLEEP_TASK, "ms=1", 5).getAccepted());
            servers.get(3).shutdownNow();
            leader.set(0);   // an election is running
            new Thread(() -> {
                sleep(300);
                leader.set(2);
            }).start();

            TaskResponse reply = client.submitTask("b", TaskType.SLEEP_TASK, "ms=1", 5);
            assertTrue(reply.getAccepted(), reply.getMessage());
            assertEquals(List.of("a"), accepted.get(3));
            assertEquals(List.of("b"), accepted.get(2));
        }
    }

    private void startSchedulers(int count) throws Exception {
        for (int id = 1; id <= count; id++) {
            int self = id;
            accepted.put(id, new CopyOnWriteArrayList<>());
            servers.put(id, InProcessServerBuilder.forName(prefix + id)
                    .addService(new SchedulerServiceGrpc.SchedulerServiceImplBase() {
                        @Override
                        public void submitTask(TaskRequest request,
                                StreamObserver<TaskResponse> observer) {
                            int current = leader.get();
                            TaskResponse.Builder reply =
                                    TaskResponse.newBuilder().setTaskId(request.getTaskId());
                            if (current == self) {
                                accepted.get(self).add(request.getTaskId());
                                reply.setAccepted(true).setMessage("queued");
                            } else {
                                reply.setAccepted(false).setMessage("not the leader; leader="
                                        + (current == 0 ? "unknown" : String.valueOf(current)));
                            }
                            observer.onNext(reply.build());
                            observer.onCompleted();
                        }
                    })
                    .addService(new ElectionServiceGrpc.ElectionServiceImplBase() {
                        @Override
                        public void ping(Ack request, StreamObserver<Ack> observer) {
                            int current = leader.get();
                            observer.onNext(Ack.newBuilder().setOk(true).setMessage("node=" + self
                                    + " leader=" + (current == 0 ? "none" : current)).build());
                            observer.onCompleted();
                        }
                    })
                    .build().start());
            channels.put(id, InProcessChannelBuilder.forName(prefix + id).build());
        }
    }

    private static SchedulerClient.Failover fast() {
        return new SchedulerClient.Failover(30, 20, 200, 1_000);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
