package com.predisched.common.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.time.ClockServiceImpl;
import com.predisched.common.time.PhysicalClock;
import com.predisched.proto.ClockServiceGrpc;
import com.predisched.proto.TimeRequest;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Any service behind the interceptor will do; the clock service is the smallest in common. */
class AuthInterceptorTest {

    private static final String SECRET = "a-test-secret-that-is-at-least-32-bytes-long";
    private final ClientDirectory clients = new ClientDirectory(List.of(
            new ClientDirectory.Client("client-1", ClientDirectory.sha256("good-key"), 10, 10, 10)),
            ClientDirectory.sha256("node-key"));
    private final JwtVerifier jwt = new JwtVerifier(SECRET);
    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    void stop() {
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }

    private ClockServiceGrpc.ClockServiceBlockingStub stub(boolean clientsAllowed,
            String authorization) throws Exception {
        String name = "auth-" + UUID.randomUUID();
        servers.add(InProcessServerBuilder.forName(name)
                .addService(new ClockServiceImpl("n", new PhysicalClock(0, 0)))
                .intercept(new AuthInterceptor(clients, jwt, clientsAllowed))
                .build().start());
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        channels.add(channel);
        ClockServiceGrpc.ClockServiceBlockingStub stub = ClockServiceGrpc.newBlockingStub(channel);
        if (authorization == null) {
            return stub;
        }
        Metadata headers = new Metadata();
        headers.put(AuthInterceptor.AUTHORIZATION, authorization);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static Status.Code codeOf(Runnable call) {
        return assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode();
    }

    @Test
    void aValidKeyIsAccepted() throws Exception {
        assertEquals("n", stub(true, "ApiKey good-key").getTime(TimeRequest.getDefaultInstance())
                .getNodeId());
    }

    @Test
    void aWrongKeyIsUnauthenticated() throws Exception {
        var stub = stub(true, "ApiKey wrong");
        assertEquals(Status.Code.UNAUTHENTICATED,
                codeOf(() -> stub.getTime(TimeRequest.getDefaultInstance())));
    }

    @Test
    void aMissingKeyIsUnauthenticated() throws Exception {
        var stub = stub(true, null);
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> stub.getTime(TimeRequest.getDefaultInstance()));
        assertEquals(Status.Code.UNAUTHENTICATED, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains("missing credentials"));
    }

    @Test
    void aValidJwtIsAcceptedAndAnExpiredOneIsNot() throws Exception {
        String fresh = jwt.issue("client-1", Instant.now().plusSeconds(60));
        assertEquals("n", stub(true, "Bearer " + fresh)
                .getTime(TimeRequest.getDefaultInstance()).getNodeId());

        String expired = jwt.issue("client-1", Instant.now().minusSeconds(60));
        var stub = stub(true, "Bearer " + expired);
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> stub.getTime(TimeRequest.getDefaultInstance()));
        assertEquals(Status.Code.UNAUTHENTICATED, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains("expired"));

        String forged = new JwtVerifier("another-secret-that-is-32-bytes-or-more")
                .issue("client-1", Instant.now().plusSeconds(60));
        var forgedStub = stub(true, "Bearer " + forged);
        assertEquals(Status.Code.UNAUTHENTICATED,
                codeOf(() -> forgedStub.getTime(TimeRequest.getDefaultInstance())));
    }

    @Test
    void aNodeOnlyServerTakesTheNodeKeyAndRefusesClients() throws Exception {
        assertEquals("n", stub(false, "ApiKey node-key")
                .getTime(TimeRequest.getDefaultInstance()).getNodeId());
        var client = stub(false, "ApiKey good-key");
        assertEquals(Status.Code.PERMISSION_DENIED,
                codeOf(() -> client.getTime(TimeRequest.getDefaultInstance())));
    }
}
