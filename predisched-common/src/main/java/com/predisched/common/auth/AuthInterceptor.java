package com.predisched.common.auth;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;

/**
 * Authenticates every call on a server (F9, FR30). The {@code authorization} header must be
 * {@code ApiKey <key>} or {@code Bearer <jwt>}; the caller becomes a {@link Principal} in the gRPC
 * {@link Context} for the rest of the call. Missing or bad credentials end the call with
 * {@code UNAUTHENTICATED}. A server that only serves other nodes (a worker) refuses clients with
 * {@code PERMISSION_DENIED}.
 */
public final class AuthInterceptor implements ServerInterceptor {

    public static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /** Who is calling: a client with limits, or another node of the cluster. */
    public record Principal(String clientId, boolean node) {
        public static final Principal NODE = new Principal("node", true);
    }

    public static final Context.Key<Principal> PRINCIPAL = Context.key("predisched-principal");

    private final ClientDirectory clients;
    private final JwtVerifier jwt;
    private final boolean clientsAllowed;

    /**
     * @param jwt null when no JWT secret is configured: bearer tokens are then refused
     * @param clientsAllowed false on servers only nodes may call
     */
    public AuthInterceptor(ClientDirectory clients, JwtVerifier jwt, boolean clientsAllowed) {
        this.clients = clients;
        this.jwt = jwt;
        this.clientsAllowed = clientsAllowed;
    }

    /** The authenticated caller of the current call, if auth is on. */
    public static Optional<Principal> current() {
        return Optional.ofNullable(PRINCIPAL.get());
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String header = headers.get(AUTHORIZATION);
        Principal principal;
        try {
            principal = authenticate(header);
        } catch (AuthException e) {
            call.close(e.status, new Metadata());
            return new ServerCall.Listener<>() {};
        }
        if (!principal.node() && !clientsAllowed) {
            call.close(Status.PERMISSION_DENIED.withDescription(
                    "only cluster nodes may call this server"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        Context context = Context.current().withValue(PRINCIPAL, principal);
        return Contexts.interceptCall(context, call, headers, next);
    }

    Principal authenticate(String header) throws AuthException {
        if (header == null || header.isBlank()) {
            throw new AuthException(Status.UNAUTHENTICATED.withDescription(
                    "missing credentials: send 'authorization: ApiKey <key>' or 'Bearer <jwt>'"));
        }
        if (header.startsWith("ApiKey ")) {
            String key = header.substring("ApiKey ".length()).trim();
            if (clients.isNodeKey(key)) {
                return Principal.NODE;
            }
            return clients.byKey(key)
                    .map(client -> new Principal(client.id(), false))
                    .orElseThrow(() -> new AuthException(
                            Status.UNAUTHENTICATED.withDescription("unknown API key")));
        }
        if (header.startsWith("Bearer ")) {
            if (jwt == null) {
                throw new AuthException(Status.UNAUTHENTICATED.withDescription(
                        "bearer tokens are not enabled on this server"));
            }
            try {
                String clientId = jwt.verify(header.substring("Bearer ".length()).trim());
                if (clients.byId(clientId).isEmpty()) {
                    throw new AuthException(Status.UNAUTHENTICATED.withDescription(
                            "token for unknown client " + clientId));
                }
                return new Principal(clientId, false);
            } catch (JwtVerifier.InvalidTokenException e) {
                throw new AuthException(Status.UNAUTHENTICATED.withDescription(e.getMessage()));
            }
        }
        throw new AuthException(Status.UNAUTHENTICATED.withDescription(
                "unsupported authorization scheme; use ApiKey or Bearer"));
    }

    static final class AuthException extends Exception {
        final Status status;

        AuthException(Status status) {
            super(status.getDescription());
            this.status = status;
        }
    }
}
