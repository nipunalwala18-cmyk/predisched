package com.predisched.common.net;

import com.predisched.common.NodeConfig;
import com.predisched.common.auth.AuthInterceptor;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerBuilder;
import io.grpc.TlsChannelCredentials;
import io.grpc.TlsServerCredentials;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * How this process talks gRPC: plaintext or TLS, and the credential it presents on every call
 * (F9). Installed once at startup, like {@code Clocks}, and read by everything that opens a
 * channel or a server, so a node's internal traffic carries its node key without each caller
 * having to know about auth. The default is plaintext with no credential (local development).
 */
public final class Transport {

    private static volatile Transport current = new Transport(null, null);

    private final NodeConfig.TlsConfig tls;
    private final String authorization;

    /**
     * @param tls null or disabled for plaintext
     * @param authorization the header value to send, e.g. {@code ApiKey <key>}; null for none
     */
    public Transport(NodeConfig.TlsConfig tls, String authorization) {
        this.tls = tls != null && tls.isEnabled() ? tls : null;
        this.authorization = authorization;
    }

    public static void install(Transport transport) {
        current = transport;
    }

    public static Transport get() {
        return current;
    }

    public boolean tls() {
        return tls != null;
    }

    /** A channel to {@code host:port} with this process's TLS setting and credential. */
    public ManagedChannel channel(String host, int port, ClientInterceptor... interceptors) {
        ManagedChannelBuilder<?> builder;
        if (tls != null) {
            try {
                TlsChannelCredentials.Builder credentials = TlsChannelCredentials.newBuilder();
                if (!tls.getTrustCert().isEmpty()) {
                    credentials.trustManager(new File(tls.getTrustCert()));
                }
                builder = Grpc.newChannelBuilderForAddress(host, port, credentials.build());
            } catch (IOException e) {
                throw new UncheckedIOException("cannot read TLS trust cert", e);
            }
        } else {
            builder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
        }
        List<ClientInterceptor> all = new ArrayList<>(List.of(interceptors));
        if (authorization != null) {
            all.add(new CredentialInterceptor(authorization));
        }
        return builder.intercept(all).build();
    }

    /** A server builder on {@code port} with this process's TLS setting. */
    public ServerBuilder<?> server(int port) {
        if (tls == null) {
            return ServerBuilder.forPort(port);
        }
        try {
            return Grpc.newServerBuilderForPort(port, TlsServerCredentials.create(
                    new File(tls.getCertChain()), new File(tls.getPrivateKey())));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read TLS certificate or key", e);
        }
    }

    /** Adds the {@code authorization} header to every outgoing call. */
    private static final class CredentialInterceptor implements ClientInterceptor {
        private final String value;

        CredentialInterceptor(String value) {
            this.value = value;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions options, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<>(
                    next.newCall(method, options)) {
                @Override
                public void start(Listener<RespT> listener, Metadata headers) {
                    headers.put(AuthInterceptor.AUTHORIZATION, value);
                    super.start(listener, headers);
                }
            };
        }
    }
}
