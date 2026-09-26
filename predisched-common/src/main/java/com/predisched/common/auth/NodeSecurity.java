package com.predisched.common.auth;

import com.predisched.common.NodeConfig;
import com.predisched.common.net.Transport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Start-up wiring of F9 for a node: installs the {@link Transport} (TLS if configured, and the
 * node key on every outgoing call when auth is on) and builds the server-side
 * {@link AuthInterceptor}. With {@code auth.enabled: false} it installs plain transport and
 * returns null, which is how local development stays key-free.
 */
public final class NodeSecurity {

    /** What a node's server needs: the interceptor, and the clients for their limits. */
    public record Security(AuthInterceptor interceptor, ClientDirectory clients) {}

    private NodeSecurity() {}

    /**
     * @param clientsAllowed true for the scheduler; false for workers, which only nodes call
     */
    public static Security install(NodeConfig config, boolean clientsAllowed) throws IOException {
        NodeConfig.AuthConfig auth = config.getAuth();
        if (!auth.isEnabled()) {
            Transport.install(new Transport(config.getTls(), null));
            return null;
        }
        Transport.install(new Transport(config.getTls(), "ApiKey " + nodeKey(auth)));
        ClientDirectory clients = ClientDirectory.load(Path.of(auth.getClientsFile()));
        String secret = System.getenv(auth.getJwtSecretEnv());
        JwtVerifier jwt = secret == null || secret.isBlank() ? null : new JwtVerifier(secret);
        return new Security(new AuthInterceptor(clients, jwt, clientsAllowed), clients);
    }

    private static String nodeKey(NodeConfig.AuthConfig auth) throws IOException {
        String fromEnv = System.getenv(auth.getNodeKeyEnv());
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim();
        }
        if (!auth.getNodeKeyFile().isEmpty() && Files.exists(Path.of(auth.getNodeKeyFile()))) {
            return Files.readString(Path.of(auth.getNodeKeyFile()), StandardCharsets.UTF_8).trim();
        }
        throw new IllegalStateException("auth.enabled needs a node key: set "
                + auth.getNodeKeyEnv() + " or auth.nodeKeyFile");
    }
}
