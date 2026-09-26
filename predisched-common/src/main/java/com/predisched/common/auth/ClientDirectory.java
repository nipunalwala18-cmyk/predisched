package com.predisched.common.auth;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.yaml.snakeyaml.Yaml;

/**
 * The clients allowed to call the scheduler and their limits, from {@code configs/clients.yaml}.
 * Keys are stored only as SHA-256 hashes, so the file can be committed without the keys in it.
 *
 * <pre>
 * nodeKeySha256: "..."            # the key nodes present to each other
 * clients:
 *   - id: dev-client-1
 *     keySha256: "..."
 *     ratePerSecond: 10           # token bucket refill
 *     burst: 10                   # token bucket size
 *     maxQueued: 200              # tasks not yet finished at any one time
 * </pre>
 *
 * Immutable after loading.
 */
public final class ClientDirectory {

    /** One client and its limits. */
    public record Client(String id, String keySha256, double ratePerSecond, int burst,
            int maxQueued) {}

    private final Map<String, Client> byKeyHash;
    private final Map<String, Client> byId;
    private final String nodeKeySha256;

    public ClientDirectory(List<Client> clients, String nodeKeySha256) {
        Map<String, Client> keys = new LinkedHashMap<>();
        Map<String, Client> ids = new LinkedHashMap<>();
        for (Client client : clients) {
            keys.put(client.keySha256().toLowerCase(Locale.ROOT), client);
            ids.put(client.id(), client);
        }
        this.byKeyHash = Map.copyOf(keys);
        this.byId = Map.copyOf(ids);
        this.nodeKeySha256 = nodeKeySha256 == null ? "" : nodeKeySha256.toLowerCase(Locale.ROOT);
    }

    @SuppressWarnings("unchecked")
    public static ClientDirectory load(Path file) throws IOException {
        Map<String, Object> root;
        try (InputStream in = Files.newInputStream(file)) {
            root = new Yaml().load(in);
        }
        if (root == null) {
            throw new IllegalArgumentException(file + " is empty");
        }
        List<Client> clients = new java.util.ArrayList<>();
        Object listed = root.get("clients");
        if (listed instanceof List<?> entries) {
            for (Object entry : entries) {
                Map<String, Object> client = (Map<String, Object>) entry;
                clients.add(new Client(
                        String.valueOf(client.get("id")),
                        String.valueOf(client.get("keySha256")),
                        Double.parseDouble(String.valueOf(client.getOrDefault("ratePerSecond", 10))),
                        Integer.parseInt(String.valueOf(client.getOrDefault("burst", 10))),
                        Integer.parseInt(String.valueOf(client.getOrDefault("maxQueued", 1000)))));
            }
        }
        Object node = root.get("nodeKeySha256");
        return new ClientDirectory(clients, node == null ? "" : String.valueOf(node));
    }

    public Optional<Client> byKey(String plainKey) {
        return Optional.ofNullable(byKeyHash.get(sha256(plainKey)));
    }

    public Optional<Client> byId(String id) {
        return Optional.ofNullable(byId.get(id));
    }

    public boolean isNodeKey(String plainKey) {
        return !nodeKeySha256.isEmpty() && nodeKeySha256.equals(sha256(plainKey));
    }

    /** Lower-case hex SHA-256 of a key, the form stored in the clients file. */
    public static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is missing from this JVM", e);
        }
    }
}
