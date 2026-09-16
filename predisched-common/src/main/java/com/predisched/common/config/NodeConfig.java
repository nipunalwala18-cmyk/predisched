package com.predisched.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.yaml.snakeyaml.Yaml;

/**
 * Node configuration loaded from YAML.
 *
 * <p>Resolution order: first CLI argument, then {@code PREDISCHED_CONFIG} environment
 * variable. Fails fast with a clear message on a missing file or a missing {@code nodeId}.
 */
public final class NodeConfig {

  /** Node role. */
  public enum Role {
    SCHEDULER,
    WORKER,
    CLIENT
  }

  /** A peer node (id/host/port). */
  public record Peer(String id, String host, int port) {}

  private final String nodeId;
  private final Role role;
  private final String host;
  private final int port;
  private final List<Peer> peers;
  private final long seed;
  private final Map<String, Object> settings;

  private NodeConfig(
      String nodeId,
      Role role,
      String host,
      int port,
      List<Peer> peers,
      long seed,
      Map<String, Object> settings) {
    this.nodeId = nodeId;
    this.role = role;
    this.host = host;
    this.port = port;
    this.peers = Collections.unmodifiableList(new ArrayList<>(peers));
    this.seed = seed;
    this.settings = Collections.unmodifiableMap(new LinkedHashMap<>(settings));
  }

  public String nodeId() {
    return nodeId;
  }

  public Role role() {
    return role;
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public List<Peer> peers() {
    return peers;
  }

  public long seed() {
    return seed;
  }

  public Map<String, Object> settings() {
    return settings;
  }

  /**
   * Load config using the standard resolution: {@code args[0]} if present, else
   * {@code PREDISCHED_CONFIG}.
   */
  public static NodeConfig load(String[] args) {
    String path = null;
    if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) {
      path = args[0].trim();
    } else {
      path = System.getenv("PREDISCHED_CONFIG");
    }
    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException(
          "No config file given: pass a YAML path as the first CLI argument "
              + "or set PREDISCHED_CONFIG");
    }
    return load(Path.of(path));
  }

  /** Load config from an explicit path. */
  public static NodeConfig load(Path path) {
    Objects.requireNonNull(path, "path");
    if (!Files.isRegularFile(path)) {
      throw new IllegalArgumentException("Config file not found: " + path.toAbsolutePath());
    }
    Map<String, Object> raw;
    try (InputStream in = Files.newInputStream(path)) {
      Yaml yaml = new Yaml();
      Object loaded = yaml.load(in);
      if (!(loaded instanceof Map)) {
        throw new IllegalArgumentException(
            "Invalid config file (expected a YAML mapping): " + path.toAbsolutePath());
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) loaded;
      raw = map;
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Cannot read config file: " + path.toAbsolutePath() + ": " + e.getMessage(), e);
    }
    return fromMap(raw, path.toString());
  }

  @SuppressWarnings("unchecked")
  static NodeConfig fromMap(Map<String, Object> raw, String source) {
    Object nodeIdObj = raw.get("nodeId");
    if (nodeIdObj == null || nodeIdObj.toString().isBlank()) {
      throw new IllegalArgumentException(
          "Invalid config (" + source + "): missing required field 'nodeId'");
    }
    String nodeId = nodeIdObj.toString().trim();

    Role role = Role.SCHEDULER;
    Object roleObj = raw.get("role");
    if (roleObj != null && !roleObj.toString().isBlank()) {
      try {
        role = Role.valueOf(roleObj.toString().trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid config (" + source + "): unknown role '" + roleObj + "' "
                + "(expected SCHEDULER, WORKER or CLIENT)");
      }
    }

    String host = raw.getOrDefault("host", "localhost").toString();
    int port = toInt(raw.getOrDefault("port", 0), "port", source);
    long seed = toLong(raw.getOrDefault("seed", 0L), "seed", source);

    List<Peer> peers = new ArrayList<>();
    Object peersObj = raw.get("peers");
    if (peersObj instanceof List<?> list) {
      for (Object item : list) {
        if (!(item instanceof Map)) {
          throw new IllegalArgumentException(
              "Invalid config (" + source + "): each 'peers' entry must be a mapping");
        }
        Map<String, Object> pm = (Map<String, Object>) item;
        Object idObj = pm.get("id");
        if (idObj == null || idObj.toString().isBlank()) {
          throw new IllegalArgumentException(
              "Invalid config (" + source + "): each peer needs an 'id'");
        }
        String pid = idObj.toString().trim();
        String phost = pm.getOrDefault("host", "localhost").toString();
        int pport = toInt(pm.getOrDefault("port", 0), "peers[].port", source);
        peers.add(new Peer(pid, phost, pport));
      }
    } else if (peersObj != null) {
      throw new IllegalArgumentException(
          "Invalid config (" + source + "): 'peers' must be a list");
    }

    Map<String, Object> settings = new LinkedHashMap<>();
    Object settingsObj = raw.get("settings");
    if (settingsObj instanceof Map) {
      settings.putAll((Map<String, Object>) settingsObj);
    } else if (settingsObj != null) {
      throw new IllegalArgumentException(
          "Invalid config (" + source + "): 'settings' must be a mapping");
    }

    return new NodeConfig(nodeId, role, host, port, peers, seed, settings);
  }

  private static int toInt(Object value, String field, String source) {
    if (value instanceof Number n) {
      return n.intValue();
    }
    try {
      return Integer.parseInt(value.toString().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid config (" + source + "): field '" + field + "' must be an integer");
    }
  }

  private static long toLong(Object value, String field, String source) {
    if (value instanceof Number n) {
      return n.longValue();
    }
    try {
      return Long.parseLong(value.toString().trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid config (" + source + "): field '" + field + "' must be an integer");
    }
  }
}
