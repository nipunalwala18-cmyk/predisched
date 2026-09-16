package com.predisched.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeConfigTest {

  @TempDir Path tmp;

  @Test
  void loadsValidFile() throws Exception {
    Path cfg =
        tmp.resolve("node.yaml");
    Files.writeString(
        cfg,
        """
        nodeId: scheduler-1
        role: SCHEDULER
        host: localhost
        port: 50051
        seed: 42
        peers:
          - id: scheduler-2
            host: localhost
            port: 50052
        settings:
          strategy: round-robin
        """);
    NodeConfig c = NodeConfig.load(cfg);
    assertEquals("scheduler-1", c.nodeId());
    assertEquals(NodeConfig.Role.SCHEDULER, c.role());
    assertEquals(50051, c.port());
    assertEquals(42L, c.seed());
    assertEquals(1, c.peers().size());
    assertEquals("scheduler-2", c.peers().get(0).id());
    assertEquals("round-robin", c.settings().get("strategy"));
  }

  @Test
  void rejectsMissingFile() {
    Path missing = tmp.resolve("does-not-exist.yaml");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.load(missing));
    assertTrue(e.getMessage().contains("not found"));
  }

  @Test
  void rejectsFileWithoutNodeId() throws Exception {
    Path cfg = tmp.resolve("bad.yaml");
    Files.writeString(cfg, "role: WORKER\nport: 50261\n");
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> NodeConfig.load(cfg));
    assertTrue(e.getMessage().contains("nodeId"));
  }
}
