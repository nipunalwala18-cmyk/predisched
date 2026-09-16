package com.predisched.common.clock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Berkeley rounds with fake clocks on one shared base. The daemon's own clock gets a
 * simulated offset placing it on the same base, so the average is deterministic.
 */
class BerkeleyTest {

  static final long BASE = 1_000_000;

  static class FakePeer implements BerkeleyTimeDaemon.PeerClock {
    final String id;
    long offset;

    FakePeer(String id, long offset) {
      this.id = id;
      this.offset = offset;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public long read() {
      return BASE + offset;
    }

    @Override
    public void adjust(long offsetMs) {
      offset += offsetMs;
    }
  }

  private static BerkeleyTimeDaemon daemonWith(List<BerkeleyTimeDaemon.PeerClock> remotes) {
    // Instant slew so assertions read final values; simulated offset puts the daemon on BASE.
    NodeClock daemonClock =
        new NodeClock(BASE - System.currentTimeMillis(), 0, Double.MAX_VALUE);
    return new BerkeleyTimeDaemon(daemonClock, "daemon", remotes, 1000);
  }

  @Test
  void roundConvergesAndExcludesOutlier() {
    FakePeer a = new FakePeer("a", -300);
    FakePeer b = new FakePeer("b", 120);
    FakePeer c = new FakePeer("c", 500);
    FakePeer outlier = new FakePeer("outlier", 5000);
    BerkeleyTimeDaemon daemon = daemonWith(List.of(a, b, c, outlier));

    var round = daemon.synchronizeOnce();

    // +5000 is more than outlierMs (1000) from the median: excluded from the average...
    assertEquals(List.of("outlier"), round.excluded());
    // ...but still corrected toward it.
    assertTrue(Math.abs(outlier.offset - (round.averageMs() - BASE)) <= 5);
    // Every kept node lands within 5 ms of the mean.
    for (FakePeer peer : List.of(a, b, c)) {
      assertTrue(
          Math.abs(peer.offset - (round.averageMs() - BASE)) <= 5,
          peer.id + " off by " + (peer.offset - (round.averageMs() - BASE)));
    }
    // The daemon corrected itself too: 5 records (self + 4 remotes).
    assertEquals(5, round.records().size());
    assertEquals(5, daemon.lastRound().size());
    assertTrue(round.records().stream().allMatch(r -> r.algorithm().equals("berkeley")));
  }

  @Test
  void unreachablePeerDoesNotFailRound() {
    BerkeleyTimeDaemon.PeerClock dead =
        new BerkeleyTimeDaemon.PeerClock() {
          @Override
          public String id() {
            return "dead";
          }

          @Override
          public long read() throws Exception {
            throw new java.io.IOException("unreachable");
          }

          @Override
          public void adjust(long offsetMs) {}
        };
    FakePeer alive = new FakePeer("alive", -300);
    BerkeleyTimeDaemon daemon = daemonWith(List.of(alive, dead));

    var round = daemon.synchronizeOnce();

    assertEquals(2, round.records().size()); // self + alive
    assertTrue(round.records().stream().anyMatch(r -> r.nodeId().equals("alive")));
  }
}
