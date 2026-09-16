package com.predisched.common.clock;

import java.util.ArrayList;
import java.util.List;

/**
 * Berkeley time daemon (runs on the coordinating scheduler).
 *
 * <p>Every round it polls all nodes with {@code GetTime}, compensates each reading
 * with RTT/2 (done inside {@link PeerClock#read()}), discards readings more than
 * {@code outlierMs} from the median, averages the rest including itself, and sends
 * each node its own correction through {@code Adjust}. Corrections go through
 * {@link NodeClock#adjust}, so they are slewed gradually and never step time
 * backwards. Outliers are excluded from the average but still corrected.
 */
public class BerkeleyTimeDaemon {

  /** A node the daemon can read and correct; gRPC or fake (tests). */
  public interface PeerClock {
    String id();

    /** RTT-compensated estimate of the node's current time. */
    long read() throws Exception;

    void adjust(long offsetMs) throws Exception;
  }

  public record RoundResult(long averageMs, List<ClockSyncRecord> records, List<String> excluded) {}

  private final NodeClock local;
  private final String localId;
  private final List<PeerClock> remotes;
  private final long outlierMs;
  private volatile List<ClockSyncRecord> lastRound = List.of();

  public BerkeleyTimeDaemon(NodeClock local, String localId, List<PeerClock> remotes, long outlierMs) {
    this.local = local;
    this.localId = localId;
    this.remotes = List.copyOf(remotes);
    this.outlierMs = outlierMs;
  }

  /** Last round's records (served to `admin clocks`). */
  public List<ClockSyncRecord> lastRound() {
    return lastRound;
  }

  public RoundResult synchronizeOnce() {
    long now = local.now();
    List<PeerClock> nodes = new ArrayList<>();
    List<Long> readings = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    nodes.add(new SelfPeer());
    for (PeerClock peer : remotes) {
      nodes.add(peer);
    }
    for (PeerClock node : nodes) {
      try {
        readings.add(node.read());
        ids.add(node.id());
      } catch (Exception e) {
        // Unreachable node: excluded from this round, corrected next time.
      }
    }
    List<Long> sorted = new ArrayList<>(readings);
    sorted.sort(Long::compareTo);
    long median = sorted.get(sorted.size() / 2);
    List<Long> kept = new ArrayList<>();
    List<String> excluded = new ArrayList<>();
    for (int i = 0; i < readings.size(); i++) {
      if (Math.abs(readings.get(i) - median) <= outlierMs) {
        kept.add(readings.get(i));
      } else {
        excluded.add(ids.get(i));
      }
    }
    long average = kept.stream().mapToLong(Long::longValue).sum() / kept.size();
    List<ClockSyncRecord> records = new ArrayList<>();
    for (int i = 0; i < readings.size(); i++) {
      long offset = average - readings.get(i);
      try {
        nodes.get(i).adjust(offset);
      } catch (Exception e) {
        // Best effort; the next round retries.
      }
      records.add(
          new ClockSyncRecord(ids.get(i), now, readings.get(i) - now, offset, "berkeley"));
    }
    lastRound = List.copyOf(records);
    return new RoundResult(average, records, excluded);
  }

  private class SelfPeer implements PeerClock {
    @Override
    public String id() {
      return localId;
    }

    @Override
    public long read() {
      return local.now();
    }

    @Override
    public void adjust(long offsetMs) {
      local.adjust(offsetMs);
    }
  }
}
