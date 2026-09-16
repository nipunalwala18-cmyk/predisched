package com.predisched.common.clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lamport logical clock: {@code tick()} for a local event or send,
 * {@code update(received)} = {@code max(local, received) + 1} on receive.
 *
 * <p>Thread-safe with an {@link AtomicLong} CAS loop; concurrent ticks never
 * produce duplicates.
 */
public class LamportClock {

  private final AtomicLong time = new AtomicLong(0);

  /** A local event or a send. Returns the new time. */
  public long tick() {
    return time.incrementAndGet();
  }

  /**
   * A receive: adopt the incoming time if newer, then tick. Returns the new time.
   */
  public long update(long received) {
    long prev, next;
    do {
      prev = time.get();
      next = Math.max(prev, received) + 1;
    } while (!time.compareAndSet(prev, next));
    return next;
  }

  public long current() {
    return time.get();
  }
}
