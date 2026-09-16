package com.predisched.common.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Small cache holding one {@link ManagedChannel} per {@code host:port}.
 *
 * <p>Channels are shut down via {@link #shutdown()} (also registered as a JVM shutdown hook).
 */
public final class Channels implements AutoCloseable {

  private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
  private volatile boolean closed;

  public Channels() {
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown, "channels-shutdown"));
  }

  /** Return the cached channel for {@code host:port}, creating it on first use. */
  public ManagedChannel get(String host, int port) {
    if (closed) {
      throw new IllegalStateException("Channels is closed");
    }
    String key = host + ":" + port;
    return channels.computeIfAbsent(
        key,
        k ->
            ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build());
  }

  /** Shut down every cached channel. */
  public void shutdown() {
    closed = true;
    for (ManagedChannel ch : channels.values()) {
      try {
        ch.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    channels.clear();
  }

  @Override
  public void close() {
    shutdown();
  }

  int size() {
    return channels.size();
  }
}
