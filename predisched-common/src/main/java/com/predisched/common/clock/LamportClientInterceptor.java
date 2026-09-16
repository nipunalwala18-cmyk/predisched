package com.predisched.common.clock;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.MethodDescriptor;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.MDC;

/**
 * Ticks the node's Lamport clock on every outgoing call and attaches the value as
 * {@code lamport-time} metadata. Sets MDC {@code node}/{@code lamport} around
 * callbacks so every log line carries them.
 */
public class LamportClientInterceptor implements ClientInterceptor {

  static final Metadata.Key<String> LAMPORT_KEY =
      Metadata.Key.of("lamport-time", Metadata.ASCII_STRING_MARSHALLER);

  private final String nodeId;
  private final LamportClock clock;
  private final AtomicLong lastSent = new AtomicLong(-1);

  public LamportClientInterceptor(String nodeId, LamportClock clock) {
    this.nodeId = nodeId;
    this.clock = clock;
  }

  /** Last Lamport value sent (-1 if none yet; test hook). */
  public long lastSent() {
    return lastSent.get();
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    long tick = clock.tick();
    lastSent.set(tick);
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        headers.put(LAMPORT_KEY, Long.toString(tick));
        super.start(
            new Listener<RespT>() {
              @Override
              public void onHeaders(Metadata headers) {
                withMdc(tick, () -> responseListener.onHeaders(headers));
              }

              @Override
              public void onMessage(RespT message) {
                withMdc(tick, () -> responseListener.onMessage(message));
              }

              @Override
              public void onClose(io.grpc.Status status, Metadata trailers) {
                withMdc(tick, () -> responseListener.onClose(status, trailers));
              }
            },
            headers);
      }
    };
  }

  private void withMdc(long tick, Runnable action) {
    // Save/restore instead of clearing: this may run on the caller's own thread
    // (blocking stubs), whose context must survive nested calls.
    java.util.Map<String, String> previous = MDC.getCopyOfContextMap();
    MDC.put("node", nodeId);
    MDC.put("lamport", Long.toString(tick));
    try {
      action.run();
    } finally {
      if (previous == null) {
        MDC.clear();
      } else {
        MDC.setContextMap(previous);
      }
    }
  }
}
