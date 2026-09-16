package com.predisched.common.clock;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.MDC;

/**
 * Updates the node's Lamport clock from the {@code lamport-time} metadata on every
 * incoming call. Sets MDC {@code node}/{@code lamport} around handler callbacks so
 * every log line carries them.
 */
public class LamportServerInterceptor implements ServerInterceptor {

  private final String nodeId;
  private final LamportClock clock;
  private final AtomicLong lastReceived = new AtomicLong(-1);

  public LamportServerInterceptor(String nodeId, LamportClock clock) {
    this.nodeId = nodeId;
    this.clock = clock;
  }

  /** Last Lamport value received (-1 if none yet; test hook). */
  public long lastReceived() {
    return lastReceived.get();
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    String value = headers.get(LamportClientInterceptor.LAMPORT_KEY);
    long lamport = value != null ? clock.update(parse(value)) : clock.current();
    if (value != null) {
      lastReceived.set(parse(value));
    }
    final long captured = lamport;
    ServerCall.Listener<ReqT> listener = next.startCall(call, headers);
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
      @Override
      public void onMessage(ReqT message) {
        withMdc(captured, () -> super.onMessage(message));
      }

      @Override
      public void onHalfClose() {
        withMdc(captured, super::onHalfClose);
      }

      @Override
      public void onCancel() {
        withMdc(captured, super::onCancel);
      }

      @Override
      public void onComplete() {
        withMdc(captured, super::onComplete);
      }
    };
  }

  private void withMdc(long tick, Runnable action) {
    // Save/restore instead of clearing: handler threads are pooled and may carry
    // a context from an enclosing task (e.g. in-process calls in tests).
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

  private static long parse(String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
