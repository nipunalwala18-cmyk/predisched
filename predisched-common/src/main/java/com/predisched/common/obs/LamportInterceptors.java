package com.predisched.common.obs;

import com.predisched.common.time.Clocks;
import com.predisched.common.time.LamportClock;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.MDC;

/**
 * Carries the Lamport timestamp and trace id on every gRPC call, in both directions (FR8, FR38).
 *
 * <p>Doing it here means no service method has to remember to tick the clock: sending a message is
 * a Lamport event, receiving one merges the sender's time, and both sides log with the same trace
 * id. The {@code lamport_time} fields inside the messages are filled separately, for readers of a
 * captured message.
 */
public final class LamportInterceptors {

    public static final Metadata.Key<String> LAMPORT_KEY =
            Metadata.Key.of("x-lamport-time", Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> TRACE_KEY =
            Metadata.Key.of("x-trace-id", Metadata.ASCII_STRING_MARSHALLER);

    private LamportInterceptors() {}

    /** Ticks before sending and attaches the clock value and current trace id. */
    public static ClientInterceptor client(LamportClock clock) {
        return new ClientInterceptor() {
            @Override
            public <Q, S> ClientCall<Q, S> interceptCall(
                    MethodDescriptor<Q, S> method, CallOptions options, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                        next.newCall(method, options)) {
                    @Override
                    public void start(Listener<S> listener, Metadata headers) {
                        headers.put(LAMPORT_KEY, Long.toString(clock.tick()));
                        String trace = TraceContext.current();
                        if (!trace.isEmpty()) {
                            headers.put(TRACE_KEY, trace);
                        }
                        // The reply is a message too: merging its timestamp keeps the caller
                        // causally after the work the callee did.
                        super.start(new ForwardingClientCallListener
                                .SimpleForwardingClientCallListener<>(listener) {
                            @Override
                            public void onHeaders(Metadata responseHeaders) {
                                String replied = responseHeaders.get(LAMPORT_KEY);
                                if (replied != null) {
                                    clock.update(parse(replied));
                                }
                                super.onHeaders(responseHeaders);
                            }
                        }, headers);
                    }
                };
            }
        };
    }

    /** Merges the sender's Lamport time and puts node, lamport and trace into the MDC. */
    public static ServerInterceptor server(String nodeId, LamportClock clock) {
        return new ServerInterceptor() {
            @Override
            public <Q, S> ServerCall.Listener<Q> interceptCall(
                    ServerCall<Q, S> call, Metadata headers, ServerCallHandler<Q, S> next) {
                String received = headers.get(LAMPORT_KEY);
                long now = received == null
                        ? clock.tick()
                        : clock.update(parse(received));
                String trace = headers.get(TRACE_KEY);
                applyMdc(nodeId, now, trace);
                ServerCall<Q, S> stamped = new ForwardingServerCall
                        .SimpleForwardingServerCall<>(call) {
                    @Override
                    public void sendHeaders(Metadata responseHeaders) {
                        responseHeaders.put(LAMPORT_KEY, Long.toString(clock.tick()));
                        super.sendHeaders(responseHeaders);
                    }
                };
                ServerCall.Listener<Q> delegate = next.startCall(stamped, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                        delegate) {
                    @Override
                    public void onMessage(Q message) {
                        applyMdc(nodeId, clock.current(), trace);
                        TraceContext.set(trace == null ? "" : trace);
                        try {
                            super.onMessage(message);
                        } finally {
                            TraceContext.clear();
                        }
                    }

                    @Override
                    public void onHalfClose() {
                        applyMdc(nodeId, clock.current(), trace);
                        TraceContext.set(trace == null ? "" : trace);
                        try {
                            super.onHalfClose();
                        } finally {
                            TraceContext.clear();
                        }
                    }
                };
            }
        };
    }

    /** Sets the MDC keys every log line uses (rule 6). */
    public static void applyMdc(String nodeId, long lamport, String trace) {
        MDC.put("node", nodeId);
        MDC.put("lamport", Long.toString(lamport));
        if (trace == null || trace.isEmpty()) {
            MDC.remove(TraceContext.MDC_KEY);
        } else {
            MDC.put(TraceContext.MDC_KEY, trace);
        }
    }

    /** Sets the MDC for this node from the installed clocks, for threads outside a gRPC call. */
    public static void applyMdc() {
        applyMdc(Clocks.nodeId(), Clocks.lamport().current(), TraceContext.current());
    }

    private static long parse(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
