package com.predisched.common.obs;

import java.util.UUID;
import org.slf4j.MDC;

/**
 * The trace id of the work this thread is doing (F10, FR38). One id follows a task from the client
 * through the scheduler to the worker, and appears on every log line in between.
 */
public final class TraceContext {

    public static final String MDC_KEY = "trace";

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private TraceContext() {}

    public static String newTraceId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

    public static String current() {
        String trace = CURRENT.get();
        return trace == null ? "" : trace;
    }

    /** Sets the trace id for this thread and the MDC, returning what was there before. */
    public static String set(String traceId) {
        String previous = current();
        if (traceId == null || traceId.isEmpty()) {
            clear();
        } else {
            CURRENT.set(traceId);
            MDC.put(MDC_KEY, traceId);
        }
        return previous;
    }

    public static void clear() {
        CURRENT.remove();
        MDC.remove(MDC_KEY);
    }

    /** Runs the action with this trace id in scope, restoring the previous one afterwards. */
    public static void with(String traceId, Runnable action) {
        String previous = set(traceId);
        try {
            action.run();
        } finally {
            if (previous.isEmpty()) {
                clear();
            } else {
                set(previous);
            }
        }
    }
}
