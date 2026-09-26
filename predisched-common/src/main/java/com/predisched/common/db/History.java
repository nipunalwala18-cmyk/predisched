package com.predisched.common.db;

/**
 * This JVM's {@link HistorySink}, like {@code EventLog} and {@code Clocks}: a node with a database
 * installs its {@link HistoryWriter}; everything else reports to a sink that drops it.
 */
public final class History {

    private static volatile HistorySink sink = HistorySink.NONE;

    private History() {}

    public static HistorySink get() {
        return sink;
    }

    public static void install(HistorySink installed) {
        sink = installed == null ? HistorySink.NONE : installed;
    }
}
