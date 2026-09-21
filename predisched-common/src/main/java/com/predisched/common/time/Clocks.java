package com.predisched.common.time;

/**
 * The clocks of the node running in this JVM. Set once at startup by the node's main, so shared
 * classes such as {@code TaskRecord} can timestamp without every caller threading a clock through.
 */
public final class Clocks {

    private static volatile PhysicalClock physical = PhysicalClock.system();
    private static volatile LamportClock lamport = new LamportClock();
    private static volatile String nodeId = "unset";

    private Clocks() {}

    public static void install(String node, PhysicalClock physicalClock, LamportClock lamportClock) {
        nodeId = node;
        physical = physicalClock;
        lamport = lamportClock;
    }

    public static PhysicalClock physical() {
        return physical;
    }

    public static LamportClock lamport() {
        return lamport;
    }

    public static String nodeId() {
        return nodeId;
    }

    /** This node's current physical time, offset and drift included. */
    public static long now() {
        return physical.now();
    }
}
