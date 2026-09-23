package com.predisched.client.workload;

/** Arrival processes for the workload generator (spec §12.1, F8). */
public enum ArrivalPattern {
    /** Poisson arrivals at a constant rate. */
    STEADY,
    /** Idle, then a flood: 10% of tasks over the first 60% of the window, the rest after. */
    BURSTY,
    /** Sine-modulated rate around the base rate (Poisson thinning). */
    PERIODIC;

    public static ArrivalPattern parse(String raw) {
        for (ArrivalPattern pattern : values()) {
            if (pattern.name().equalsIgnoreCase(raw)) {
                return pattern;
            }
        }
        throw new IllegalArgumentException(
                "pattern must be one of steady|bursty|periodic (got '" + raw + "')");
    }
}
