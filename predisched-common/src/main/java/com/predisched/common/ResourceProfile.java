package com.predisched.common;

/** Resource profile of a task type (spec section 7.1). Shared with the ML side. */
public enum ResourceProfile {
    CPU_BOUND,
    MEMORY_BOUND,
    IO_BOUND,
    NETWORK_BOUND,
    PARALLEL
}
