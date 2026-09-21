package com.predisched.common.time;

import com.predisched.proto.ClockServiceGrpc;

/** One node the Berkeley daemon can poll: its id and a stub for its {@code ClockService}. */
public record ClockPeer(String id, ClockServiceGrpc.ClockServiceBlockingStub stub) {}
