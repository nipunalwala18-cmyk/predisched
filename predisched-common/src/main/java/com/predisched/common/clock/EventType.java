package com.predisched.common.clock;

/** Event types recorded in the {@link EventLog}. */
public enum EventType {
  SUBMIT,
  ENQUEUE,
  DISPATCH,
  START,
  COMPLETE,
  FAIL,
  CANCEL,
  REGISTER,
  // Used from Prompt 05 (election) and Prompt 07 (failover) onward.
  ELECTION,
  COORDINATOR,
  FAILOVER
}
