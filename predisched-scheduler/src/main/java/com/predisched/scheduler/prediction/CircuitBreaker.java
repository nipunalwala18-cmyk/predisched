package com.predisched.scheduler.prediction;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Closed, open, half-open (prompt 17). The breaker opens after {@code failureThreshold}
 * consecutive failures, so calls fail fast (no call at all) while the server is down. After
 * {@code coolDownMs} it lets exactly one trial call through. That call's success closes the
 * breaker; its failure opens it for another cool-down.
 */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final int failureThreshold;
    private final long coolDownNanos;
    private final LongSupplier nanoClock;

    private State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAt;
    private boolean trialInFlight;
    private long opens;

    public CircuitBreaker(int failureThreshold, long coolDownMs) {
        this(failureThreshold, coolDownMs, System::nanoTime);
    }

    public CircuitBreaker(int failureThreshold, long coolDownMs, LongSupplier nanoClock) {
        this.failureThreshold = Math.max(1, failureThreshold);
        this.coolDownNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, coolDownMs));
        this.nanoClock = nanoClock;
    }

    /** Whether a call may go out now. In half-open state, only one trial at a time. */
    public synchronized boolean allowRequest() {
        switch (state) {
            case CLOSED:
                return true;
            case OPEN:
                if (nanoClock.getAsLong() - openedAt >= coolDownNanos) {
                    state = State.HALF_OPEN;
                    trialInFlight = true;
                    return true;
                }
                return false;
            case HALF_OPEN:
            default:
                if (trialInFlight) {
                    return false;
                }
                trialInFlight = true;
                return true;
        }
    }

    public synchronized void onSuccess() {
        state = State.CLOSED;
        consecutiveFailures = 0;
        trialInFlight = false;
    }

    public synchronized void onFailure() {
        trialInFlight = false;
        if (state == State.HALF_OPEN) {
            open();
            return;
        }
        consecutiveFailures++;
        if (state == State.CLOSED && consecutiveFailures >= failureThreshold) {
            open();
        }
    }

    private void open() {
        state = State.OPEN;
        openedAt = nanoClock.getAsLong();
        opens++;
    }

    public synchronized State state() {
        return state;
    }

    /** How many times the breaker has opened. */
    public synchronized long opens() {
        return opens;
    }
}
