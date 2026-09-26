package com.predisched.scheduler;

import java.util.Optional;

/**
 * Whether this scheduler may accept and dispatch tasks (FR9): in a cluster only the elected
 * leader does, and followers point clients at it.
 */
public interface Leadership {

    boolean isLeader();

    /** The leader's election id, when known, for the hint a follower gives a client. */
    Optional<Integer> leader();

    /** A scheduler running on its own leads itself. */
    Leadership ALONE = new Leadership() {
        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public Optional<Integer> leader() {
            return Optional.empty();
        }
    };
}
