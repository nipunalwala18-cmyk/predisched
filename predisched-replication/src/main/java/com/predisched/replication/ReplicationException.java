package com.predisched.replication;

/** A write or read could not reach the quorum its consistency mode requires. */
public class ReplicationException extends RuntimeException {

    public ReplicationException(String message) {
        super(message);
    }
}
