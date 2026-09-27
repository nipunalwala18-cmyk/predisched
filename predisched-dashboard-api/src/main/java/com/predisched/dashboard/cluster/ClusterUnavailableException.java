package com.predisched.dashboard.cluster;

/** No scheduler answered, or none is the primary: the API answers 503. */
public class ClusterUnavailableException extends RuntimeException {

    public ClusterUnavailableException(String message) {
        super(message);
    }
}
