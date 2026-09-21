package com.predisched.scheduler;

import com.predisched.proto.WorkerServiceGrpc;

/**
 * Where the dispatcher gets a stub for a worker. {@link WorkerClients} is the real implementation;
 * tests supply in-process stubs through the same seam.
 */
public interface WorkerStubs {

    WorkerServiceGrpc.WorkerServiceBlockingStub stubFor(WorkerInfo worker);
}
