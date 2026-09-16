package com.predisched.worker;

import com.predisched.proto.ExecuteRequest;
import com.predisched.proto.ExecuteResult;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.EnumMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes tasks synchronously on the calling gRPC thread (Prompt 02 makes it async
 * with a thread pool). A thrown exception becomes {@code success=false}, never a gRPC error.
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

  private static final Logger log = LoggerFactory.getLogger(WorkerServiceImpl.class);

  private final Map<TaskType, TaskExecutor> executors = new EnumMap<>(TaskType.class);
  private final String workerId;

  public WorkerServiceImpl(String workerId) {
    this.workerId = workerId;
    register(new CpuTaskExecutor());
    register(new MatrixTaskExecutor());
    register(new SleepTaskExecutor());
  }

  void register(TaskExecutor executor) {
    executors.put(executor.type(), executor);
  }

  @Override
  public void executeTask(ExecuteRequest req, StreamObserver<ExecuteResult> obs) {
    String taskId = req.getTask().getTaskId();
    TaskExecutor executor = executors.get(req.getTask().getType());
    long start = System.nanoTime();
    if (executor == null) {
      obs.onNext(
          ExecuteResult.newBuilder()
              .setTaskId(taskId)
              .setSuccess(false)
              .setOutput("unsupported task type: " + req.getTask().getType())
              .build());
      obs.onCompleted();
      return;
    }
    try {
      String output = executor.execute(req.getTask().getInput());
      long execMs = (System.nanoTime() - start) / 1_000_000;
      log.info("task {} done in {} ms", taskId, execMs);
      obs.onNext(
          ExecuteResult.newBuilder()
              .setTaskId(taskId)
              .setSuccess(true)
              .setOutput(output)
              .setExecTimeMs(execMs)
              .build());
    } catch (Exception e) {
      long execMs = (System.nanoTime() - start) / 1_000_000;
      log.warn("task {} failed: {}", taskId, e.toString());
      obs.onNext(
          ExecuteResult.newBuilder()
              .setTaskId(taskId)
              .setSuccess(false)
              .setOutput(e.toString())
              .setExecTimeMs(execMs)
              .build());
    }
    obs.onCompleted();
  }
}
