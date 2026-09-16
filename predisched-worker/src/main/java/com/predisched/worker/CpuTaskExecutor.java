package com.predisched.worker;

import com.predisched.proto.TaskType;

/** CPU_TASK: sum 1..N. */
public class CpuTaskExecutor implements TaskExecutor {

  @Override
  public TaskType type() {
    return TaskType.CPU_TASK;
  }

  @Override
  public String execute(String input) {
    long n = Long.parseLong(input.trim());
    // Always the real loop: CPU_TASK is meant to burn CPU (benchmarks rely on it).
    long sum = 0;
    for (long i = 1; i <= n; i++) {
      sum += i;
    }
    return Long.toString(sum);
  }
}
