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
    long sum = n * (n + 1) / 2;
    // Do the actual loop for small N so CPU time is real; arithmetic shortcut for huge N.
    if (n <= 1_000_000) {
      long acc = 0;
      for (long i = 1; i <= n; i++) {
        acc += i;
      }
      sum = acc;
    }
    return Long.toString(sum);
  }
}
