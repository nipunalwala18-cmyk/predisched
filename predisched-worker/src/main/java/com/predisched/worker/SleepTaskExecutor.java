package com.predisched.worker;

import com.predisched.proto.TaskType;

/** SLEEP_TASK: sleep for the given milliseconds. */
public class SleepTaskExecutor implements TaskExecutor {

  @Override
  public TaskType type() {
    return TaskType.SLEEP_TASK;
  }

  @Override
  public String execute(String input) throws InterruptedException {
    long ms = Long.parseLong(input.trim());
    Thread.sleep(ms);
    return "slept " + ms + " ms";
  }
}
