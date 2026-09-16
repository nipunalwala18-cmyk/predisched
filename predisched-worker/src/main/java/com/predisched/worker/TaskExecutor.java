package com.predisched.worker;

import com.predisched.proto.TaskType;

/** Runs one task input and returns its output string. */
public interface TaskExecutor {

  TaskType type();

  String execute(String input) throws Exception;
}
