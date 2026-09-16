package com.predisched.scheduler.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.model.TaskRecord;
import com.predisched.common.model.WorkerInfo;
import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class StrategyTestSupport {

  static WorkerInfo worker(String id) {
    return worker(id, 0, 0, 0, 0, 4);
  }

  static WorkerInfo worker(String id, int queue, int active, double cpu, double mem, int pool) {
    WorkerInfo info = new WorkerInfo(id, "localhost", 5000, 4, 4096, pool);
    info.queueLen(queue);
    info.activeThreads(active);
    info.cpuPct(cpu);
    info.memPct(mem);
    return info;
  }

  static TaskRecord task() {
    return new TaskRecord("t", TaskType.CPU_TASK, "100", 5, 0);
  }

  static List<String> pickIds(SchedulingStrategy strategy, List<WorkerInfo> alive, int n) {
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      ids.add(strategy.select(task(), alive).orElseThrow().workerId());
    }
    return ids;
  }
}
