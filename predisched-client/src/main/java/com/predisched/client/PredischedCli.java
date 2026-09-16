package com.predisched.client;

import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/** {@code predisched} CLI: submit, status, cancel, watch, admin, events. */
@Command(
    name = "predisched",
    mixinStandardHelpOptions = true,
    description = "PrediSched client",
    subcommands = {
      PredischedCli.Submit.class,
      PredischedCli.Status.class,
      PredischedCli.Cancel.class,
      PredischedCli.Watch.class,
      PredischedCli.Admin.class,
      PredischedCli.Events.class
    })
public class PredischedCli implements Callable<Integer> {

  @Override
  public Integer call() {
    CommandLine.usage(this, System.out);
    return 0;
  }

  static SchedulerClient client(String scheduler) {
    String[] parts = scheduler.split(":");
    return new SchedulerClient(parts[0], Integer.parseInt(parts[1]));
  }

  @Command(name = "submit", description = "Submit a task")
  static class Submit implements Callable<Integer> {
    @Option(names = "--type", required = true, description = "CPU_TASK, MATRIX_TASK, SLEEP_TASK")
    String type;

    @Option(names = "--input", required = true)
    String input;

    @Option(names = "--priority", defaultValue = "5")
    int priority;

    @Option(names = "--scheduler", defaultValue = "localhost:50051")
    String scheduler;

    @Option(names = "--id", description = "Task id (generated if absent)")
    String id;

    @Override
    public Integer call() {
      String taskId = id != null ? id : SchedulerClient.newId();
      try (SchedulerClient c = client(scheduler)) {
        var res =
            c.submit(taskId, TaskType.valueOf(type), input, priority);
        System.out.println(res.getTaskId() + " accepted=" + res.getAccepted() + " " + res.getMessage());
        return res.getAccepted() ? 0 : 1;
      }
    }
  }

  @Command(name = "status", description = "Query task status")
  static class Status implements Callable<Integer> {
    @Parameters(index = "0") String id;

    @Option(names = "--scheduler", defaultValue = "localhost:50051")
    String scheduler;

    @Override
    public Integer call() {
      try (SchedulerClient c = client(scheduler)) {
        TaskStatusResponse res = c.status(id);
        System.out.println(
            res.getTaskId() + " " + res.getStatus() + " worker=" + res.getWorkerId()
                + " execMs=" + res.getExecTimeMs() + " result=" + res.getResult());
        return 0;
      }
    }
  }

  @Command(name = "cancel", description = "Cancel a queued task")
  static class Cancel implements Callable<Integer> {
    @Parameters(index = "0") String id;

    @Option(names = "--scheduler", defaultValue = "localhost:50051")
    String scheduler;

    @Override
    public Integer call() {
      try (SchedulerClient c = client(scheduler)) {
        var res = c.cancel(id);
        System.out.println(res.getTaskId() + " accepted=" + res.getAccepted() + " " + res.getMessage());
        return res.getAccepted() ? 0 : 1;
      }
    }
  }

  @Command(name = "watch", description = "Poll until the task finishes")
  static class Watch implements Callable<Integer> {
    @Parameters(index = "0") String id;

    @Option(names = "--scheduler", defaultValue = "localhost:50051")
    String scheduler;

    @Override
    public Integer call() throws Exception {
      try (SchedulerClient c = client(scheduler)) {
        TaskStatusResponse res = c.watch(id, 500, 120_000);
        System.out.println(
            res.getTaskId() + " " + res.getStatus() + " worker=" + res.getWorkerId()
                + " execMs=" + res.getExecTimeMs() + " result=" + res.getResult());
        return 0;
      }
    }
  }

  public static void main(String[] args) {
    org.slf4j.MDC.put("node", "client");
    System.exit(new CommandLine(new PredischedCli()).execute(args));
  }

  @Command(
      name = "admin",
      description = "Operator commands",
      subcommands = {PredischedCli.AdminStrategy.class, PredischedCli.AdminClocks.class})
  static class Admin implements Callable<Integer> {
    @Override
    public Integer call() {
      CommandLine.usage(this, System.out);
      return 0;
    }
  }

  @Command(name = "strategy", description = "Switch the scheduling strategy without a restart")
  static class AdminStrategy implements Callable<Integer> {
    @Parameters(index = "0", description = "round-robin, random, least-loaded, resource-aware")
    String name;

    @Option(names = "--scheduler", defaultValue = "localhost:50051")
    String scheduler;

    @Override
    public Integer call() {
      try (SchedulerClient c = client(scheduler)) {
        var res = c.setStrategy(name);
        System.out.println("ok=" + res.getOk() + " " + res.getMessage());
        return res.getOk() ? 0 : 1;
      }
    }
  }

  @Command(name = "clocks", description = "Show each node's clock offset from the Berkeley daemon")
  static class AdminClocks implements Callable<Integer> {
    @Option(names = "--scheduler", defaultValue = "localhost:50051")
    String scheduler;

    @Override
    public Integer call() {
      try (SchedulerClient c = client(scheduler)) {
        var res = c.clocks();
        if (res.getOffsetsCount() == 0) {
          System.out.println("no sync round yet");
          return 0;
        }
        System.out.println("node | offset_ms | round_ts");
        for (var offset : res.getOffsetsList()) {
          System.out.println(
              offset.getNodeId() + " | " + offset.getOffsetMs() + " | " + offset.getRoundTs());
        }
        return 0;
      }
    }
  }

  @Command(name = "events", description = "Print cluster events merged by (Lamport time, node id)")
  static class Events implements Callable<Integer> {
    @Option(names = "--task", description = "Only events for this task id")
    String task;

    @Option(names = "--scheduler", defaultValue = "localhost:50051")
    String scheduler;

    @Override
    public Integer call() {
      try (SchedulerClient c = client(scheduler)) {
        var res = c.events(task);
        if (res.getEventsCount() == 0) {
          System.out.println("no events" + (task != null ? " for task " + task : ""));
          return 0;
        }
        System.out.println("lamport | node | type | task | details");
        for (var e : res.getEventsList()) {
          System.out.println(
              e.getLamport() + " | " + e.getNodeId() + " | " + e.getType() + " | "
                  + e.getTaskId() + " | " + e.getDetails());
        }
        return 0;
      }
    }
  }
}
