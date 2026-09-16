package com.predisched.benchmark;

/** Entry point for the benchmark jars: {@code pool-scaling}, ... (one grows per prompt). */
public class BenchmarkMain {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
      System.exit(2);
    }
    switch (args[0]) {
      case "pool-scaling" -> PoolScaling.run();
      case "strategy-spread" -> {
        int tasks = 300;
        for (int i = 1; i < args.length - 1; i++) {
          if (args[i].equals("--tasks")) {
            tasks = Integer.parseInt(args[i + 1]);
          }
        }
        StrategySpread.run(tasks);
      }
      default -> {
        System.out.println("unknown benchmark: " + args[0]);
        usage();
        System.exit(2);
      }
    }
  }

  private static void usage() {
    System.out.println("usage: predisched-benchmark.jar <benchmark>");
    System.out.println("  pool-scaling   worker thread-pool scaling (writes results/pool-scaling.csv)");
    System.out.println("  strategy-spread [--tasks N]  strategy comparison (writes results/strategy-spread.csv)");
  }
}
