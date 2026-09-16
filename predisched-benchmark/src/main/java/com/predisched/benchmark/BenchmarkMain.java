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
  }
}
