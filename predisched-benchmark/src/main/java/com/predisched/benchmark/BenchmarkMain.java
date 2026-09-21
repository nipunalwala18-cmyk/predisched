package com.predisched.benchmark;

import java.util.Arrays;
import java.util.Locale;

/**
 * Entry point for the benchmark harness. Each measurement is a subcommand, so the harness grows
 * with the product rather than turning into separate demo programs (rule 1).
 */
public class BenchmarkMain {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        String command = args[0].toLowerCase(Locale.ROOT);
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (command) {
            case "pool-size" -> PoolSizeBenchmark.main(rest);
            default -> {
                System.err.println("Unknown command: " + command);
                usage();
                System.exit(2);
            }
        }
    }

    private static void usage() {
        System.err.println("""
                Usage: predisched-benchmark <command> [options]

                Commands:
                  pool-size   Throughput and latency for worker pool sizes 1, 2, 4, 8
                              [--tasks 40] [--input n=20000000] [--reps 3]
                              [--out results/exp2-pool-size.csv]
                """);
    }
}
