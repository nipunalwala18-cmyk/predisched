package com.predisched.worker;

import com.predisched.proto.TaskType;
import java.util.Random;

/**
 * MATRIX_TASK: seeded random N×N multiply, returns a checksum.
 *
 * <p>Seed is fixed at 42 and the checksum is the sum of all elements of C, so the same
 * task gives the same result on the Java backend and (Prompt 09) the MPI backend.
 */
public class MatrixTaskExecutor implements TaskExecutor {

  static final long SEED = 42;

  @Override
  public TaskType type() {
    return TaskType.MATRIX_TASK;
  }

  @Override
  public String execute(String input) {
    int n = Integer.parseInt(input.trim());
    double[][] c = multiply(n, SEED);
    return "checksum=" + checksum(c);
  }

  static double[][] multiply(int n, long seed) {
    Random rnd = new Random(seed);
    double[][] a = new double[n][n];
    double[][] b = new double[n][n];
    for (int i = 0; i < n; i++) {
      for (int j = 0; j < n; j++) {
        a[i][j] = rnd.nextDouble();
        b[i][j] = rnd.nextDouble();
      }
    }
    double[][] c = new double[n][n];
    for (int i = 0; i < n; i++) {
      for (int k = 0; k < n; k++) {
        double aik = a[i][k];
        for (int j = 0; j < n; j++) {
          c[i][j] += aik * b[k][j];
        }
      }
    }
    return c;
  }

  static double checksum(double[][] c) {
    double sum = 0;
    for (double[] row : c) {
      for (double v : row) {
        sum += v;
      }
    }
    return sum;
  }
}
