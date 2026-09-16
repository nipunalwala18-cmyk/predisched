package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ExecutorsTest {

  @Test
  void cpuSum() throws Exception {
    assertEquals("5050", new CpuTaskExecutor().execute("100"));
    assertEquals("1", new CpuTaskExecutor().execute("1"));
  }

  @Test
  void matrixChecksumStable() throws Exception {
    String a = new MatrixTaskExecutor().execute("10");
    String b = new MatrixTaskExecutor().execute("10");
    assertEquals(a, b);
    assertTrue(a.startsWith("checksum="));
    double v = Double.parseDouble(a.substring("checksum=".length()));
    assertTrue(v > 0);
  }

  @Test
  void sleepZeroIsFast() throws Exception {
    long start = System.nanoTime();
    assertEquals("slept 0 ms", new SleepTaskExecutor().execute("0"));
    assertTrue((System.nanoTime() - start) / 1_000_000 < 2000);
  }
}
