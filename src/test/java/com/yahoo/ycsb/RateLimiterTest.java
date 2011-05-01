package com.yahoo.ycsb;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

public class RateLimiterTest extends TestCase {
  @Test
  public void testBasicRate() throws DBException, WorkloadException {
    RateLimiter<SmoothTask> x = new RateLimiter<SmoothTask>(2000, 0.7, new SmoothTask());
    long start = System.currentTimeMillis();
    assertEquals(2000, x.call().intValue());
    long end = System.currentTimeMillis();
    assertEquals(2000 / 0.7, end - start, 200);
  }

  private static class SmoothTask extends ClientTask {
    private static Random gen = new Random();

    @Override
    public Integer call() throws DBException {
      try {
        // exponentially distributed wait time with mean 0.2
        double dt = gen.nextDouble() < 0.01 ? 20 : 1;
        if (dt > 1) {
          Thread.sleep((long) dt);
        }
      } catch (InterruptedException e) {
        // ignore
      }
      opCompleted();
      return 1;
    }

    @Override
    public int finish() throws DBException {
      // ignore
      return getOpsDone();
    }
  }

  private static class BumpyTask extends ClientTask {

  }
}
