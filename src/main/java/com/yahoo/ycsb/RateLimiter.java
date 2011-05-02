package com.yahoo.ycsb;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Causes a ClientTask-like thing to be run at some specified average rate.  This is done by keeping a moving window of
 * 200 start times and adjusting the next start time to make this window have the right average speed..
 */
class RateLimiter<T extends ClientTask> implements Callable<Integer> {
  static Random random = new Random();

  private double opCount;

  private volatile int opsDone;
  private double target;
  private ClientTask task;
  private AtomicBoolean done = new AtomicBoolean(false);
  private AtomicBoolean shouldContinue = new AtomicBoolean(true);

  /**
   * A rate limiter object wil try to call the task object the specified number of times
   * at the specified rate or until the cancel method is called.  A moving window of start
   * times is kept so that we can keep the average rate tightly controlled.
   * @param opCount               Desired number of operations except that opCount == 0 implies no end.
   * @param targetOpsPerMs        Number of operations per millisecond, on average.
   * @param task                  The task to call.
   */
  public RateLimiter(double opCount, double targetOpsPerMs, ClientTask task) {
    this.opCount = opCount;
    this.target = targetOpsPerMs;
    this.task = task;
  }

  public Integer call() throws DBException, WorkloadException {
    // soft start over a 100ms period
    try {
      Thread.sleep(random.nextInt(100));
    } catch (InterruptedException e) {
      //do nothing
    }

    // we keep a moving window of start times to get better estimates
    Deque<Long> starts = new ArrayDeque<Long>(200);
    while (shouldContinue.get() && ((opCount == 0) || (opsDone < opCount))) {
      // remember start time
      starts.add(System.currentTimeMillis());

      // do the work
      task.call();

      opsDone++;

      // compute desired time for next step
      double nextT = starts.peekFirst() + starts.size() / target;

      while (starts.size() > 200) {
        starts.pollFirst();
      }
      final long t = System.currentTimeMillis();
      // triangular dithering gives effect of more time resolution in delay
      double delay = nextT - t + random.nextDouble() - random.nextDouble();
      if (delay > 1) {
        try {
          Thread.sleep((long) delay);
        } catch (InterruptedException e) {
          // ignore interruptions
        }
      }
    }

    done.set(true);

    return task.finish();
  }

  /**
   * Causes at most one more operation to be done.
   */
  public void cancel() {
    shouldContinue.set(false);
  }

  /**
   * Returns number of operations already completed.
   * @return  The number of operations done.
   */
  public int getOpsDone() {
    return task.getOpsDone();
  }

  /**
   * @return true if no more tasks are to be done.
   */
  public boolean isFinished() {
    return done.get();
  }
}
