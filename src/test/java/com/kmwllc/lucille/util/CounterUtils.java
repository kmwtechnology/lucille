package com.kmwllc.lucille.util;

import org.apache.commons.lang3.time.StopWatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CounterUtils {

  public static long DEFAULT_TIMEOUT_MS = 10000;
  public static long DEFAULT_END_LAG_MS = 500;

  public static void wait(AtomicLong counter, long targetValue) {
    wait(counter, targetValue, DEFAULT_TIMEOUT_MS, DEFAULT_END_LAG_MS);
  }

  /**
   * Waits until the counter reaches or exceeds the targetValue, followed by the designated
   * end pad duration. Times out with a RuntimeException after the designated timeout.
   *
   */
  public static void wait(AtomicLong counter, long targetValue, long timeoutMs, long endPad)  {

    StopWatch watch = new StopWatch();
    watch.start();

    while (true) {
      if (watch.getTime(TimeUnit.MILLISECONDS) > timeoutMs) {
        throw new RuntimeException("Timeout while waiting for counter to reach target value: " + targetValue);
      }

      long val = counter.get();

      if (val >= targetValue) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }

    // in many testing scenarios, we would like to wait for a short time
    // after the counter reaches the target value, to see if it is incremented
    // any further before the active components are stopped
    try {
      Thread.sleep(endPad);
    } catch (InterruptedException e) {
    }
  }
}
