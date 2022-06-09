package com.kmwllc.lucille.util;

import org.apache.commons.lang3.time.StopWatch;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CounterUtils {

  public static long DEFAULT_TIMEOUT_MS = 10000;
  public static long DEFAULT_END_LAG_MS = 500;

  /**
   * Returns a threadsafe Set that can be used to track the unique document IDs processed
   * by a group of threads.
   *
   * We use a set of IDs instead of an AtomicLong or other counter because we want to track the number
   * of _unique_ documents processed. If we simply counted the number of documents seen, the count
   * might include duplicate documents. Duplicates can arise when multiple consumers are consuming from
   * a kafka topic and there is a consumer group rebalance before all offsets have been committed.
   *
   */
  public static Set<String> getThreadSafeSet() {
    return ConcurrentHashMap.newKeySet();
  }

  public static void waitUnique(Set<String> mySet, long targetValue) {
    waitUnique(mySet, targetValue, DEFAULT_TIMEOUT_MS, DEFAULT_END_LAG_MS);
  }

  /**
   * Waits until the set reaches or exceeds the targetSize, followed by the designated
   * end pad duration. Times out with a RuntimeException after the designated timeout.
   *
   */
  public static void waitUnique(Set<String> mySet, long targetSize, long timeoutMs, long endPad)  {

    StopWatch watch = new StopWatch();
    watch.start();

    while (true) {
      if (watch.getTime(TimeUnit.MILLISECONDS) > timeoutMs) {
        throw new RuntimeException("Timeout while waiting for counter to reach target size: " + targetSize);
      }

      int val = mySet.size();

      if (val >= targetSize) {
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
