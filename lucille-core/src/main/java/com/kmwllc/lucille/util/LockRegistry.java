package com.kmwllc.lucille.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps String keys to fair, non-reentrant locks. Keeps reference counts to
 * prevent unbounded memory consumption - locks are removed from the map
 * when no longer in use.
 *
 * <p> A lock is not owned by the thread that acquired it, so it may be released by a different thread.
 * This is needed because a Connector's execute() and close() can run on different threads.
 */
public class LockRegistry {

  private final ConcurrentMap<String, LockInfo> locks =
      new ConcurrentHashMap<>();

  private static final Logger log = LoggerFactory.getLogger(LockRegistry.class);

  /**
   * Acquire a lock from this registry. This is a blocking call, but it can be interrupted.
   * @param key The key for the lock.
   * @throws InterruptedException If the thread is interrupted while waiting. The lock is not held in that case.
   */
  public void acquire(String key) throws InterruptedException {
    LockInfo lockInfo =
        locks.compute(key, (k, existing) -> {
          if (existing == null) {
            existing = new LockInfo();
          }

          existing.refCount.incrementAndGet();
          return existing;
        });

    try {
      // using timed tryAcquire respects ordering on fair semaphores; untimed does not.
      if (!lockInfo.lock.tryAcquire(0, TimeUnit.SECONDS)) {
        log.warn("Waiting for lock \"{}\", held by another FileConnector in the JVM.", key);
        lockInfo.lock.acquire();
      }
    } catch (InterruptedException e) {
      // interrupted during acquire means refCount was incremented but the permit
      // (lock) was never actually acquired - so we just remove ourselves from the refCount
      dereference(key);
      throw e;
    }
  }

  /**
   * Releases the lock for the provided key from this registry
   * @param key The key for the lock.
   */
  public void release(String key) {
    // in one action, get the lock, unlock, and refCount...
    // if refCount is now zero, return null --> cleanup preventing unbounded usage
    locks.compute(key, (k, existing) -> {
      if (existing == null) {
        throw new IllegalStateException("No lock for key \"" + key + "\".");
      }

      existing.lock.release();

      return existing.refCount.decrementAndGet() == 0
          ? null
          : existing;
    });
  }

  /**
   * Drops a reference to the lock for the given key without releasing it, removing the lock from the map
   * when no references remain.
   */
  private void dereference(String key) {
    locks.computeIfPresent(key, (k, existing) ->
        existing.refCount.decrementAndGet() == 0
            ? null
            : existing);
  }

  /**
   * Get keys from the stored locks. Package access for unit testing.
   * @return Keys from the stored locks.
   */
  Set<String> getKeys() {
    return locks.keySet();
  }

  /**
   * Values held in the lock map.
   */
  private static final class LockInfo {
    // a single-permit Semaphore rather than a ReentrantLock, so it can be released from any thread
    private final Semaphore lock = new Semaphore(1, true);
    // we track the number of times this lock is being "referenced"
    // or being locked so that we know when we can safely
    // "cleanup" the locks map and prevent unbounded heap usage
    private final AtomicInteger refCount = new AtomicInteger();
  }
}