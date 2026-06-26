package com.kmwllc.lucille.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Maps String keys to fair ReentrantLocks. Keeps reference counts to
 * prevent unbounded memory consumption - locks are removed from the map
 * when no longer in use.
 */
public class LockRegistry {

  private final ConcurrentMap<String, LockInfo> locks =
      new ConcurrentHashMap<>();

  /**
   * Acquire a lock from this registry. This is a blocking call.
   * @param key The key for the lock.
   */
  public void acquire(String key) {
    LockInfo lockInfo =
        locks.compute(key, (k, existing) -> {
          if (existing == null) {
            existing = new LockInfo();
          }

          existing.refCount.incrementAndGet();
          return existing;
        });

    lockInfo.lock.lock();
  }

  /**
   * Releases the lock for the provided key from this registry
   * @param key The key for the lock.
   */
  public void release(String key) {
    // in one action, get the lock, unlock, and mgrCount...
    // if mgrCount is now zero, return null --> cleanup preventing unbounded usage
    locks.compute(key, (k, existing) -> {
      if (existing == null) {
        throw new IllegalStateException("No lock for key \"" + key + "\".");
      }

      existing.lock.unlock();

      return existing.refCount.decrementAndGet() == 0
          ? null
          : existing;
    });
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
    private final ReentrantLock lock = new ReentrantLock(true);
    // we track the number of times this lock is being "referenced"
    // or being locked so that we know when we can safely
    // "cleanup" the locks map and prevent unbounded heap usage
    private final AtomicInteger refCount = new AtomicInteger();
  }
}