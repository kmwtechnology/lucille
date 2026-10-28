package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class LockRegistryTest {

  @Test
  public void testLockRegistryClearing() throws Exception {
    LockRegistry registry = new LockRegistry();

    registry.acquire("key-1");
    registry.acquire("key-2");
    assertEquals(2, registry.getKeys().size());

    registry.release("key-1");
    assertEquals(1, registry.getKeys().size());

    registry.release("key-2");
    assertEquals(0, registry.getKeys().size());
  }

  @Test
  public void testLockRegistryBlocking() throws Exception {
    LockRegistry registry = new LockRegistry();

    registry.acquire("key-1");

    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch acquired = new CountDownLatch(1);

    Thread t = new Thread(() -> {
      started.countDown();

      try {
        registry.acquire("key-1");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      assertEquals(1, registry.getKeys().size());
      acquired.countDown();

      registry.release("key-1");
    });

    t.start();

    started.await();
    assertEquals(1, acquired.getCount());

    registry.release("key-1");
    assertTrue(acquired.await(1, TimeUnit.SECONDS));

    // prevent small race
    t.join();

    assertEquals(0, registry.getKeys().size());
  }

  @Test
  public void testInterruptWhileWaiting() throws Exception {
    LockRegistry registry = new LockRegistry();

    registry.acquire("key-1");

    AtomicReference<Throwable> thrown = new AtomicReference<>();
    Thread t = new Thread(() -> {
      try {
        registry.acquire("key-1");
      } catch (Throwable e) {
        thrown.set(e);
      }
    });

    t.start();

    // wait until the thread is parked on the lock, not merely started
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (t.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(Thread.State.WAITING, t.getState());

    t.interrupt();
    t.join(5000);

    assertFalse(t.isAlive());
    assertTrue(thrown.get() instanceof InterruptedException);

    // while we still hold the lock, a second thread must still be blocked. This checks the same lock the
    // interrupted thread waited on - if it had added a permit, the second thread would get the lock here.
    CountDownLatch acquired = new CountDownLatch(1);
    Thread second = new Thread(() -> {
      try {
        registry.acquire("key-1");
        acquired.countDown();
        registry.release("key-1");
      } catch (InterruptedException e) {
        // ends the thread; the test fails on the latch below
      }
    });
    second.start();

    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (second.getState() != Thread.State.WAITING && acquired.getCount() > 0 && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(1, acquired.getCount());

    // the interrupted thread's reference is gone, so once both holders release, the map is empty
    registry.release("key-1");
    assertTrue(acquired.await(5, TimeUnit.SECONDS));
    second.join(5000);

    assertEquals(0, registry.getKeys().size());
  }

  @Test
  public void testReleaseMissingKey() {
    LockRegistry registry = new LockRegistry();
    assertThrows(IllegalStateException.class, () -> registry.release("key-1"));
  }
}
