package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class LockRegistryTest {

  @Test
  public void testLockRegistryClearing() {
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

      registry.acquire("key-1");
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
  public void testReleaseMissingKey() {
    LockRegistry registry = new LockRegistry();
    assertThrows(IllegalStateException.class, () -> registry.release("key-1"));
  }
}
