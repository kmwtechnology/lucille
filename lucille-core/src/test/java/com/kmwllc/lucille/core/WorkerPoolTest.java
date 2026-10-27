package com.kmwllc.lucille.core;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import static com.kmwllc.lucille.util.ThreadNameUtils.areLucilleThreadsRunning;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(JUnit4.class)
public class WorkerPoolTest {

  @Test
  public void testParseConfig() throws Exception {
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");

    WorkerPool pool1 = new WorkerPool(config, "pipeline1", null, null, "");
    WorkerPool pool2 = new WorkerPool(config, "pipeline2", null, null, "");
    WorkerPool pool3 = new WorkerPool(config, "pipeline3", null, null, "");
    WorkerPool pool4 = new WorkerPool(ConfigFactory.empty(), "pipeline4", null, null, "");

    assertEquals(14, pool1.getNumWorkers());
    assertEquals(23, pool2.getNumWorkers());
    assertEquals(7, pool3.getNumWorkers());
    assertEquals(WorkerPool.DEFAULT_POOL_SIZE, pool4.getNumWorkers());


    pool1.stop();
    pool2.stop();
    pool3.stop();
    pool4.stop();
    pool1.join();
    pool2.join();
    pool3.join();
    pool4.join();
  }

  /**
   * Confirm that a worker's messenger is closed when the worker stopped.
   * This is important in kafka mode where we want to close any kafka client connections before
   * shutting down.
   *
   */
  @Test
  public void testManagerClose() throws Exception {
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    WorkerMessengerFactory factory = WorkerMessengerFactory.getConstantFactory(messenger);
    WorkerPool pool = new WorkerPool(ConfigFactory.load("WorkerPoolTest/onePipeline.conf"),
        "pipeline1", null, factory, "metricsPrefix");
    pool.start();
    pool.stop();
    pool.join();
    verify(messenger, times(1)).close();
  }

  @Test
  public void testThreadCleanupUponEncounteringConfigProblem() throws Exception {

    TestMessenger messenger = Mockito.spy(new TestMessenger());
    WorkerMessengerFactory factory = WorkerMessengerFactory.getConstantFactory(messenger);
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");
    // pipeline12345 is not present in config.conf
    WorkerPool pool = new WorkerPool(config, "pipeline12345", null, factory, "");

    assertThrows(Exception.class, () -> { pool.start(); });

    assertFalse(areLucilleThreadsRunning());
  }

  @Test
  public void testThreadCleanupUponEncounteringNullMessenger() throws Exception {
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");
    WorkerPool pool = new WorkerPool(config, "pipeline1", null, null, "");

    assertThrows(Exception.class, () -> { pool.start(); });

    assertFalse(areLucilleThreadsRunning());
  }

  // ---------------------------------------------------------------------------------------------------------------
  // failure detection (LC-987)

  @Test
  public void testDeadWorkerIsReportedAndFailsThePool() throws Exception {
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doThrow(new java.io.IOException("topic authorization failed")).when(messenger).pollDocToProcess();
    WorkerMessengerFactory factory = WorkerMessengerFactory.getConstantFactory(messenger);
    WorkerPool pool = new WorkerPool(ConfigFactory.load("WorkerPoolTest/onePipeline.conf"),
        "pipeline1", null, factory, "metricsPrefix");

    java.util.concurrent.CountDownLatch notified = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.atomic.AtomicReference<Throwable> reported = new java.util.concurrent.atomic.AtomicReference<>();
    pool.setFailureListener(t -> {
      reported.set(t);
      notified.countDown();
    });
    pool.start();
    try {
      assertTrue(notified.await(15, java.util.concurrent.TimeUnit.SECONDS));
      assertTrue(pool.hasFailed());
      assertTrue(String.valueOf(reported.get()), reported.get() instanceof java.io.IOException);
      assertFalse(pool.isAlive());
    } finally {
      pool.stop();
      pool.join();
    }
  }

  @Test
  public void testStoppedWorkersAreNotAFailure() throws Exception {
    TestMessenger messenger = new TestMessenger();
    WorkerMessengerFactory factory = WorkerMessengerFactory.getConstantFactory(messenger);
    WorkerPool pool = new WorkerPool(ConfigFactory.load("WorkerPoolTest/onePipeline.conf"),
        "pipeline1", null, factory, "metricsPrefix");
    pool.start();
    assertTrue(pool.isAlive());
    pool.stop();
    pool.join();
    assertFalse(pool.isAlive());
    assertFalse(pool.hasFailed());
  }

  @Test
  public void testExitOnTimeoutFailsThePoolInsteadOfTheJvm() throws Exception {
    // a worker that has not polled for maxProcessingSecs is interrupted and reported; nothing calls System.exit
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doAnswer(invocation -> {
      Thread.sleep(60000);
      return null;
    }).when(messenger).pollDocToProcess();
    WorkerMessengerFactory factory = WorkerMessengerFactory.getConstantFactory(messenger);
    WorkerPool pool = new WorkerPool(ConfigFactory.load("WorkerPoolTest/exitOnTimeout.conf"),
        "pipeline1", null, factory, "metricsPrefix");

    java.util.concurrent.CountDownLatch notified = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.atomic.AtomicReference<Throwable> reported = new java.util.concurrent.atomic.AtomicReference<>();
    pool.setFailureListener(t -> {
      reported.set(t);
      notified.countDown();
    });
    pool.start();
    try {
      assertTrue(notified.await(15, java.util.concurrent.TimeUnit.SECONDS));
      assertTrue(String.valueOf(reported.get()), reported.get() instanceof java.util.concurrent.TimeoutException);
      pool.join(5000);
      assertFalse(pool.isAlive());
    } finally {
      pool.stop();
      pool.join();
    }
  }

}
