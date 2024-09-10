package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.lang3.ThreadUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(JUnit4.class)
public class WorkerPoolTest {

  private static final Logger log = LoggerFactory.getLogger(WorkerPoolTest.class);

  @Test
  public void testParseConfig() throws Exception {
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");

    WorkerPool pool1 = new WorkerPool(config, "pipeline1", null, "");
    WorkerPool pool2 = new WorkerPool(config, "pipeline2", null, "");
    WorkerPool pool3 = new WorkerPool(config, "pipeline3", null, "");
    WorkerPool pool4 = new WorkerPool(ConfigFactory.empty(), "pipeline4", null, "");

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
        "pipeline1", factory, "metricsPrefix");
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
    WorkerPool pool = new WorkerPool(config, "pipeline12345", factory, "");

    assertThrows(Exception.class, () -> { pool.start(); });

  }

  @Test
  public void testThreadCleanupUponEncounteringNullMessenger() throws Exception {

    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");
    WorkerPool pool = new WorkerPool(config, "pipeline1", null, "");

    assertThrows(Exception.class, () -> { pool.start(); });
  }
}
