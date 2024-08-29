package com.kmwllc.lucille.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collection;
import org.apache.commons.lang3.ThreadUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests WorkerIndexerPool.
 *
 * Note: There is additional coverage in HybridKafkaTest.testWorkerIndexerPool();
 * that test is in a separate class because it depends on an embedded test environment
 */
@RunWith(JUnit4.class)
public class WorkerIndexerPoolTest {

  @Test
  public void testParseConfig() throws Exception {
    // reuse basic config from WorkerPoolTest
    Config config = ConfigFactory.load("WorkerPoolTest/config.conf");

    WorkerIndexerPool pool1 = new WorkerIndexerPool(config, "pipeline1", true, null);
    WorkerIndexerPool pool2 = new WorkerIndexerPool(config, "pipeline2", true, null);
    WorkerIndexerPool pool3 = new WorkerIndexerPool(config, "pipeline3", true, null);
    WorkerIndexerPool pool4 = new WorkerIndexerPool(ConfigFactory.empty(), "pipeline4", true, null);

    assertEquals(14, pool1.getNumWorkers());
    assertEquals(23, pool2.getNumWorkers());
    assertEquals(7, pool3.getNumWorkers());
    assertEquals(WorkerPool.DEFAULT_POOL_SIZE, pool4.getNumWorkers());
  }

  @Test
  public void testThreadCleanupUponEncounteringConfigProblem() throws Exception {
    Collection<Thread> nonSystemThreadsBefore =
        ThreadUtils.findThreads(t -> !ThreadUtils.getSystemThreadGroup().equals(t.getThreadGroup()));

    // simulate a config error by passing an empty config to WorkerIndexerPool
    Config config = ConfigFactory.empty();
    WorkerIndexerPool pool = new WorkerIndexerPool(config, "pipeline12345", true, null);

    assertThrows(Exception.class, () -> { pool.start(); });

    Collection<Thread> nonSystemThreadsAfter =
        ThreadUtils.findThreads(t -> !ThreadUtils.getSystemThreadGroup().equals(t.getThreadGroup()));
    assertArrayEquals(nonSystemThreadsBefore.toArray(), nonSystemThreadsAfter.toArray());
  }

}
