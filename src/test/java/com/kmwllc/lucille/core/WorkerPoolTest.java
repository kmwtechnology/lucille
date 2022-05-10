package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class WorkerPoolTest {

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
  }
}

