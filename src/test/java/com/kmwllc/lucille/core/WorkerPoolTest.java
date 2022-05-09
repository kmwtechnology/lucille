package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.LocalMessageManager;
import com.kmwllc.lucille.message.WorkerMessageManagerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


import nl.altindag.console.ConsoleCaptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

  @Test
  public void testWatcher() throws Exception {
    // use of console captor to capture the heartbeat logs
    ConsoleCaptor consoleCaptor = new ConsoleCaptor();

    Config config = ConfigFactory.load("WorkerPoolTest/watcher.conf");
    WorkerPool pool1 = new WorkerPool(config, "pipeline1", WorkerMessageManagerFactory.getConstantFactory(new LocalMessageManager()), "");

    pool1.start();

    // sleep for more than 1 second
    Thread.sleep(3000);

    pool1.stop();

    consoleCaptor.close();

    String workerLog = consoleCaptor.getStandardOutput().get(0);
    String heartbeatLog = consoleCaptor.getStandardOutput().get(1);

    assertTrue(workerLog.contains("Starting 1 worker threads for pipeline pipeline1"));
    assertTrue(heartbeatLog.contains("Issuing heartbeat"));
  }
}

