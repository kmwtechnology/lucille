package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.LocalMessageManager;
import com.kmwllc.lucille.message.WorkerMessageManagerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nl.altindag.console.ConsoleCaptor;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class HeartbeatTest {
  @Test
  public void testWatcher() throws Exception {
    // use of console captor to capture the heartbeat logs
    ConsoleCaptor consoleCaptor = new ConsoleCaptor();
    consoleCaptor.clearOutput();

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
