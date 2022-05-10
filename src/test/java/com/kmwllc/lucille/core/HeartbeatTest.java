package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.LocalMessageManager;
import com.kmwllc.lucille.message.WorkerMessageManagerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.io.File;
import java.util.Scanner;

import static org.junit.Assert.assertTrue;

public class HeartbeatTest {
  @Test
  public void testWatcher() throws Exception {
    File heartbeatLog = new File("log/heartbeat.log");
    if (heartbeatLog.exists()) {
      heartbeatLog.delete();
    }

    Config config = ConfigFactory.load("WorkerPoolTest/watcher.conf");
    WorkerPool pool1 = new WorkerPool(config, "pipeline1", WorkerMessageManagerFactory.getConstantFactory(new LocalMessageManager()), "");

    pool1.start();

    // sleep for more than 1 second
    Thread.sleep(3000);

    pool1.stop();

    assertTrue(heartbeatLog.exists());

    Scanner scanner = new Scanner(heartbeatLog);
    while (scanner.hasNextLine()) {
      String heartbeat = scanner.nextLine();
      assertTrue(heartbeat.contains("INFO Heartbeat: Issuing heartbeat"));
    }

    if (heartbeatLog.exists()) {
      heartbeatLog.delete();
    }
  }
}
