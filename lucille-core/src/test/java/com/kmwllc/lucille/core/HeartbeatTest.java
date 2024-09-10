package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.kmwllc.lucille.util.ThreadNameUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Collection;
import org.apache.commons.lang3.ThreadUtils;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HeartbeatTest {

  private static final Logger log = LoggerFactory.getLogger(HeartbeatTest.class);

  @Test
  public void testWatcher() throws Exception {

    // We might like to delete the heartbeat log and see if it is recreated.
    // But that's not safe to do. The logging system is initialized once for the whole test run
    // and may not recover if a logfile that was written in a previous test gets
    // unexpectedly deleted here. So instead we check that the line count of the heartbeat log
    // increases.
    File heartbeatLog = new File("log/heartbeat.log");
    long previousLineCount = 0;
    if (heartbeatLog.exists()) {
      previousLineCount = Files.lines(heartbeatLog.toPath()).count();
    }

    Config config = ConfigFactory.load("WorkerPoolTest/watcher.conf");
    WorkerPool pool1 = new WorkerPool(config, "pipeline1",
        WorkerMessengerFactory.getConstantFactory(new LocalMessenger()), "");

    pool1.start();

    // give the worker time to begin generating heartbeats
    Thread.sleep(3000);

    pool1.stop();
    pool1.join();

    assertTrue(heartbeatLog.exists());

    Stream<String> lines = Files.lines(heartbeatLog.toPath());
    long currentLineCount = Files.lines(heartbeatLog.toPath()).count();

    assertTrue(currentLineCount > previousLineCount);

    String lastLine = lines.skip(currentLineCount - 1).findFirst().get();
    assertTrue(lastLine.contains("INFO Heartbeat: Issuing heartbeat"));
  }

}
