package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.LocalMessageManager;
import com.kmwllc.lucille.message.WorkerMessageManagerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import nl.altindag.log.LogCaptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import java.util.List;

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

  @Test
  public void testWatcher() throws Exception {

    LogCaptor logCaptor = LogCaptor.forClass(Worker.class);

    Config config = ConfigFactory.load("WorkerPoolTest/watcher.conf");

    // start a worker
    WorkerPool pool1 = new WorkerPool(config, "pipeline1", WorkerMessageManagerFactory.getConstantFactory(new LocalMessageManager()), "");

    // here, we want to check both of the logs to find what they've written
    // at least one info event, at least one error event in the worker log


    pool1.start();

    // sleep for more than one second (3 seconds)
    Thread.sleep(3000);


    List<String> infoLogs = logCaptor.getInfoLogs();
    //assertTrue(logCaptor.getInfoLogs().contains("Keyboard not responding. Press any key to continue...");
    //assertTrue(logCaptor.getWarnLogs()).containsExactly("Congratulations, you are pregnant!");

  System.out.println(infoLogs.get(0));
  }


//  public static class TestAppender extends AppenderSkeleton {
//    public List<String> messages = new ArrayList<String>();
//
//    @Override
//    protected void append(org.apache.log4j.spi.LoggingEvent loggingEvent) {
//      messages.add(loggingEvent.getMessage().toString());
//    }
//
//    @Override
//    public void close() {
//    }
//
//    @Override
//    public boolean requiresLayout() {
//      return false;
//    }
//  }
}
