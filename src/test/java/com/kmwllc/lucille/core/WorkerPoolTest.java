package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.slf4j.event.LoggingEvent;
import org.slf

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
    AppenderSkeleton appender = mock(AppenderSkeleton.class);
    ArgumentCaptor<LoggingEvent> logCaptor = ArgumentCaptor.forClass(LoggingEvent.class);

    Config config = ConfigFactory.load("WorkerPoolTest/watcher.conf");

    // start a worker
    WorkerPool pool1 = new WorkerPool(config, "pipeline1", null, "");

    pool1.start();

    // sleep for more than one second (3 seconds)
    Thread.sleep(3000);

    // here, we want to check both of the logs to find what they've written
    // at least one info event, at least one error event in the worker log

    Logger.getRootLogger().addAppender(appender);

    verify(appender).doAppend(logCaptor.capture());

    assertEquals("Warning message should have been logged", "Caution!", logCaptor.getValue().getRenderedMessage());
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
