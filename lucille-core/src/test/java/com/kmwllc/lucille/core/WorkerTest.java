package com.kmwllc.lucille.core;
import com.kmwllc.lucille.stage.StartStopCaptureStage;
import com.kmwllc.lucille.message.TestMessenger;
import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class WorkerTest {

  @Test
  public void testMainArgs() throws Exception {
    Map<WorkerPool, List<Object>> args = new HashMap<>();
    Config config = ConfigFactory.load("WorkerTest/config.conf");
    WorkerMessengerFactory mockFactory = new WorkerMessengerFactory() {
      @Override
      public WorkerMessenger create() {
        throw new UnsupportedOperationException("Unimplemented method 'create'");
      }
    };
    try (MockedStatic<Launch.SystemHelper> systemHelper = Mockito.mockStatic(Launch.SystemHelper.class);
        MockedStatic<ConfigFactory> configFactory = Mockito.mockStatic(ConfigFactory.class);
        MockedConstruction<WorkerPool> workerPool = Mockito.mockConstruction(WorkerPool.class, (mock, context) -> {
          args.put(mock, new ArrayList<>(context.arguments()));
        });
        MockedStatic<WorkerMessengerFactory> factory = Mockito.mockStatic(WorkerMessengerFactory.class)) {

      configFactory.when(ConfigFactory::load).thenReturn(config);

      factory.when(() -> {
        WorkerMessengerFactory.getKafkaFactory(config, "foo");
      }).thenReturn(mockFactory);

      Worker.main(new String[] {"foo"});

      assertEquals(1, workerPool.constructed().size());
      WorkerPool pool = workerPool.constructed().get(0);

      assertEquals(config, args.get(pool).get(0));
      assertEquals("foo", args.get(pool).get(1));
      assertNull(args.get(pool).get(2));
      assertEquals(mockFactory, args.get(pool).get(3));
      assertEquals("foo", args.get(pool).get(4));
    }
  }
  @Test
  public void testMainNoArgs() throws Exception {
    Map<WorkerPool, List<Object>> args = new HashMap<>();
    Config config = ConfigFactory.load("WorkerTest/config.conf");
    WorkerMessengerFactory mockFactory2 = new WorkerMessengerFactory() {
      @Override
      public WorkerMessenger create() {
        throw new UnsupportedOperationException("Unimplemented method 'create'");
      }
    };
    try (MockedStatic<Launch.SystemHelper> systemHelper = Mockito.mockStatic(Launch.SystemHelper.class);
        MockedStatic<ConfigFactory> configFactory = Mockito.mockStatic(ConfigFactory.class);
        MockedConstruction<WorkerPool> workerPool = Mockito.mockConstruction(WorkerPool.class, (mock, context) -> {
          args.put(mock, new ArrayList<>(context.arguments()));
        });
        MockedStatic<WorkerMessengerFactory> factory = Mockito.mockStatic(WorkerMessengerFactory.class)) {

      configFactory.when(ConfigFactory::load).thenReturn(config);

      factory.when(() -> {
        WorkerMessengerFactory.getKafkaFactory(config, "foo2");
      }).thenReturn(mockFactory2);

      Worker.main(new String[0]);

      List<WorkerPool> constructed = workerPool.constructed();
      assertEquals(1, constructed.size());
      WorkerPool pool = constructed.get(0);

      assertEquals(config, args.get(pool).get(0));
      assertEquals("foo2", args.get(pool).get(1));
      assertNull(args.get(pool).get(2));
      assertEquals(mockFactory2, args.get(pool).get(3));
      assertEquals("foo2", args.get(pool).get(4));
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // failure handling (LC-987)

  private static final Config PIPELINES = ConfigFactory.load("WorkerTest/pipelines.conf");

  /** Starts a worker on its own thread and returns the thread; the caller terminates and joins it. */
  private static WorkerThread startWorker(Config config, WorkerMessenger messenger, String pipeline) throws Exception {
    Worker worker = new Worker(config, messenger, "run1", pipeline, "metrics");
    return Worker.startThread(worker, "Lucille-Test-Worker-" + pipeline);
  }

  private static void waitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 15000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Condition not met within 15 seconds");
      }
      Thread.sleep(20);
    }
  }

  private static long count(TestMessenger messenger, Event.Type type) {
    return messenger.getSentEvents().stream().filter(e -> e.getType() == type).count();
  }

  @Test
  public void testStageErrorFailsDocumentAndWorkerContinues() throws Exception {
    // RUN-02: a StackOverflowError from a stage is local to the document: FAIL event, commit, next document
    TestMessenger messenger = new TestMessenger();
    WorkerThread thread = startWorker(PIPELINES, messenger, "stackOverflowForDoc1");
    messenger.sendForProcessing(Document.create("doc1"));
    messenger.sendForProcessing(Document.create("doc2"));

    waitUntil(() -> messenger.getDocsSentForIndexing().size() == 1 && count(messenger, Event.Type.FAIL) == 1);
    assertEquals("doc2", messenger.getDocsSentForIndexing().get(0).getId());
    assertEquals("doc1", messenger.getSentEvents().get(0).getDocumentId());
    assertTrue(thread.isAlive());
    assertNull(thread.getFailure());

    thread.terminate();
    thread.join(5000);
    assertFalse(thread.isAlive());
    assertNull(thread.getFailure());
  }

  @Test
  public void testOutOfMemoryErrorIsFatalToTheWorker() throws Exception {
    // the JVM cannot be trusted after a VirtualMachineError: record it, make a best-effort FAIL, clean up, and die
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    WorkerThread thread = startWorker(PIPELINES, messenger, "outOfMemory");
    messenger.sendForProcessing(Document.create("doc1"));

    thread.join(15000);
    assertFalse(thread.isAlive());
    assertTrue(String.valueOf(thread.getFailure()), thread.getFailure() instanceof OutOfMemoryError);
    assertEquals(1, count(messenger, Event.Type.FAIL));
    Mockito.verify(messenger, Mockito.times(1)).close();
  }

  @Test
  public void testMessengerFailureIsFatalAndIsNotTurnedIntoADocumentFailure() throws Exception {
    // RUN-05: a send failure means the messenger is unhealthy; the document must not be marked FAILED and committed
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doThrow(new java.io.IOException("broker unavailable")).when(messenger).sendForIndexing(Mockito.any());
    WorkerThread thread = startWorker(PIPELINES, messenger, "nop");
    messenger.sendForProcessing(Document.create("doc1"));

    thread.join(15000);
    assertFalse(thread.isAlive());
    assertTrue(String.valueOf(thread.getFailure()), thread.getFailure() instanceof java.io.IOException);
    assertEquals(0, count(messenger, Event.Type.FAIL));
    Mockito.verify(messenger, Mockito.times(1)).close();
  }

  @Test
  public void testRecordTooLargeFailsTheDocumentAndItsAnnouncedChild() throws Exception {
    // RUN-03: the child's CREATE was sent, so without a FAIL for the child the publisher would wait for it forever;
    // RecordTooLargeException is specific to the record, so the worker itself carries on
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doAnswer(invocation -> {
      Document doc = invocation.getArgument(0);
      if (doc.getId().contains("_child")) {
        throw new java.util.concurrent.ExecutionException(
            new org.apache.kafka.common.errors.RecordTooLargeException("too large"));
      }
      return invocation.callRealMethod();
    }).when(messenger).sendForIndexing(Mockito.any());
    WorkerThread thread = startWorker(PIPELINES, messenger, "oneChild");
    messenger.sendForProcessing(Document.create("doc1"));
    messenger.sendForProcessing(Document.create("doc2"));

    waitUntil(() -> count(messenger, Event.Type.FAIL) == 4);
    List<String> events = messenger.getSentEvents().stream().map(e -> e.getType() + ":" + e.getDocumentId()).toList();
    assertEquals(List.of("CREATE:doc1_child1", "FAIL:doc1_child1", "FAIL:doc1",
        "CREATE:doc2_child1", "FAIL:doc2_child1", "FAIL:doc2"), events);
    assertTrue(thread.isAlive());
    assertNull(thread.getFailure());

    thread.terminate();
    thread.join(5000);
    assertNull(thread.getFailure());
  }

  @Test
  public void testPollFailureStopsTheWorkerWithAFailureAndCleansUp() throws Exception {
    // RUN-04: a poll failure used to be one INFO line and a thread that vanished without closing anything
    StartStopCaptureStage.reset();
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doThrow(new java.io.IOException("authentication failed")).when(messenger).pollDocToProcess();
    WorkerThread thread = startWorker(PIPELINES, messenger, "startStop");

    thread.join(15000);
    assertFalse(thread.isAlive());
    assertTrue(String.valueOf(thread.getFailure()), thread.getFailure() instanceof java.io.IOException);
    Mockito.verify(messenger, Mockito.times(1)).close();
    assertTrue(StartStopCaptureStage.stopCalled);
    StartStopCaptureStage.reset();
  }

  @Test
  public void testTerminateIsNotAFailure() throws Exception {
    TestMessenger messenger = new TestMessenger();
    WorkerThread thread = startWorker(PIPELINES, messenger, "nop");
    thread.terminate();
    thread.join(5000);
    assertFalse(thread.isAlive());
    assertNull(thread.getFailure());
  }

  @Test
  public void testMaxRetriesIsIgnoredForNonKafkaMessengers() throws Exception {
    // RUN-12: without this, a local run with worker.maxRetries set needed a zookeeper.connectString and opened a
    // ZooKeeper client per worker thread
    Config config = ConfigFactory.parseString("worker.maxRetries: 3").withFallback(PIPELINES);
    Worker worker = new Worker(config, new TestMessenger(), "run1", "nop", "metrics");
    assertNull(worker.getFailure());
  }

  @Test
  public void testIsDocumentSpecific() {
    assertTrue(Worker.isDocumentSpecific(new org.apache.kafka.common.errors.RecordTooLargeException("x")));
    assertTrue(Worker.isDocumentSpecific(new java.util.concurrent.ExecutionException(
        new org.apache.kafka.common.errors.RecordTooLargeException("x"))));
    assertFalse(Worker.isDocumentSpecific(new java.util.concurrent.ExecutionException(
        new org.apache.kafka.common.errors.TimeoutException("x"))));
    assertFalse(Worker.isDocumentSpecific(new java.io.IOException("x")));
    assertFalse(Worker.isDocumentSpecific(null));
  }

}
