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
import com.kmwllc.lucille.message.MessengerException;
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
    assertEquals("Expected (java.lang.StackOverflowError)", messenger.getSentEvents().get(0).getMessage());
    assertTrue(thread.isAlive());
    assertNull(thread.getThrowable());

    thread.terminate();
    thread.join(5000);
    assertFalse(thread.isAlive());
    assertNull(thread.getThrowable());
  }

  @Test
  public void testStageExceptionMessageIsTheFailEventMessage() throws Exception {
    // the FAIL event carries the stage's own message, not that of the RuntimeException the Stage wraps it in
    TestMessenger messenger = new TestMessenger();
    WorkerThread thread = startWorker(PIPELINES, messenger, "stageExceptionForDoc1");
    messenger.sendForProcessing(Document.create("doc1"));

    waitUntil(() -> count(messenger, Event.Type.FAIL) == 1);
    Event fail = messenger.getSentEvents().get(0);
    assertEquals("doc1", fail.getDocumentId());
    assertEquals("Expected (" + StageException.class.getName() + ")", fail.getMessage());

    thread.terminate();
    thread.join(5000);
    assertNull(thread.getThrowable());
  }

  @Test
  public void testOutOfMemoryErrorIsFatalToTheWorker() throws Exception {
    // the JVM cannot be trusted after a VirtualMachineError: record it, make a best-effort FAIL, clean up, and die
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    WorkerThread thread = startWorker(PIPELINES, messenger, "outOfMemory");
    messenger.sendForProcessing(Document.create("doc1"));

    thread.join(15000);
    assertFalse(thread.isAlive());
    assertTrue(String.valueOf(thread.getThrowable()), thread.getThrowable() instanceof OutOfMemoryError);
    assertEquals(1, count(messenger, Event.Type.FAIL));
    Mockito.verify(messenger, Mockito.times(1)).close();
  }

  @Test
  public void testMessengerFailureIsFatalAndIsNotTurnedIntoADocumentFailure() throws Exception {
    // RUN-05: a send failure means the messenger is unhealthy; the document must not be marked FAILED and committed
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doThrow(new MessengerException("broker unavailable", new java.io.IOException("broker unavailable")))
        .when(messenger).sendForIndexing(Mockito.any());
    WorkerThread thread = startWorker(PIPELINES, messenger, "nop");
    messenger.sendForProcessing(Document.create("doc1"));

    thread.join(15000);
    assertFalse(thread.isAlive());
    assertTrue(String.valueOf(thread.getThrowable()), thread.getThrowable() instanceof MessengerException);
    assertTrue(thread.getThrowable().getCause() instanceof java.io.IOException);
    assertEquals(0, count(messenger, Event.Type.FAIL));
    Mockito.verify(messenger, Mockito.times(1)).close();
  }

  @Test
  public void testRecordTooLargeFailsTheDocumentAndItsAnnouncedChild() throws Exception {
    // RUN-03: the child's CREATE was sent, so without a FAIL for the child the publisher would wait for it forever;
    // a failure the messenger marks as specific to the document (e.g. a Kafka record too large) leaves the worker running
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doAnswer(invocation -> {
      Document doc = invocation.getArgument(0);
      if (doc.getId().contains("_child")) {
        throw new MessengerException("too large", new java.io.IOException("record too large"), true);
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
    List<String> failMessages = messenger.getSentEvents().stream()
        .filter(e -> e.getType() == Event.Type.FAIL).map(Event::getMessage).toList();
    assertEquals(List.of("parent doc1 failed: too large", "too large", "parent doc2 failed: too large", "too large"),
        failMessages);
    assertTrue(thread.isAlive());
    assertNull(thread.getThrowable());

    thread.terminate();
    thread.join(5000);
    assertNull(thread.getThrowable());
  }

  /** A counter that reports every document as having exhausted its retries. */
  private static final RetryCounter EXHAUSTED = new RetryCounter() {
    @Override
    public boolean add(Document document) {
      return true;
    }

    @Override
    public void remove(Document document) {
    }
  };

  private static WorkerThread startWorkerWithCounter(TestMessenger messenger, RetryCounter counter) throws Exception {
    Worker worker = new Worker(PIPELINES, messenger, "run1", "nop", "metrics", counter);
    return Worker.startThread(worker, "Lucille-Test-Worker-dlq");
  }

  @Test
  public void testExhaustedRetriesSendTheDocumentToTheDeadLetterQueue() throws Exception {
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    WorkerThread thread = startWorkerWithCounter(messenger, EXHAUSTED);
    messenger.sendForProcessing(Document.create("doc1"));

    waitUntil(() -> count(messenger, Event.Type.FAIL) == 1);
    Mockito.verify(messenger).sendFailed(Mockito.any());
    assertEquals("WORKER_MAX_RETRIES_EXCEEDED", messenger.getSentEvents().get(0).getMessage());
    assertEquals(0, messenger.getDocsSentForIndexing().size());
    assertTrue(thread.isAlive());

    thread.terminate();
    thread.join(5000);
    assertNull(thread.getThrowable());
  }

  @Test
  public void testDeadLetterQueueRejectingTheDocumentStillFailsAndCommitsIt() throws Exception {
    // a record too large for the fail topic is the document's fault: redelivering it would only repeat the rejection,
    // so the document is failed and committed and the worker carries on
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doThrow(new MessengerException("too large", new java.io.IOException("record too large"), true))
        .when(messenger).sendFailed(Mockito.any());
    WorkerThread thread = startWorkerWithCounter(messenger, EXHAUSTED);
    messenger.sendForProcessing(Document.create("doc1"));
    messenger.sendForProcessing(Document.create("doc2"));

    waitUntil(() -> count(messenger, Event.Type.FAIL) == 2);
    assertTrue(thread.isAlive());
    assertNull(thread.getThrowable());

    thread.terminate();
    thread.join(5000);
    assertNull(thread.getThrowable());
  }

  @Test
  public void testDeadLetterQueueFailureIsFatalWhenNotDocumentSpecific() throws Exception {
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    Mockito.doThrow(new MessengerException("broker unavailable", new java.io.IOException("broker unavailable")))
        .when(messenger).sendFailed(Mockito.any());
    WorkerThread thread = startWorkerWithCounter(messenger, EXHAUSTED);
    messenger.sendForProcessing(Document.create("doc1"));

    thread.join(15000);
    assertFalse(thread.isAlive());
    assertTrue(String.valueOf(thread.getThrowable()), thread.getThrowable() instanceof MessengerException);
    assertEquals(0, count(messenger, Event.Type.FAIL));
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
    assertTrue(String.valueOf(thread.getThrowable()), thread.getThrowable() instanceof java.io.IOException);
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
    assertNull(thread.getThrowable());
  }

  @Test
  public void testMaxRetriesIsIgnoredForNonKafkaMessengers() throws Exception {
    // RUN-12: without this, a local run with worker.maxRetries set needed a zookeeper.connectString and opened a
    // ZooKeeper client per worker thread
    Config config = ConfigFactory.parseString("worker.maxRetries: 3").withFallback(PIPELINES);
    Worker worker = new Worker(config, new TestMessenger(), "run1", "nop", "metrics");
    assertNull(worker.getThrowable());
  }

}
