package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.message.MessengerException;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.Test;

/**
 * Tests the Worker's "send failed documents to the fail topic" behavior: the {@code worker.sendFailedDocsToTopic}
 * gate on the pipeline-exception path, the failure metadata stamped on documents that are routed to the fail topic,
 * and what happens when the fail topic can't take a document. The pipeline used here is a single {@link com.kmwllc.lucille.stage.ErrorStage}, which throws for every
 * document, so every document fails during pipeline processing.
 */
public class WorkerFailTopicTest {

  private static final String PIPELINE_NAME = "pipeline1";

  /**
   * Drives a real Worker against a TestMessenger: a single document is sent for processing, the worker is run on a
   * thread until it has routed the (failed) document to the fail topic, and then terminated.
   */
  private TestMessenger runWorkerOnOneDoc(Config config, Document doc) throws Exception {
    TestMessenger messenger = new TestMessenger();
    messenger.initialize("run1", PIPELINE_NAME);
    messenger.sendForProcessing(doc);

    Worker worker = new Worker(config, messenger, "run1", PIPELINE_NAME, PIPELINE_NAME);
    WorkerThread workerThread = Worker.startThread(worker, "fail-topic-test-worker");
    try {
      // Wait until the worker has emitted the FAIL event for the document, which happens on the same path as the
      // fail-topic send. 10s is generous; the local poll timeout is 50ms.
      long deadline = System.currentTimeMillis() + 10000;
      while (messenger.getSentEvents().isEmpty() && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
    } finally {
      workerThread.terminate();
      workerThread.join(10000);
    }
    assertFalse("worker thread should have stopped", workerThread.isAlive());
    return messenger;
  }

  @Test
  public void testFailedDocRoutedToFailTopicWhenEnabled() throws Exception {
    Config config = ConfigFactory.load("WorkerFailTopicTest/sendFailedEnabled.conf");
    Document doc = Document.create("doc1", "run1");

    TestMessenger messenger = runWorkerOnOneDoc(config, doc);

    List<Document> failed = messenger.getDocsSentToFailTopic();
    assertEquals("the failed document should be routed to the fail topic", 1, failed.size());
    assertEquals("doc1", failed.get(0).getId());
  }

  @Test
  public void testFailureMetadataStampedOnFailedDoc() throws Exception {
    Config config = ConfigFactory.load("WorkerFailTopicTest/sendFailedEnabled.conf");
    Document doc = Document.create("doc1", "run1");

    TestMessenger messenger = runWorkerOnOneDoc(config, doc);

    Document failed = messenger.getDocsSentToFailTopic().get(0);
    assertTrue(failed.has(Document.FAILURE_REASON_FIELD));
    assertEquals("worker", failed.getString(Document.FAILURE_COMPONENT_FIELD));
    assertTrue("a failure time should be stamped", failed.has(Document.FAILURE_TIME_FIELD));
  }

  @Test
  public void testFailedDocNotRoutedWhenDisabled() throws Exception {
    Config config = ConfigFactory.load("WorkerFailTopicTest/sendFailedDisabled.conf");
    Document doc = Document.create("doc1", "run1");

    TestMessenger messenger = runWorkerOnOneDoc(config, doc);

    // The document still fails and a FAIL event is still emitted, but nothing is routed to the fail topic.
    assertFalse("a FAIL event should still be emitted", messenger.getSentEvents().isEmpty());
    assertTrue("nothing should be routed to the fail topic when the flag is off",
        messenger.getDocsSentToFailTopic().isEmpty());
    assertFalse("no failure metadata should be stamped when the flag is off",
        doc.has(Document.FAILURE_REASON_FIELD));
  }

  /** A TestMessenger whose fail topic rejects every document. */
  private static TestMessenger failTopicFailingMessenger(boolean documentSpecific) {
    return new TestMessenger() {
      @Override
      public void sendFailed(Document document) throws MessengerException {
        throw new MessengerException("Simulated fail topic failure", null, documentSpecific);
      }
    };
  }

  private static WorkerThread startWorker(TestMessenger messenger) throws Exception {
    messenger.initialize("run1", PIPELINE_NAME);
    messenger.sendForProcessing(Document.create("doc1", "run1"));
    Config config = ConfigFactory.load("WorkerFailTopicTest/sendFailedEnabled.conf");
    return Worker.startThread(new Worker(config, messenger, "run1", PIPELINE_NAME, PIPELINE_NAME), "fail-topic-test-worker");
  }

  /** As for any other messenger failure: no FAIL event and no commit, so the document is redelivered rather than lost. */
  @Test
  public void testWorkerStopsWhenFailTopicIsUnavailable() throws Exception {
    TestMessenger messenger = failTopicFailingMessenger(false);
    WorkerThread workerThread = startWorker(messenger);
    workerThread.join(10000);

    assertFalse("worker should stop", workerThread.isAlive());
    assertTrue(workerThread.getThrowable() instanceof MessengerException);
    assertTrue("no FAIL event should be sent", messenger.getSentEvents().isEmpty());
  }

  /** Redelivering a document the fail topic itself rejects would only repeat the rejection. */
  @Test
  public void testWorkerContinuesWhenFailTopicRejectsTheDocument() throws Exception {
    TestMessenger messenger = failTopicFailingMessenger(true);
    WorkerThread workerThread = startWorker(messenger);
    try {
      long deadline = System.currentTimeMillis() + 10000;
      while (messenger.getSentEvents().isEmpty() && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      assertEquals(Event.Type.FAIL, messenger.getSentEvents().get(0).getType());
      assertTrue("worker should keep running", workerThread.isAlive());
    } finally {
      workerThread.terminate();
      workerThread.join(10000);
    }
    assertNull(workerThread.getThrowable());
  }

  /** A document that exhausts its retries goes to the fail topic whatever worker.sendFailedDocsToTopic says. */
  @Test
  public void testExhaustedRetriesStampFailureMetadata() throws Exception {
    Config config = ConfigFactory.load("WorkerFailTopicTest/sendFailedDisabled.conf");
    TestMessenger messenger = new TestMessenger();
    messenger.initialize("run1", PIPELINE_NAME);
    messenger.sendForProcessing(Document.create("doc1", "run1"));
    RetryCounter exhausted = new RetryCounter() {
      @Override
      public boolean add(Document document) {
        return true;
      }

      @Override
      public void remove(Document document) {
      }
    };

    WorkerThread workerThread =
        Worker.startThread(new Worker(config, messenger, "run1", PIPELINE_NAME, PIPELINE_NAME, exhausted), "fail-topic-test-worker");
    try {
      long deadline = System.currentTimeMillis() + 10000;
      while (messenger.getSentEvents().isEmpty() && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
    } finally {
      workerThread.terminate();
      workerThread.join(10000);
    }

    Document failed = messenger.getDocsSentToFailTopic().get(0);
    assertEquals("Retry count exceeded", failed.getString(Document.FAILURE_REASON_FIELD));
    assertEquals("worker", failed.getString(Document.FAILURE_COMPONENT_FIELD));
  }
}
