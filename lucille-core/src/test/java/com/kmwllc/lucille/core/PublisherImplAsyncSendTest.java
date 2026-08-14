package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.message.PublisherMessenger;
import com.kmwllc.lucille.message.SendCallback;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Covers what the Publisher does when a messenger reports a send failure *after* it has already
 * returned from sendForProcessing -- the case that only exists once the send path is asynchronous.
 *
 * The hazard is silent data loss: the failed document can never receive a terminal Event, so if the
 * Publisher kept tracking it the run would hang until the connector timeout, and if it simply
 * stopped tracking it the run would report success having dropped the document on the floor.
 */
@RunWith(JUnit4.class)
public class PublisherImplAsyncSendTest {

  @Test
  public void testAsyncFailureStopsTrackingTheDocumentAndCountsIt() throws Exception {
    DeferredMessenger messenger = new DeferredMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    publisher.publish(Document.create("doc1"));

    // the messenger has accepted the document but not yet reported an outcome
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());
    assertEquals(0, publisher.numFailed());

    messenger.failAll(new RuntimeException("broker unavailable"));

    // no worker will ever emit a terminal Event for this document, so it must not stay pending
    assertEquals(0, publisher.numPending());
    assertFalse(publisher.hasPending());
    assertEquals(1, publisher.numFailed());
  }

  @Test
  public void testSuccessfulAsyncSendLeavesTheDocumentPending() throws Exception {
    DeferredMessenger messenger = new DeferredMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    publisher.publish(Document.create("doc1"));
    messenger.succeedAll();

    // acceptance by the destination is not completion: the document is still pending until a
    // worker reports a terminal Event for it
    assertEquals(1, publisher.numPending());
    assertEquals(0, publisher.numFailed());

    publisher.handleEvent(new Event("doc1", "run1", "", Event.Type.FINISH));
    assertEquals(0, publisher.numPending());
    assertEquals(1, publisher.numSucceeded());
  }

  @Test
  public void testAsyncFailureSurfacesOnTheNextPublish() throws Exception {
    DeferredMessenger messenger = new DeferredMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    publisher.publish(Document.create("doc1"));
    messenger.failAll(new RuntimeException("broker unavailable"));

    // the failure happened after publish() returned, so the next call is the first chance to raise
    // it -- otherwise a connector would keep feeding a destination that is not accepting anything
    Exception thrown = assertThrows(Exception.class, () -> publisher.publish(Document.create("doc2")));
    assertTrue(thrown.getMessage().contains("could not be sent for processing"));
    assertEquals("broker unavailable", thrown.getCause().getMessage());

    // doc2 was never handed to the messenger
    assertEquals(1, messenger.sent.size());
  }

  @Test
  public void testAsyncFailureFailsTheRunRatherThanReportingSuccess() throws Exception {
    DeferredMessenger messenger = new DeferredMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    publisher.publish(Document.create("doc1"));
    messenger.failAll(new RuntimeException("broker unavailable"));

    // an unstarted ConnectorThread stands in for a connector that has finished: not alive, no
    // exception of its own. Nothing is pending, so waitForCompletion terminates immediately.
    ConnectorThread finishedConnector = new ConnectorThread(null, publisher, "run1", "connector");
    PublisherResult result = publisher.waitForCompletion(finishedConnector, -1);

    assertFalse("a run that lost a document must not report success", result.getStatus());
    assertTrue(result.getMessage().contains("Publish failure"));
  }

  @Test
  public void testRunSucceedsWhenEverySendIsAccepted() throws Exception {
    DeferredMessenger messenger = new DeferredMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    publisher.publish(Document.create("doc1"));
    messenger.succeedAll();
    publisher.handleEvent(new Event("doc1", "run1", "", Event.Type.FINISH));

    ConnectorThread finishedConnector = new ConnectorThread(null, publisher, "run1", "connector");
    PublisherResult result = publisher.waitForCompletion(finishedConnector, -1);

    assertTrue(result.getStatus());
  }

  /**
   * Callbacks arrive on the messenger's I/O thread, not the publishing thread, so the counters and
   * tracking state they touch are written concurrently with publish(). Runs enough documents to
   * make a lost update likely if any of that state were non-atomic.
   */
  @Test
  public void testCallbacksFromOtherThreadsDoNotLoseCounts() throws Exception {
    int threads = 8;
    int docsPerThread = 250;

    DeferredMessenger messenger = new DeferredMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    for (int t = 0; t < threads; t++) {
      final int threadNum = t;
      pool.submit(() -> {
        try {
          for (int i = 0; i < docsPerThread; i++) {
            publisher.publish(Document.create("doc-" + threadNum + "-" + i));
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    pool.shutdown();
    assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

    assertEquals(threads * docsPerThread, publisher.numPublished());
    assertEquals(threads * docsPerThread, publisher.numPending());

    // now complete every send from a thread other than the publishing threads
    messenger.succeedAll();
    assertEquals(0, publisher.numFailed());
    assertEquals(threads * docsPerThread, publisher.numPending());

    // and drain them through the event path, which mutates the same counters
    for (Document doc : messenger.sent) {
      publisher.handleEvent(new Event(doc.getId(), "run1", "", Event.Type.FINISH));
    }
    assertEquals(threads * docsPerThread, publisher.numSucceeded());
    assertEquals(0, publisher.numPending());
  }

  /**
   * A messenger that accepts documents without completing them, so a test can decide when -- and on
   * which thread -- each send is reported as succeeded or failed.
   */
  private static class DeferredMessenger implements PublisherMessenger {

    private final List<Document> sent = new ArrayList<>();
    private final List<SendCallback> pending = new ArrayList<>();
    private String runId;

    @Override
    public void initialize(String runId, String pipelineName) {
      this.runId = runId;
    }

    @Override
    public String getRunId() {
      return runId;
    }

    @Override
    public void sendForProcessing(Document document) {
      throw new UnsupportedOperationException("the Publisher should use the callback overload");
    }

    @Override
    public synchronized void sendForProcessing(Document document, SendCallback callback) {
      sent.add(document);
      pending.add(callback);
    }

    @Override
    public Event pollEvent() {
      return null;
    }

    @Override
    public void close() {
    }

    private void failAll(Exception exception) throws Exception {
      completeAll(exception);
    }

    private void succeedAll() throws Exception {
      completeAll(null);
    }

    /** Completes on a separate thread, as a real asynchronous client's I/O thread would. */
    private void completeAll(Exception exception) throws Exception {
      List<SendCallback> toComplete;
      synchronized (this) {
        toComplete = new ArrayList<>(pending);
        pending.clear();
      }
      Thread ioThread = new Thread(() -> toComplete.forEach(c -> c.onCompletion(exception)));
      ioThread.start();
      ioThread.join();
    }
  }
}
