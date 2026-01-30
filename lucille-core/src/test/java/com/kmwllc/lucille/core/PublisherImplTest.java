package com.kmwllc.lucille.core;

import static java.time.Duration.ofMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTimeout;

import com.kmwllc.lucille.core.Event.Type;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PublisherImplTest {

  @Test
  public void testOutOfOrderCreateEvents() throws Exception {

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Event createEvent = new Event("doc1", "run1", "", Event.Type.CREATE);
    Event finishEvent = new Event("doc1", "run1", "", Event.Type.FINISH);

    // if we receive a late CREATE event (i.e. one that arrives after the corresponding FINISH event)
    // we shouldn't treat this as a newly created document that adds to the count of pending documents
    publisher.handleEvent(finishEvent);
    assertEquals(0, publisher.numPending());
    publisher.handleEvent(createEvent);
    assertEquals(0, publisher.numPending());

    // we can handle late CREATE events but not redundant ones currently...
    // so if the same CREATE event is received again, it will increase the count of pending documents
    publisher.handleEvent(createEvent);
    assertEquals(1, publisher.numPending());
  }

  @Test
  public void testCreateAndFinish() throws Exception {

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Event createEvent = new Event("doc1", "run1", "", Event.Type.CREATE);
    Event finishEvent = new Event("doc1", "run1", "", Event.Type.FINISH);

    publisher.handleEvent(createEvent);
    assertEquals(1, publisher.numPending());
    publisher.handleEvent(finishEvent);
    assertEquals(0, publisher.numPending());

    // child documents that we're notified about via a CREATE event aren't considered to be published by the publisher
    // although they are included in the count of pending documents
    assertEquals(0, publisher.numPublished());
  }

  @Test
  public void testPublishAndFinish() throws Exception {

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Document doc = Document.create("doc1");
    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.publish(doc);
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());

    assertEquals(1, messenger.getDocsSentForProcessing().size());
    assertEquals(doc, messenger.getDocsSentForProcessing().get(0));

    Event finishEvent = new Event(doc.getId(), "run1", "", Event.Type.FINISH);
    publisher.handleEvent(finishEvent);
    assertEquals(1, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.handleEvent(finishEvent); // redundant finish event
    assertEquals(1, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.handleEvent(finishEvent); // another redundant finish event
    assertEquals(1, publisher.numPublished());
    assertEquals(0, publisher.numPending());
  }

  @Test
  public void testPublishAndError() throws Exception {

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Document doc = Document.create("doc1");
    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.publish(doc);
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());

    Event failEvent = new Event(doc.getId(), "run1", "", Event.Type.FAIL);
    publisher.handleEvent(failEvent);
    assertEquals(1, publisher.numPublished());
    assertEquals(0, publisher.numPending());
  }

  @Test
  public void testRedundantDocIDs() throws Exception {

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Document doc = Document.create("doc1");
    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.publish(doc);
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());

    publisher.publish(Document.create("doc1"));
    assertEquals(2, publisher.numPublished());
    assertEquals(2, publisher.numPending());

    Event finishEvent = new Event(doc.getId(), "run1", "", Event.Type.FINISH);
    publisher.handleEvent(finishEvent);
    assertEquals(2, publisher.numPublished());
    assertEquals(1, publisher.numPending());

    publisher.handleEvent(finishEvent); // redundant finish event
    assertEquals(2, publisher.numPublished());
    assertEquals(0, publisher.numPending());
  }

  @Test
  public void testSucceededFailedCreatedCounts() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());
    assertEquals(0, publisher.numSucceeded());
    assertEquals(0, publisher.numFailed());
    assertEquals(0, publisher.numCreated());

    publisher.publish(Document.create("doc1"));
    publisher.publish(Document.create("doc2"));
    publisher.publish(Document.create("doc3"));
    publisher.publish(Document.create("doc4"));
    publisher.handleEvent(new Event("doc3-child1", "run1", "", Event.Type.CREATE));
    publisher.handleEvent(new Event("doc3-child2", "run1", "", Event.Type.CREATE));

    publisher.handleEvent(new Event("doc1", "run1", "", Event.Type.FINISH));
    publisher.handleEvent(new Event("doc2", "run1", "", Event.Type.FAIL));
    publisher.handleEvent(new Event("doc3-child1", "run1", "", Event.Type.FAIL));
    publisher.handleEvent(new Event("doc3-child2", "run1", "", Event.Type.FINISH));
    publisher.handleEvent(new Event("doc3", "run1", "", Event.Type.FINISH));
    publisher.handleEvent(new Event("doc4", "run1", "", Event.Type.DROP));

    assertEquals(4, publisher.numPublished());
    assertEquals(0, publisher.numPending());
    assertEquals(3, publisher.numSucceeded());
    assertEquals(2, publisher.numFailed());
    assertEquals(2, publisher.numCreated());
    assertEquals(1, publisher.numDropped());
  }

  @Test
  public void testRedundantFinishEvents() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    assertEquals(0, publisher.numSucceeded());

    Event event = new Event("doc1", "run1", "", Event.Type.FINISH);

    publisher.handleEvent(event);
    publisher.handleEvent(event);
    publisher.handleEvent(event);

    // unfortunately redundant events will increase the numSucceeded count because
    // the publisher deliberately forgets about the documents except the "pending" ones
    assertEquals(3, publisher.numSucceeded());
  }

  @Test
  public void testBlockOnQueueCapacity() throws Exception {
    Config config = ConfigFactory.parseString("publisher {queueCapacity: 5}");
    LocalMessenger messenger = new LocalMessenger(config);
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Thread publisherThread = new Thread() {
      public void run() {
        for (int i = 0; i < 100; i++) {
          try {
            publisher.publish(Document.create("doc" + i));
          } catch (Exception e) {
            return;
          }
        }
      }
    };

    // make sure that after running for a half-second, the publisher is able to
    // publish up to the queue capacity but not further
    publisherThread.start();
    Thread.sleep(500);
    publisherThread.interrupt();
    publisherThread.join();
    assertEquals(5, publisher.numPublished());

    // create space in the queue and make sure the publisher is able to publish another document
    messenger.pollDocToProcess();
    publisher.publish(Document.create("doc6"));
    assertEquals(6, publisher.numPublished());
  }

  @Test
  public void testCollapse() throws Exception {

    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher =
        new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1", "", true);

    Document doc1 = Document.create("before");

    Document doc2 = Document.create("collapseMe");
    doc2.setField("field1", "val1");

    Document doc3 = Document.create("collapseMe");
    doc3.setField("field1", "val2");

    Document doc4 = Document.create("after");

    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());
    assertEquals(0, messenger.getDocsSentForProcessing().size());
    publisher.publish(doc1);
    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());
    assertEquals(0, messenger.getDocsSentForProcessing().size());
    publisher.publish(doc2);
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());
    assertEquals(1, messenger.getDocsSentForProcessing().size());
    assertEquals("before", messenger.getDocsSentForProcessing().get(0).getId());
    publisher.publish(doc3);
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());
    assertEquals(1, messenger.getDocsSentForProcessing().size());
    publisher.publish(doc4);
    assertEquals(2, publisher.numPublished());
    assertEquals(2, publisher.numPending());
    assertEquals(2, messenger.getDocsSentForProcessing().size());
    assertEquals("collapseMe", messenger.getDocsSentForProcessing().get(1).getId());
    publisher.flush();
    assertEquals(3, publisher.numPublished());
    assertEquals(3, publisher.numPending());
    assertEquals(3, messenger.getDocsSentForProcessing().size());
    assertEquals("after", messenger.getDocsSentForProcessing().get(2).getId());

    Document collapsedDoc = messenger.getDocsSentForProcessing().get(1);
    assertEquals("run1", collapsedDoc.getRunId());
    assertEquals(Arrays.asList(new String[]{"val1", "val2"}), collapsedDoc.getStringList("field1"));
  }

  @Test
  public void testBlockOnMaxPendingDocs() throws Exception {
    Config config = ConfigFactory.parseString("publisher {maxPendingDocs: 3}");
    LocalMessenger messenger = new LocalMessenger(config);
    PublisherImpl publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // test that publish() is "fast" when maxPendingDocs is not exceeded
    assertTimeout(ofMillis(500), () -> {
      publisher.publish(Document.create("doc1"));
      publisher.publish(Document.create("doc2"));
      publisher.publish(Document.create("doc3"));
      publisher.preClose();
    });

    assertEquals(3, publisher.numPending());

    // test that publish() blocks when maxPendingDocs is exceeded
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
      try {
        publisher.publish(Document.create("doc4"));
      } catch (Exception e) {
        fail();
      }
    }, executor);

    try {
      // getting the result of the CompletableFuture should timeout because publisher.publish() should block indefinitely
      // since maxPendingDocs is exceeded
      assertThrows(TimeoutException.class, () -> {
        future.get(500, TimeUnit.MILLISECONDS);
      });
    } finally {
      executor.shutdownNow();
    }

    assertEquals(3, publisher.numPending());
    publisher.handleEvent(new Event("doc1", "run1", "success", Event.Type.FINISH));
    assertEquals(2, publisher.numPending());

    // test that publish() is "fast" again now that maxPendingDocs is no longer exceeded
    assertTimeout(ofMillis(500), () -> {
      publisher.publish(Document.create("doc4"));
    });

    publisher.close();
  }

  @Test
  public void testBlockOnMaxPendingDocsConcurrent() throws Exception {
    Config config = ConfigFactory.parseString("publisher {maxPendingDocs: 3}");
    LocalMessenger messenger = new LocalMessenger(config);
    PublisherImpl publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // this thread should publish the 3 docs and block when it tries to publish the fourth
    // until the number of pending docs is brought back underneath maxPendingDocs via a call to publisher.handleEvent()
    Thread publisherThread = new Thread(() -> {
        try {
          publisher.publish(Document.create("doc1"));
          publisher.publish(Document.create("doc2"));
          publisher.publish(Document.create("doc3"));
          publisher.publish(Document.create("doc4"));
          publisher.preClose();
        } catch (Exception e) {
          fail();
        }
    });

    publisherThread.start();
    Thread.sleep(500);
    assertEquals(3, publisher.numPublished());

    // after we handle a FINISH event, the number of pending docs should fall below maxPendingDocs, which should cause
    // the publication of the 4th document to unblock, allowing publisherThread to complete
    publisher.handleEvent(new Event("doc1", "run1", "success", Event.Type.FINISH));
    publisherThread.join();
    publisher.close();
    assertEquals(4, publisher.numPublished());
  }

  /**
   * Attempt to establish that PublisherImpl.publish() is thread-safe.
   * This test does not consider all multi-threading issues that could arise, but it does confirm the following:
   *
   * If we have 10 threads calling publish() concurrently on the same PublisherImpl instance, no
   * unexpected exceptions are thrown, and the count of published documents is accurately maintained
   */
  @Test
  public void testPublishThreadSafety() throws Exception {
    Config config = ConfigFactory.parseString("publisher {queueCapacity: 100000}");
    LocalMessenger messenger = new LocalMessenger(config);
    PublisherImpl publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // create, start, and join 10 threads, each of which publishes 10K documents
    List<Thread> threads = new ArrayList();
    for (int i = 0; i < 10; i++) {
      final int i2 = i;
      threads.add(new Thread(() -> {
        try {
          for (int j = 0; j < 10000; j++) {
            publisher.publish(Document.create(i2 + "_" + j));
          }
          publisher.preClose();
        } catch (Exception e) {
          e.printStackTrace();
          fail();
        }
      }));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    publisher.close();

    // make sure that the number of published documents is accurately reported as 100K (via 10 threads publishing 10K each)
    // an accurate number here builds confidence that the internal counter of published documents is mantained in a thread-safe way
    assertEquals(100000, publisher.numPublished());

    // the number of pending documents should also be 100K because none have been processed;
    // an accurate number here builds confidence that the internal Bag of pending docIds has not been corrupted
    assertEquals(100000, publisher.numPending());
  }

  @Test
  public void testMultiThreadedPublishingWithThrottlingEnabled() throws Exception {

    // enable publisher throttling via maxPendingDocs=100
    Config config = ConfigFactory.parseString("publisher {queueCapacity: 100000, maxPendingDocs: 100}");
    LocalMessenger messenger = new LocalMessenger(config);
    PublisherImpl publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // publishing threads will add ids of published docs to this queue; event handling thread will drain it
    final LinkedBlockingQueue<String> publishedIds = new LinkedBlockingQueue<>();

    // keep a list of all threads we'll start in this test;
    // the event handling thread will be added at position 0, followed by publishing threads
    List<Thread> threads = new ArrayList();

    // the publishing threads will publish exactly 10000 docs combined and add their ids to the publishedIds queue
    // this event handling thread tries to consume 10000 ids from the publishedIds queue and stops when that number is reached
    Thread eventHandlingThread = new Thread(() -> {
      int numEventsHandled = 0;
      while (true) {
        String id;
        try {
          id = publishedIds.take();
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        }
        publisher.handleEvent(new Event(id, "run1", "message", Type.FINISH));
        numEventsHandled++;
        if (numEventsHandled == 10000) {
          return;
        }
      }
    });

    threads.add(eventHandlingThread);

    // now add 10 publishing threads to our list of threads
    // each publishing thread will publish 1000 documents
    // doc IDs will be formated as <thread ID from 0 to 9>_<doc sequence from 0 to 999>
    for (int i = 0; i < 10; i++) {
      final int i2 = i;
      threads.add(new Thread(() -> {
        for (int j = 0; j < 1000; j++) {
          String id = i2 + "_" + j;
          try {
            publisher.publish(Document.create(id));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          publishedIds.add(id);

          // periodically check that the number of pending docs is not reported as much higher than declared max of 100
          // if we find too many pending docs, we interrupt all the threads (to prevent the test from hanging) and
          // throw an exception; note that Junit assertions inside threads don't work as expected so we don't use one here;
          // also note that publisher.maxPendingDocs is a non-strict max and can be exceeded by
          // the number of publishing threads - 1 (here, that's the declared setting of 100 plus 10 threads - 1 = 109)
          if (publisher.numPending() > 109) {
            for (Thread thread: threads) {
              thread.interrupt();
            }
            throw new RuntimeException();
          }
        }

        try {
          publisher.preClose();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }));
    }

    // start all the threads beginning with the event handling thread and then the 10 publishing threads
    for (Thread thread : threads) {
      thread.start();
    }

    // join all the threads
    for (Thread thread : threads) {
      thread.join();
    }

    publisher.close();

    // the publisher should report 10000 published documents with 0 pending
    assertEquals(10000, publisher.numPublished());
    assertEquals(0, publisher.numPending());
  }

  @Test
  public void testMaxPendingDocsBelowZero() throws Exception {
    Config config = ConfigFactory.parseString("publisher {maxPendingDocs: 3}");
    LocalMessenger messenger = new LocalMessenger(config);
    PublisherImpl publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    assertEquals(3, publisher.getMaxPendingDocs().intValue());

    config = ConfigFactory.parseString("publisher {maxPendingDocs: -1}");
    messenger = new LocalMessenger(config);
    publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    assertEquals(null, publisher.getMaxPendingDocs());
  }

  /**
   * Helper method to wait for all Threads to be in the WAITING state before proceeding. This is necessary to ensure consistency in
   * the testPauseAndResume and ensure that all threads have finished processing before we record any values about the number of
   * docs processed or pending.
   * @param threads
   */
  private void waitForAllThreads(List<Thread> threads) {
    boolean allWaiting = false;
    while (!allWaiting) {
      allWaiting = threads.stream().allMatch(t -> t.getState() == State.WAITING);
    }
  }

  @Test
  public void testPauseAndResume() throws Exception {
    Config config = ConfigFactory.parseString("publisher {queueCapacity: 100000}");
    LocalMessenger messenger = new LocalMessenger(config);
    PublisherImpl publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    // publishing threads will add ids of published docs to this queue; event handling thread will drain it
    final LinkedBlockingQueue<String> publishedIds = new LinkedBlockingQueue<>();

    // publishing threads will use this to know when to stop
    AtomicBoolean stop = new AtomicBoolean(false);

    // each thread attempts to publish up to 10000 docs with a pause in between; stopping when told to
    List<Thread> threads = new ArrayList();
    for (int i = 0; i < 10; i++) {
      final int i2 = i;
      threads.add(new Thread(() -> {
        try {
          for (int j = 0; j < 10000; j++) {
            publisher.publish(Document.create(i2 + "_" + j));
            publishedIds.add(i2 + "_" + j);
            Thread.sleep(50);
            if (stop.get()) {
              return;
            }
          }
          publisher.preClose();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }));
    }

    assertEquals(0, publisher.numPublished());

    for (Thread thread : threads) {
      thread.start();
    }

    Thread.sleep(300);

    // Pause and wait for all threads to finish processing before asserting the values are as expected
    publisher.pause();
    waitForAllThreads(threads);


    // after starting the threads, waiting a while, and pausing the publisher,
    // check that at least one doc has been published by now
    long numPublishedAtTime1 = publisher.numPublished();
    assertTrue(numPublishedAtTime1 > 0);
    assertEquals(numPublishedAtTime1, publisher.numPending());

    // handle success events for all docs that have been published so far
    publishedIds.forEach(id -> publisher.handleEvent(new Event(id, "run1", "message", Type.FINISH)));
    // there should be no pending docs after we've handled events for all published docs
    assertEquals(0, publisher.numPending());

    Thread.sleep(200);

    // after waiting a while longer, we should find that no more docs have been published (because the publisher is still paused)
    assertEquals(numPublishedAtTime1, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.resume();
    Thread.sleep(200);

    // Pause and wait for all threads to finish processing before asserting the values are as expected
    publisher.pause();
    waitForAllThreads(threads);

    // having resumed the publisher and waited a bit longer, we should find that more docs have been published
    long numPublishedAtTime2 = publisher.numPublished();
    assertTrue(numPublishedAtTime2 > numPublishedAtTime1);
    assertEquals(numPublishedAtTime2 - numPublishedAtTime1, publisher.numPending());
    publisher.resume();

    // now we try quickly toggling between pause and resume just to make sure nothing goes wrong
    publisher.pause();
    publisher.resume();
    publisher.pause();
    publisher.resume();
    publisher.pause();
    waitForAllThreads(threads);

    long numPublishedAtTime3 = publisher.numPublished();
    Thread.sleep(200);

    // no more docs should be published while we're paused
    long numPublishedAtTime4 = publisher.numPublished();
    assertEquals(numPublishedAtTime3, numPublishedAtTime4);

    publisher.resume();
    Thread.sleep(200);

    // having resumed the publisher and waited a bit longer, we should find that still more docs have been published
    // Since we are only checking that more documents have been published and not looking for an exact count, we do not need to pause
    // or wait for the threads here
    long numPublishedAtTime5 = publisher.numPublished();
    assertTrue(numPublishedAtTime5 > numPublishedAtTime4);

    // stop the threads, wait for them to terminate, and close the publisher
    stop.set(true);

    for (Thread thread : threads) {
      thread.join();
    }

    publisher.close();
  }

  @Test
  public void testPauseAndResumeBadOrdering() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher =
        new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1", "", true);
    assertThrows(IllegalStateException.class, () -> publisher.resume()); // resume() called when not paused
    publisher.pause();
    assertThrows(IllegalStateException.class, () -> publisher.pause()); // pause() called when already paused
    publisher.resume();
    assertThrows(IllegalStateException.class, () -> publisher.resume()); // resume() called when not paused
  }

}
