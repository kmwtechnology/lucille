package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.LocalMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class PublisherImplTest {

  @Test
  public void testOutOfOrderCreateEvents() throws Exception {

    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

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

    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

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

    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

    Document doc = new Document("doc1");
    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.publish(doc);
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());

    assertEquals(1, manager.getSavedDocumentsSentForProcessing().size());
    assertEquals(doc, manager.getSavedDocumentsSentForProcessing().get(0));

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

    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

    Document doc = new Document("doc1");
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

    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

    Document doc = new Document("doc1");
    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());

    publisher.publish(doc);
    assertEquals(1, publisher.numPublished());
    assertEquals(1, publisher.numPending());

    publisher.publish(new Document("doc1"));
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
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

    assertEquals(0, publisher.numPublished());
    assertEquals(0, publisher.numPending());
    assertEquals(0, publisher.numSucceeded());
    assertEquals(0, publisher.numFailed());
    assertEquals(0, publisher.numCreated());

    publisher.publish(new Document("doc1"));
    publisher.publish(new Document("doc2"));
    publisher.publish(new Document("doc3"));
    publisher.handleEvent(new Event("doc4", "run1", "", Event.Type.CREATE));
    publisher.handleEvent(new Event("doc5", "run1", "", Event.Type.CREATE));

    publisher.handleEvent(new Event("doc1", "run1", "", Event.Type.FINISH));
    publisher.handleEvent(new Event("doc2", "run1", "", Event.Type.FAIL));
    publisher.handleEvent(new Event("doc3", "run1", "", Event.Type.FINISH));
    publisher.handleEvent(new Event("doc4", "run1", "", Event.Type.FAIL));
    publisher.handleEvent(new Event("doc5", "run1", "", Event.Type.FINISH));

    assertEquals(3, publisher.numPublished());
    assertEquals(0, publisher.numPending());
    assertEquals(3, publisher.numSucceeded());
    assertEquals(2, publisher.numFailed());
    assertEquals(2, publisher.numCreated());
  }

  @Test
  public void testRedundantFinishEvents() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

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
    LocalMessageManager manager = new LocalMessageManager(config);
    PublisherImpl publisher = new PublisherImpl(manager, "run1", "pipeline1");

    Thread publisherThread = new Thread() {
      public void run() {
        for (int i=0; i<100; i++) {
          try {
            publisher.publish(new Document("doc" + i));
          } catch (Exception e) {
            return;
          }
        }
      }
    };

    // make sure that after running for a second, the publisher is able to
    // publish up to the queue capacity but not further
    publisherThread.start();
    Thread.sleep(1000);
    publisherThread.interrupt();
    publisherThread.join();
    assertEquals(5, publisher.numPublished());

    // create space in the queue and make sure the publisher is able to publish another document
    manager.pollDocToProcess();
    publisher.publish(new Document("doc6"));
    assertEquals(6, publisher.numPublished());
  }
}