package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Runner;
import com.kmwllc.lucille.message.MessageManagerFactory;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class RunnerTest {

  @Test
  public void testRunnerWithSingleDoc() throws Exception {

    // statically place the MessageManagerFactory in "local mode" so that it will return references to
    // a local message manager that does not connect to Kafka but instead stores all messages in in-memory queues
    MessageManagerFactory.getInstance().setLocalMode();

    // instantiate a Runner using a dedicated Config for this test; run it
    Config config = ConfigFactory.load("runner-application.conf");
    Runner runner = new Runner(config);
    runner.runConnectors(true);

    // obtain the singleton instance of the message manager that the factory returns when in local mode
    // this is a persisting message manager that saves all message traffic so we can review it
    // even after various simulated "topics" have been fully consumed/cleared
    PersistingLocalMessageManager manager = PersistingLocalMessageManager.getInstance();

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = manager.getSavedDocumentsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());

    // confirm doc 1 was processed by the pipeline and sent to the destination topic
    List<Document> docsCompleted = manager.getSavedCompletedDocuments();
    assertEquals(1, docsCompleted.size());
    assertEquals("1", docsCompleted.get(0).getId());

    // confirm doc 1 was sent to solr
    List<Document> docsSentToSolr = manager.getSavedDocsSentToSolr();
    assertEquals(1, docsSentToSolr.size());
    assertEquals("1", docsSentToSolr.get(0).getId());

    // confirm that an INDEX event was sent for doc 1
    List<Event> events = manager.getSavedEvents();
    assertEquals(1, events.size());
    assertEquals("1", events.get(0).getDocumentId());
    assertEquals(runner.getRunId(), events.get(0).getRunId());
    assertEquals(Event.Type.INDEX, events.get(0).getType());

    // confirm that topics are empty
    assertFalse(manager.hasEvents(runner.getRunId()));
    assertNull(manager.pollCompleted());
    assertNull(manager.pollDocToProcess());
    assertNull(manager.pollEvent());

  }

}
