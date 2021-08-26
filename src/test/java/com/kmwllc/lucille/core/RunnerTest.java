package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class RunnerTest {

  @Test
  public void testRunnerWithSingleDoc() throws Exception {

    // run connectors and pipeline; acquire a persisting message manager that allows
    // for reviewing saved message traffic
    PersistingLocalMessageManager manager = RunUtils.runLocal("runner-application.conf");

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

    // confirm that an INDEX event was sent for doc 1 and is stamped with the proper run ID
    List<Event> events = manager.getSavedEvents();
    assertEquals(1, events.size());
    assertEquals("1", events.get(0).getDocumentId());
    assertNotNull(manager.getRunId());
    assertEquals(manager.getRunId(), events.get(0).getRunId());
    assertEquals(Event.Type.INDEX, events.get(0).getType());

    // confirm that topics are empty
    assertFalse(manager.hasEvents());
    assertNull(manager.pollCompleted());
    assertNull(manager.pollDocToProcess());
    assertNull(manager.pollEvent());

  }

}
