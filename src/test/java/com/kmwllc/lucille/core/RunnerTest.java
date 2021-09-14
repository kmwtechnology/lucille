package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class RunnerTest {

  /**
   * Test an end-to-end run with a single connector that generates 1 document, and a no-op pipeline
   */
  @Test
  public void testRunnerWithSingleDoc() throws Exception {

    // run connectors and pipeline; acquire a persisting message manager that allows
    // for reviewing saved message traffic
    PersistingLocalMessageManager manager =
      Runner.runInTestMode("RunnerTest/singleDoc.conf").get("connector1");

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = manager.getSavedDocumentsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());

    // confirm doc 1 was processed by the pipeline and sent to the destination topic
    List<Document> docsCompleted = manager.getSavedCompletedDocuments();
    assertEquals(1, docsCompleted.size());
    assertEquals("1", docsCompleted.get(0).getId());

    // confirm doc 1 was sent to solr
    //List<Document> docsSentToSolr = manager.getSavedDocsSentToSolr();
    //assertEquals(1, docsSentToSolr.size());
    //assertEquals("1", docsSentToSolr.get(0).getId());

    // confirm that a terminal event was sent for doc 1 and is stamped with the proper run ID
    List<Event> events = manager.getSavedEvents();
    assertEquals(1, events.size());
    assertEquals("1", events.get(0).getDocumentId());
    assertNotNull(manager.getRunId());
    assertEquals(manager.getRunId(), events.get(0).getRunId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());

    // confirm that topics are empty
    assertFalse(manager.hasEvents());
    assertNull(manager.pollCompleted());
    assertNull(manager.pollDocToProcess());
    assertNull(manager.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document, and a pipeline
   * that throws an exception
   */
  @Test
  public void testRunnerWithSingleFailingDoc() throws Exception {

    // run connectors and pipeline; acquire a persisting message manager that allows
    // for reviewing saved message traffic
    PersistingLocalMessageManager manager =
      Runner.runInTestMode("RunnerTest/singleDocFailure.conf").get("connector1");

    // confirm doc 1 sent for processing but no docs completed or sent to solr
    List<Document> docsSentForProcessing = manager.getSavedDocumentsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());
    assertEquals(0, manager.getSavedCompletedDocuments().size());
    //assertEquals(0, manager.getSavedDocsSentToSolr().size());

    // confirm that an ERROR event was sent for doc 1 and is stamped with the proper run ID
    List<Event> events = manager.getSavedEvents();
    assertEquals(1, events.size());
    assertEquals("1", events.get(0).getDocumentId());
    assertNotNull(manager.getRunId());
    assertEquals(manager.getRunId(), events.get(0).getRunId());
    assertEquals(Event.Type.FAIL, events.get(0).getType());

    // confirm that topics are empty
    assertFalse(manager.hasEvents());
    assertNull(manager.pollCompleted());
    assertNull(manager.pollDocToProcess());
    assertNull(manager.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document, an indexer
   * with a batch timeout of 10 seconds, and a runner with timeout of .1 second
   */
  @Test
  public void testRunnerTimeout() throws Exception {

    Config config = ConfigFactory.load("RunnerTest/singleDocTimeout.conf");
    Runner runner = new Runner(config);
    Connector connector = Connector.fromConfig(config).get(0);
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(manager, runner.getRunId(), connector.getPipelineName());
    Instant start = Instant.now();
    boolean result = runner.runConnector(connector, publisher);
    Instant end = Instant.now();

    assertFalse(result);
    assertTrue(ChronoUnit.SECONDS.between(start, end) < 10);
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document, and a pipeline that
   * generates one child document for every incoming document
   */
  @Test
  public void testChildHandling() throws Exception {

    PersistingLocalMessageManager manager =
      Runner.runInTestMode("RunnerTest/singleDocSingleChild.conf").get("connector1");;

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = manager.getSavedDocumentsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());

    // confirm doc 1 and its child were processed by the pipeline and sent to the destination topic
    List<Document> docsCompleted = manager.getSavedCompletedDocuments();
    assertEquals(2, docsCompleted.size());

    // confirm doc 1 and its child were sent to solr
    //List<Document> docsSentToSolr = manager.getSavedDocsSentToSolr();
    //assertEquals(2, docsSentToSolr.size());

    // confirm that a CREATE event was sent for doc 1's child; followed by terminal events for both docs
    List<Event> events = manager.getSavedEvents();
    assertEquals(3, events.size());
    assertEquals(Event.Type.CREATE, events.get(0).getType());
    assertEquals(Event.Type.FINISH, events.get(1).getType());
    assertEquals(Event.Type.FINISH, events.get(2).getType());

    assertNotNull(manager.getRunId());
    assertEquals(manager.getRunId(), events.get(1).getRunId());
    assertEquals(manager.getRunId(), events.get(2).getRunId());

    // confirm that topics are empty
    assertFalse(manager.hasEvents());
    assertNull(manager.pollCompleted());
    assertNull(manager.pollDocToProcess());
    assertNull(manager.pollEvent());
  }

  /**
   * Test an end-to-end run with two connectors that each generate a single document, and a no-op pipeline
   */
  @Test
  public void testTwoConnectors() throws Exception {

    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/twoConnectors.conf");

    assertEquals(2, map.size());

    PersistingLocalMessageManager manager1 = map.get("connector1");
    PersistingLocalMessageManager manager2 = map.get("connector2");

    // confirm doc 1 sent for processing (via first connector) and doc 2 sent (via second connector)
    assertEquals(1, manager1.getSavedDocumentsSentForProcessing().size());
    assertEquals(1, manager2.getSavedDocumentsSentForProcessing().size());
    assertEquals("1", manager1.getSavedDocumentsSentForProcessing().get(0).getId());
    assertEquals("2", manager2.getSavedDocumentsSentForProcessing().get(0).getId());

    // confirm both docs were processed and sent to the destination topic
    assertEquals(1, manager1.getSavedCompletedDocuments().size());
    assertEquals(1, manager2.getSavedCompletedDocuments().size());
    assertEquals("1", manager1.getSavedCompletedDocuments().get(0).getId());
    assertEquals("2", manager2.getSavedCompletedDocuments().get(0).getId());

    // confirm both docs were sent to solr
    //assertEquals(1, manager1.getSavedDocsSentToSolr().size());
    //assertEquals(1, manager2.getSavedDocsSentToSolr().size());
    //assertEquals("1", manager1.getSavedDocsSentToSolr().get(0).getId());
    //assertEquals("2", manager2.getSavedDocsSentToSolr().get(0).getId());

    // confirm that terminal events were sent for both docs
    assertEquals(1, manager1.getSavedEvents().size());
    assertEquals(1, manager2.getSavedEvents().size());
    assertEquals(Event.Type.FINISH, manager1.getSavedEvents().get(0).getType());
    assertEquals(Event.Type.FINISH, manager2.getSavedEvents().get(0).getType());
    assertEquals("1", manager1.getSavedEvents().get(0).getDocumentId());
    assertEquals("2", manager2.getSavedEvents().get(0).getDocumentId());

    assertNotNull(manager1.getRunId());
    assertEquals(manager1.getRunId(), manager2.getRunId());
    assertEquals(manager1.getRunId(), manager1.getSavedEvents().get(0).getRunId());
    assertEquals(manager2.getRunId(), manager2.getSavedEvents().get(0).getRunId());

    // confirm that topics are empty
    assertFalse(manager1.hasEvents());
    assertNull(manager1.pollCompleted());
    assertNull(manager1.pollDocToProcess());
    assertNull(manager1.pollEvent());

    assertFalse(manager2.hasEvents());
    assertNull(manager2.pollCompleted());
    assertNull(manager2.pollDocToProcess());
    assertNull(manager2.pollEvent());
  }

  /**
   * Attempt to run three connectors, where the second connector throws an exception.
   * Confirm that the third connector is not run.
   */
  @Test
  public void testThreeConnectorsWithFailure() throws Exception {
    Map<String, PersistingLocalMessageManager> map =
      Runner.runInTestMode("RunnerTest/threeConnectorsWithFailure.conf");
    assertEquals(2, map.size());
  }

}
