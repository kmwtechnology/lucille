package com.kmwllc.lucille.core;

import com.apptasticsoftware.rssreader.RssReader;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.connector.NoOpConnector;
import com.kmwllc.lucille.connector.PostCompletionCSVConnector;
import com.kmwllc.lucille.connector.RSSConnector;
import com.kmwllc.lucille.connector.RunSummaryMessageConnector;
import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.stage.StartStopCaptureStage;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.util.stream.Stream;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class RunnerTest {
  @Test
  public void testRunnerWithNoDocs() throws Exception {
    // we should be able to run a connector that generates no documents
    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/noDocs.conf");
    assertEquals(1, map.size());

    TestMessenger messenger = map.get("connector1");

    assertEquals(0, messenger.getDocsSentForProcessing().size());
    assertEquals(0, messenger.getDocsSentForIndexing().size());
    assertEquals(0, messenger.getDocsSentForIndexing().size());
    assertEquals(0, messenger.getSentEvents().size());
    assertNull(messenger.pollDocToIndex());
    assertNull(messenger.pollDocToProcess());
    assertNull(messenger.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document, and a no-op pipeline
   */
  @Test
  public void testRunnerWithSingleDoc() throws Exception {

    // run connectors and pipeline; acquire a persisting message messenger that allows
    // for reviewing saved message traffic
    TestMessenger messenger =
        Runner.runInTestMode("RunnerTest/singleDoc.conf").get("connector1");

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());

    // confirm doc 1 was processed by the pipeline and sent to the destination topic
    List<Document> docsCompleted = messenger.getDocsSentForIndexing();
    assertEquals(1, docsCompleted.size());
    assertEquals("1", docsCompleted.get(0).getId());

    // confirm doc 1 was sent to solr
    //List<Document> docsSentToSolr = messenger.getSavedDocsSentToSolr();
    //assertEquals(1, docsSentToSolr.size());
    //assertEquals("1", docsSentToSolr.get(0).getId());

    // confirm that a terminal event was sent for doc 1 and is stamped with the proper run ID
    List<Event> events = messenger.getSentEvents();
    assertEquals(1, events.size());
    assertEquals("1", events.get(0).getDocumentId());
    assertNotNull(messenger.getRunId());
    assertEquals(messenger.getRunId(), events.get(0).getRunId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());

    // confirm that topics are empty
    assertNull(messenger.pollDocToIndex());
    assertNull(messenger.pollDocToProcess());
    assertNull(messenger.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 3 documents, where
   * the second document causes a stage exception but the first and third should
   * finish without out errors
   */
  @Test
  public void testRunnerWithFailingDoc() throws Exception {

    // run connectors and pipeline; acquire a persisting message messenger that allows
    // for reviewing saved message traffic
    TestMessenger messenger =
        Runner.runInTestMode("RunnerTest/threeDocsOneFailure.conf").get("connector1");

    // confirm doc 3 docs sent for processing but only 2 docs completed
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(3, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());
    assertEquals("2", docsSentForProcessing.get(1).getId());
    assertEquals("3", docsSentForProcessing.get(2).getId());

    assertEquals(2, messenger.getDocsSentForIndexing().size());
    assertEquals("1", messenger.getDocsSentForIndexing().get(0).getId());
    assertEquals("3", messenger.getDocsSentForIndexing().get(1).getId());

    // confirm that the proper events were sent for all three documents
    List<Event> events = messenger.getSentEvents();
    assertNotNull(messenger.getRunId());
    assertEquals(3, events.size());

    events.sort(Comparator.comparing(Event::getDocumentId));
    assertEquals("1", events.get(0).getDocumentId());
    assertEquals(messenger.getRunId(), events.get(0).getRunId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());

    assertEquals("2", events.get(1).getDocumentId());
    assertEquals(messenger.getRunId(), events.get(1).getRunId());
    assertEquals(Event.Type.FAIL, events.get(1).getType());

    assertEquals("3", events.get(2).getDocumentId());
    assertEquals(messenger.getRunId(), events.get(2).getRunId());
    assertEquals(Event.Type.FINISH, events.get(2).getType());

    // confirm that topics are empty
    assertNull(messenger.pollDocToIndex());
    assertNull(messenger.pollDocToProcess());
    assertNull(messenger.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document, an indexer
   * with a batch timeout of 10 seconds, and a runner with connectorTimeout of .1 second
   */
  @Test
  public void testConnectorTimeout() throws Exception {

    Config config = ConfigFactory.load("RunnerTest/singleDocTimeout.conf");
    Connector connector = Connector.fromConfig(config).get(0);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", connector.getPipelineName());
    Instant start = Instant.now();
    boolean result = Runner.runConnector(config, "run1", connector, publisher).getStatus();
    Instant end = Instant.now();

    assertFalse(result);
    assertTrue(ChronoUnit.SECONDS.between(start, end) < 10);
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document.
   * Confirm that this run fails when the Runner's connectorTimeout is set to 1 millisecond
   * but that it succeeds when the timeout is set to -1 milliseconds to disable it.
   */
  @Test
  public void testDisabledConnectorTimeout() throws Exception {
    Config verySmallTimeout = ConfigFactory.load("RunnerTest/singleDoc.conf").withValue("runner.connectorTimeout", ConfigValueFactory.fromAnyRef(1));
    RunResult result = Runner.run(verySmallTimeout, Runner.RunType.TEST);
    assertFalse(result.getStatus());

    Config disabledTimeout = ConfigFactory.load("RunnerTest/singleDoc.conf").withValue("runner.connectorTimeout", ConfigValueFactory.fromAnyRef(-1));
    RunResult result2 = Runner.run(disabledTimeout, Runner.RunType.TEST);
    assertTrue(result2.getStatus());
  }
  
  /**
   * Test an end-to-end run with a single connector that generates 1 document, and a pipeline that
   * generates one child document for every incoming document
   */
  @Test
  public void testChildHandling() throws Exception {

    TestMessenger messenger =
        Runner.runInTestMode("RunnerTest/singleDocSingleChild.conf").get("connector1");

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());

    // confirm doc 1 and its child were processed by the pipeline and sent to the destination topic
    List<Document> docsCompleted = messenger.getDocsSentForIndexing();
    assertEquals(2, docsCompleted.size());

    // confirm doc 1 and its child were sent to solr
    //List<Document> docsSentToSolr = messenger.getSavedDocsSentToSolr();
    //assertEquals(2, docsSentToSolr.size());

    // confirm that a CREATE event was sent for doc 1's child; followed by terminal events for both docs
    List<Event> events = messenger.getSentEvents();
    assertEquals(3, events.size());
    assertEquals(Event.Type.CREATE, events.get(0).getType());
    assertEquals(Event.Type.FINISH, events.get(1).getType());
    assertEquals(Event.Type.FINISH, events.get(2).getType());

    assertNotNull(messenger.getRunId());
    assertEquals(messenger.getRunId(), events.get(1).getRunId());
    assertEquals(messenger.getRunId(), events.get(2).getRunId());

    // confirm that topics are empty
    assertNull(messenger.pollDocToIndex());
    assertNull(messenger.pollDocToProcess());
    assertNull(messenger.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document, and a pipeline that
   * generates two children for every incoming document, dropping the document itself
   */
  @Test
  public void testDropDocument() throws Exception {

    TestMessenger messenger =
        Runner.runInTestMode("RunnerTest/twoChildrenDropParent.conf").get("connector1");
    ;

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    Document parent = docsSentForProcessing.get(0);
    assertEquals("1", parent.getId());
    assertTrue(parent.has("before"));
    assertFalse(parent.has("after1"));
    assertFalse(parent.has("after2"));

    // confirm the two children were processed by the pipeline and sent to the destination topic
    List<Document> docsCompleted = messenger.getDocsSentForIndexing();
    assertEquals(2, docsCompleted.size());
    Document child1 = docsCompleted.get(0);
    Document child2 = docsCompleted.get(1);
    assertEquals("1_child1", child1.getId());
    assertFalse(child1.has("before"));
    assertTrue(child1.has("after1"));
    assertTrue(child1.has("after2"));
    assertEquals("1_child2", child2.getId());
    assertFalse(child2.has("before"));
    assertTrue(child2.has("after1"));
    assertTrue(child2.has("after2"));

    // confirm that a CREATE event was sent for the doc1's children
    // confirm that a DROP event was sent for doc1 no FINISH event was sent for it
    List<Event> events = messenger.getSentEvents();
    assertEquals(5, events.size());
    assertEquals(Event.Type.CREATE, events.get(0).getType());
    assertEquals("1_child1", events.get(0).getDocumentId());
    assertEquals(Event.Type.CREATE, events.get(1).getType());
    assertEquals("1_child2", events.get(1).getDocumentId());
    assertEquals(Event.Type.DROP, events.get(2).getType());
    assertEquals("1", events.get(2).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(3).getType());
    assertEquals("1_child1", events.get(3).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(4).getType());
    assertEquals("1_child2", events.get(4).getDocumentId());

    // confirm that topics are empty
    assertNull(messenger.pollDocToIndex());
    assertNull(messenger.pollDocToProcess());
    assertNull(messenger.pollEvent());
  }

  /**
   * A stage could emit a child that is marked as dropped. This is likely an edge case but we still want to
   * make sure the event accounting mechanism works properly here. When the worker sees the dropped child
   * it should send a CREATE event for the child document and then immediately follow up with a DROP event.
   */
  @Test
  public void testDropChildDocument() throws Exception {

    TestMessenger messenger =
        Runner.runInTestMode("RunnerTest/threeChildrenDropMiddle.conf").get("connector1");
    ;

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    Document parent = docsSentForProcessing.get(0);
    assertEquals("1", parent.getId());

    // confirm the two children were processed by the pipeline and sent to the destination topic,
    // while the middle one was dropped and not sent for processing
    List<Document> docsCompleted = messenger.getDocsSentForIndexing();
    assertEquals(3, docsCompleted.size());
    assertEquals("1_child1", docsCompleted.get(0).getId());
    assertEquals("1_child3", docsCompleted.get(1).getId());
    assertEquals("1", docsCompleted.get(2).getId());

    // confirm that a CREATE event was sent for doc1's children, including the dropped one;
    // confirm that a DROP event was sent for the middle child and no FINISH event was sent for it
    List<Event> events = messenger.getSentEvents();
    assertEquals(7, events.size());
    assertEquals(Event.Type.CREATE, events.get(0).getType());
    assertEquals("1_child1", events.get(0).getDocumentId());
    assertEquals(Event.Type.CREATE, events.get(1).getType());
    assertEquals("1_child2", events.get(1).getDocumentId());
    assertEquals(Event.Type.DROP, events.get(2).getType());
    assertEquals("1_child2", events.get(2).getDocumentId());
    assertEquals(Event.Type.CREATE, events.get(3).getType());
    assertEquals("1_child3", events.get(3).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(4).getType());
    assertEquals("1_child1", events.get(4).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(5).getType());
    assertEquals("1_child3", events.get(5).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(6).getType());
    assertEquals("1", events.get(6).getDocumentId());

    // confirm that topics are empty
    assertNull(messenger.pollDocToIndex());
    assertNull(messenger.pollDocToProcess());
    assertNull(messenger.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 1 document, and a pipeline that
   * attempts to generate 5 children but fails when the 3rd child is requested
   */
  @Test
  public void testErrorGeneratingChildren() throws Exception {

    TestMessenger messenger =
        Runner.runInTestMode("RunnerTest/failAfterTwoChildren.conf").get("connector1");
    ;

    // confirm doc 1 sent for processing
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(1, docsSentForProcessing.size());
    Document parent = docsSentForProcessing.get(0);
    assertEquals("1", parent.getId());

    // confirm the two children were processed by the pipeline and sent to the destination topic
    List<Document> docsCompleted = messenger.getDocsSentForIndexing();
    assertEquals(2, docsCompleted.size());
    Document child1 = docsCompleted.get(0);
    Document child2 = docsCompleted.get(1);
    assertEquals("1_child1", child1.getId());
    assertEquals("1_child2", child2.getId());

    // confirm that a CREATE event was sent for the doc1's children;
    // confirm that a FAIL event was sent for doc1 no DROP or FINISH event was sent for it;
    // note that the CreateChildrenStage in the pipeline is configured to mark the parent as dropped,
    // but the worker sends a FAIL event for the parent instead of a DROP event, because
    // the error in generating children is considered as a failure in processing the parent;
    // also note that the failure arises when requesting the 3rd child and therefore a CREATE event
    // is not sent for that 3rd child
    List<Event> events = messenger.getSentEvents();
    assertEquals(5, events.size());
    assertEquals(Event.Type.CREATE, events.get(0).getType());
    assertEquals("1_child1", events.get(0).getDocumentId());
    assertEquals(Event.Type.CREATE, events.get(1).getType());
    assertEquals("1_child2", events.get(1).getDocumentId());
    assertEquals(Event.Type.FAIL, events.get(2).getType());
    assertEquals("1", events.get(2).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(3).getType());
    assertEquals("1_child1", events.get(3).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(4).getType());
    assertEquals("1_child2", events.get(4).getDocumentId());

    // confirm that topics are empty
    assertNull(messenger.pollDocToIndex());
    assertNull(messenger.pollDocToProcess());
    assertNull(messenger.pollEvent());
  }

  /**
   * Test an end-to-end run with two connectors that each generate a single document, and a no-op pipeline
   */
  @Test
  public void testTwoConnectors() throws Exception {
    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/twoConnectors.conf");

    assertEquals(2, map.size());

    TestMessenger messenger1 = map.get("connector1");
    TestMessenger messenger2 = map.get("connector2");

    // confirm doc 1 sent for processing (via first connector) and doc 2 sent (via second connector)
    assertEquals(1, messenger1.getDocsSentForProcessing().size());
    assertEquals(1, messenger2.getDocsSentForProcessing().size());
    assertEquals("1", messenger1.getDocsSentForProcessing().get(0).getId());
    assertEquals("2", messenger2.getDocsSentForProcessing().get(0).getId());

    // confirm both docs were processed and sent to the destination topic
    assertEquals(1, messenger1.getDocsSentForIndexing().size());
    assertEquals(1, messenger2.getDocsSentForIndexing().size());
    assertEquals("1", messenger1.getDocsSentForIndexing().get(0).getId());
    assertEquals("2", messenger2.getDocsSentForIndexing().get(0).getId());

    // confirm both docs were sent to solr
    //assertEquals(1, messenger1.getSavedDocsSentToSolr().size());
    //assertEquals(1, messenger2.getSavedDocsSentToSolr().size());
    //assertEquals("1", messenger1.getSavedDocsSentToSolr().get(0).getId());
    //assertEquals("2", messenger2.getSavedDocsSentToSolr().get(0).getId());

    // confirm that terminal events were sent for both docs
    assertEquals(1, messenger1.getSentEvents().size());
    assertEquals(1, messenger2.getSentEvents().size());
    assertEquals(Event.Type.FINISH, messenger1.getSentEvents().get(0).getType());
    assertEquals(Event.Type.FINISH, messenger2.getSentEvents().get(0).getType());
    assertEquals("1", messenger1.getSentEvents().get(0).getDocumentId());
    assertEquals("2", messenger2.getSentEvents().get(0).getDocumentId());

    assertNotNull(messenger1.getRunId());
    assertEquals(messenger1.getRunId(), messenger2.getRunId());
    assertEquals(messenger1.getRunId(), messenger1.getSentEvents().get(0).getRunId());
    assertEquals(messenger2.getRunId(), messenger2.getSentEvents().get(0).getRunId());

    // confirm that topics are empty
    assertNull(messenger1.pollDocToIndex());
    assertNull(messenger1.pollDocToProcess());
    assertNull(messenger1.pollEvent());

    assertNull(messenger2.pollDocToIndex());
    assertNull(messenger2.pollDocToProcess());
    assertNull(messenger2.pollEvent());
  }

  /**
   * Attempt to run three connectors, where the second connector throws an exception.
   * Confirm that the third connector is not run.
   */
  @Test
  public void testThreeConnectorsWithFailure() throws Exception {
    Map<String, TestMessenger> map =
        Runner.runInTestMode("RunnerTest/threeConnectorsWithFailure.conf");
    assertEquals(2, map.size());
  }

  @Test
  public void testRunStatus() throws Exception {
    // successful run
    RunResult result =
        Runner.run(ConfigFactory.load("RunnerTest/singleDoc.conf"), Runner.RunType.TEST);
    assertTrue(result.getStatus());

    // successful run with failing documents
    result =
        Runner.run(ConfigFactory.load("RunnerTest/threeDocsOneFailure.conf"), Runner.RunType.TEST);
    assertTrue(result.getStatus());

    // failing run
    result =
        Runner.run(ConfigFactory.load("RunnerTest/threeConnectorsWithFailure.conf"), Runner.RunType.TEST);
    assertFalse(result.getStatus());
    
    // should have a message
    assertNotNull(result.getMessage());
    
  }

  /**
   * Attempt to run three connectors, where the second connector feeds to a pipeline
   * that throws an exception while starting a stage.
   * Confirm that the third connector is not run.
   * An exception should not bubble up.
   */
  @Test
  public void testThreeConnectorsWithStageStartFailure() throws Exception {
    Map<String, TestMessenger> map =
        Runner.runInTestMode("RunnerTest/threeConnectorsWithStageStartFailure.conf");
    assertEquals(2, map.size());
  }

  /**
   * Test that the post completion events occur after all of the documents have been fully processed.
   */
  @Test
  public void testPostCompletionActions() throws Exception {
    PostCompletionCSVConnector.reset();
    TestMessenger messenger = Runner.runInTestMode("RunnerTest/postCompletionActions.conf").get("connector1");
    assertTrue(PostCompletionCSVConnector.didPostCompletionActionsOccur());
    List<Document> docs = messenger.getDocsSentForIndexing();
    Instant stageInstant = Instant.parse(docs.get(0).getString("timestamp"));
    Instant postCompletionInstant = PostCompletionCSVConnector.getPostCompletionInstant();
    assertTrue(postCompletionInstant.isAfter(stageInstant));
  }

  /**
   * Test that the post completion events occur as expected in the case where no pipeline is configured.
   */
  @Test
  public void testPostCompletionActionsWithoutPipeline() throws Exception {
    PostCompletionCSVConnector.reset();
    Runner.runInTestMode("RunnerTest/postCompletionActionsWithoutPipeline.conf").get("connector1");
    assertTrue(PostCompletionCSVConnector.didPostCompletionActionsOccur());
  }

  /**
   * Ensure that if pre completion events fail, further connectors in the pipeline will not run.
   */
  @Test
  public void testFailingPreCompletionActions() throws Exception {
    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/failingPreCompletionActions.conf");
    assertEquals(2, map.size());
  }

  /**
   * Ensure that if post completion events fail, further connectors in the pipeline will not run.
   */
  @Test
  public void testFailingPostCompletionActions() throws Exception {
    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/failingPostCompletionActions.conf");
    assertEquals(2, map.size());
  }

  @Test
  public void testLifecycleMethods() throws Exception {

    // preExecute(), execute(), postExecute(), and close() should be called when running a connector
    NoOpConnector connector = mock(NoOpConnector.class);
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    PublisherImpl publisher =
        Mockito.spy(new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1"));
    assertTrue(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());
    verify(connector, times(1)).execute(any());
    verify(connector, times(1)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
    verify(messenger, times(1)).close();
  }

  @Test
  public void testLifecycleMethodsWithPreExecuteException() throws Exception {

    // if preExecute() throws an exception, execute() and postExecute() should not be called, but close() should be
    NoOpConnector connector = mock(NoOpConnector.class);
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    PublisherImpl publisher =
        Mockito.spy(new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1"));
    doThrow(new ConnectorException()).when(connector).preExecute(any());
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());
    verify(connector, times(0)).execute(any());
    verify(connector, times(0)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
    verify(messenger, times(1)).close();
  }

  @Test
  public void testLifecycleMethodsWithExecuteException() throws Exception {

    // if execute() throws an exception, postExecute() should not be called, but close() should be
    NoOpConnector connector = mock(NoOpConnector.class);
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    PublisherImpl publisher =
        Mockito.spy(new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1"));
    doThrow(new ConnectorException()).when(connector).execute(any());
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());
    verify(connector, times(1)).execute(any());
    verify(connector, times(0)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
    verify(messenger, times(1)).close();
  }

  @Test
  public void testLifecycleMethodsWithPostExecuteException() throws Exception {

    // if postExecute() throws an exception, close() should still be called
    NoOpConnector connector = mock(NoOpConnector.class);
    TestMessenger messenger = Mockito.spy(new TestMessenger());
    PublisherImpl publisher =
        Mockito.spy(new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1"));
    doThrow(new ConnectorException()).when(connector).postExecute(any());
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());
    verify(connector, times(1)).execute(any());
    verify(connector, times(1)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
    verify(messenger, times(1)).close();
  }

  /**
   * If a connector sets a custom message, the message should be included in the run summary, toString version.
   */
  @Test
  public void testCustomRunSummaryToString() throws Exception {
    RunResult result =
        Runner.run(ConfigFactory.load("RunnerTest/runSummaryMessage.conf"), Runner.RunType.TEST);
    assertTrue(result.toString().contains(RunSummaryMessageConnector.MESSAGE));
  }

  /**
   * If a connector sets a custom message, the message should be included in the run summary getMessage version.
   */
  @Test
  public void testCustomRunSummaryMessage() throws Exception {
    RunResult result =
        Runner.run(ConfigFactory.load("RunnerTest/runSummaryMessage.conf"), Runner.RunType.TEST);
    assertTrue(result.getMessage().contains(RunSummaryMessageConnector.MESSAGE));
  }


  @Test
  public void testPublisherException() throws Exception {
    NoOpConnector connector = mock(NoOpConnector.class);
    PublisherImpl publisher = mock(PublisherImpl.class);
    doThrow(new Exception()).when(publisher).waitForCompletion(any(), anyInt());

    // if the publisher throws an exception during execute(), postExecute() should not be called
    // both the publisher and connector should be closed
    // runConnector should return false and not propagate the exception
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());

    // We can assume that Runner started a ConnectorThread and
    // went on to call publisher.waitForCompletion(); however,
    // we don't know if the ConnectorThread called Connector.execute() yet,
    // even though that's the first thing it does,
    // so we can't make an assertion about that
    //verify(connector, times(1)).execute(any());

    verify(connector, times(0)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
  }

  @Test
  public void testPublisherCloseException() throws Exception {
    NoOpConnector connector = mock(NoOpConnector.class);
    PublisherImpl publisher = mock(PublisherImpl.class);
    assertTrue(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());

    // if the publisher throws an exception during close(),
    // runConnector should return false and not propagate the exception
    connector = mock(NoOpConnector.class);
    publisher = mock(PublisherImpl.class);
    doThrow(new Exception()).when(publisher).close();
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());

    verify(connector, times(1)).preExecute(any());

    // execute() is called in a separate thread so we verify it with a timeout;
    // without the timeout we were seeing transient failures here
    verify(connector, timeout(2000).times(1)).execute(publisher);

    verify(connector, times(1)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
  }

  @Test
  public void testConnectorCloseException() throws Exception {
    NoOpConnector connector = mock(NoOpConnector.class);
    PublisherImpl publisher = mock(PublisherImpl.class);
    assertTrue(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());

    // if the connector throws an exception during close(),
    // runConnector should return false and not propagate the exception
    connector = mock(NoOpConnector.class);
    publisher = mock(PublisherImpl.class);
    doThrow(new ConnectorException()).when(connector).close();
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());

    verify(connector, times(1)).preExecute(any());

    // execute() is called in a separate thread so we verify it with a timeout;
    // without the timeout we were seeing transient failures here
    verify(connector, timeout(2000).times(1)).execute(any());

    verify(connector, times(1)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
  }

  @Test
  public void testConnectorWithoutPipeline() throws Exception {
    // we should be able to run a connector that has no associated pipeline
    // in this case, the runner should not start Worker or Indexer threads
    // currently we're not testing that these threads aren't started, but we're making sure this configuration
    // doesn't error out
    NoOpConnector.reset();
    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/connectorWithoutPipeline.conf");
    assertEquals(2, map.size());
    assertNull(NoOpConnector.getSuppliedPublisher());
  }

  @Test
  public void testConnectorExecutionFailureWithoutPipeline() throws Exception {
    // if a connector's execute() method fails, the run should fail,
    // even in the case where connector is not feeding to a pipeline
    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/failingExecuteWithoutPipeline.conf");
    // the run should have been aborted with the second connector; the third should not have have been run
    assertEquals(2, map.size());
  }

  @Test
  public void testConnectorsWithoutName() throws Exception {
    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/connectorsWithoutName.conf");
    assertEquals(3, map.size());
    // when connector names are not provided in the config, they are assigned as "connector_N"
    assertTrue(map.containsKey("connector_1"));
    assertTrue(map.containsKey("connector_2"));
    assertTrue(map.containsKey("connector_3"));
  }

  @Test
  public void testConnectorWithUnrecognizedPipeline() throws Exception {
    RunResult result =
        Runner.run(ConfigFactory.load("RunnerTest/pipelineNotFound.conf"), Runner.RunType.TEST);
    assertFalse(result.getStatus());
  }

  @Test
  public void testStartStopCalledOnPipelineStages() throws Exception {
    StartStopCaptureStage.reset();
    Runner.runInTestMode("RunnerTest/stageStartStop.conf");
    assertTrue(StartStopCaptureStage.startCalled);
    assertTrue(StartStopCaptureStage.stopCalled);
    StartStopCaptureStage.reset();
  }


  @Test
  public void testCollapse() throws Exception {

    // DATA:
    //
    // 0,a,b,c
    // 1,foo,bar,baz
    // 1,foo,bar,baz
    // 1,foo2,bar2,baz2
    // 2,a,b,c
    // 1,non-consecutive,bar,baz

    Map<String, TestMessenger> map = Runner.runInTestMode("RunnerTest/collapse.conf");
    assertEquals(2, map.size());
    TestMessenger messenger1 = map.get("connector1");
    TestMessenger messenger2 = map.get("connector2");

    // docs IDs should have the specified docIdPrefix;
    // when collapsing, the three consecutive docs with id=1 should be collapsed into one doc, but the fourth
    // doc with id=1 should not be collapsed because it is out of sequence;
    // so, when collapsing we expect a total of 4 docs, when not collapsing we expect 6

    assertEquals(4, messenger1.getDocsSentForProcessing().size());
    assertEquals(6, messenger2.getDocsSentForProcessing().size());
    List<String> expectedIdsFromCollapsingConnector =
        Arrays.asList(new String[]{"connector1-0", "connector1-1", "connector1-2", "connector1-1"});
    List<String> expectedIdsFromNonCollapsingConnector =
        Arrays.asList(new String[]{"connector2-0", "connector2-1", "connector2-1", "connector2-1", "connector2-2", "connector2-1"});
    assertEquals(expectedIdsFromCollapsingConnector,
        messenger1.getDocsSentForProcessing().stream().map(d -> d.getId()).collect(Collectors.toList()));
    assertEquals(expectedIdsFromNonCollapsingConnector,
        messenger2.getDocsSentForProcessing().stream().map(d -> d.getId()).collect(Collectors.toList()));
    assertEquals(4, messenger1.getSentEvents().size());
    assertEquals(6, messenger2.getSentEvents().size());

    assertEquals(Arrays.asList(new String[]{"foo", "foo", "foo2"}),
        messenger1.getDocsSentForProcessing().get(1).getStringList("field1"));
    assertEquals("non-consecutive",
        messenger1.getDocsSentForProcessing().get(3).getString("field1"));
  }

  @Test
  public void testIndexerConnectFailure() throws Exception {
    // use runLocal() instead of runInTestMode() so we attempt to start an Indexer and
    // handle the failure
    assertFalse(Runner.run(ConfigFactory.load("RunnerTest/indexerConnectFailure.conf"),
        Runner.RunType.LOCAL).getStatus());
  }

  @Test
  public void testIndexerDeleteByFieldConfig() throws Exception {
    assertFalse(Runner.run(ConfigFactory.load("RunnerTest/indexerDeleteByFieldInvalid1.conf"), Runner.RunType.LOCAL).getStatus());
    assertFalse(Runner.run(ConfigFactory.load("RunnerTest/indexerDeleteByFieldInvalid2.conf"), Runner.RunType.LOCAL).getStatus());
    assertTrue(Runner.run(ConfigFactory.load("RunnerTest/indexerDeleteByFieldValid.conf"), Runner.RunType.LOCAL).getStatus());
  }

  @Test
  public void testIndexerDeleteMarkerConfig() throws Exception {
    assertFalse(Runner.run(ConfigFactory.load("RunnerTest/indexerDeleteInvalid1.conf"), Runner.RunType.LOCAL).getStatus());
    assertFalse(Runner.run(ConfigFactory.load("RunnerTest/indexerDeleteInvalid2.conf"), Runner.RunType.LOCAL).getStatus());
    assertTrue(Runner.run(ConfigFactory.load("RunnerTest/indexerDeleteValid.conf"), Runner.RunType.LOCAL).getStatus());
  }

  @Test
  public void testMetrics() throws Exception {
    SharedMetricRegistries.clear();
    assertEquals(0, SharedMetricRegistries.names().size());
    Map<String, TestMessenger> runnerMessengerMap = Runner.runInTestMode("RunnerTest/twoConnectors.conf");
    String run1ID = runnerMessengerMap.get("connector1").getRunId();
    assertEquals(run1ID, runnerMessengerMap.get("connector2").getRunId());
    assertEquals(1, SharedMetricRegistries.names().size());
    assertEquals(LogUtils.METRICS_REG, SharedMetricRegistries.names().toArray()[0]);
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);

    // confirm that each stage instance within each connector/pipeline pair creates a timer with a unique name
    // note: it's important for names to be unique, otherwise metrics across instances of a stage would be combined
    SortedMap<String, Timer> run1Connector1Pipeline1Stage1Timers =
        metrics.getTimers(MetricFilter.contains(run1ID + ".connector1.pipeline1.stage.stage_1.processDocumentTime"));
    SortedMap<String, Timer> run1Connector2Pipeline2Stage1Timers =
        metrics.getTimers(MetricFilter.contains(run1ID + ".connector2.pipeline2.stage.stage_1.processDocumentTime"));

    assertEquals(1, run1Connector1Pipeline1Stage1Timers.size());
    assertEquals(1, run1Connector2Pipeline2Stage1Timers.size());

    // each timer should have a count of one because each connector produces one document
    assertEquals(1, run1Connector1Pipeline1Stage1Timers.get(run1Connector1Pipeline1Stage1Timers.firstKey()).getCount());
    assertEquals(1, run1Connector2Pipeline2Stage1Timers.get(run1Connector2Pipeline2Stage1Timers.firstKey()).getCount());

    // metrics for the same connector/pipeline pairs are independent across multiple runs in the same JVM.
    runnerMessengerMap = Runner.runInTestMode("RunnerTest/twoConnectors.conf");
    String run2ID = runnerMessengerMap.get("connector1").getRunId();
    assertEquals(run2ID, runnerMessengerMap.get("connector2").getRunId());

    SortedMap<String, Timer> run2Connector1Pipeline1Stage1Timers =
        metrics.getTimers(MetricFilter.contains(run2ID + ".connector1.pipeline1.stage.stage_1.processDocumentTime"));
    SortedMap<String, Timer> run2Connector2Pipeline2Stage1Timers =
        metrics.getTimers(MetricFilter.contains(run2ID + ".connector2.pipeline2.stage.stage_1.processDocumentTime"));

    // Checking the first run wasn't affected by the second one.
    assertEquals(1, run1Connector1Pipeline1Stage1Timers.size());
    assertEquals(1, run1Connector2Pipeline2Stage1Timers.size());
    assertEquals(1, run1Connector1Pipeline1Stage1Timers.get(run1Connector1Pipeline1Stage1Timers.firstKey()).getCount());
    assertEquals(1, run1Connector2Pipeline2Stage1Timers.get(run1Connector2Pipeline2Stage1Timers.firstKey()).getCount());

    // Checking the results of the second run itself
    assertEquals(1, run2Connector1Pipeline1Stage1Timers.size());
    assertEquals(1, run2Connector2Pipeline2Stage1Timers.size());
    assertEquals(1, run2Connector1Pipeline1Stage1Timers.get(run2Connector1Pipeline1Stage1Timers.firstKey()).getCount());
    assertEquals(1, run2Connector2Pipeline2Stage1Timers.get(run2Connector2Pipeline2Stage1Timers.firstKey()).getCount());

    // not tested:
    // 1) other metrics collected by Stage beyond the counts in the processDocumentTime timer
    // 2) metrics collected by Indexer, Worker, Publisher
  }

  @Test
  public void testRenderFlag() throws Exception {
    Config config = ConfigFactory.load("RunnerTest/render.conf");
    Runner.renderConfig(config);
  }

  @Test
  public void testStringifyValidationExceptionsMap() {
    Map<String, List<Exception>> exceptions = new LinkedHashMap<>();
    exceptions.put("pipeline1", List.of(new Exception("exception 1"), new Exception("exception 2")));
    exceptions.put("pipeline2", List.of(new Exception("exception 3")));

    String expected = "Pipeline Configuration is invalid. See exceptions for each element:\n"
        + "\tpipeline1:\n"
        + "\t\texception 1\n"
        + "\t\texception 2\n"
        + "\tpipeline2:\n"
        + "\t\texception 3";
    assertEquals(expected, Runner.stringifyValidation(exceptions, "Pipeline"));
  }

  @Test
  public void testStringifyValidationExceptionsList() {
    List<Exception> exceptions = List.of(new Exception("exception 1"), new Exception("exception 2"), new Exception("exception 3"));

    String expected = "Pipeline Configuration is invalid. Errors:\n"
        + "\texception 1\n"
        + "\texception 2\n"
        + "\texception 3";

    assertEquals(expected, Runner.stringifyValidation(exceptions, "Pipeline"));
  }

  @Test
  public void testStringifyValidationNoExceptions() {
    Map<String, List<Exception>> exceptions = new LinkedHashMap<>();
    assertEquals("Pipeline Configuration is valid.", Runner.stringifyValidation(exceptions, "Pipeline"));
  }

  /**
   * Attempt to reproduce a deadlock that was seen in a pipeline that dropped a high percentage of docs, but not all docs.
   * The deadlock is also sensitive to how many docs are processed in total, the batch size, and the logging configuration.
   *
   * This test will possibly hang if the deadlock potential still exists in the code. It should pass in several seconds
   * assuming the deadlock has been fixed, otherwise it will time out and fail after 30 seconds.
   */
  @Test(timeout = 30000)
  public void testDropDeadlock() throws Exception {
    RunResult result =
        Runner.run(ConfigFactory.load("RunnerTest/dropDeadlock.conf"), Runner.RunType.LOCAL);
    assertTrue(result.getStatus());
  }

  /**
   * Test the publisher.maxPendingDocs setting:
   * Run a pipeline where publisher.maxPendingDocs is set to 8000 (with 20K docs created and roughly half dropped).
   * Confirm that the pipeline completes without errors in under 3 seconds with setting in place.
   *
   * Note: this test does not verify that maxPendingDocs is actually respected by the publisher, it simply
   * checks that the setting does not cause obvious problems.
   */
  @Test(timeout = 30000)
  public void testMaxPendingDocs() throws Exception {
    RunResult result =
        Runner.run(ConfigFactory.load("RunnerTest/maxPendingDocs.conf"), Runner.RunType.LOCAL);
    assertTrue(result.getStatus());
  }

  @Test
  public void testPreRunValidationAbortsRunOnInvalidOtherConfig() throws Exception {
    Config base = ConfigFactory.load("RunnerTest/singleDoc.conf");
    Config invalid = ConfigFactory.parseString("worker { threadsX = 10 }").withFallback(base);

    RunResult result = Runner.run(invalid, Runner.RunType.TEST);

    assertFalse(result.getStatus());
    assertNotNull(result.getHistory());
    assertTrue(result.getHistory().isEmpty());
  }

  @Test
  public void testRunnerStateSetAndCleared() throws Exception {

    // when executing a run in a way that doesn't involve main(), no instances of RunnerState should be created
    try (MockedConstruction<Runner.RunnerState> mockedConstruction = Mockito.mockConstruction(Runner.RunnerState.class)) {
      Runner.runInTestMode("RunnerTest/singleDocSendEnabledFalse.conf").get("connector1");
      List<Runner.RunnerState> instances = mockedConstruction.constructed();
      assertEquals(0, instances.size());
    }

    // when executing a run via main(), an instance of RunnerState should be created, populated, and cleared
    // to test this, first we mock Runner.SystemHelper so that System.exit() calls inside Runner.main() do not interfere
    try (MockedStatic<Runner.SystemHelper> mockedStatic = mockStatic(Runner.SystemHelper.class)) {
      // now mock RunnerState's constructor so we can see how many instances are created and inspect them
      try (MockedConstruction<Runner.RunnerState> mockedConstruction = Mockito.mockConstruction(Runner.RunnerState.class)) {
        assertNull(System.getProperty("config.file")); // tests should not be run with config.file already set
        System.setProperty("config.file", "src/test/resources/RunnerTest/singleDocSendEnabledFalse.conf");
        Runner.main(new String[] {});
        List<Runner.RunnerState> instances = mockedConstruction.constructed();
        assertEquals(1, instances.size());
        Runner.RunnerState state = instances.get(0);
        verify(state, times(1)).set(any(),any(),any(),any(),any()); // state should have been set
        verify(state, times(1)).clear(); // state should have been cleared
        verify(state, times(0)).close(); // state.close() should not have been called because we're not handling an INT signal
      } finally {
        System.clearProperty("config.file");
      }
    }
  }

  @Test
  public void testRunnerStateClose() throws Exception {

    Runner.RunnerState state = spy(new Runner.RunnerState());

    Publisher publisher = mock(Publisher.class);
    Connector connector = mock(Connector.class);
    WorkerPool workerPool = mock(WorkerPool.class);
    Indexer indexer = mock(Indexer.class);
    Thread indexerThread = mock(Thread.class);

    state.set(publisher, connector, workerPool, indexer, indexerThread);

    // test that state.close() attempts to close/stop all the stored resources
    state.close();
    verify(publisher, times(1)).close();
    verify(connector, times(1)).close();
    verify(workerPool, times(1)).stop();
    verify(indexer, times(1)).terminate();
    verify(indexerThread, times(1)).join(anyLong());
  }
}
