package com.kmwllc.lucille.core;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.kmwllc.lucille.connector.NoOpConnector;
import com.kmwllc.lucille.connector.PostCompletionCSVConnector;
import com.kmwllc.lucille.connector.RunSummaryMessageConnector;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.kmwllc.lucille.stage.StartStopCaptureStage;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class RunnerTest {

  @Test
  public void testRunnerWithNoDocs() throws Exception {
    // we should be able to run a connector that generates no documents
    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/noDocs.conf");
    assertEquals(1, map.size());

    PersistingLocalMessageManager manager = map.get("connector1");

    assertEquals(0, manager.getSavedDocumentsSentForProcessing().size());
    assertEquals(0, manager.getSavedCompletedDocuments().size());
    assertEquals(0, manager.getSavedCompletedDocuments().size());
    assertEquals(0, manager.getSavedEvents().size());
    assertNull(manager.pollCompleted());
    assertNull(manager.pollDocToProcess());
    assertNull(manager.pollEvent());
  }

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
    assertNull(manager.pollCompleted());
    assertNull(manager.pollDocToProcess());
    assertNull(manager.pollEvent());
  }

  /**
   * Test an end-to-end run with a single connector that generates 3 documents, where
   * the second document causes a stage exception but the first and third should
   * finish without out errors
   */
  @Test
  public void testRunnerWithFailingDoc() throws Exception {

    // run connectors and pipeline; acquire a persisting message manager that allows
    // for reviewing saved message traffic
    PersistingLocalMessageManager manager =
      Runner.runInTestMode("RunnerTest/threeDocsOneFailure.conf").get("connector1");

    // confirm doc 3 docs sent for processing but only 2 docs completed
    List<Document> docsSentForProcessing = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docsSentForProcessing.size());
    assertEquals("1", docsSentForProcessing.get(0).getId());
    assertEquals("2", docsSentForProcessing.get(1).getId());
    assertEquals("3", docsSentForProcessing.get(2).getId());

    assertEquals(2, manager.getSavedCompletedDocuments().size());
    assertEquals("1", manager.getSavedCompletedDocuments().get(0).getId());
    assertEquals("3", manager.getSavedCompletedDocuments().get(1).getId());

    // confirm that the proper events were sent for all three documents
    List<Event> events = manager.getSavedEvents();
    assertNotNull(manager.getRunId());
    assertEquals(3, events.size());

    events.sort(Comparator.comparing(Event::getDocumentId));
    assertEquals("1", events.get(0).getDocumentId());
    assertEquals(manager.getRunId(), events.get(0).getRunId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());

    assertEquals("2", events.get(1).getDocumentId());
    assertEquals(manager.getRunId(), events.get(1).getRunId());
    assertEquals(Event.Type.FAIL, events.get(1).getType());

    assertEquals("3", events.get(2).getDocumentId());
    assertEquals(manager.getRunId(), events.get(2).getRunId());
    assertEquals(Event.Type.FINISH, events.get(2).getType());

    // confirm that topics are empty
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
    Connector connector = Connector.fromConfig(config).get(0);
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", connector.getPipelineName());
    Instant start = Instant.now();
    boolean result = Runner.runConnector(config, "run1", connector, publisher).getStatus();
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
    assertNull(manager1.pollCompleted());
    assertNull(manager1.pollDocToProcess());
    assertNull(manager1.pollEvent());

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
  }

  /**
   * Attempt to run three connectors, where the second connector feeds to a pipeline
   * that throws an exception while starting a stage.
   * Confirm that the third connector is not run.
   * An exception should not bubble up.
   */
  @Test
  public void testThreeConnectorsWithStageStartFailure() throws Exception {
    Map<String, PersistingLocalMessageManager> map =
      Runner.runInTestMode("RunnerTest/threeConnectorsWithStageStartFailure.conf");
    assertEquals(2, map.size());
  }

  /**
   * Test that the post completion events occur after all of the documents have been fully processed.
   */
  @Test
  public void testPostCompletionActions() throws Exception {
    PostCompletionCSVConnector.reset();
    PersistingLocalMessageManager manager = Runner.runInTestMode("RunnerTest/postCompletionActions.conf").get("connector1");
    assertTrue(PostCompletionCSVConnector.didPostCompletionActionsOccur());
    List<Document> docs = manager.getSavedCompletedDocuments();
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
    PersistingLocalMessageManager manager =
      Runner.runInTestMode("RunnerTest/postCompletionActionsWithoutPipeline.conf").get("connector1");
    assertTrue(PostCompletionCSVConnector.didPostCompletionActionsOccur());
  }

  /**
   * Ensure that if pre completion events fail, further connectors in the pipeline will not run.
   */
  @Test
  public void testFailingPreCompletionActions() throws Exception {
    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/failingPreCompletionActions.conf");
    assertEquals(2, map.size());
  }

  /**
   * Ensure that if post completion events fail, further connectors in the pipeline will not run.
   */
  @Test
  public void testFailingPostCompletionActions() throws Exception {
    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/failingPostCompletionActions.conf");
    assertEquals(2, map.size());
  }

  @Test
  public void testLifecycleMethods() throws Exception {

    // preExecute(), execute(), postExecute(), and close() should be called when running a connector
    NoOpConnector connector = mock(NoOpConnector.class);
    PersistingLocalMessageManager manager = Mockito.spy(new PersistingLocalMessageManager());
    PublisherImpl publisher =
      Mockito.spy(new PublisherImpl(ConfigFactory.empty(), manager, "run1", "pipeline1"));
    assertTrue(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());
    verify(connector, times(1)).execute(any());
    verify(connector, times(1)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
    verify(manager, times(1)).close();
  }

    @Test
    public void testLifecycleMethodsWithPreExecuteException() throws Exception {

      // if preExecute() throws an exception, execute() and postExecute() should not be called, but close() should be
      NoOpConnector connector = mock(NoOpConnector.class);
      PersistingLocalMessageManager manager = Mockito.spy(new PersistingLocalMessageManager());
      PublisherImpl publisher =
        Mockito.spy(new PublisherImpl(ConfigFactory.empty(), manager, "run1", "pipeline1"));
      doThrow(new ConnectorException()).when(connector).preExecute(any());
      assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
      verify(connector, times(1)).preExecute(any());
      verify(connector, times(0)).execute(any());
      verify(connector, times(0)).postExecute(any());
      verify(connector, times(1)).close();
      verify(publisher, times(1)).close();
      verify(manager, times(1)).close();
    }

  @Test
  public void testLifecycleMethodsWithExecuteException() throws Exception {

    // if execute() throws an exception, postExecute() should not be called, but close() should be
    NoOpConnector connector = mock(NoOpConnector.class);
    PersistingLocalMessageManager manager = Mockito.spy(new PersistingLocalMessageManager());
    PublisherImpl publisher =
      Mockito.spy(new PublisherImpl(ConfigFactory.empty(), manager, "run1", "pipeline1"));
    doThrow(new ConnectorException()).when(connector).execute(any());
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());
    verify(connector, times(1)).execute(any());
    verify(connector, times(0)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
    verify(manager, times(1)).close();
  }

  @Test
  public void testLifecycleMethodsWithPostExecuteException() throws Exception {

    // if postExecute() throws an exception, close() should still be called
    NoOpConnector connector = mock(NoOpConnector.class);
    PersistingLocalMessageManager manager = Mockito.spy(new PersistingLocalMessageManager());
    PublisherImpl publisher =
      Mockito.spy(new PublisherImpl(ConfigFactory.empty(), manager, "run1", "pipeline1"));
    doThrow(new ConnectorException()).when(connector).postExecute(any());
    assertFalse(Runner.runConnector(ConfigFactory.empty(), "run1", connector, publisher).getStatus());
    verify(connector, times(1)).preExecute(any());
    verify(connector, times(1)).execute(any());
    verify(connector, times(1)).postExecute(any());
    verify(connector, times(1)).close();
    verify(publisher, times(1)).close();
    verify(manager, times(1)).close();
  }

  /**
   * If a connector sets a custom message, the message should be included in the run summary.
   */
  @Test
  public void testCustomRunSummaryMessage() throws Exception {
    RunResult result =
      Runner.run(ConfigFactory.load("RunnerTest/runSummaryMessage.conf"), Runner.RunType.TEST);
    assertTrue(result.toString().contains(RunSummaryMessageConnector.MESSAGE));
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
    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/connectorWithoutPipeline.conf");
    assertEquals(2, map.size());
    assertNull(NoOpConnector.getSuppliedPublisher());
  }

  @Test
  public void testConnectorExecutionFailureWithoutPipeline() throws Exception {
    // if a connector's execute() method fails, the run should fail,
    // even in the case where connector is not feeding to a pipeline
    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/failingExecuteWithoutPipeline.conf");
    // the run should have been aborted with the second connector; the third should not have have been run
    assertEquals(2, map.size());
  }

  @Test
  public void testConnectorsWithoutName() throws Exception {
    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/connectorsWithoutName.conf");
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

    Map<String, PersistingLocalMessageManager> map = Runner.runInTestMode("RunnerTest/collapse.conf");
    assertEquals(2, map.size());
    PersistingLocalMessageManager manager1 = map.get("connector1");
    PersistingLocalMessageManager manager2 = map.get("connector2");

    // docs IDs should have the specified docIdPrefix;
    // when collapsing, the three consecutive docs with id=1 should be collapsed into one doc, but the fourth
    // doc with id=1 should not be collapsed because it is out of sequence;
    // so, when collapsing we expect a total of 4 docs, when not collapsing we expect 6

    assertEquals(4, manager1.getSavedDocumentsSentForProcessing().size());
    assertEquals(6, manager2.getSavedDocumentsSentForProcessing().size());
    List<String> expectedIdsFromCollapsingConnector =
      Arrays.asList(new String[] {"connector1-0", "connector1-1", "connector1-2", "connector1-1"});
    List<String> expectedIdsFromNonCollapsingConnector =
      Arrays.asList(new String[] {"connector2-0", "connector2-1", "connector2-1", "connector2-1", "connector2-2", "connector2-1"});
    assertEquals(expectedIdsFromCollapsingConnector,
      manager1.getSavedDocumentsSentForProcessing().stream().map(d -> d.getId()).collect(Collectors.toList()));
    assertEquals(expectedIdsFromNonCollapsingConnector,
      manager2.getSavedDocumentsSentForProcessing().stream().map(d -> d.getId()).collect(Collectors.toList()));
    assertEquals(4, manager1.getSavedEvents().size());
    assertEquals(6, manager2.getSavedEvents().size());

    assertEquals(Arrays.asList(new String[] {"foo", "foo", "foo2"}),
      manager1.getSavedDocumentsSentForProcessing().get(1).getStringList("field1"));
    assertEquals("non-consecutive",
      manager1.getSavedDocumentsSentForProcessing().get(3).getString("field1"));
  }

  @Test
  public void testIndexerConnectFailure() throws Exception {
    // use runLocal() instead of runInTestMode() so we attempt to start an Indexer and
    // handle the failure
    assertFalse(Runner.run(ConfigFactory.load("RunnerTest/indexerConnectFailure.conf"),
      Runner.RunType.LOCAL).getStatus());
  }

  @Test
  public void testMetrics() throws Exception {
    SharedMetricRegistries.clear();
    assertEquals(0, SharedMetricRegistries.names().size());
    Runner.runInTestMode("RunnerTest/twoConnectors.conf");
    assertEquals(1, SharedMetricRegistries.names().size());
    assertEquals(LogUtils.METRICS_REG, SharedMetricRegistries.names().toArray()[0]);
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);

    // confirm that each stage instance within each connector/pipeline pair creates a timer with a unique name
    // note: it's important for names to be unique, otherwise metrics across instances of a stage would be combined
    SortedMap<String, Timer> connector1Pipeline1Stage1Timers =
      metrics.getTimers(MetricFilter.contains("connector1.pipeline1.stage.stage_1.processDocumentTime"));
    SortedMap<String, Timer> connector2Pipeline2Stage1Timers =
      metrics.getTimers(MetricFilter.contains("connector2.pipeline2.stage.stage_1.processDocumentTime"));
    assertEquals(1, connector1Pipeline1Stage1Timers.size());
    assertEquals(1, connector2Pipeline2Stage1Timers.size());

    // each timer should have a count of one because each connector produces one document
    assertEquals(1, connector1Pipeline1Stage1Timers.get(connector1Pipeline1Stage1Timers.firstKey()).getCount());
    assertEquals(1, connector2Pipeline2Stage1Timers.get(connector2Pipeline2Stage1Timers.firstKey()).getCount());

    // currently, metrics for the same connector/pipeline pairs will be combined across mulitple runs occurring in the
    // same JVM;
    // if we re-execute the run from above the counts will not be reset but will increase from their earlier values;
    // we may wish to prevent this in the future by including the runId in the metrics naming scheme
    // or by updating Runner to call SharedMetricRegistries.clear() before each run, assuming runs are sequential
    Runner.runInTestMode("RunnerTest/twoConnectors.conf");
    assertEquals(2, connector1Pipeline1Stage1Timers.get(connector1Pipeline1Stage1Timers.firstKey()).getCount());
    assertEquals(2, connector2Pipeline2Stage1Timers.get(connector2Pipeline2Stage1Timers.firstKey()).getCount());

    // not tested:
    // 1) other metrics collected by Stage beyond the counts in the processDocumentTime timer
    // 2) metrics collected by Indexer, Worker, Publisher
  }
}
