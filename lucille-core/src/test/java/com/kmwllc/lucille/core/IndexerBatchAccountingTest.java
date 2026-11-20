package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.NopIndexer;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.message.UndeserializableRecordException;
import com.kmwllc.lucille.util.LogUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests how the Indexer base class accounts for the failures sendToIndex returns, and how it enforces bypass.
 */
public class IndexerBatchAccountingTest {

  private static final Config BATCH_OF_TWO = ConfigFactory.parseString("batchSize: 2");

  private static List<Event> eventsOfType(TestMessenger messenger, Event.Type type) throws Exception {
    return messenger.getSentEvents().stream().filter(e -> e.getType() == type).toList();
  }

  private static void send(TestMessenger messenger, Document... docs) throws Exception {
    for (Document doc : docs) {
      messenger.sendForIndexing(doc);
    }
  }

  @Test
  public void testContentIdenticalSiblingOfFailedDocumentFinishes() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Document doc = Document.create("doc1", "run1");
    Document sibling = Document.create("doc1", "run1");
    assertEquals(doc, sibling);

    Indexer indexer = new FailingIndexer(BATCH_OF_TWO, messenger, "contentIdentical",
        batch -> List.of(new IndexerFailure(batch.get(0), new IndexerException("rejected"))));
    send(messenger, doc, sibling);
    indexer.run(2);

    assertEquals(1, eventsOfType(messenger, Event.Type.FAIL).size());
    assertEquals(1, eventsOfType(messenger, Event.Type.FINISH).size());
  }

  @Test
  public void testDocumentReportedTwiceGetsOneEventWithFirstCause() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Indexer indexer = new FailingIndexer(ConfigFactory.parseString("batchSize: 1"), messenger, "reportedTwice",
        batch -> List.of(new IndexerFailure(batch.get(0), new IndexerException("first")),
            new IndexerFailure(batch.get(0), new IndexerException("second"))));
    send(messenger, Document.create("doc1", "run1"));
    indexer.run(1);

    List<Event> events = messenger.getSentEvents();
    assertEquals(1, events.size());
    assertEquals(Event.Type.FAIL, events.get(0).getType());
    assertTrue(events.get(0).getMessage(), events.get(0).getMessage().contains("first"));
  }

  @Test
  public void testCopyOfDocumentFailsIt() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Indexer indexer = new FailingIndexer(ConfigFactory.parseString("batchSize: 1"), messenger, "copyOfDocument",
        batch -> List.of(new IndexerFailure(batch.get(0).deepCopy(), new IndexerException("rejected"))));
    send(messenger, Document.create("doc1", "run1"));
    indexer.run(1);

    List<Event> events = messenger.getSentEvents();
    assertEquals(1, events.size());
    assertEquals(Event.Type.FAIL, events.get(0).getType());
    assertTrue(events.get(0).getMessage(), events.get(0).getMessage().contains("rejected"));
  }

  @Test
  public void testUnmatchedFailureFailsRestOfBatch() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Indexer indexer = new FailingIndexer(ConfigFactory.parseString("batchSize: 3"), messenger, "unmatchedFailure",
        batch -> List.of(new IndexerFailure(batch.get(1).deepCopy(), new IndexerException("copy")),
            new IndexerFailure(batch.get(0), new IndexerException("matched"))));
    send(messenger, Document.create("doc1", "run1"), Document.create("doc2", "run1"), Document.create("doc3", "run1"));
    indexer.run(3);

    assertEquals(0, eventsOfType(messenger, Event.Type.FINISH).size());
    List<Event> fails = eventsOfType(messenger, Event.Type.FAIL);
    assertEquals(3, fails.size());
    // a failure that does match keeps its own cause, wherever it comes in the list
    assertEquals("doc1", fails.get(0).getDocumentId());
    assertTrue(fails.get(0).getMessage(), fails.get(0).getMessage().contains("matched"));
    assertEquals("doc2", fails.get(1).getDocumentId());
    assertEquals("doc3", fails.get(2).getDocumentId());
    assertEquals(0, SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG)
        .meter("unmatchedFailure.indexer.docsIndexed").getCount());
  }

  @Test
  public void testFailureForUnknownIdFailsBatchWithNoEventForIt() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Indexer indexer = new FailingIndexer(BATCH_OF_TWO, messenger, "unknownId",
        batch -> List.of(new IndexerFailure(Document.create("unknown", "run1"), new IndexerException("rejected"))));
    send(messenger, Document.create("doc1", "run1"), Document.create("doc2", "run1"));
    indexer.run(2);

    List<Event> events = messenger.getSentEvents();
    assertEquals(List.of("doc1", "doc2"), events.stream().map(Event::getDocumentId).toList());
    assertEquals(2, eventsOfType(messenger, Event.Type.FAIL).size());
  }

  @Test
  public void testDocumentReportedTwiceFailsItsSibling() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Indexer indexer = new FailingIndexer(BATCH_OF_TWO, messenger, "reportedTwiceWithSibling",
        batch -> List.of(new IndexerFailure(batch.get(1), new IndexerException("first")),
            new IndexerFailure(batch.get(1), new IndexerException("second"))));
    send(messenger, Document.create("doc1", "run1"), Document.create("doc1", "run1"));
    indexer.run(2);

    assertEquals(0, eventsOfType(messenger, Event.Type.FINISH).size());
    assertEquals(2, eventsOfType(messenger, Event.Type.FAIL).size());
  }

  @Test
  public void testDocsIndexedCountsOnlySucceededDocuments() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Indexer indexer = new FailingIndexer(BATCH_OF_TWO, messenger, "docsIndexedCount",
        batch -> List.of(new IndexerFailure(batch.get(0), new IndexerException("rejected"))));
    send(messenger, Document.create("doc1", "run1"), Document.create("doc2", "run1"));
    indexer.run(2);

    assertEquals(1, SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG)
        .meter("docsIndexedCount.indexer.docsIndexed").getCount());
  }

  @Test
  public void testBypassSkipsSendAndConnectionCheck() throws Exception {
    TestMessenger messenger = new TestMessenger();
    FailingIndexer indexer = new FailingIndexer(ConfigFactory.parseString("batchSize: 1"), messenger, true, "bypass",
        batch -> List.of());
    send(messenger, Document.create("doc1", "run1"));
    indexer.run(1);

    assertEquals(0, indexer.sends.size());
    assertTrue(indexer.validateConnection());
    assertEquals(0, indexer.connectionChecks);
    assertEquals(1, eventsOfType(messenger, Event.Type.FINISH).size());
  }

  @Test
  public void testConnectionCheckRunsWithoutBypass() {
    FailingIndexer indexer = new FailingIndexer(BATCH_OF_TWO, new TestMessenger(), "notBypass", batch -> List.of());
    indexer.connectionResult = false;

    assertFalse(indexer.validateConnection());
    assertEquals(1, indexer.connectionChecks);
  }

  @Test
  public void testGetDestinationId() {
    FailingIndexer noOverride = new FailingIndexer(BATCH_OF_TWO, new TestMessenger(), "noOverride", batch -> List.of());
    FailingIndexer override = new FailingIndexer(
        ConfigFactory.parseString("idOverrideField: \"other_id\""), new TestMessenger(), "override", batch -> List.of());

    Document withField = Document.create("doc1");
    withField.setField("other_id", "overridden");
    Document withoutField = Document.create("doc2");

    assertEquals("doc1", noOverride.getDestinationId(withField));
    assertEquals("overridden", override.getDestinationId(withField));
    assertEquals("doc2", override.getDestinationId(withoutField));
  }

  @Test(timeout = 60000)
  public void testUndeserializableRecordStopsIndexer() throws Exception {
    IndexerMessenger messenger = Mockito.mock(IndexerMessenger.class);
    Mockito.when(messenger.pollDocToIndex()).thenThrow(new UndeserializableRecordException("Could not deserialize record", null));

    new NopIndexer(ConfigFactory.empty(), new IndexerContext(messenger, false, "testing", null)).run();

    Mockito.verify(messenger, Mockito.times(1)).pollDocToIndex();
    Mockito.verify(messenger).close();
  }

  @Test
  public void testIsRetryable() {
    FailingIndexer noRetries = new FailingIndexer(BATCH_OF_TWO, new TestMessenger(), "noRetries", batch -> List.of());
    assertFalse(noRetries.isRetryable(new IndexerRetryableException(503, "unavailable", null)));

    FailingIndexer retries = new FailingIndexer(
        ConfigFactory.parseString("maxRetries: 2, retryableStatusCodes: [503]"), new TestMessenger(), "retries", batch -> List.of());
    assertTrue(retries.isRetryable(new IndexerRetryableException(503, "unavailable", null)));
    assertFalse(retries.isRetryable(new IndexerRetryableException(400, "bad request", null)));
    assertFalse(retries.isRetryable(new IndexerException("permanent")));
  }

  /** An Indexer whose sendToIndex returns whatever the given function produces for the batch. */
  private static class FailingIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    private final Function<List<Document>, List<IndexerFailure>> failures;
    final List<List<Document>> sends = new ArrayList<>();
    int connectionChecks = 0;
    boolean connectionResult = true;

    FailingIndexer(Config config, TestMessenger messenger, String metricsPrefix,
        Function<List<Document>, List<IndexerFailure>> failures) {
      this(config, messenger, false, metricsPrefix, failures);
    }

    FailingIndexer(Config config, TestMessenger messenger, boolean bypass, String metricsPrefix,
        Function<List<Document>, List<IndexerFailure>> failures) {
      super(config, new IndexerContext(messenger, bypass, metricsPrefix, null));
      this.failures = failures;
    }

    @Override
    protected String getIndexerConfigKey() {
      return null;
    }

    @Override
    protected boolean checkConnection() {
      connectionChecks++;
      return connectionResult;
    }

    @Override
    public void closeConnection() {
    }

    @Override
    protected List<IndexerFailure> sendToIndex(PendingSend send) {
      List<Document> documents = send.documents();
      sends.add(documents);
      return failures.apply(documents);
    }
  }
}
