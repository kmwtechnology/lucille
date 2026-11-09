package com.kmwllc.lucille.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.MessengerException;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Tests the Indexer's "send failed documents to the fail topic" behavior: the {@code indexer.sendFailedDocsToTopic}
 * gate, the failure metadata stamped on routed documents, what happens when the fail topic can't take a document, and
 * the fact that the Lucille-internal bookkeeping fields (failure metadata) are stripped from the document sent to a search index.
 */
public class IndexerFailTopicTest {

  private static final String PIPELINE_NAME = "pipeline1";

  /** A NopIndexer whose send always throws, forcing the whole-batch failure path through sendFailEvent. */
  private static class ErroringNopIndexer extends NopIndexer {

    ErroringNopIndexer(Config config, IndexerMessenger messenger) {
      super(config, new IndexerContext(messenger, false, PIPELINE_NAME, null));
    }

    @Override
    protected List<IndexerFailure> sendToIndex(List<Document> documents) {
      throw new IllegalStateException("Simulated indexing failure");
    }
  }

  private static Config config(String resource) {
    return ConfigFactory.load(resource).getConfig("indexer");
  }

  private static TestMessenger runIndexerOnOneDoc(TestMessenger messenger, Config config) throws Exception {
    messenger.sendForIndexing(Document.create("doc1", "run1"));
    new ErroringNopIndexer(config, messenger).run(1);
    return messenger;
  }

  private static TestMessenger runIndexerOnOneDoc(Config config) throws Exception {
    return runIndexerOnOneDoc(new TestMessenger(), config);
  }

  /**
   * A TestMessenger whose fail topic rejects every document, recording how many copies were attempted and whether the
   * batch was marked complete.
   */
  private static class FailTopicFailingMessenger extends TestMessenger {

    private final boolean documentSpecific;
    int sendFailedAttempts = 0;
    boolean batchCompleted = false;

    FailTopicFailingMessenger(boolean documentSpecific) {
      this.documentSpecific = documentSpecific;
    }

    @Override
    public void sendFailed(Document document) throws MessengerException {
      sendFailedAttempts++;
      throw new MessengerException("Simulated fail topic failure", null, documentSpecific);
    }

    @Override
    public void batchComplete(List<Document> batch) throws MessengerException {
      batchCompleted = true;
      super.batchComplete(batch);
    }
  }

  @Test
  public void testFailedDocRoutedToFailTopicWhenEnabled() throws Exception {
    Config config = config("IndexerFailTopicTest/sendFailedEnabled.conf");

    TestMessenger messenger = runIndexerOnOneDoc(config);

    List<Document> failed = messenger.getDocsSentToFailTopic();
    assertEquals("the failed document should be routed to the fail topic", 1, failed.size());

    Document failedDoc = failed.get(0);
    assertEquals("doc1", failedDoc.getId());
    assertTrue(failedDoc.has(Document.FAILURE_REASON_FIELD));
    assertEquals("indexer", failedDoc.getString(Document.FAILURE_COMPONENT_FIELD));
    assertTrue("a failure time should be stamped", failedDoc.has(Document.FAILURE_TIME_FIELD));
  }

  @Test
  public void testFailedDocNotRoutedWhenDisabled() throws Exception {
    Config config = config("IndexerFailTopicTest/sendFailedDisabled.conf");

    TestMessenger messenger = runIndexerOnOneDoc(config);

    assertTrue("nothing should be routed to the fail topic when the flag is off",
        messenger.getDocsSentToFailTopic().isEmpty());
  }

  @Test
  public void testOnlyTheRejectedDocIsRouted() throws Exception {
    TestMessenger messenger = new TestMessenger();
    messenger.sendForIndexing(Document.create("doc1", "run1"));
    messenger.sendForIndexing(Document.create("doc2", "run1"));
    Indexer indexer = new ErroringNopIndexer(config("IndexerFailTopicTest/sendFailedEnabled.conf").withValue("batchSize", ConfigValueFactory.fromAnyRef(2)), messenger) {
      @Override
      protected List<IndexerFailure> sendToIndex(List<Document> documents) {
        return List.of(new IndexerFailure(documents.get(1), new IndexerException("rejected")));
      }
    };
    indexer.run(2);

    List<Document> failed = messenger.getDocsSentToFailTopic();
    assertEquals(1, failed.size());
    assertEquals("doc2", failed.get(0).getId());
    assertTrue(failed.get(0).getString(Document.FAILURE_REASON_FIELD).contains("rejected"));
  }

  /** The copy is best-effort: the FAIL event records the document, so the batch is still marked complete. */
  @Test
  public void testBatchCompletedWhenFailTopicIsUnavailable() throws Exception {
    FailTopicFailingMessenger messenger = new FailTopicFailingMessenger(false);
    runIndexerOnOneDoc(messenger, config("IndexerFailTopicTest/sendFailedEnabled.conf"));

    assertTrue(messenger.batchCompleted);
    assertEquals(Event.Type.FAIL, messenger.getSentEvents().get(0).getType());
  }

  /** Once the fail topic is unavailable, the rest of the batch skips the copy rather than waiting out the same failure. */
  @Test
  public void testRestOfBatchSkipsCopyWhenFailTopicIsUnavailable() throws Exception {
    FailTopicFailingMessenger messenger = new FailTopicFailingMessenger(false);
    messenger.sendForIndexing(Document.create("doc1", "run1"));
    messenger.sendForIndexing(Document.create("doc2", "run1"));
    Indexer indexer = new ErroringNopIndexer(config("IndexerFailTopicTest/sendFailedEnabled.conf")
        .withValue("batchSize", ConfigValueFactory.fromAnyRef(2)), messenger);
    indexer.run(2);

    assertEquals(1, messenger.sendFailedAttempts);
    assertTrue(messenger.batchCompleted);
    assertEquals(2, messenger.getSentEvents().stream().filter(e -> e.getType() == Event.Type.FAIL).count());
  }

  /** A document-specific rejection doesn't mean the fail topic is unavailable, so the next document is still copied. */
  @Test
  public void testRestOfBatchCopiedWhenFailTopicRejectsOneDocument() throws Exception {
    FailTopicFailingMessenger messenger = new FailTopicFailingMessenger(true);
    messenger.sendForIndexing(Document.create("doc1", "run1"));
    messenger.sendForIndexing(Document.create("doc2", "run1"));
    Indexer indexer = new ErroringNopIndexer(config("IndexerFailTopicTest/sendFailedEnabled.conf")
        .withValue("batchSize", ConfigValueFactory.fromAnyRef(2)), messenger);
    indexer.run(2);

    assertEquals(2, messenger.sendFailedAttempts);
  }

  /** Re-delivering a document the fail topic itself rejects would only repeat the rejection. */
  @Test
  public void testBatchCompletedWhenFailTopicRejectsTheDocument() throws Exception {
    FailTopicFailingMessenger messenger = new FailTopicFailingMessenger(true);
    runIndexerOnOneDoc(messenger, config("IndexerFailTopicTest/sendFailedEnabled.conf"));

    assertTrue(messenger.batchCompleted);
    assertEquals(Event.Type.FAIL, messenger.getSentEvents().get(0).getType());
  }

  /**
   * The Lucille-internal bookkeeping fields (failure metadata) travel with a document but must
   * never be written to the search index. getConvertedIndexerDoc and getRawIndexerDoc are what the concrete indexers
   * send, so both must strip these fields regardless of any field-filter configuration.
   */
  @Test
  public void testInternalFieldsStrippedFromIndexerDoc() throws Exception {
    Config config = config("IndexerFailTopicTest/sendFailedEnabled.conf");
    TestMessenger messenger = new TestMessenger();
    FieldAccessIndexer indexer = new FieldAccessIndexer(config, messenger);

    // as read back from the fail topic
    Document doc = Document.createFromJson("{\"id\": \"doc1\", \"run_id\": \"run1\", \"keepMe\": \"value\", "
        + "\"___failure_reason\": \"some reason\", \"___failure_component\": \"indexer\", \"___failure_time\": \"1970-01-01T00:00:00Z\"}");

    Map<String, Object> converted = indexer.convertedDoc(doc);
    Map<String, Object> raw = indexer.rawDoc(doc);

    for (String internalField : Document.INDEXING_EXCLUDED_FIELDS) {
      assertFalse("getConvertedIndexerDoc must strip internal field " + internalField,
          converted.containsKey(internalField));
      assertFalse("getRawIndexerDoc must strip internal field " + internalField,
          raw.containsKey(internalField));
    }
    // Ordinary fields and the id are preserved.
    assertTrue(converted.containsKey("keepMe"));
    assertTrue(raw.containsKey("keepMe"));
  }

  /** Exposes the protected indexer-doc conversion methods so the stripping behavior can be asserted directly. */
  private static class FieldAccessIndexer extends NopIndexer {

    FieldAccessIndexer(Config config, IndexerMessenger messenger) {
      super(config, new IndexerContext(messenger, false, PIPELINE_NAME, null));
    }

    Map<String, Object> convertedDoc(Document doc) {
      return getConvertedIndexerDoc(doc);
    }

    Map<String, Object> rawDoc(Document doc) {
      return getRawIndexerDoc(doc);
    }
  }
}
