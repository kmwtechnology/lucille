package com.kmwllc.lucille.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Tests the Indexer's "send failed documents to the fail topic" behavior: the {@code indexer.sendFailedDocsToTopic}
 * gate, the failure metadata stamped on routed documents, and the fact that the Lucille-internal bookkeeping fields
 * (source provenance and failure metadata) are stripped from the document sent to a search index.
 */
public class IndexerFailTopicTest {

  private static final String PIPELINE_NAME = "pipeline1";

  /** A NopIndexer whose send always throws, forcing the whole-batch failure path through sendFailEvent. */
  private static class ErroringNopIndexer extends NopIndexer {

    ErroringNopIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
      super(config, messenger, false, metricsPrefix);
    }

    @Override
    protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
      throw new Exception("Simulated indexing failure");
    }
  }

  private TestMessenger runIndexerOnOneDoc(Config config) throws Exception {
    TestMessenger messenger = new TestMessenger();
    Document doc = Document.create("doc1", "run1");
    messenger.sendForIndexing(doc);

    Indexer indexer = new ErroringNopIndexer(config, messenger, PIPELINE_NAME);
    indexer.run(1);
    return messenger;
  }

  @Test
  public void testFailedDocRoutedToFailTopicWhenEnabled() throws Exception {
    Config config = ConfigFactory.load("IndexerFailTopicTest/sendFailedEnabled.conf");

    TestMessenger messenger = runIndexerOnOneDoc(config);

    List<Document> failed = messenger.getDocsSentToFailTopic();
    assertEquals("the failed document should be routed to the fail topic", 1, failed.size());

    Document failedDoc = failed.get(0);
    assertEquals("doc1", failedDoc.getId());
    assertTrue(failedDoc.has(Document.FAILURE_REASON_FIELD));
    assertEquals("indexer", failedDoc.getString(Document.FAILURE_COMPONENT_FIELD));
    assertEquals(PIPELINE_NAME, failedDoc.getString(Document.FAILURE_PIPELINE_FIELD));
    assertTrue("a failure time should be stamped", failedDoc.has(Document.FAILURE_TIME_FIELD));
  }

  @Test
  public void testFailedDocNotRoutedWhenDisabled() throws Exception {
    Config config = ConfigFactory.load("IndexerFailTopicTest/sendFailedDisabled.conf");

    TestMessenger messenger = runIndexerOnOneDoc(config);

    assertTrue("nothing should be routed to the fail topic when the flag is off",
        messenger.getDocsSentToFailTopic().isEmpty());
  }

  /**
   * The Lucille-internal bookkeeping fields (source provenance, failure metadata) travel with a document but must
   * never be written to the search index. getConvertedIndexerDoc and getRawIndexerDoc are what the concrete indexers
   * send, so both must strip these fields regardless of any field-filter configuration.
   */
  @Test
  public void testInternalFieldsStrippedFromIndexerDoc() throws Exception {
    Config config = ConfigFactory.load("IndexerFailTopicTest/sendFailedEnabled.conf");
    TestMessenger messenger = new TestMessenger();
    FieldAccessIndexer indexer = new FieldAccessIndexer(config, messenger, PIPELINE_NAME);

    Document doc = Document.create("doc1", "run1");
    doc.setField("keepMe", "value");
    // Stamp the internal fields the way Lucille does, via the dedicated reserved-field setters.
    doc.setSourceMetadata("pipeline1_source", 0, 42L);
    doc.setFailureMetadata("some reason", "indexer", Instant.now(), PIPELINE_NAME);

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

    FieldAccessIndexer(Config config, IndexerMessenger messenger, String metricsPrefix) {
      super(config, messenger, false, metricsPrefix);
    }

    Map<String, Object> convertedDoc(Document doc) {
      return getConvertedIndexerDoc(doc);
    }

    Map<String, Object> rawDoc(Document doc) {
      return getRawIndexerDoc(doc);
    }
  }
}
