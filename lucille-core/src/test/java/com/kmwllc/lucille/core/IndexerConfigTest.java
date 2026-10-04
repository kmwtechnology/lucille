package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.CSVIndexer;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Tests validation of the generic {@code indexer} config block performed by the {@link Indexer} base class.
 *
 * <p> Currently this covers only indexer.maxConcurrentBatches: the default, explicit and string values, the lower
 * bound, and the {@link Indexer#supportsConcurrentSends()} capability gate. (Concurrency behavior itself is not
 * exercised here — the dispatcher is still synchronous at this stage.)
 *
 * <p> The Indexer base class also validates other generic parameters that currently have no direct coverage here —
 * the deletionMarkerField / deletionMarkerFieldValue and deleteByFieldField / deleteByFieldValue "both or neither"
 * rules, and the retry parameters (maxRetries greater than 0, the retry sub-parameters requiring maxRetries, and a
 * non-empty retryableStatusCodes). Those predate this test and should be backfilled here in a separate PR.
 */
public class IndexerConfigTest {

  @Test
  public void testDefaultIsOneWhenUnset() {
    assertEquals(1, Indexer.getMaxConcurrentBatches(ConfigFactory.empty()));
  }

  @Test
  public void testReadsExplicitValue() {
    Config config = ConfigFactory.parseMap(Map.of("indexer.maxConcurrentBatches", 4));
    assertEquals(4, Indexer.getMaxConcurrentBatches(config));
  }

  @Test
  public void testReadsStringValue() {
    // An environment substitution always yields a string, which getInt accepts as a number.
    Config config = ConfigFactory.parseMap(Map.of("indexer.maxConcurrentBatches", "3"));
    assertEquals(3, Indexer.getMaxConcurrentBatches(config));
  }

  @Test
  public void testRejectsLessThanOne() {
    assertThrows(IllegalArgumentException.class, () -> new ConcurrentCapableIndexer(configWith(0)));
    assertThrows(IllegalArgumentException.class, () -> new ConcurrentCapableIndexer(configWith(-2)));
  }

  @Test
  public void testRejectsGreaterThanOneWhenUnsupported() {
    Config config = ConfigFactory.parseMap(Map.of(
        "indexer.type", "csv", "indexer.maxConcurrentBatches", 2,
        "csv.columns", List.of("id"), "csv.path", "target/IndexerConfigTest.csv"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new CSVIndexer(config, new TestMessenger(), false, "testing"));
    assertTrue(e.getMessage(), e.getMessage().contains("does not support concurrent sends"));
  }

  @Test
  public void testAcceptsOneWhenUnsupported() {
    Config config = ConfigFactory.parseMap(Map.of(
        "indexer.type", "csv", "indexer.maxConcurrentBatches", 1,
        "csv.columns", List.of("id"), "csv.path", "target/IndexerConfigTest.csv"));
    new CSVIndexer(config, new TestMessenger(), false, "testing").closeConnection();
  }

  @Test
  public void testAcceptsGreaterThanOneWhenSupported() {
    // TestMessenger supports concurrent batches; constructing without throwing is the assertion.
    new ConcurrentCapableIndexer(configWith(2), new TestMessenger());
  }

  @Test
  public void testRejectsGreaterThanOneWhenMessengerUnsupported() {
    // The indexer supports concurrent sends, but the messenger commits at poll (supportsConcurrentBatches() false), so
    // K > 1 must be rejected to avoid losing in-flight documents on a crash.
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new ConcurrentCapableIndexer(configWith(2), new CommitAtPollMessenger()));
    assertTrue(e.getMessage(), e.getMessage().contains("does not support concurrent batches"));
  }

  @Test
  public void testAcceptsGreaterThanOneWithNullMessenger() {
    // A null messenger (as used by some benchmarks) is not a data-loss risk, so concurrency is allowed.
    new ConcurrentCapableIndexer(configWith(2), null);
  }

  private static Config configWith(int maxConcurrentBatches) {
    return ConfigFactory.parseMap(Map.of("indexer.maxConcurrentBatches", maxConcurrentBatches));
  }

  /** A minimal Indexer that opts into concurrent sends, so the capability gate accepts maxConcurrentBatches > 1. */
  private static class ConcurrentCapableIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    ConcurrentCapableIndexer(Config config) {
      this(config, new TestMessenger());
    }

    ConcurrentCapableIndexer(Config config, IndexerMessenger messenger) {
      super(config, messenger, false, "IndexerConfigTest", null);
    }

    @Override
    protected boolean supportsConcurrentSends() {
      return true;
    }

    @Override
    protected String getIndexerConfigKey() {
      return null;
    }

    @Override
    public boolean validateConnection() {
      return true;
    }

    @Override
    public void closeConnection() {
    }

    @Override
    protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) {
      return Set.of();
    }
  }

  /** A messenger that leaves supportsConcurrentBatches() at its unsafe default (false), standing in for one that
   * commits its input at poll. Only that one method matters for these tests; the rest are inert. */
  private static class CommitAtPollMessenger implements IndexerMessenger {
    @Override
    public Document pollDocToIndex() {
      return null;
    }

    @Override
    public void sendEvent(Event event) {
    }

    @Override
    public void sendEvent(Document document, String message, Event.Type type) {
    }

    @Override
    public void sendEvents(List<Document> documents, String message, Event.Type type) {
    }

    @Override
    public void batchComplete(List<Document> batch) {
    }

    @Override
    public void close() {
    }
  }
}
