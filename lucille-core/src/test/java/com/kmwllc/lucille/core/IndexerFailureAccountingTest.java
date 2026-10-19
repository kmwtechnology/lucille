package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Verifies how {@link Indexer} splits a batch into FAIL and FINISH events when {@code sendToIndex} reports
 * per-document failures.
 */
public class IndexerFailureAccountingTest {

  /**
   * Fails every document whose id is in {@code failIds}, reporting each failure via {@code reportAs}. The
   * returned set is identity-based so that the test indexer itself never hashes a document.
   */
  public static class FailingIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    private final Function<Document, Boolean> shouldFail;
    private final Function<Document, Document> reportAs;

    public FailingIndexer(Config config, IndexerMessenger messenger, Function<Document, Boolean> shouldFail,
        Function<Document, Document> reportAs) {
      super(config, messenger, false, "test", null);
      this.shouldFail = shouldFail;
      this.reportAs = reportAs;
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
    protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) {
      Set<Pair<Document, Exception>> failed = Collections.newSetFromMap(new IdentityHashMap<>());
      for (Document d : documents) {
        if (shouldFail.apply(d)) {
          failed.add(Pair.of(reportAs.apply(d), new Exception("failed " + d.getId())));
        }
      }
      return failed;
    }

    @Override
    public void closeConnection() {
    }
  }

  private static final Config CONFIG = ConfigFactory.parseString("indexer { batchSize: 100, batchTimeout: 60000 }");

  /** Runs the given docs through a FailingIndexer and returns event types keyed by "id:FAIL" / "id:FINISH" counts. */
  private static Map<String, Integer> run(List<Document> docs, Function<Document, Boolean> shouldFail,
      Function<Document, Document> reportAs) throws Exception {
    TestMessenger messenger = new TestMessenger();
    FailingIndexer indexer = new FailingIndexer(CONFIG, messenger, shouldFail, reportAs);
    for (Document d : docs) {
      messenger.sendForIndexing(d);
    }
    indexer.run(docs.size());

    Map<String, Integer> counts = new HashMap<>();
    for (Event e : messenger.getSentEvents()) {
      counts.merge(e.getDocumentId() + ":" + e.getType(), 1, Integer::sum);
    }
    return counts;
  }

  @Test
  public void testPartialFailureSameInstances() throws Exception {
    List<Document> docs = List.of(Document.create("a"), Document.create("b"), Document.create("c"));

    Map<String, Integer> counts = run(docs, d -> d.getId().equals("b"), d -> d);

    assertEquals(Map.of("a:FINISH", 1, "b:FAIL", 1, "c:FINISH", 1), counts);
  }

  @Test
  public void testFailureReportedAsCopyIsStillFailed() throws Exception {
    Document b = Document.create("b");
    b.setField("text", "some extracted text");
    List<Document> docs = List.of(Document.create("a"), b);

    Map<String, Integer> counts = run(docs, d -> d.getId().equals("b"), d -> {
      Document copy = d.deepCopy();
      assertNotSame(d, copy);
      return copy;
    });

    // the copy is equal() to the sent document, so b must get FAIL and no FINISH
    assertEquals(Map.of("a:FINISH", 1, "b:FAIL", 1), counts);
  }

  @Test
  public void testDistinctDocsSharingAnIdAreAccountedSeparately() throws Exception {
    Document first = Document.create("dup");
    first.setField("version", "1");
    Document second = Document.create("dup");
    second.setField("version", "2");

    // only the second version fails; the first must still be reported as succeeded
    Map<String, Integer> counts = run(List.of(first, second), d -> "2".equals(d.getString("version")), d -> d);

    assertEquals(Map.of("dup:FINISH", 1, "dup:FAIL", 1), counts);
  }

  @Test
  public void testFailureAccountingDoesNotHashDocuments() throws Exception {
    AtomicInteger hashCalls = new AtomicInteger();
    List<Document> docs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      docs.add(new JsonDocument("doc" + i) {
        @Override
        public int hashCode() {
          hashCalls.incrementAndGet();
          return super.hashCode();
        }
      });
    }

    Map<String, Integer> counts = run(docs, d -> d.getId().equals("doc3"), d -> d);

    assertEquals(5, counts.values().stream().mapToInt(Integer::intValue).sum());
    assertEquals(Integer.valueOf(1), counts.get("doc3:FAIL"));
    assertEquals(0, hashCalls.get());
  }
}
