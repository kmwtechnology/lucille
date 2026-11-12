package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Test;

/**
 * Tests how the Indexer base class retries a batch: what each attempt's {@link PendingSend} holds, and how the
 * outcomes of the attempts are combined into the batch's events.
 */
public class IndexerRetryTest {

  // Two retries, so up to three attempts.
  private static final Config TWO_RETRIES =
      ConfigFactory.parseString("batchSize: 10, maxRetries: 2, retryWaitDurationMs: 1");

  private static IndexerFailure retryable(Document doc) {
    return new IndexerFailure(doc, new IndexerRetryableException(503, "unavailable " + doc.getId(), null));
  }

  private static IndexerFailure permanent(Document doc) {
    return new IndexerFailure(doc, new IndexerException("rejected " + doc.getId()));
  }

  private static List<Document> docs(String... ids) {
    List<Document> docs = new ArrayList<>();
    for (String id : ids) {
      docs.add(Document.create(id, "run1"));
    }
    return docs;
  }

  /** Sends the documents to the indexer as one batch and runs it to completion. */
  private static void runBatch(Indexer indexer, TestMessenger messenger, List<Document> docs) throws Exception {
    for (Document doc : docs) {
      messenger.sendForIndexing(doc);
    }
    indexer.run(docs.size());
  }

  private static List<String> idsWithEvent(TestMessenger messenger, Event.Type type) {
    return messenger.getSentEvents().stream()
        .filter(e -> e.getType() == type)
        .map(Event::getDocumentId)
        .sorted()
        .toList();
  }

  private static Map<String, String> failMessages(TestMessenger messenger) {
    return messenger.getSentEvents().stream()
        .filter(e -> e.getType() == Event.Type.FAIL)
        .collect(Collectors.toMap(Event::getDocumentId, Event::getMessage));
  }

  @Test
  public void testRetryReceivesOnlyRetryablyFailedDocumentsInInputOrder() throws Exception {
    TestMessenger messenger = new TestMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> send.attempt() == 1
        // reported out of input order
        ? List.of(retryable(send.documents().get(3)), retryable(send.documents().get(1)))
        : List.of());

    runBatch(indexer, messenger, docs("a", "b", "c", "d"));

    assertEquals(List.of(List.of("a", "b", "c", "d"), List.of("b", "d")), indexer.sentIds());
    assertEquals(List.of("a", "b", "c", "d"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of(), idsWithEvent(messenger, Event.Type.FAIL));
  }

  @Test
  public void testRetrySendsTheSameDocumentInstances() throws Exception {
    TestMessenger messenger = new TestMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger,
        send -> send.attempt() == 1 ? List.of(retryable(send.documents().get(1))) : List.of());

    runBatch(indexer, messenger, docs("a", "b"));

    assertEquals(2, indexer.sends.size());
    assertTrue(indexer.sends.get(0).documents().get(1) == indexer.sends.get(1).documents().get(0));
  }

  @Test
  public void testPermanentFailureIsNotResentAndIsInTheResult() throws Exception {
    TestMessenger messenger = new TestMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> send.attempt() == 1
        ? List.of(permanent(send.documents().get(0)), retryable(send.documents().get(1)))
        : List.of());

    runBatch(indexer, messenger, docs("a", "b", "c"));

    assertEquals(List.of(List.of("a", "b", "c"), List.of("b")), indexer.sentIds());
    assertEquals(List.of("b", "c"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("a"), idsWithEvent(messenger, Event.Type.FAIL));
    assertTrue(failMessages(messenger).get("a").contains("rejected a"));
  }

  @Test
  public void testPermanentFailureOnALaterAttemptIsInTheResult() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // a and b fail retryably; on the retry a is rejected for good and b fails retryably again; then b is applied.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> switch (send.attempt()) {
      case 1 -> List.of(retryable(send.documents().get(0)), retryable(send.documents().get(1)));
      case 2 -> List.of(permanent(send.documents().get(0)), retryable(send.documents().get(1)));
      default -> List.of();
    });

    runBatch(indexer, messenger, docs("a", "b", "c"));

    assertEquals(List.of(List.of("a", "b", "c"), List.of("a", "b"), List.of("b")), indexer.sentIds());
    assertEquals(List.of("b", "c"), idsWithEvent(messenger, Event.Type.FINISH));
    assertTrue(failMessages(messenger).get("a").contains("rejected a"));
  }

  @Test
  public void testAttemptAndIsFinalAcrossAttempts() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // a fails retryably on every attempt
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger,
        send -> List.of(retryable(send.documents().get(0))));

    runBatch(indexer, messenger, docs("a", "b"));

    // b was applied by the first attempt and is never resent
    assertEquals(List.of(List.of("a", "b"), List.of("a"), List.of("a")), indexer.sentIds());
    assertEquals(List.of(1, 2, 3), indexer.sends.stream().map(PendingSend::attempt).toList());
    assertEquals(List.of(false, false, true), indexer.sends.stream().map(PendingSend::isFinal).toList());
    assertEquals(List.of("b"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("a"), idsWithEvent(messenger, Event.Type.FAIL));
    assertTrue(failMessages(messenger).get("a").contains("unavailable a"));
  }

  @Test
  public void testThrownRetryableExceptionRetriesWithTheSameDocuments() throws Exception {
    TestMessenger messenger = new TestMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> {
      if (send.attempt() == 1) {
        throw new IndexerRetryableException(503, "unavailable", null);
      }
      return List.of();
    });

    runBatch(indexer, messenger, docs("a", "b"));

    assertEquals(List.of(List.of("a", "b"), List.of("a", "b")), indexer.sentIds());
    assertEquals(List.of(1, 2), indexer.sends.stream().map(PendingSend::attempt).toList());
    assertEquals(List.of("a", "b"), idsWithEvent(messenger, Event.Type.FINISH));
  }

  @Test
  public void testThrownExceptionAfterProgressFailsOnlyThePendingDocuments() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // First attempt: a is rejected for good, b fails retryably, c is applied. Second attempt (b alone) throws.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> {
      if (send.attempt() == 1) {
        return List.of(permanent(send.documents().get(0)), retryable(send.documents().get(1)));
      }
      throw new IndexerException("connection refused");
    });

    runBatch(indexer, messenger, docs("a", "b", "c"));

    assertEquals(List.of(List.of("a", "b", "c"), List.of("b")), indexer.sentIds());
    assertEquals(List.of("c"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("a", "b"), idsWithEvent(messenger, Event.Type.FAIL));
    assertTrue(failMessages(messenger).get("a").contains("rejected a"));
    assertTrue(failMessages(messenger).get("b").contains("connection refused"));
  }

  @Test
  public void testThrownRetryableExceptionAfterProgressFailsOnlyThePendingDocumentsWhenRetriesRunOut()
      throws Exception {
    TestMessenger messenger = new TestMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> {
      if (send.attempt() == 1) {
        return List.of(retryable(send.documents().get(0)));
      }
      throw new IndexerRetryableException(503, "still unavailable", null);
    });

    runBatch(indexer, messenger, docs("a", "b"));

    assertEquals(List.of(List.of("a", "b"), List.of("a"), List.of("a")), indexer.sentIds());
    assertEquals(List.of("b"), idsWithEvent(messenger, Event.Type.FINISH));
    assertTrue(failMessages(messenger).get("a").contains("still unavailable"));
  }

  @Test
  public void testThrownExceptionWithNoProgressFailsTheWholeBatch() throws Exception {
    TestMessenger messenger = new TestMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> {
      throw new IndexerException("connection refused");
    });

    runBatch(indexer, messenger, docs("a", "b"));

    // not retryable, so one attempt
    assertEquals(List.of(List.of("a", "b")), indexer.sentIds());
    assertEquals(List.of(), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("a", "b"), idsWithEvent(messenger, Event.Type.FAIL));
  }

  @Test
  public void testThrownExceptionAfterAttemptsThatResolvedNothingFailsTheWholeBatch() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // Every document fails retryably, then the retry throws: nothing was ever resolved.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> {
      if (send.attempt() == 1) {
        return send.documents().stream().map(IndexerRetryTest::retryable).toList();
      }
      throw new IndexerException("connection refused");
    });

    runBatch(indexer, messenger, docs("a", "b"));

    assertEquals(List.of(List.of("a", "b"), List.of("a", "b")), indexer.sentIds());
    assertEquals(List.of("a", "b"), idsWithEvent(messenger, Event.Type.FAIL));
    failMessages(messenger).values().forEach(message -> assertTrue(message.contains("connection refused")));
  }

  @Test
  public void testNotSentDocumentStaysPending() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // a fails retryably, so the indexer holds b back; c was applied.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> send.attempt() == 1
        ? List.of(retryable(send.documents().get(0)), IndexerFailure.notSent(send.documents().get(1)))
        : List.of());

    runBatch(indexer, messenger, docs("a", "b", "c"));

    assertEquals(List.of(List.of("a", "b", "c"), List.of("a", "b")), indexer.sentIds());
    assertEquals(List.of("a", "b", "c"), idsWithEvent(messenger, Event.Type.FINISH));
  }

  @Test
  public void testNotSentDocumentAloneDoesNotTriggerARetry() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // a is rejected for good and b is held back. With no retryable failure, no retry is made.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger,
        send -> List.of(permanent(send.documents().get(0)), IndexerFailure.notSent(send.documents().get(1))));

    runBatch(indexer, messenger, docs("a", "b", "c"));

    assertEquals(List.of(List.of("a", "b", "c")), indexer.sentIds());
    assertEquals(List.of("c"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("a", "b"), idsWithEvent(messenger, Event.Type.FAIL));
    assertTrue(failMessages(messenger).get("a").contains("rejected a"));
    assertTrue(failMessages(messenger).get("b").contains("Not sent"));
  }

  @Test
  public void testNotSentDocumentFailsWithTheBlockingCauseWhenRetriesRunOut() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // On every attempt b fails retryably, and a (reported first) and c are held back.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger,
        send -> List.of(IndexerFailure.notSent(send.documents().get(0)), retryable(send.documents().get(1)),
            IndexerFailure.notSent(send.documents().get(2))));

    runBatch(indexer, messenger, docs("a", "b", "c"));

    assertEquals(3, indexer.sends.size());
    assertEquals(List.of("a", "b", "c"), idsWithEvent(messenger, Event.Type.FAIL));
    Map<String, String> messages = failMessages(messenger);
    assertTrue(messages.get("b").contains("unavailable b"));
    for (String id : List.of("a", "c")) {
      assertTrue(messages.get(id), messages.get(id).contains("Not sent"));
      assertTrue(messages.get(id), messages.get(id).contains("unavailable b"));
    }
  }

  @Test
  public void testIsNotSent() {
    Document doc = Document.create("a");
    assertTrue(IndexerFailure.notSent(doc).isNotSent());
    assertFalse(permanent(doc).isNotSent());
    assertFalse(retryable(doc).isNotSent());
  }

  @Test
  public void testUnmatchedFailureStopsRetriesAndFailsTheBatch() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // A failure for a document that is not in the send, alongside a retryable one.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger,
        send -> List.of(retryable(send.documents().get(0)), permanent(Document.create("stranger"))));

    runBatch(indexer, messenger, docs("a", "b"));

    assertEquals(List.of(List.of("a", "b")), indexer.sentIds());
    assertEquals(List.of(), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("a", "b"), idsWithEvent(messenger, Event.Type.FAIL));
  }

  @Test
  public void testFailureForAnAlreadyResolvedDocumentIsUnmatched() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // The retry holds only b, but reports a failure for a, which the first attempt applied.
    List<Document> docs = docs("a", "b");
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> send.attempt() == 1
        ? List.of(retryable(docs.get(1)))
        : List.of(retryable(docs.get(0))));

    runBatch(indexer, messenger, docs);

    // a was not in the send that made the bad report, so its result stands. b was, and can't be counted as applied.
    assertEquals(List.of(List.of("a", "b"), List.of("b")), indexer.sentIds());
    assertEquals(List.of("a"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("b"), idsWithEvent(messenger, Event.Type.FAIL));
  }

  @Test
  public void testUnmatchedFailureOnALaterAttemptFailsOnlyThatSend() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // First attempt: a is applied, b is rejected for good, and c, d and e fail retryably. The retry (c, d, e) reports
    // c with a retryable failure of its own, and a failure for a document that is not in the send.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> send.attempt() == 1
        ? List.of(permanent(send.documents().get(1)), retryable(send.documents().get(2)),
            retryable(send.documents().get(3)), retryable(send.documents().get(4)))
        : List.of(retryable(send.documents().get(0)), permanent(Document.create("stranger"))));

    runBatch(indexer, messenger, docs("a", "b", "c", "d", "e"));

    // no third attempt, although c's failure is retryable
    assertEquals(List.of(List.of("a", "b", "c", "d", "e"), List.of("c", "d", "e")), indexer.sentIds());
    assertEquals(List.of("a"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("b", "c", "d", "e"), idsWithEvent(messenger, Event.Type.FAIL));
    Map<String, String> messages = failMessages(messenger);
    assertTrue(messages.get("b").contains("rejected b"));
    assertTrue(messages.get("c").contains("unavailable c"));
    for (String id : List.of("d", "e")) {
      assertTrue(messages.get(id), messages.get(id).contains("could not be matched"));
    }
  }

  @Test
  public void testRetriesDisabledMakesOneCallWithTheWholeBatch() throws Exception {
    TestMessenger messenger = new TestMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(ConfigFactory.parseString("batchSize: 10"), messenger,
        send -> List.of(retryable(send.documents().get(0))));

    runBatch(indexer, messenger, docs("a", "b"));

    assertEquals(List.of(List.of("a", "b")), indexer.sentIds());
    assertEquals(1, indexer.sends.get(0).attempt());
    assertTrue(indexer.sends.get(0).isFinal());
    assertEquals(List.of("b"), idsWithEvent(messenger, Event.Type.FINISH));
    assertEquals(List.of("a"), idsWithEvent(messenger, Event.Type.FAIL));
  }

  @Test
  public void testPendingSendOf() {
    List<Document> docs = docs("a", "b");
    PendingSend send = PendingSend.of(docs);
    assertEquals(1, send.attempt());
    assertTrue(send.isFinal());
    assertEquals(2, send.documents().size());
    assertTrue(send.documents().get(0) == docs.get(0));
    assertTrue(send.documents().get(1) == docs.get(1));
  }

  @Test
  public void testOwnershipIsCheckedOnTheWholeBatchBeforeEachAttempt() throws Exception {
    OwnershipMessenger messenger = new OwnershipMessenger();
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger,
        send -> send.attempt() == 1 ? List.of(retryable(send.documents().get(1))) : List.of());

    runBatch(indexer, messenger, docs("a", "b"));

    // the retry sends only b, but a's input is checked again too
    assertEquals(List.of(List.of("a", "b"), List.of("b")), indexer.sentIds());
    assertEquals(List.of("a", "b", "a", "b"), messenger.ownershipChecks);
    assertEquals(List.of("a", "b"), idsWithEvent(messenger, Event.Type.FINISH));
  }

  @Test
  public void testLosingOwnershipOfAnAppliedDocumentStopsTheRetry() throws Exception {
    OwnershipMessenger messenger = new OwnershipMessenger();
    // a is applied by the first attempt; its input is lost before the retry, which would send only b.
    ScriptedIndexer indexer = new ScriptedIndexer(TWO_RETRIES, messenger, send -> {
      messenger.unowned = "a";
      return List.of(retryable(send.documents().get(1)));
    });

    runBatch(indexer, messenger, docs("a", "b"));

    assertEquals(List.of(List.of("a", "b")), indexer.sentIds());
    assertEquals(List.of(), messenger.getSentEvents());
    assertEquals(List.of(List.of("a", "b")), messenger.batchesNotSent);
  }

  @Test
  public void testConcurrentBatchesDoNotShareState() throws Exception {
    TestMessenger messenger = new TestMessenger();
    // Batches of three, sent four at a time. In each batch the second document fails retryably on the first attempt and
    // the third on the first two attempts. The sends take long enough for the batches' attempts to interleave.
    Config config = ConfigFactory.parseString(
        "batchSize: 3, maxConcurrentBatches: 4, maxRetries: 2, retryWaitDurationMs: 1");
    ScriptedIndexer indexer = new ScriptedIndexer(config, messenger, send -> {
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return send.documents().stream()
          .filter(doc -> doc.getInt("failUntil") >= send.attempt())
          .map(IndexerRetryTest::retryable)
          .toList();
    });

    int batches = 40;
    List<Document> docs = new ArrayList<>();
    for (int batch = 0; batch < batches; batch++) {
      for (int position = 0; position < 3; position++) {
        Document doc = Document.create(batch + "-" + position, "run1");
        doc.setField("failUntil", position);
        docs.add(doc);
      }
    }
    runBatch(indexer, messenger, docs);

    assertEquals(batches * 3, idsWithEvent(messenger, Event.Type.FINISH).size());
    assertEquals(List.of(), idsWithEvent(messenger, Event.Type.FAIL));

    // Every batch was sent three times, each time with exactly its own pending documents.
    assertEquals(batches * 3, indexer.sends.size());
    for (int batch = 0; batch < batches; batch++) {
      String prefix = batch + "-";
      Map<Integer, List<String>> idsByAttempt = indexer.sends.stream()
          .filter(send -> send.documents().get(0).getId().startsWith(prefix))
          .collect(Collectors.toMap(PendingSend::attempt,
              send -> send.documents().stream().map(Document::getId).toList()));
      assertEquals(Map.of(
          1, List.of(prefix + 0, prefix + 1, prefix + 2),
          2, List.of(prefix + 1, prefix + 2),
          3, List.of(prefix + 2)), idsByAttempt);
    }
  }

  /** What a ScriptedIndexer does with each send. */
  private interface Script {
    List<IndexerFailure> apply(PendingSend send) throws IndexerException;
  }

  /** An Indexer that records every PendingSend it is given, and answers it as its script says. */
  private static class ScriptedIndexer extends Indexer {

    public static final Spec SPEC = SpecBuilder.indexer().build();

    private final Script script;
    final List<PendingSend> sends = Collections.synchronizedList(new ArrayList<>());

    ScriptedIndexer(Config config, TestMessenger messenger, Script script) {
      // a metrics prefix of its own, so that tests do not share metrics
      super(config, new IndexerContext(messenger, false, "retryTest-" + UUID.randomUUID(), null));
      this.script = script;
    }

    /** The ids of the documents in each send, in the order the sends were made. */
    List<List<String>> sentIds() {
      return sends.stream().map(send -> send.documents().stream().map(Document::getId).toList()).toList();
    }

    @Override
    protected String getIndexerConfigKey() {
      return null;
    }

    @Override
    protected boolean supportsConcurrentSends() {
      return true;
    }

    @Override
    protected boolean checkConnection() {
      return true;
    }

    @Override
    public void closeConnection() {
    }

    @Override
    protected List<IndexerFailure> sendToIndex(PendingSend send) throws IndexerException {
      sends.add(send);
      return script.apply(send);
    }
  }

  /** A messenger that records each ownership check, and can be told that it no longer owns a document's input. */
  private static class OwnershipMessenger extends TestMessenger {

    final List<String> ownershipChecks = new ArrayList<>();
    final List<List<String>> batchesNotSent = new ArrayList<>();
    volatile String unowned;

    @Override
    public boolean ownsInput(Document doc) {
      ownershipChecks.add(doc.getId());
      return !doc.getId().equals(unowned);
    }

    @Override
    public void batchNotSent(List<Document> batch) {
      batchesNotSent.add(batch.stream().map(Document::getId).toList());
    }
  }
}
