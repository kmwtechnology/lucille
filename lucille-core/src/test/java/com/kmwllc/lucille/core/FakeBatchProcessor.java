package com.kmwllc.lucille.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.MDC;

/**
 * A {@link BatchProcessor} for testing a {@link BatchDispatcher} on its own, without an Indexer. It records what the
 * dispatcher does with each batch: which thread each send and completion ran on, how many sends ran at once, the order
 * of completions, and the outcomes passed through. Sends can be held on a latch until the test releases them, made to
 * return a failed outcome, or made to throw (which the contract forbids, so the dispatcher's handling of that can be
 * checked).
 *
 * <p> Batches are identified by a label: every document in a batch carries it in {@link #BATCH_FIELD}, and the label of
 * a batch is read from its first document (see {@link #batch(String, String...)}).
 */
final class FakeBatchProcessor implements BatchProcessor {

  /** The document field carrying the label of the batch the document belongs to. */
  static final String BATCH_FIELD = "batch";
  /** The MDC key whose value on the sending thread is recorded for each send, to check MDC propagation. */
  static final String MDC_KEY = "testKey";

  private final Map<String, CountDownLatch> holds = new ConcurrentHashMap<>();
  private final Map<String, Throwable> sendFailures = new ConcurrentHashMap<>();
  private final Set<String> throwingSends = ConcurrentHashMap.newKeySet();

  private final List<String> sendsStarted = Collections.synchronizedList(new ArrayList<>());
  private final List<String> sendsFinished = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, String> sendThreads = new ConcurrentHashMap<>();
  private final Map<String, String> sendMdcValues = new ConcurrentHashMap<>();
  private final Map<String, SendOutcome> sentOutcomes = new ConcurrentHashMap<>();
  private final Set<String> interruptedSends = ConcurrentHashMap.newKeySet();
  private final AtomicInteger concurrentSends = new AtomicInteger();
  private final AtomicInteger peakConcurrentSends = new AtomicInteger();

  private final List<String> completed = Collections.synchronizedList(new ArrayList<>());
  private final Map<String, SendOutcome> completedOutcomes = new ConcurrentHashMap<>();
  private final Map<String, String> completionThreads = new ConcurrentHashMap<>();
  private final AtomicInteger concurrentCompletions = new AtomicInteger();
  private volatile boolean completionsOverlapped;

  /** A batch with the given label, holding one document per id. */
  static List<Document> batch(String label, String... ids) {
    List<Document> docs = new ArrayList<>();
    for (String id : ids) {
      Document doc = Document.create(id);
      doc.setField(BATCH_FIELD, label);
      docs.add(doc);
    }
    return docs;
  }

  private static String label(List<Document> docs) {
    return docs.get(0).getString(BATCH_FIELD);
  }

  @Override
  public SendOutcome sendWithRetry(List<Document> batchedDocs) {
    String label = label(batchedDocs);
    int now = concurrentSends.incrementAndGet();
    peakConcurrentSends.accumulateAndGet(now, Math::max);
    sendThreads.put(label, Thread.currentThread().getName());
    sendMdcValues.put(label, String.valueOf(MDC.get(MDC_KEY)));
    sendsStarted.add(label);
    SendOutcome outcome;
    try {
      CountDownLatch hold = holds.get(label);
      if (hold != null) {
        hold.await();
      }
      if (throwingSends.contains(label)) {
        throw new IllegalStateException("send of " + label + " threw");
      }
      Throwable failure = sendFailures.get(label);
      outcome = failure != null ? new SendOutcome(null, failure, 1) : new SendOutcome(Set.of(), null, 1);
    } catch (InterruptedException e) {
      interruptedSends.add(label);
      outcome = new SendOutcome(null, e, 1);
    } finally {
      concurrentSends.decrementAndGet();
      sendsFinished.add(label);
    }
    sentOutcomes.put(label, outcome);
    return outcome;
  }

  @Override
  public void completeBatch(List<Document> batchedDocs, SendOutcome outcome) {
    if (concurrentCompletions.incrementAndGet() > 1) {
      completionsOverlapped = true;
    }
    try {
      String label = label(batchedDocs);
      completionThreads.put(label, Thread.currentThread().getName());
      completed.add(label);
      completedOutcomes.put(label, outcome);
    } finally {
      concurrentCompletions.decrementAndGet();
    }
  }

  // ---- controls ----

  /** Holds the sends of the given batches until released. */
  void hold(String... labels) {
    for (String label : labels) {
      holds.put(label, new CountDownLatch(1));
    }
  }

  void release(String... labels) {
    for (String label : labels) {
      CountDownLatch hold = holds.get(label);
      if (hold != null) {
        hold.countDown();
      }
    }
  }

  void releaseAll() {
    holds.values().forEach(CountDownLatch::countDown);
  }

  /** Makes the send of the given batch return a failed outcome carrying the given error. */
  void failSend(String label, Throwable failure) {
    sendFailures.put(label, failure);
  }

  /** Makes the send of the given batch throw, in violation of the BatchProcessor contract. */
  void throwFromSend(String label) {
    throwingSends.add(label);
  }

  // ---- observations ----

  boolean hasStartedSend(String label) {
    return sendsStarted.contains(label);
  }

  boolean hasStartedSends(String... labels) {
    return sendsStarted.containsAll(List.of(labels));
  }

  boolean hasFinishedSend(String label) {
    return sendsFinished.contains(label);
  }

  boolean wasInterrupted(String label) {
    return interruptedSends.contains(label);
  }

  /** The name of the thread the given batch's send ran on, or null if it was never sent. */
  String sendThread(String label) {
    return sendThreads.get(label);
  }

  String sendMdcValue(String label) {
    return sendMdcValues.get(label);
  }

  SendOutcome sentOutcome(String label) {
    return sentOutcomes.get(label);
  }

  int concurrentSends() {
    return concurrentSends.get();
  }

  int peakConcurrentSends() {
    return peakConcurrentSends.get();
  }

  /** The labels of the completed batches, in completion order. */
  List<String> completed() {
    synchronized (completed) {
      return new ArrayList<>(completed);
    }
  }

  SendOutcome completedOutcome(String label) {
    return completedOutcomes.get(label);
  }

  /** The name of the thread the given batch's completion ran on, or null if it was never completed. */
  String completionThread(String label) {
    return completionThreads.get(label);
  }

  /** The names of all threads any completion ran on. */
  Set<String> completionThreads() {
    return new HashSet<>(completionThreads.values());
  }

  boolean completionsOverlapped() {
    return completionsOverlapped;
  }
}
