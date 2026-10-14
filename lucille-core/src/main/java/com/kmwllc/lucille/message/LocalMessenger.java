package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of the messenger APIs used by Indexers, Publishers, and Workers, suitable for
 * sharing among those three components when executing Lucille in "local" mode.
 */
public class LocalMessenger implements IndexerMessenger, PublisherMessenger, WorkerMessenger {

  private static final Logger log = LoggerFactory.getLogger(LocalMessenger.class);

  public static final int POLL_TIMEOUT_MS = 50;
  public static final int DEFAULT_QUEUE_CAPACITY = 10000;

  private final BlockingQueue<Event> pipelineEvents = new LinkedBlockingQueue<>();
  private final BlockingQueue<Document> pipelineSource;
  private final BlockingQueue<Document> pipelineDest;
  private final AtomicBoolean warnedAboutSendFailed = new AtomicBoolean(false);

  public LocalMessenger() {
    this.pipelineSource = new LinkedBlockingQueue<>();
    this.pipelineDest = new LinkedBlockingQueue<>();
  }

  public LocalMessenger(int capacity) {
    this.pipelineSource = new LinkedBlockingQueue<>(capacity);
    this.pipelineDest = new LinkedBlockingQueue<>(capacity);
  }

  public LocalMessenger(Config config) {
    this.pipelineSource = config.hasPath("publisher.queueCapacity") ?
        new LinkedBlockingQueue<>(config.getInt("publisher.queueCapacity")) :
        new LinkedBlockingQueue<>(DEFAULT_QUEUE_CAPACITY);
    this.pipelineDest = config.hasPath("publisher.queueCapacity") ?
        new LinkedBlockingQueue<>(config.getInt("publisher.queueCapacity")) :
        new LinkedBlockingQueue<>(DEFAULT_QUEUE_CAPACITY);
  }

  private String runId = null;
  private String pipelineName;

  @Override
  public Document pollDocToIndex() throws Exception {
    return pipelineDest.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public Document pollDocToProcess() throws Exception {
    return pipelineSource.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void commitPendingDocOffsets() throws Exception {
  }

  @Override
  public void sendForIndexing(Document document) throws Exception {
    pipelineDest.put(document);
  }

  /**
   * In local mode there is no fail topic (that is a Kafka/Hybrid concept), so failed documents cannot be
   * republished anywhere. This is a no-op by design; the FAIL event and the FailedDocuments log remain the
   * signal for failures in local mode. We log a single warning the first time this is invoked so that an
   * operator who enabled {@code sendFailedDocsToTopic} in local mode is not left thinking docs are being
   * routed to a fail topic.
   */
  @Override
  public void sendFailed(Document document) throws Exception {
    if (warnedAboutSendFailed.compareAndSet(false, true)) {
      log.warn("sendFailed was called in local mode, where there is no fail topic; failed documents are not "
          + "republished. The FAIL event and FailedDocuments log are the only failure signals in local mode. "
          + "The 'sendFailedDocsToTopic' option only takes effect in Kafka/Hybrid modes.");
    }
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    Event event = new Event(document, message, type);
    sendEvent(event);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    pipelineEvents.add(event);
  }

  @Override
  public Event pollEvent() throws Exception {
    return pipelineEvents.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void initialize(String runId, String pipelineName) throws Exception {
    if (this.runId != null) {
      throw new Exception("Already initialized.");
    }
    this.runId = runId;
    this.pipelineName = pipelineName;
  }

  @Override
  public String getRunId() {
    return runId;
  }

  @Override
  public void sendForProcessing(Document document) throws Exception {
    pipelineSource.put(document);
  }

  @Override
  public void sendEvents(List<Document> documents, String message, Event.Type type) throws Exception {
    for (Document document : documents) {
      sendEvent(document, message, type);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void batchComplete(List<Document> batch) throws Exception {
  }
}
