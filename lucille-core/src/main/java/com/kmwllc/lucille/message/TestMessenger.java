package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The recording of a test-mode run ({@link com.kmwllc.lucille.core.Runner#runInTestMode(com.typesafe.config.Config)}):
 * the documents sent for processing and for indexing, and the events sent, in the order they occurred.
 *
 * <p> This class ships in lucille-core rather than a test artifact so that test mode is available wherever
 * Lucille is, for pipeline development as well as for tests. Its supported API is
 * {@link #getDocsSentForProcessing()}, {@link #getDocsSentForIndexing()} and {@link #getSentEvents()}; the
 * messenger methods it implements are internal to Lucille and may change.
 */
public class TestMessenger implements IndexerMessenger, PublisherMessenger,
    WorkerMessenger {

  private final LocalMessenger messenger;

  private final List<Event> savedEventMessages = Collections.synchronizedList(new ArrayList<Event>());
  private final List<Document> savedSourceMessages = Collections.synchronizedList(new ArrayList<Document>());
  private final List<Document> savedDestMessages = Collections.synchronizedList(new ArrayList<Document>());

  public TestMessenger() {
    this.messenger = new LocalMessenger();
  }

  public TestMessenger(LocalMessenger messenger) {
    this.messenger = messenger;
  }

  @Override
  public Document pollDocToIndex() throws Exception {
    return messenger.pollDocToIndex();
  }

  @Override
  public Document pollDocToProcess() throws Exception {
    return messenger.pollDocToProcess();
  }

  @Override
  public void commitPendingDocOffsets() throws Exception {
    messenger.commitPendingDocOffsets();
  }

  @Override
  public void sendForIndexing(Document document) throws MessengerException {
    savedDestMessages.add(document);
    messenger.sendForIndexing(document);
  }

  @Override
  public void sendFailed(Document document) throws MessengerException {
    messenger.sendFailed(document);
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws MessengerException {
    Event event = new Event(document, message, type);
    sendEvent(event);
  }

  @Override
  public void sendEvent(Event event) throws MessengerException {
    savedEventMessages.add(event);
    messenger.sendEvent(event);
  }

  @Override
  public Event pollEvent() throws Exception {
    return messenger.pollEvent();
  }

  @Override
  public void initialize(String runId, String pipelineName) throws Exception {
    messenger.initialize(runId, pipelineName);
  }

  @Override
  public String getRunId() {
    return messenger.getRunId();
  }


  @Override
  public void sendForProcessing(Document document) throws Exception {
    savedSourceMessages.add(document);
    messenger.sendForProcessing(document);
  }

  @Override
  public void sendEvents(List<Document> documents, String message, Event.Type type) throws Exception {
    for (Document document : documents) {
      sendEvent(document, message, type);
    }
  }

  @Override
  public void close() {
    messenger.close();
  }

  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    messenger.batchComplete(batch);
  }

  // Wraps a LocalMessenger, which is safe for concurrent batches.
  @Override
  public boolean commitsOnBatchCompletion() {
    return true;
  }

  /**
   * Returns the ordered history of all Events sent via sendEvent().
   */
  public List<Event> getSentEvents() {
    return savedEventMessages;
  }

  /**
   * Returns the ordered history of all Documents sent via sendForProcessing().
   */
  public List<Document> getDocsSentForProcessing() {
    return savedSourceMessages;
  }

  /**
   * Returns the ordered history of all Documents sent via sendForIndexing().
   */
  public List<Document> getDocsSentForIndexing() {
    return savedDestMessages;
  }

}
