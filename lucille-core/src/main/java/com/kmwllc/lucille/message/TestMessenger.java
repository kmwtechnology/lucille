package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wrapper around LocalMessenger that stores message traffic so that it can be retrieved later.
 * Intended for use in a testing context.
 */
public class TestMessenger implements IndexerMessenger, PublisherMessenger,
    WorkerMessenger {

  private final LocalMessenger messenger;

  private List<Event> savedEventMessages = Collections.synchronizedList(new ArrayList<Event>());
  private List<Document> savedSourceMessages = Collections.synchronizedList(new ArrayList<Document>());
  private List<Document> savedDestMessages = Collections.synchronizedList(new ArrayList<Document>());

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
  public void sendForIndexing(Document document) throws Exception {
    savedDestMessages.add(document);
    messenger.sendForIndexing(document);
  }

  @Override
  public void sendFailed(Document document) throws Exception {
    messenger.sendFailed(document);
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    Event event = new Event(document, message, type);
    sendEvent(event);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
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
  public void close() {
    messenger.close();
  }

  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    messenger.batchComplete(batch);
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
