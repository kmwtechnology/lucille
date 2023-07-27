package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wrapper around LocalMessageManager that stores message traffic so that it can be retrieved later.
 * Intended for use in a testing context.
 */
public class PersistingLocalMessageManager implements IndexerMessageManager, PublisherMessageManager,
  WorkerMessageManager {

  private final LocalMessageManager manager;

  private List<Event> savedEventMessages = Collections.synchronizedList(new ArrayList<Event>());
  private List<Document> savedSourceMessages = Collections.synchronizedList(new ArrayList<Document>());
  private List<Document> savedDestMessages = Collections.synchronizedList(new ArrayList<Document>());

  public PersistingLocalMessageManager() {
    this.manager = new LocalMessageManager();
  }

  public PersistingLocalMessageManager(LocalMessageManager manager) {
    this.manager = manager;
  }

  @Override
  public Document pollCompleted() throws Exception {
    return manager.pollCompleted();
  }

  @Override
  public Document pollDocToProcess() throws Exception {
    return manager.pollDocToProcess();
  }

  @Override
  public void commitPendingDocOffsets() throws Exception {
    manager.commitPendingDocOffsets();
  }

  @Override
  public void sendCompleted(Document document) throws Exception {
    savedDestMessages.add(document);
    manager.sendCompleted(document);
  }

  @Override
  public void sendFailed(Document document) throws Exception {
    manager.sendFailed(document);
  }

  @Override
  public void sendEvent(Document document, String message, Event.Type type) throws Exception {
    Event event = new Event(document, message, type);
    sendEvent(event);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    savedEventMessages.add(event);
    manager.sendEvent(event);
  }

  @Override
  public Event pollEvent() throws Exception {
    return manager.pollEvent();
  }

  @Override
  public void initialize(String runId, String pipelineName) throws Exception {
    manager.initialize(runId, pipelineName);
  }

  @Override
  public String getRunId() {
    return manager.getRunId();
  }


  @Override
  public void sendForProcessing(Document document) throws Exception {
    savedSourceMessages.add(document);
    manager.sendForProcessing(document);
  }

  @Override
  public void close() {
    manager.close();
  }

  @Override
  public void batchComplete(List<Document> batch) throws Exception {
    manager.batchComplete(batch);
  }

  public List<Event> getSavedEvents() {
    return savedEventMessages;
  }

  public List<Document> getSavedDocumentsSentForProcessing() {
    return savedSourceMessages;
  }

  public List<Document> getSavedCompletedDocuments() {
    return savedDestMessages;
  }

}
