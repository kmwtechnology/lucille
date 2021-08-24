package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.Document;

import java.util.ArrayList;
import java.util.List;

public class PersistingLocalMessageManager implements IndexerMessageManager, PublisherMessageManager, WorkerMessageManager,
  RunnerMessageManager {

  private final LocalMessageManager manager;

  private List<Event> savedEventMessages= new ArrayList<Event>();
  private List<Document> savedSourceMessages = new ArrayList<Document>();
  private List<Document> savedDestMessages = new ArrayList<Document>();
  private List<Document> savedDocsSentToSolr = new ArrayList<Document>();

  private static volatile PersistingLocalMessageManager INSTANCE = null;

  public static PersistingLocalMessageManager getInstance() {
    if (INSTANCE!=null) {
      return INSTANCE;
    }
    synchronized (PersistingLocalMessageManager.class) {
      if (INSTANCE == null) {
        INSTANCE = new PersistingLocalMessageManager(LocalMessageManager.getInstance());
      }
      return INSTANCE;
    }
  }

  private PersistingLocalMessageManager(LocalMessageManager manager) {
    this.manager = manager;
  }

  @Override
  public synchronized Document pollCompleted() throws Exception {
    return manager.pollCompleted();
  }

  @Override
  public void sendToSolr(List<Document> documents) throws Exception {
    savedDocsSentToSolr.addAll(documents);
    manager.sendToSolr(documents);
  }

  @Override
  public synchronized Document pollDocToProcess() throws Exception {
    return manager.pollDocToProcess();
  }

  @Override
  public synchronized void sendCompleted(Document document) throws Exception {
    savedDestMessages.add(document);
    manager.sendCompleted(document);
  }

  @Override
  public synchronized void sendEvent(Event event) throws Exception {
    savedEventMessages.add(event);
    manager.sendEvent(event);
  }

  @Override
  public synchronized Event pollEvent() throws Exception {
    return manager.pollEvent();
  }

  @Override
  public synchronized boolean hasEvents(String runId) throws Exception {
    return manager.hasEvents(runId);
  }

  @Override
  public synchronized void sendForProcessing(Document document) {
    savedSourceMessages.add(document);
    manager.sendForProcessing(document);
  }

  @Override
  public void close() {
    manager.close();
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

  public List<Document> getSavedDocsSentToSolr() {
    return savedDocsSentToSolr;
  }

}
