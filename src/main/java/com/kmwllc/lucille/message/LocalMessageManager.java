package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LocalMessageManager implements IndexerMessageManager, PublisherMessageManager, WorkerMessageManager {

  public static final Logger log = LoggerFactory.getLogger(LocalMessageManager.class);

  private Queue<Event> pipelineEvents = new LinkedList<Event>();
  private Queue<Document> pipelineSource = new LinkedList<Document>();
  private Queue<Document> pipelineDest = new LinkedList<Document>();

  private static volatile LocalMessageManager INSTANCE = null;

  public static LocalMessageManager getInstance() {
    if (INSTANCE!=null) {
      return INSTANCE;
    }
    synchronized (LocalMessageManager.class) {
      if (INSTANCE == null) {
        INSTANCE = new LocalMessageManager();
      }
      return INSTANCE;
    }
  }

  @Override
  public synchronized Document pollCompleted() throws Exception {
    return pipelineDest.poll();
  }

  @Override
  public void sendToSolr(List<Document> documents) throws Exception {
    log.info("sendToSolr called (not actually sending): " + documents);
  }

  @Override
  public synchronized Document pollDocToProcess() throws Exception {
    return pipelineSource.poll();
  }

  @Override
  public synchronized void sendCompleted(Document document) throws Exception {
    pipelineDest.add(document);
  }

  @Override
  public synchronized void sendEvent(Event event) throws Exception {
    pipelineEvents.add(event);
  }

  @Override
  public synchronized Event pollEvent() throws Exception {
    return pipelineEvents.poll();
  }

  @Override
  public synchronized boolean hasEvents(String runId) throws Exception {
    return !pipelineEvents.isEmpty();
  }

  @Override
  public synchronized void sendForProcessing(Document document) {
    pipelineSource.add(document);
  }

  @Override
  public void close() {
  }
}
