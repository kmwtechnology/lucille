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
  private String runId = null;
  private String pipelineName;

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
  public synchronized boolean hasEvents() throws Exception {
    return !pipelineEvents.isEmpty();
  }

  @Override
  public void initialize(String runId, String pipelineName) throws Exception {
    if (this.runId!=null) {
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
  public synchronized void sendForProcessing(Document document) {
    pipelineSource.add(document);
  }

  @Override
  public void close() {
  }
}
