package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LocalMessageManager implements IndexerMessageManager, PublisherMessageManager, WorkerMessageManager {

  public static final Logger log = LoggerFactory.getLogger(LocalMessageManager.class);

  private final Queue<Event> pipelineEvents = new ConcurrentLinkedQueue<>();
  private final Queue<Document> pipelineSource = new ConcurrentLinkedQueue<>();
  private final Queue<Document> pipelineDest = new ConcurrentLinkedQueue<>();

  private String runId = null;
  private String pipelineName;

  @Override
  public Document pollCompleted() throws Exception {
    return pipelineDest.poll();
  }

  @Override
  public Document pollDocToProcess() throws Exception {
    return pipelineSource.poll();
  }

  @Override
  public void sendCompleted(Document document) throws Exception {
    pipelineDest.add(document);
  }

  @Override
  public void sendEvent(Event event) throws Exception {
    pipelineEvents.add(event);
  }

  @Override
  public Event pollEvent() throws Exception {
    return pipelineEvents.poll();
  }

  @Override
  public boolean hasEvents() throws Exception {
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
  public void sendForProcessing(Document document) {
    pipelineSource.add(document);
  }

  @Override
  public void close() {
  }
}
