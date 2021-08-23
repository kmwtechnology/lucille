package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

class Indexer implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(Indexer.class);
  
  private final IndexerMessageManager manager;

  private volatile boolean running = true;

  private Config config;

  public void terminate() {
    running = false;
    log.info("terminate");
  }

  public Indexer(Config config) {
    this.config = config;
    this.manager = MessageManagerFactory.getInstance().getIndexerMessageManager();
  }

  @Override
  public void run() {
    while (running) {
      Document doc;
      try {
        log.info("polling");
        doc = manager.pollCompleted();
      } catch (Exception e) {
        log.info("Indexer interrupted ", e);
        terminate();
        return;
      }
      if (doc == null) {
        log.info("received nothing");
        continue;
      }

      // TODO
      if (!doc.has("run_id")) {
        continue;
      }

      String runId = doc.getString("run_id");
      try {
        manager.sendToSolr(Collections.singletonList(doc));
        Event event = new Event(doc.getId(), runId, "SUCCEEDED", Event.Type.INDEX, Event.Status.SUCCESS);
        log.info("submitting receipt " + event);
        manager.sendEvent(event);
      } catch (Exception e) {
        try {
          manager.sendEvent(new Event(doc.getId(), runId,
            "FAILED" + e.getMessage(), Event.Type.INDEX, Event.Status.FAILURE));
        } catch (Exception e2) {
          e2.printStackTrace();
        }
      }
    }
    try {
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    log.info("exit");
  }

}
