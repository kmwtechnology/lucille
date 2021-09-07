package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class Indexer implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(Indexer.class);

  private final IndexerMessageManager manager;
  private final Batch batch;

  private volatile boolean running = true;

  private Config config;

  public void terminate() {
    running = false;
    log.info("terminate");
  }

  public Indexer(Config config, IndexerMessageManager manager) {
    this.config = config;
    this.manager = manager;
    this.batch = new Batch(config.getInt("indexer.batchSize"), config.getInt("indexer.batchTimeout"));
  }

  @Override
  public void run() {
    while (running) {
      checkForDoc();
    }

    sendToSolr(batch.flush());
    try {
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    log.info("exit");
  }

  protected void run(int iterations) {
    for (int i = 0; i < iterations; i++) {
      checkForDoc();
    }

    sendToSolr(batch.flush());
    try {
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    log.info("exit");
  }

  private void checkForDoc() {
    Document doc;
    try {
      doc = manager.pollCompleted();
    } catch (Exception e) {
      log.info("Indexer interrupted ", e);
      terminate();
      return;
    }
    if (doc == null) {
      sendToSolr(batch.add(null));
      return;
    }

    // TODO
    if (!doc.has("run_id")) {
      return;
    }

    sendToSolr(batch.add(doc));
  }

  private void sendToSolr(List<Document> batchedDocs) {
    if (!batchedDocs.isEmpty()) {
      try {
        manager.sendToSolr(batchedDocs);
      } catch (Exception e) {
        for (Document d : batchedDocs) {
          try {
            manager.sendEvent(new Event(d.getId(), d.getRunID(),
                "FAILED" + e.getMessage(), Event.Type.INDEX, Event.Status.FAILURE));
          } catch (Exception e2) {
            // TODO : Do something special if we get an error when sending Failure events
            e2.printStackTrace();
          }
        }
      }

      try {
        for (Document d : batchedDocs) {
          Event event = new Event(d.getId(), d.getRunID(), "SUCCEEDED", Event.Type.INDEX, Event.Status.SUCCESS);
          log.info("submitting completion event " + event);
          manager.sendEvent(event);
        }
      } catch (Exception e) {
        // TODO : Do something special if we get an error when sending Success events
        e.printStackTrace();
      }
    }
  }

  public static Indexer startThread(Config config, IndexerMessageManager manager) throws Exception {
    Indexer indexer = new Indexer(config, manager);
    Thread indexerThread = new Thread(indexer);
    indexerThread.start();
    return indexer;
  }

}
