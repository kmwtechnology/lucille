package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.List;

class Indexer implements Runnable {

  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int DEFAULT_BATCH_TIMEOUT = 100;

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
    int batchSize = config.hasPath("indexer.batchSize") ? config.getInt("indexer.batchSize") : DEFAULT_BATCH_SIZE;
    int batchTimeout = config.hasPath("indexer.batchTimeout") ? config.getInt("indexer.batchTimeout") : DEFAULT_BATCH_TIMEOUT;
    this.batch = new Batch(batchSize, batchTimeout);
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

    if (!doc.has("run_id")) {
      log.error("Received document without run_id. Doc ID: " + doc.getId());
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
            manager.sendEvent(new Event(d.getId(), d.getRunId(),
                "FAILED" + e.getMessage(), Event.Type.FAIL));
          } catch (Exception e2) {
            // TODO : Do something special if we get an error when sending Failure events
            e2.printStackTrace();
          }
        }
        return;
      }

      try {
        for (Document d : batchedDocs) {
          Event event = new Event(d.getId(), d.getRunId(), "SUCCEEDED", Event.Type.FINISH);
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

  public static void main(String[] args) throws Exception {
    Config config = ConfigAccessor.loadConfig();
    String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
    log.info("Starting Indexer for pipeline: " + pipelineName);
    IndexerMessageManager manager = new KafkaIndexerMessageManager(config, pipelineName);
    Indexer indexer = new Indexer(config, manager);
    Thread indexerThread = new Thread(indexer);
    indexerThread.start();

    Signal.handle(new Signal("INT"), signal -> {
      indexer.terminate();
      log.info("Indexer shutting down");
      try {
        indexerThread.join();
      } catch (InterruptedException e) {
        log.error("Interrupted", e);
      }
      System.exit(0);
    });
  }

}
