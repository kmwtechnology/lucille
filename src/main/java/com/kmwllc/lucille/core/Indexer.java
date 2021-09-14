package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.typesafe.config.Config;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class Indexer implements Runnable {

  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int DEFAULT_BATCH_TIMEOUT = 100;

  private static final Logger log = LoggerFactory.getLogger(Indexer.class);

  private final IndexerMessageManager manager;
  private final Batch batch;
  private final SolrClient solrClient;

  private volatile boolean running = true;

  private Config config;

  public void terminate() {
    running = false;
    log.info("terminate");
  }

  public Indexer(Config config, IndexerMessageManager manager, boolean bypass) {
    this.config = config;
    this.manager = manager;
    int batchSize = config.hasPath("indexer.batchSize") ? config.getInt("indexer.batchSize") : DEFAULT_BATCH_SIZE;
    int batchTimeout = config.hasPath("indexer.batchTimeout") ? config.getInt("indexer.batchTimeout") : DEFAULT_BATCH_TIMEOUT;
    this.batch = new Batch(batchSize, batchTimeout);
    if (bypass) {
      this.solrClient = null;
    } else {
      this.solrClient = new HttpSolrClient.Builder(config.getString("solr.url")).build();
    }
  }

  @Override
  public void run() {
    while (running) {
      checkForDoc();
    }

    sendToSolrWithAccounting(batch.flush());
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

    sendToSolrWithAccounting(batch.flush());
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
      sendToSolrWithAccounting(batch.add(null));
      return;
    }

    if (!doc.has("run_id")) {
      log.error("Received document without run_id. Doc ID: " + doc.getId());
      return;
    }

    sendToSolrWithAccounting(batch.add(doc));
  }

  private void sendToSolrWithAccounting(List<Document> batchedDocs) {
    if (!batchedDocs.isEmpty()) {
      try {
        sendToSolr(batchedDocs);
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

  protected void sendToSolr(List<Document> documents) throws Exception {

    if (solrClient==null) {
      log.info("sendToSolr bypassed for documents: " + documents);
      return;
    }

    List<SolrInputDocument> solrDocs = new ArrayList();
    for (Document doc : documents) {
      Map<String,Object> map = doc.asMap();
      SolrInputDocument solrDoc = new SolrInputDocument();
      for (String key : map.keySet()) {
        Object value = map.get(key);
        solrDoc.setField(key,value);
      }
      solrDocs.add(solrDoc);
    }
    solrClient.add(solrDocs);
  }


  public static Indexer startThread(Config config, IndexerMessageManager manager, boolean bypass) throws Exception {
    Indexer indexer = new Indexer(config, manager, bypass);
    Thread indexerThread = new Thread(indexer);
    indexerThread.start();
    return indexer;
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigAccessor.loadConfig();
    String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
    log.info("Starting Indexer for pipeline: " + pipelineName);
    IndexerMessageManager manager = new KafkaIndexerMessageManager(config, pipelineName);
    Indexer indexer = new Indexer(config, manager, false);
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
