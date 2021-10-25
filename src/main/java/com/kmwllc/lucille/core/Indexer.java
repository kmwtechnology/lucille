package com.kmwllc.lucille.core;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.SolrUtils;
import com.typesafe.config.Config;
import org.apache.solr.client.solrj.SolrClient;
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

  private long numIndexed;

  // Determines the number of documents which must be indexed in between each metric info log.
  private final int logRate;

  private final MetricRegistry metrics;
  private final Meter meter;

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
      this.solrClient = SolrUtils.getSolrClient(config);
    }
    this.logRate = ConfigUtils.getOrDefault(config, "indexer.logRate", 1000);
    this.metrics = SharedMetricRegistries.getOrCreate("default");
    this.meter = metrics.meter("indexer.meter");
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
    // meter.mark();
    numIndexed++;
  }

  private void sendToSolrWithAccounting(List<Document> batchedDocs) {
    if (numIndexed % logRate == 0) {
      log.info(String.format("%d documents have been indexed so far. Documents are currently being indexed at a rate of %.2f documents/second",
          meter.getCount(), meter.getMeanRate()));
    }

    if (batchedDocs.isEmpty()) {
      return;
    }

    try {
      sendToSolr(batchedDocs);
      meter.mark(batchedDocs.size());
    } catch (Exception e) {
      log.error("Error sending documents to solr: " + e.getMessage(), e);

      for (Document d : batchedDocs) {
        try {
          manager.sendEvent(new Event(d.getId(), d.getRunId(),
              "FAILED: " + e.getMessage(), Event.Type.FAIL));
        } catch (Exception e2) {
          // TODO: The run won't be able to finish if this event isn't received; can we do something special here?
          log.error("Couldn't send failure event for doc " + d.getId(), e2);
        }
      }
      return;
    }

    for (Document d : batchedDocs) {
      Event event = new Event(d.getId(), d.getRunId(), "SUCCEEDED", Event.Type.FINISH);
      try {
        manager.sendEvent(event);
      } catch (Exception e) {
        // TODO: The run won't be able to finish if this event isn't received; can we do something special here?
        log.error("Error sending completion event for doc " + d.getId(), e);
      }
    }


  }

  protected void sendToSolr(List<Document> documents) throws Exception {

    if (solrClient==null) {
      // log.info("sendToSolr bypassed for documents: " + documents);
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

  public static void main(String[] args) throws Exception {
    Config config = ConfigUtils.loadConfig();
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
