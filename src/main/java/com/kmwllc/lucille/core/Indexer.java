package com.kmwllc.lucille.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.LogUtils;
import com.kmwllc.lucille.util.SolrUtils;
import com.typesafe.config.Config;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

class Indexer implements Runnable {

  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int DEFAULT_BATCH_TIMEOUT = 100;

  private static final Logger log = LoggerFactory.getLogger(Indexer.class);

  private final IndexerMessageManager manager;
  private final Batch batch;
  private final SolrClient solrClient;

  private volatile boolean running = true;

  private final int logSeconds;

  private final StopWatch stopWatch;
  private final Meter meter;
  private final Histogram histogram;

  private final String idOverrideField;

  private Instant lastLog = Instant.now();

  public void terminate() {
    running = false;
    log.debug("terminate");
  }

  public Indexer(Config config, IndexerMessageManager manager, SolrClient solrClient, String metricsPrefix) {
    this.manager = manager;
    this.solrClient = solrClient;
    this.idOverrideField = config.hasPath("indexer.idOverrideField") ? config.getString("indexer.idOverrideField") : null;
    int batchSize = config.hasPath("indexer.batchSize") ? config.getInt("indexer.batchSize") : DEFAULT_BATCH_SIZE;
    int batchTimeout = config.hasPath("indexer.batchTimeout") ? config.getInt("indexer.batchTimeout") : DEFAULT_BATCH_TIMEOUT;
    this.batch = new Batch(batchSize, batchTimeout);
    this.logSeconds = ConfigUtils.getOrDefault(config, "log.seconds", LogUtils.DEFAULT_LOG_SECONDS);
    MetricRegistry metrics = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);
    this.stopWatch = new StopWatch();
    this.meter = metrics.meter(metricsPrefix + ".indexer.docsIndexed");
    this.histogram = metrics.histogram(metricsPrefix + ".indexer.batchTimeOverSize");
  }

  public Indexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
    this(config, manager, getSolrClient(config, bypass), metricsPrefix);
  }

  private static SolrClient getSolrClient(Config config, boolean bypass) {
    return bypass ? null : SolrUtils.getSolrClient(config);
  }

  public boolean validateConnection() {
    if (solrClient==null) {
      return true;
    }
    SolrPingResponse response = null;
    try {
      response = solrClient.ping();
    } catch (Exception e) {
      log.error("Couldn't ping Solr ", e);
      return false;
    }
    if (response==null) {
      log.error("Null response when pinging solr");
      return false;
    }
    if (response.getStatus()!=0) {
      log.error("Non zero response when pinging solr: " + response.getStatus());
    }
    return true;
  }


  @Override
  public void run() {
    try {
      while (running) {
        checkForDoc();
      }
      sendToSolrWithAccounting(batch.flush()); // handle final batch
    } finally {
      close();
    }
  }

  protected void run(int iterations) {
    try {
      for (int i = 0; i < iterations; i++) {
        checkForDoc();
      }
      sendToSolrWithAccounting(batch.flush()); // handle final batch
    } finally {
      close();
    }
  }

  private void close() {
    if (manager!=null) {
      try {
        manager.close();
      } catch (Exception e) {
        log.error("Error closing message manager", e);
      }
    }

    if (solrClient!=null) {
      try {
        solrClient.close();
      } catch (Exception e) {
        log.error("Error closing SolrClient", e);
      }
    }
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
    if (ChronoUnit.SECONDS.between(lastLog, Instant.now())>logSeconds) {
      log.info(String.format("%d docs indexed. One minute rate: %.2f docs/sec. Mean Solr latency: %.2f ms/doc.",
        meter.getCount(), meter.getOneMinuteRate(), histogram.getSnapshot().getMean()/1000000));
      lastLog = Instant.now();
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
      log.debug("sendToSolr bypassed for documents: " + documents);
      return;
    }

    List<SolrInputDocument> solrDocs = new ArrayList();
    for (Document doc : documents) {

      Map<String,Object> map = doc.asMap();
      SolrInputDocument solrDoc = new SolrInputDocument();

      for (String key : map.keySet()) {

        if (Document.CHILDREN_FIELD.equals(key)) {
          continue;
        }

        // if an id override field has been specified, use its value as the id to send to solr, instead
        // of the document's own id
        if (idOverrideField!=null && Document.ID_FIELD.equals(key) && doc.has(idOverrideField)) {
          solrDoc.setField(Document.ID_FIELD, doc.getString(idOverrideField));
          continue;
        }

        Object value = map.get(key);
        solrDoc.setField(key,value);
      }

      addChildren(doc, solrDoc);
      solrDocs.add(solrDoc);
    }

    stopWatch.reset();
    stopWatch.start();
    solrClient.add(solrDocs);
    stopWatch.stop();
    histogram.update(stopWatch.getNanoTime() / solrDocs.size());
  }

  private void addChildren(Document doc, SolrInputDocument solrDoc) {
    List<Document> children = doc.getChildren();
    if (children==null || children.isEmpty()) {
      return;
    }
    for (Document child : children) {
      Map<String,Object> map = child.asMap();
      SolrInputDocument solrChild = new SolrInputDocument();
      for (String key : map.keySet()) {
        // we don't support children that contain nested children
        if (Document.CHILDREN_FIELD.equals(key)) {
          continue;
        }
        Object value = map.get(key);
        solrChild.setField(key,value);
      }
      solrDoc.addChildDocument(solrChild);
    }
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigUtils.loadConfig();
    String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
    log.info("Starting Indexer for pipeline: " + pipelineName);
    IndexerMessageManager manager = new KafkaIndexerMessageManager(config, pipelineName);
    Indexer indexer = new Indexer(config, manager, false, pipelineName);
    if (!indexer.validateConnection()) {
      log.error("Indexer could not connect");
      System.exit(1);
    }

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
