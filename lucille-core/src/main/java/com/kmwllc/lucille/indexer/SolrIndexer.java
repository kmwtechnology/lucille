package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.SolrUtils;
import com.typesafe.config.Config;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(SolrIndexer.class);

  private final SolrClient solrClient;

  public SolrIndexer(Config config, IndexerMessageManager manager, SolrClient solrClient, String metricsPrefix) {
    super(config, manager, metricsPrefix);
    this.solrClient = solrClient;
  }

  public SolrIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
    this(config, manager, getSolrClient(config, bypass), metricsPrefix);
  }

  private static SolrClient getSolrClient(Config config, boolean bypass) {
    return bypass ? null : SolrUtils.getSolrClient(config);
  }

  @Override
  public boolean validateConnection() {
    if (solrClient==null) {
      return true;
    }
    SolrPingResponse response;
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
      return false;
    }
    return true;
  }

  @Override
  public void closeConnection() {
    if (solrClient!=null) {
      try {
        solrClient.close();
      } catch (Exception e) {
        log.error("Error closing SolrClient", e);
      }
    }
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {

    if (solrClient==null) {
      log.debug("sendToSolr bypassed for documents: " + documents);
      return;
    }

    List<SolrInputDocument> solrDocs = new ArrayList();
    for (Document doc : documents) {

      Map<String,Object> map = getIndexerDoc(doc);
      SolrInputDocument solrDoc = new SolrInputDocument();

      // if an id override field has been specified, use its value as the id to send to solr, instead
      // of the document's own id
      String idOverride = getDocIdOverride(doc);

      for (String key : map.keySet()) {

        if (Document.CHILDREN_FIELD.equals(key)) {
          continue;
        }

        if (idOverride!=null && Document.ID_FIELD.equals(key)) {
          solrDoc.setField(Document.ID_FIELD, idOverride);
          continue;
        }

        Object value = map.get(key);
        if (value instanceof Map || value instanceof JsonNode) {
          throw new IndexerException(String.format("Object field '%s' on document id=%s is not supported by the SolrIndexer.", key, doc.getId()));
        }
        solrDoc.setField(key,value);
      }

      addChildren(doc, solrDoc);
      solrDocs.add(solrDoc);
    }

    solrClient.add(solrDocs);
  }

  private void addChildren(Document doc, SolrInputDocument solrDoc) throws IndexerException {
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
        if (value instanceof Map || value instanceof JsonNode) {
          throw new IndexerException(String.format("Object field '%s' on child document id=%s of document id=%s is not supported by the SolrIndexer.", key, child.getId(), doc.getId()));
        }
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
    Indexer indexer = new SolrIndexer(config, manager, false, pipelineName);
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
