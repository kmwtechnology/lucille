package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.ElasticsearchUtils;
import com.kmwllc.lucille.util.OpenSearchUtils;
import com.typesafe.config.Config;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OESearchIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(OESearchIndexer.class);

  private SearchProxy proxy;

  public OESearchIndexer(Config config, IndexerMessageManager manager, String metricsPrefix) {
    this(config, manager, createProxy(config), metricsPrefix);
  }

  public OESearchIndexer(Config config, IndexerMessageManager manager, SearchProxy proxy, String metricsPrefix) {
    super(config, manager, metricsPrefix);
    this.proxy = proxy;
  }

  private static SearchProxy createProxy(Config config) {
    if (config.hasPath("elasticsearch")) {
      String index = ElasticsearchUtils.getElasticsearchIndex(config);
      return new ElasticsearchProxy(ElasticsearchUtils.getElasticsearchRestClient(config), index);
    } else if (config.hasPath("opensearch")) {
      String index = OpenSearchUtils.getOpenSearchIndex(config);
      return new OpenSearchProxy(OpenSearchUtils.getOpenSearchRestClient(config), index);
    }

    // probably throw exception
    return null;
  }

  @Override
  public boolean validateConnection() {
    try {
      return proxy.ping();
    } catch (Exception e) {
      log.error("Could not ping connection", e);
      return false;
    }
  }

  @Override
  public void closeConnection() {
    try {
      proxy.close();
    } catch (Exception e) {
      log.error("Could not close connection to client", e);
    }
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (proxy == null) return;

    // determine what field to use as id field and iterate over the documents
    for (Document doc : documents) {
      Map<String, Object> indexerDoc = doc.asMap();

      // remove children documents field from indexer doc (processed from doc by addChildren method call below)
      indexerDoc.remove(Document.CHILDREN_FIELD);

      // if a doc id override value exists, make sure it is used instead of pre-existing doc id
      String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());
      indexerDoc.put(Document.ID_FIELD, docId);

      // handle special operations required to add children documents
      addChildren(doc, indexerDoc);

      proxy.addToBulkRequest(docId, indexerDoc);
    }

    proxy.sendAndResetBulkRequest();
  }

  private void addChildren(Document doc, Map<String, Object> indexerDoc) {
    List<Document> children = doc.getChildren();
    if (children == null || children.isEmpty()) {
      return;
    }
    for (Document child : children) {
      Map<String, Object> map = child.asMap();
      Map<String, Object> indexerChildDoc = new HashMap<>();
      for (String key : map.keySet()) {
        // we don't support children that contain nested children
        if (Document.CHILDREN_FIELD.equals(key)) {
          continue;
        }
        Object value = map.get(key);
        indexerChildDoc.put(key, value);
      }
      // TODO: Do nothing for now, add support for child docs like SolrIndexer does in future (_childDocuments_)
    }
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigUtils.loadConfig();
    String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
    log.info("Starting Indexer for pipeline: " + pipelineName);
    IndexerMessageManager manager = new KafkaIndexerMessageManager(config, pipelineName);
    Indexer indexer = new OESearchIndexer(config, manager, pipelineName);
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