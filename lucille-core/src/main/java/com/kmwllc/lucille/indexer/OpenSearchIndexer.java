package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.OpenSearchUtils;
import com.typesafe.config.Config;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.VersionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class OpenSearchIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(OpenSearchIndexer.class);

  private final RestHighLevelClient client;
  private final String index;

  private final String routingField;

  private final VersionType versionType;

  public OpenSearchIndexer(Config config, IndexerMessageManager manager, RestHighLevelClient client, String metricsPrefix) {
    super(config, manager, metricsPrefix);
    if (this.indexOverrideField != null) {
      throw new IllegalArgumentException("Cannot create OpenSearchIndexer. Config setting 'indexer.indexOverrideField' is not supported by OpenSearchIndexer.");
    }
    this.client = client;
    this.index = OpenSearchUtils.getOpenSearchIndex(config);
    this.routingField = config.hasPath("indexer.routingField") ? config.getString("indexer.routingField") : null;
    this.versionType = config.hasPath("indexer.versionType") ? VersionType.fromString(config.getString("indexer.versionType")) : null;
  }

  public OpenSearchIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
    this(config, manager, getClient(config, bypass), metricsPrefix);
  }

  private static RestHighLevelClient getClient(Config config, boolean bypass) {
    return bypass ? null : OpenSearchUtils.getOpenSearchRestClient(config);
  }

  @Override
  public boolean validateConnection() {
    if (client == null) {
      return true;
    }
    boolean response;
    try {
      response = client.ping(RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.error("Couldn't ping OpenSearch ", e);
      return false;
    }
    if (!response) {
      log.error("Non true response when pinging OpenSearch: " + response);
      return false;
    }
    return true;
  }

  @Override
  public void closeConnection() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        log.error("Error closing OpenSearchClient", e);
      }
    }
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (client == null) return;

    BulkRequest bulkRequest = new BulkRequest(index);

    // determine what field to use as id field and iterate over the documents
    for (Document doc : documents) {
      Map<String, Object> indexerDoc = getIndexerDoc(doc);

      // remove children documents field from indexer doc (processed from doc by addChildren method call below)
      indexerDoc.remove(Document.CHILDREN_FIELD);

      // if a doc id override value exists, make sure it is used instead of pre-existing doc id
      String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());

      // handle special operations required to add children documents
      addChildren(doc, indexerDoc);

      // create new IndexRequest
      IndexRequest indexRequest = new IndexRequest(index);
      indexRequest.id(docId);
      if (routingField != null) {
        indexRequest.routing(doc.getString(routingField));
      }
      indexRequest.source(indexerDoc);

      if (versionType != null) {
        indexRequest.versionType(versionType);
        if (versionType == VersionType.EXTERNAL || versionType == VersionType.EXTERNAL_GTE) {
          // the partition doesn’t need to be included in the version. We assume the doc id is used as the
          // kafka message key, which should guarantee that all “versions” of a given document have the
          // same kafka message key and therefore get mapped to the same topic partition.
          indexRequest.version(((KafkaDocument) doc).getOffset());
        }
      }

      // add indexRequest to bulkRequest
      bulkRequest.add(indexRequest);
    }

    BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
    if (response.hasFailures()) {
      log.error(response.buildFailureMessage());
    }
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
    Indexer indexer = new OpenSearchIndexer(config, manager, false, pipelineName);
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
