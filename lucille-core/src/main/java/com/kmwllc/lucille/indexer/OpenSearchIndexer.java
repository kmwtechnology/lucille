package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.OpenSearchUtils;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;


public class OpenSearchIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(OpenSearchIndexer.class);

  private final OpenSearchClient client;
  private final String index;

  private final String routingField;

  private final VersionType versionType;

  //flag for using partial update API when sending documents to opensearch
  private final boolean update;

  public OpenSearchIndexer(Config config, IndexerMessageManager manager, OpenSearchClient client, String metricsPrefix) {
    super(config, manager, metricsPrefix);
    if (this.indexOverrideField != null) {
      throw new IllegalArgumentException(
          "Cannot create OpenSearchIndexer. Config setting 'indexer.indexOverrideField' is not supported by OpenSearchIndexer.");
    }
    this.client = client;
    this.index = OpenSearchUtils.getOpenSearchIndex(config);
    this.routingField = config.hasPath("indexer.routingField") ? config.getString("indexer.routingField") : null;
    this.update = config.hasPath("opensearch.update") ? config.getBoolean("opensearch.update") : false;
    this.versionType =
        config.hasPath("indexer.versionType") ? VersionType.valueOf(config.getString("indexer.versionType")) : null;
  }

  public OpenSearchIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
    this(config, manager, getClient(config, bypass), metricsPrefix);
  }

  private static OpenSearchClient getClient(Config config, boolean bypass) {
    return bypass ? null : OpenSearchUtils.getOpenSearchRestClient(config);
  }

  @Override
  public boolean validateConnection() {
    if (client == null) {
      return true;
    }
    boolean response;
    try {
      response = client.ping().value();
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
    if (client != null && client._transport() != null) {
      try {
        client._transport().close();
      } catch (Exception e) {
        log.error("Error closing Opensearchclient", e);
      }
    }
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (client == null) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Document doc : documents) {
      Map<String, Object> indexerDoc = doc.asMap();

      // remove children documents field from indexer doc (processed from doc by addChildren method call below)
      indexerDoc.remove(Document.CHILDREN_FIELD);

      // if a doc id override value exists, make sure it is used instead of pre-existing doc id
      String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());
      indexerDoc.put(Document.ID_FIELD, docId);

      // handle special operations required to add children documents
      addChildren(doc, indexerDoc);

      String routing = doc.getString(routingField);
      Long versionNum = (versionType == VersionType.External || versionType == VersionType.ExternalGte)
          ? ((KafkaDocument) doc).getOffset()
          : null;

      if (update) {
        br.operations(op -> op
            .update((up) -> {
              up.index(index).id(docId);
              if (routingField != null) {
                up.routing(routing);
              }
              if (versionNum != null) {
                up.versionType(versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      } else {
        br.operations(op -> op
            .index((up) -> {
              up.index(index).id(docId);
              if (routingField != null) {
                up.routing(routing);
              }
              if (versionNum != null) {
                up.versionType(versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      }
    }
    BulkResponse response = client.bulk(br.build());
    if (response != null && response.errors()) {
      for (BulkResponseItem item : response.items()) {
        throw new IndexerException(item.error().reason());
      }
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
