package com.kmwllc.lucille.indexer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.KafkaIndexerMessageManager;
import com.kmwllc.lucille.util.ElasticsearchUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// TODO: upgrade the ElasticsearchIndexer to use the Elasticsearch Java API Client
public class ElasticsearchIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchIndexer.class);

  private final ElasticsearchClient client;
  private final String index;

  //flag for using partial update API when sending documents to elastic
  private final boolean update;
  private final ElasticJoinData joinData;
  private final String routingField;
  private final VersionType versionType;

  public ElasticsearchIndexer(Config config, IndexerMessageManager manager, ElasticsearchClient client,
      String metricsPrefix) {
    super(config, manager, metricsPrefix);
    if (this.indexOverrideField != null) {
      throw new IllegalArgumentException(
          "Cannot create ElasticsearchIndexer. Config setting 'indexer.indexOverrideField' is not supported by ElasticsearchIndexer.");
    }
    this.client = client;
    this.index = ElasticsearchUtils.getElasticsearchIndex(config);
    this.update = config.hasPath("elasticsearch.update") ? config.getBoolean("elasticsearch.update") : false;

    joinData = ElasticJoinData.fromConfig(config);
    this.routingField = ConfigUtils.getOrDefault(config, "indexer.routingField", null);
    this.versionType = config.hasPath("indexer.versionType") ? VersionType.valueOf(config.getString("indexer.versionType")) : null;
  }

  public ElasticsearchIndexer(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) {
    this(config, manager, getClient(config, bypass), metricsPrefix);
  }

  private static ElasticsearchClient getClient(Config config, boolean bypass) {
    return bypass ? null : ElasticsearchUtils.getElasticsearchOfficialClient(config);
  }

  @Override
  public boolean validateConnection() {
    if (client == null) {
      return false;
    }
    BooleanResponse response;
    try {
      response = client.ping();
    } catch (Exception e) {
      log.error("Couldn't ping Elasticsearch ", e);
      return false;
    }
    if (response == null || !response.value()) {
      log.error("Non true response when pinging Elasticsearch: " + response);
      return false;
    }
    return true;
  }

  @Override
  protected void sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (client == null) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Document doc : documents) {

      // populate join data to document
      joinData.populateJoinData(doc);

      Map<String, Object> indexerDoc = doc.asMap();

      // remove children documents field from indexer doc (processed from doc by addChildren method call below)
      indexerDoc.remove(Document.CHILDREN_FIELD);

      // if a doc id override value exists, make sure it is used instead of pre-existing doc id
      String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());
      indexerDoc.put(Document.ID_FIELD, docId);

      // handle special operations required to add children documents
      addChildren(doc, indexerDoc);

      Long versionNum = (versionType == VersionType.External || versionType == VersionType.ExternalGte)
          ? ((KafkaDocument) doc).getOffset()
          : null;

      if (update) {
        br.operations(op -> op
            .update(updateBuilder -> {

              if (routingField != null) {
                updateBuilder.routing(doc.getString(routingField));
              }
              if (versionNum != null) {
                updateBuilder.versionType(versionType).version(versionNum);
              }

              return updateBuilder
                  .id(docId)
                  .index(index)
                  .action(upx -> upx
                      .doc(indexerDoc)
                  );
            }));
      } else {
        br.operations(op -> op
            .index(operationBuilder -> {

                  if (routingField != null) {
                    operationBuilder.routing(doc.getString(routingField));
                  }
                  if (versionNum != null) {
                    operationBuilder.versionType(versionType).version(versionNum);
                  }

                  return operationBuilder
                      .id(docId)
                      .index(index)
                      .document(indexerDoc);
                }
            ));
      }
    }
    BulkResponse response = client.bulk(br.build());
    // We're choosing not to check response.errors(), instead iterating to be sure whether errors exist
    if (response != null) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          throw new IndexerException(item.error().reason());
        }
      }
    }
  }

  @Override
  public void closeConnection() {
    if (client != null && client._transport() != null) {
      try {
        client._transport().close();
      } catch (Exception e) {
        log.error("Error closing ElasticsearchClient", e);
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

  public static class ElasticJoinData {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String joinFieldName;
    private boolean isChild;
    private String parentName;
    private String childName;
    private String parentDocumentIdSource;

    public void populateJoinData(Document doc) {

      // no need to do a join
      if (joinFieldName == null) {
        return;
      }

      // check that join field doesn't already exist
      if (doc.has(joinFieldName)) {
        throw new IllegalStateException("Document already has join field: " + joinFieldName);
      }

      // set a necessary join field
      if (isChild) {
        String parentId = doc.getString(parentDocumentIdSource);
        doc.setField(joinFieldName, getChildNode(parentId));
      } else {
        doc.setField(joinFieldName, parentName);
      }
    }

    private JsonNode getChildNode(String parentId) {
      return MAPPER.createObjectNode()
          .put("name", childName)
          .put("parent", parentId);
    }

    private static String prefix() {
      return prefix(null);
    }

    private static String prefix(String path) {
      // todo consider abstracting adding prefix to config
      StringBuilder builder = new StringBuilder("elasticsearch.join");
      if (path == null || path.isEmpty()) {
        return builder.toString();
      }
      return builder.append(".").append(path).toString();
    }

    public static ElasticJoinData fromConfig(Config config) {


      ElasticJoinData data = new ElasticJoinData();

      // if no join in config will initialize all join data to null and will be skipped in the code
      if (config.hasPath(prefix())) {

        // set fields for both parent and child
        data.joinFieldName = config.getString(prefix("joinFieldName"));
        data.isChild = ConfigUtils.getOrDefault(config, prefix("isChild"), false);

        // set parent child specific fields
        if (data.isChild) {
          data.childName = config.getString(prefix("childName"));
          data.parentDocumentIdSource = config.getString(prefix("parentDocumentIdSource"));
        } else {
          data.parentName = config.getString(prefix("parentName"));
        }
      }

      return data;
    }
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    String pipelineName = args.length > 0 ? args[0] : config.getString("indexer.pipeline");
    log.info("Starting Indexer for pipeline: " + pipelineName);
    IndexerMessageManager manager = new KafkaIndexerMessageManager(config, pipelineName);
    Indexer indexer = new ElasticsearchIndexer(config, manager, false, pipelineName);
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
