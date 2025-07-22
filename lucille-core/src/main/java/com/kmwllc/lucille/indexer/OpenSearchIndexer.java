package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.util.OpenSearchUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.BulkIndexByScrollFailure;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.DeleteByQueryRequest;
import org.opensearch.client.opensearch.core.DeleteByQueryResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kmwllc.lucille.core.BaseIndexerConfig;
import com.kmwllc.lucille.core.BaseConfigException;

public class OpenSearchIndexer extends Indexer {

  private static final Logger log = LoggerFactory.getLogger(OpenSearchIndexer.class);

  private final OpenSearchClient client;

  /**
   * Configuration class for {@link OpenSearchIndexer}.
   * <p>
   * Holds parameters for connecting to and configuring the OpenSearch indexer, including:
   * <ul>
   *   <li><b>index</b>: The name of the OpenSearch index to write to (required).</li>
   *   <li><b>url</b>: The OpenSearch endpoint URL (required).</li>
   *   <li><b>routingField</b>: The document field to use for routing (optional).</li>
   *   <li><b>update</b>: Whether to use the partial update API (optional, default: false).</li>
   *   <li><b>versionType</b>: The versioning strategy for documents (optional).</li>
   *   <li><b>acceptInvalidCert</b>: Whether to accept invalid SSL certificates (optional).</li>
   * </ul>
   * Provides methods to apply configuration from a {@link Config} object,
   * validate parameter values, and access the configured values.
   */
  public static class OpenSearchIndexerConfig extends BaseIndexerConfig {
    /**
     * The name of the OpenSearch index to write to. Required.
     */
    private String index;
    /**
     * The OpenSearch endpoint URL. Required.
     */
    private String url;
    /**
     * The document field to use for routing. Optional.
     */
    private String routingField = null;
    /**
     * Whether to use the partial update API when sending documents. Optional, defaults to false.
     */
    private boolean update = false;
    /**
     * The versioning strategy for documents (e.g., "internal", "external"). Optional.
     */
    private VersionType versionType = null;
    /**
     * Whether to accept invalid SSL certificates. Optional.
     */
    private boolean acceptInvalidCert = false;

    /**
     * Applies configuration values from the provided {@link Config} object.
     *
     * @param params the configuration object containing parameters
     */
    public void apply(Config params) {
      super.apply(params);
      // index = params.getString("index");
      index = OpenSearchUtils.getOpenSearchIndex(params);

      url = OpenSearchUtils.getOpenSearchUrl(params);

      if (params.hasPath("indexer.routingField")) {
        routingField = params.getString("indexer.routingField");
      }
      if (params.hasPath("opensearch.update")) {
        update = params.getBoolean("opensearch.update");
      }
      if (params.hasPath("indexer.versionType")) {
        versionType = VersionType.valueOf(params.getString("indexer.versionType"));
      }
      if (params.hasPath("acceptInvalidCert")) {
        acceptInvalidCert = params.getBoolean("acceptInvalidCert");
      }
    }

    /**
     * Validates the configuration parameters to ensure they are within acceptable ranges.
     *
     * @throws IndexerException if any parameter is invalid
     */
    public void validate() throws IndexerException {
      try {
        super.validate();
      } catch (BaseConfigException e) {
        throw new IndexerException(e);
      }
      if (index == null || index.isEmpty()) {
        throw new IllegalArgumentException("index is required for OpenSearchIndexer");
      }
      if (url == null || url.isEmpty()) {
        throw new IllegalArgumentException("url is required for OpenSearchIndexer");
      }
    }

    /**
     * @return the OpenSearch index name
     */
    public String getIndex() { return index; }
    /**
     * @return the OpenSearch endpoint URL
     */
    public String getUrl() { return url; }
    /**
     * @return the routing field name, or null if not set
     */
    public String getRoutingField() { return routingField; }
    /**
     * @return true if partial update API should be used
     */
    public boolean isUpdate() { return update; }
    /**
     * @return the versioning strategy, or null if not set
     */
    public VersionType getVersionType() { return versionType; }
    /**
     * @return true if invalid SSL certificates should be accepted
     */
    public boolean isAcceptInvalidCert() { return acceptInvalidCert; }
  }
  
  private OpenSearchIndexerConfig config;

  public OpenSearchIndexer(Config params, IndexerMessenger messenger, OpenSearchClient client, String metricsPrefix, String localRunId) throws IndexerException {
    super(params, messenger, metricsPrefix, localRunId, Spec.indexer()
        .withRequiredProperties("index", "url")
        .withOptionalProperties("update", "acceptInvalidCert"));

      config = new OpenSearchIndexerConfig();
      config.apply(params);
      config.validate();

    if (this.indexOverrideField != null) {
      throw new IllegalArgumentException(
          "Cannot create OpenSearchIndexer. Config setting 'indexer.indexOverrideField' is not supported by OpenSearchIndexer.");
    }
    this.client = client;
    // this.index = OpenSearchUtils.getOpenSearchIndex(params);
    this.config.routingField = params.hasPath("indexer.routingField") ? params.getString("indexer.routingField") : null;
    this.config.update = params.hasPath("opensearch.update") ? params.getBoolean("opensearch.update") : false;
    this.config.versionType =
        params.hasPath("indexer.versionType") ? VersionType.valueOf(params.getString("indexer.versionType")) : null;
  }

  public OpenSearchIndexer(Config params, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    this(params, messenger, getClient(params, bypass), metricsPrefix, localRunId);
  }

  // Convenience Constructors
  public OpenSearchIndexer(Config params, IndexerMessenger messenger, OpenSearchClient client, String metricsPrefix) throws IndexerException {
    this(params, messenger, client, metricsPrefix, null);
  }

  public OpenSearchIndexer(Config params, IndexerMessenger messenger, boolean bypass, String metricsPrefix) throws IndexerException {
    this(params, messenger, getClient(params, bypass), metricsPrefix, null);
  }

  @Override
  protected String getIndexerConfigKey() { return "opensearch"; }

  private static OpenSearchClient getClient(Config params, boolean bypass) {
    return bypass ? null : OpenSearchUtils.getOpenSearchRestClient(params);
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
  protected Set<Pair<Document, String>> sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (client == null) {
      return Set.of();
    }

    Map<String, Document> documentsToUpload = new LinkedHashMap<>();
    Set<String> idsToDelete = new LinkedHashSet<>();
    Map<String, List<String>> termsToDeleteByQuery = new LinkedHashMap<>();

    // populate which collection each document belongs to
    // upload if document is not marked for deletion
    // else if document is marked for deletion ONLY, then only add to idsToDelete
    // else document is marked for deletion AND contains deleteByFieldField and deleteByFieldValue, only add to termsToDeleteByQuery
    for (Document doc : documents) {
      String id = doc.getId();
      if (!isMarkedForDeletion(doc)) {
        idsToDelete.remove(id);
        documentsToUpload.put(id, doc);
      } else {
        documentsToUpload.remove(id);
        if (!isMarkedForDeletionByField(doc)) {
          idsToDelete.add(id);
        } else {
          String field = doc.getString(deleteByFieldField);
          if (!termsToDeleteByQuery.containsKey(field)) {
            termsToDeleteByQuery.put(field, new ArrayList<>());
          }
          termsToDeleteByQuery.get(field).add(doc.getString(deleteByFieldValue));
        }
      }
    }

    Set<Pair<Document, String>> failedDocs = uploadDocuments(documentsToUpload.values());
    deleteById(new ArrayList<>(idsToDelete));
    deleteByQuery(termsToDeleteByQuery);

    return failedDocs;
  }

  private void deleteById(List<String> idsToDelete) throws Exception {
    if (idsToDelete.isEmpty()) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();
    for (String id : idsToDelete) {
      br.operations(op -> op
          .delete(d -> d
              .index(config.index)
              .id(id)
          )
      );
    }

    BulkResponse response = client.bulk(br.build());
    if (response.errors()) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          log.debug("Error while deleting id: {} because: {}", item.id(), item.error().reason());
        }
      }
      throw new IndexerException("encountered errors while deleting documents");
    }
  }

  private void deleteByQuery(Map<String, List<String>> termsToDeleteByQuery) throws Exception {
    if (termsToDeleteByQuery.isEmpty()) {
      return;
    }
    /*
      each entry from termsToDeleteByQuery would add a "terms" query to the should clause. In each entry, the key represents
      the field to look for while querying and values represent the values to look for in that field. We set the
      minimum should match to 1 such that any documents that passes a single terms query would end
      up in the result set. For a document to pass a terms query, it must contain that field and in that field
      contain any one of the field values.
      e.g. termsToDeleteByQuery -> {
                                      "field1" : ["value1", "value2", "value3"],
                                      "field2" : ["valueA", "valueB"]
                                   }
      would create a delete query:
      {
        "query": {
          "bool": {
            "should": [
              {
                "terms": {
                  "field1": ["value1", "value2", "value3"]
                }
              },
              {
                "terms": {
                  "field2": ["valueA", "valueB"]
                }
              }
            ],
            "minimum_should_match": "1"
          }
        }
      }
     */
    BoolQuery.Builder boolQuery = new BoolQuery.Builder();
    for (Map.Entry<String, List<String>> entry : termsToDeleteByQuery.entrySet()) {
      String field = entry.getKey();
      List<String> values = entry.getValue();
      boolQuery.should(s -> s
        .terms(t -> t
          .field(field)
          .terms(tt -> tt.value(values.stream()
              .map(FieldValue::of)
              .collect(Collectors.toList()))
          )
        )
      );
    }
    boolQuery.minimumShouldMatch("1");

    DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest.Builder()
        .index(config.index)
        .query(q -> q.bool(boolQuery.build()))
        .build();
    DeleteByQueryResponse response = client.deleteByQuery(deleteByQueryRequest);

    if (!response.failures().isEmpty()) {
      for (BulkIndexByScrollFailure failure : response.failures()) {
        log.debug("Error while deleting by query: {}, because of: {}", failure.cause().reason(), failure.cause());
      }
      throw new IndexerException("encountered errors while deleting by query");
    }
  }

  private Set<Pair<Document, String>> uploadDocuments(Collection<Document> documentsToUpload) throws IOException, IndexerException {
    if (documentsToUpload.isEmpty()) {
      return Set.of();
    }

    Map<String, Document> uploadedDocuments = new HashMap<>();
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Document doc : documentsToUpload) {

      // removing the fields mentioned in the ignoreFields setting in configurations
      Map<String, Object> indexerDoc = getIndexerDoc(doc);

      // remove children documents field from indexer doc (processed from doc by addChildren method call below)
      indexerDoc.remove(Document.CHILDREN_FIELD);

      // if a doc id override value exists, make sure it is used instead of pre-existing doc id
      String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());
      uploadedDocuments.put(docId, doc);

      // This condition below avoids adding id if ignoreFields contains it and edge cases:
      // - Case 1: id and idOverride in ignoreFields -> idOverride used by Indexer, both removed from Document (tested in testIgnoreFieldsWithOverride)
      // - Case 2: id in ignoreFields, idOverride exists -> idOverride used by Indexer, only id field removed from Document (tested in testIgnoreFieldsWithOverride2)
      // - Case 3: id in ignoreFields, idOverride null -> id used by Indexer, id also removed from Document (tested in testRouting)
      // - Case 4: ignoreFields null, idOverride exists -> idOverride used by Indexer, id and idOverride field exist in Document (tested in testOverride)
      // - Case 5: ignoreFields null, idOverride null -> document id remains and used by Indexer (Default case & tested)
      if (ignoreFields == null || !ignoreFields.contains(Document.ID_FIELD)) {
        indexerDoc.put(Document.ID_FIELD, docId);
      }

      // handle special operations required to add children documents
      addChildren(doc, indexerDoc);
      Long versionNum = (config.versionType == VersionType.External || config.versionType == VersionType.ExternalGte)
          ? ((KafkaDocument) doc).getOffset()
          : null;

      if (config.update) {
        br.operations(op -> op
            .update((up) -> {
              up.index(config.index).id(docId);
              if (config.routingField != null) {
                up.routing(doc.getString(config.routingField));
              }
              if (versionNum != null) {
                up.versionType(config.versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      } else {
        br.operations(op -> op
            .index((up) -> {
              up.index(config.index).id(docId);
              if (config.routingField != null) {
                up.routing(doc.getString(config.routingField));
              }
              if (versionNum != null) {
                up.versionType(config.versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      }
    }

    Set<Pair<Document, String>> failedDocs = new HashSet<>();

    BulkResponse response = client.bulk(br.build());

    if (response != null) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          // For the error - if the id is a document's id, then it failed, and we add it to the set.
          // If not, we don't know what the error is, and opt to throw an actual IndexerException instead.
          if (uploadedDocuments.containsKey(item.id())) {
            Document failedDoc = uploadedDocuments.get(item.id());
            failedDocs.add(Pair.of(failedDoc, item.error().reason()));
          } else {
            throw new IndexerException(item.error().reason());
          }
        }
      }
    }

    return failedDocs;
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

  private boolean isMarkedForDeletion(Document doc) {
    return deletionMarkerField != null
        && deletionMarkerFieldValue != null
        && doc.hasNonNull(deletionMarkerField)
        && doc.getString(deletionMarkerField).equals(deletionMarkerFieldValue);
  }

  private boolean isMarkedForDeletionByField(Document doc) {
    return deleteByFieldField != null
        && doc.has(deleteByFieldField)
        && deleteByFieldValue != null
        && doc.has(deleteByFieldValue);
  }
}
