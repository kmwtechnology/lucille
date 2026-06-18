package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.util.OpenSearchUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
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
import org.opensearch.client.opensearch._types.BulkByScrollFailure;
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

/**
 * Indexes documents to OpenSearch using the Java client.
 * Additional parameters are made available by the {@link com.kmwllc.lucille.core.Indexer} abstract class.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>index (String, Required) : Target OpenSearch index name.</li>
 *   <li>url (String, Required) : OpenSearch HTTP endpoint (e.g., https://localhost:9200).</li>
 *   <li>update (Boolean, Optional) : Use partial update API instead of index/replace. Defaults to false.</li>
 *   <li>acceptInvalidCert (Boolean, Optional) : Allow invalid TLS certificates. Defaults to false.</li>
 *   <li>indexer.routingField (String, Optional) : Document field that supplies the routing key.</li>
 *   <li>indexer.versionType (String, Optional) : Versioning type when using external versions.</li>
 * </ul>
 */
public class OpenSearchIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("index", "url")
      .optionalBoolean("update", "acceptInvalidCert").build();

  private static final Logger log = LoggerFactory.getLogger(OpenSearchIndexer.class);

  private final OpenSearchClient client;
  private final String index;

  private final String routingField;

  private final VersionType versionType;
  private final String versionField;

  //flag for using partial update API when sending documents to opensearch
  private final boolean update;

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass,
      String metricsPrefix,String localRunId, OpenSearchClient client) {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    this.client = client;
    this.index = OpenSearchUtils.getOpenSearchIndex(config);
    this.routingField = config.hasPath("indexer.routingField") ? config.getString("indexer.routingField") : null;
    this.update = config.hasPath("opensearch.update") ? config.getBoolean("opensearch.update") : false;
    this.versionType =
        config.hasPath("indexer.versionType") ? VersionType.valueOf(config.getString("indexer.versionType")) : null;
    this.versionField = config.hasPath("indexer.versionField") ? config.getString("indexer.versionField") : null;
    // validate config indexer.versionType that must be set if config indexer.versionField is set
    if (this.versionField != null && this.versionType == null) {
      throw new IllegalArgumentException("indexer.versionType must be set if indexer.versionField is set");
    }
  }

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    this(config, messenger, bypass, metricsPrefix, localRunId, getClient(config, bypass));
  }

  // Convenience Constructors
  public OpenSearchIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, OpenSearchClient client) {
    this(config, messenger, false, metricsPrefix, null, client);
  }

  @Override
  protected String getIndexerConfigKey() { return "opensearch"; }

  private static OpenSearchClient getClient(Config config, boolean bypass) throws IndexerException {
    try {
      return bypass ? null : OpenSearchUtils.getOpenSearchRestClient(config);
    } catch (Exception e) {
      throw new IndexerException("Couldn't create OpenSearch client", e);
    }
  }

  @Override
  public boolean validateConnection() {
    if (bypass) {
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
    if (!bypass && client != null && client._transport() != null) {
      try {
        client._transport().close();
      } catch (Exception e) {
        log.error("Error closing Opensearchclient", e);
      }
    }
  }

  @Override
  protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
    // skip indexing if there is no indexer client
    if (bypass) {
      return Set.of();
    }

    Map<Pair<String, String>, Document> documentsToUpload = new LinkedHashMap<>();
    Set<Pair<String, String>> idsToDelete = new LinkedHashSet<>();
    Map<Pair<String, String>, List<String>> termsToDeleteByQuery = new LinkedHashMap<>();

    // populate which collection each document belongs to
    // upload if document is not marked for deletion
    // else if document is marked for deletion ONLY, then only add to idsToDelete
    // else document is marked for deletion AND contains deleteByFieldField and deleteByFieldValue, only add to termsToDeleteByQuery
    for (Document doc : documents) {
      // use doc id override if specified, otherwise delete will try to delete wrong id
      String id = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());

      // if an index override field has been specified, use its value as the index to send to opensearch,
      final String indexToSend = Optional.ofNullable(getIndexOverride(doc)).orElse(index);

      Pair<String, String> indexAndId = Pair.of(indexToSend, id);

      if (!isMarkedForDeletion(doc)) {
        idsToDelete.remove(indexAndId);
        documentsToUpload.put(indexAndId, doc);
      } else {
        documentsToUpload.remove(indexAndId);
        if (!isMarkedForDeletionByField(doc)) {
          idsToDelete.add(indexAndId);
        } else {
          //indexer.deleteByFieldField gives you the field in the document that holds which field whose value is queried for
          // deletion, so we need to do doc.getString(deleteByFieldField).
          Pair<String, String> indexAndDeleteByField = Pair.of(indexToSend, doc.getString(deleteByFieldField));
          if (!termsToDeleteByQuery.containsKey(indexAndDeleteByField)) {
            termsToDeleteByQuery.put(indexAndDeleteByField, new ArrayList<>());
          }
          termsToDeleteByQuery.get(indexAndDeleteByField).add(doc.getString(deleteByFieldValue));
        }
      }
    }

    Set<Pair<Document, Exception>> failedDocs = uploadDocuments(documentsToUpload);
    deleteById(new ArrayList<>(idsToDelete));
    deleteByQuery(termsToDeleteByQuery);

    return failedDocs;
  }

  private void deleteById(List<Pair<String, String>> idsToDelete) throws Exception {
    if (idsToDelete.isEmpty()) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();
    for (Pair<String, String> indexAndId : idsToDelete) {
      br.operations(op -> op
          .delete(d -> d
              .index(indexAndId.getLeft())
              .id(indexAndId.getRight())
          )
      );
    }

    BulkResponse response;
    try {
      response = client.bulk(br.build());
    } catch (org.opensearch.client.opensearch._types.OpenSearchException e) {
      throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      throw new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
    }

    if (response.errors()) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          log.debug("Error while deleting id: {} because: {}", item.id(), item.error().reason());
        }
      }
      throw new IndexerException("encountered errors while deleting documents");
    }
  }

  private void deleteByQuery(Map<Pair<String, String>, List<String>> termsToDeleteByQuery) throws Exception {
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

    // Send a separate delete by query for each index.
    Map<String, BoolQuery.Builder> boolQueryBuilders = new HashMap<>();

    for (Map.Entry<Pair<String, String>, List<String>> entry : termsToDeleteByQuery.entrySet()) {
      Pair<String, String> indexAndField = entry.getKey();
      List<String> values = entry.getValue();
      BoolQuery.Builder boolQuery;
      if ( boolQueryBuilders.containsKey(indexAndField.getLeft()) ) {
        boolQuery = boolQueryBuilders.get(indexAndField.getLeft());
      } else {
        boolQuery = new BoolQuery.Builder();
        boolQueryBuilders.put(indexAndField.getLeft(), boolQuery);
      }
      boolQuery.should(s -> s
          .terms(t -> t
              .field(indexAndField.getRight())
              .terms(tt -> tt.value(values.stream()
                  .map(FieldValue::of)
                  .collect(Collectors.toList()))
              )
          )
      );
    }

    for (Map.Entry<String, BoolQuery.Builder> entry : boolQueryBuilders.entrySet()) {
      BoolQuery.Builder boolQuery = entry.getValue();

      boolQuery.minimumShouldMatch("1");

      DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest.Builder()
          .index(entry.getKey())
          .query(q -> q.bool(boolQuery.build()))
          .build();

      DeleteByQueryResponse response;
      try {
        response = client.deleteByQuery(deleteByQueryRequest);
      } catch (org.opensearch.client.opensearch._types.OpenSearchException e) {
        throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
      } catch (IOException e) {
        throw new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
      }

      if (!response.failures().isEmpty()) {
        for (BulkByScrollFailure failure : response.failures()) {
          log.debug("Error while deleting by query: {}, because of: {}", failure.cause().reason(), failure.cause());
        }
        throw new IndexerException("encountered errors while deleting by query");
      }
    }
  }

  private Set<Pair<Document, Exception>> uploadDocuments(Map<Pair<String, String>, Document> documentsToUpload) throws IOException, IndexerException {

    if (documentsToUpload.isEmpty()) {
      return Set.of();
    }

    Map<String, Document> uploadedDocuments = new HashMap<>();
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Map.Entry<Pair<String, String>, Document> entry : documentsToUpload.entrySet()) {
      String indexToSend = entry.getKey().getLeft();
      String docId = entry.getKey().getRight();
      Document doc = entry.getValue();

      // removing fields in the blacklist or not in the whitelist in configurations
      Map<String, Object> indexerDoc = getIndexerDoc(doc);

      // remove children documents field from indexer doc (processed from doc by addChildren method call below)
      indexerDoc.remove(Document.CHILDREN_FIELD);

      uploadedDocuments.put(docId, doc);

      // only add id if our fieldFilter allows it (based on our blacklist and whitelist)
      // - Case 1: id and idOverride filtered out -> idOverride used by Indexer, both removed from Document (tested in testBlacklistWithOverride)
      // - Case 2: id is filtered, idOverride exists -> idOverride used by Indexer, only id field removed from Document (tested in testBlacklistWithOverride2)
      // - Case 3: id is filtered, idOverride null -> id used by Indexer, id also removed from Document (tested in testRouting)
      // - Case 4: both unfiltered, idOverride exists -> idOverride used by Indexer, id and idOverride field exist in Document (tested in testOverride)
      // - Case 5: both unfiltered, idOverride null -> document id remains and used by Indexer (Default case & tested)
      if (fieldFilter.shouldInclude(Document.ID_FIELD)) {
        indexerDoc.put(Document.ID_FIELD, docId);
      }

      // handle special operations required to add children documents
      addChildren(doc, indexerDoc);
      Long versionNum = (versionType == VersionType.External || versionType == VersionType.ExternalGte)
          ? getVersionNum(doc)
          : null;

      if (update) {
        br.operations(op -> op
            .update((up) -> {
              up.index(indexToSend).id(docId);
              if (routingField != null) {
                up.routing(doc.getString(routingField));
              }
              if (versionNum != null) {
                up.versionType(versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      } else {
        br.operations(op -> op
            .index((up) -> {
              up.index(indexToSend).id(docId);
              if (routingField != null) {
                up.routing(doc.getString(routingField));
              }
              if (versionNum != null) {
                up.versionType(versionType).version(versionNum);
              }
              return up.document(indexerDoc);
            }));
      }
    }

    Set<Pair<Document, Exception>> failedDocs = new HashSet<>();

    BulkResponse response;
    try {
      response = client.bulk(br.build());
    } catch (org.opensearch.client.opensearch._types.OpenSearchException e) {
      // HTTP-level error with a known status code — wrap so the base Indexer can apply retry policy
      throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      // Transport-level failure (connection refused, timeout, etc.) — no status code available
      throw new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
    }

    if (response != null) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          // For the error - if the id is a document's id, then it failed, and we add it to the set.
          // If not, we don't know what the error is, and opt to throw an actual IndexerException instead.
          if (uploadedDocuments.containsKey(item.id())) {
            Document failedDoc = uploadedDocuments.get(item.id());
            // item.status() is always an HTTP status code. The OpenSearch bulk API defines the per-item
            // status field as the HTTP status of that individual operation (e.g. 200, 201, 400, 409, 429,
            // 503). This is consistent across all OpenSearch client libraries and confirmed by the
            // OpenSearch REST API documentation and source — there are no documented non-HTTP values.
            failedDocs.add(Pair.of(failedDoc, new IndexerRetryableException(item.status(),
                "OpenSearch bulk item error (status " + item.status() + "): " + item.error().reason(), null)));
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

  private Long getVersionNum(Document doc) {
    if (versionField != null && doc.has(versionField)) {
      return doc.getLong(versionField);
    }
    if (doc instanceof KafkaDocument) {
      return ((KafkaDocument) doc).getOffset();
    }
    return null;
  }
}
