package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
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

/**
 * Indexes documents to OpenSearch using the Java client.
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

  //flag for using partial update API when sending documents to opensearch
  private final boolean update;

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, OpenSearchClient client, String metricsPrefix, String localRunId) {
    super(config, messenger, metricsPrefix, localRunId);

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

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    this(config, messenger, getClient(config, bypass), metricsPrefix, localRunId);
  }

  // Convenience Constructors
  public OpenSearchIndexer(Config config, IndexerMessenger messenger, OpenSearchClient client, String metricsPrefix) {
    this(config, messenger, client, metricsPrefix, null);
  }

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, getClient(config, bypass), metricsPrefix, null);
  }

  @Override
  protected String getIndexerConfigKey() { return "opensearch"; }

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
              .index(index)
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
        .index(index)
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
      Long versionNum = (versionType == VersionType.External || versionType == VersionType.ExternalGte)
          ? ((KafkaDocument) doc).getOffset()
          : null;

      if (update) {
        br.operations(op -> op
            .update((up) -> {
              up.index(index).id(docId);
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
              up.index(index).id(docId);
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
