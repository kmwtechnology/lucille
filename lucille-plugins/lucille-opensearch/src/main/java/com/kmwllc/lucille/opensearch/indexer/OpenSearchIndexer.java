package com.kmwllc.lucille.opensearch.indexer;

import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.IndexerType;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.opensearch.util.OpenSearchUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.BulkByScrollFailure;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.OpenSearchException;
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
 *   <li>url (String or List&lt;String&gt;, Required) : OpenSearch HTTP endpoint (e.g., https://localhost:9200), or a list of
 *   endpoints for the same cluster. With a list, the client round-robins requests across the hosts and fails over between them.</li>
 *   <li>update (Boolean, Optional) : Use partial update API instead of index/replace, creating the document if it does not exist. Defaults to false.</li>
 *   <li>acceptInvalidCert (Boolean, Optional) : Allow invalid TLS certificates. Defaults to false.</li>
 *   <li>useCompression (Boolean, Optional) : Whether to use compression in the underlying OpenSearch HTTP client. Defaults to false.</li>
 *   <li>connectTimeoutMs (Long, Optional) : Maximum time to establish a connection. Defaults to 5000.</li>
 *   <li>socketTimeoutMs (Long, Optional) : Maximum time to wait for data on an open connection, including a bulk or
 *   delete-by-query response. A timeout is a transport failure, retried when indexer.maxRetries is set. Defaults to
 *   30000; raise it for long deletes, or set it to -1 for no limit.</li>
 *   <li>connectionTimeToLiveMs (Long, Optional) : Maximum lifetime of a pooled connection, so long-running indexers
 *   behind a load balancer or Kubernetes Service reconnect and reach new nodes. Defaults to 300000 (5 minutes).</li>
 *   <li>childDocumentsField (String, Optional) : Field name under which attached child documents are nested in the
 *   indexed document. If not set, child documents are not indexed. For child queries to work correctly, this field should be mapped
 *   as type "nested" in the index mapping.</li>
 *   <li>indexer.routingField (String, Optional) : Document field that supplies the routing key.</li>
 *   <li>indexer.versionType (String, Optional) : Versioning type when using external versions. With External or ExternalGte,
 *   an upload carries the document's indexer.versionField value, or else its Kafka offset.</li>
 *   <li>indexer.versionField (String, Optional) : Document field holding the external version. Requires
 *   indexer.versionType. With External or ExternalGte, a delete by id carries this field's value too; a delete without it
 *   is unversioned, and never uses the Kafka offset.</li>
 * </ul>
 */
@AutoService(Indexer.class)
@IndexerType("OpenSearch")
public class OpenSearchIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("index")
      .requiredStringOrList("url")
      .optionalBoolean("update", "acceptInvalidCert", "useCompression")
      .optionalNumber("maxConnectionsPerRoute", "maxConnectionsTotal",
          "connectTimeoutMs", "socketTimeoutMs", "connectionTimeToLiveMs")
      .optionalString("childDocumentsField").build();

  private static final Logger log = LoggerFactory.getLogger(OpenSearchIndexer.class);

  // not final: the standard constructor builds it only after validation, see there
  private OpenSearchClient client;
  private final String index;

  private final String routingField;

  private final VersionType versionType;
  private final String versionField;

  //flag for using partial update API when sending documents to opensearch
  private final boolean update;

  private final String childDocumentsField;

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass,
      String metricsPrefix,String localRunId, OpenSearchClient client) {
    super(config, messenger, bypass, metricsPrefix, localRunId);

    this.client = client;
    this.index = OpenSearchUtils.getOpenSearchIndex(config);
    this.routingField = ConfigUtils.getStringOrDefault(config, "indexer.routingField", null);
    this.update = ConfigUtils.getBooleanOrDefault(config, "opensearch.update", false);
    this.versionType =
        config.hasPath("indexer.versionType") ? parseVersionType(config.getString("indexer.versionType")) : null;
    this.childDocumentsField = ConfigUtils.getStringOrDefault(config, "opensearch.childDocumentsField", null);
    this.versionField = ConfigUtils.getStringOrDefault(config, "indexer.versionField", null);

    // validate config indexer.versionType that must be set if config indexer.versionField is set
    if (this.versionField != null && this.versionType == null) {
      throw new IllegalArgumentException("indexer.versionType must be set if indexer.versionField is set");
    }
  }

  public OpenSearchIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    this(config, messenger, bypass, metricsPrefix, localRunId, null);
    // built after super() and this class's config checks, so a constructor that throws can't leak the client's
    // connection pool and threads; nothing after this line can throw
    this.client = getClient(config, bypass);
  }

  // Convenience Constructors
  public OpenSearchIndexer(Config config, IndexerMessenger messenger, String metricsPrefix, OpenSearchClient client) {
    this(config, messenger, false, metricsPrefix, null, client);
  }

  @Override
  protected String getIndexerConfigKey() { return "opensearch"; }

  // The OpenSearch Java client is thread-safe over a shared connection pool, so concurrent sends through the single
  // shared client are safe. The pool is sized for the configured concurrency by AsyncConnectionPoolUtils.
  @Override
  protected boolean supportsConcurrentSends() {
    return true;
  }

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
      log.error("Non true response when pinging OpenSearch: {}", response);
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
  protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws IndexerException {
    // skip indexing if there is no indexer client
    if (bypass) {
      return Set.of();
    }

    Map<Pair<String, String>, Document> documentsToUpload = new LinkedHashMap<>();
    // the document that marked each (index, id) for deletion, so deleteById can send its version
    Map<Pair<String, String>, Document> idsToDelete = new LinkedHashMap<>();
    Map<Pair<String, String>, List<String>> termsToDeleteByQuery = new LinkedHashMap<>();
    Set<Pair<Document, Exception>> failedDocs = new HashSet<>();

    // populate which collection each document belongs to
    // upload if document is not marked for deletion
    // else if document is marked for deletion ONLY, then only add to idsToDelete
    // else document is marked for deletion AND contains deleteByFieldField and deleteByFieldValue, only add to termsToDeleteByQuery
    // A delete-by-query must not delete documents that come after it in the batch, so the batch is sent in segments, in
    // input order: pending uploads and deletes by id are sent before a run of delete-by-query documents, and the
    // queries are sent before the next upload or delete by id.
    for (Document doc : documents) {
      // use doc id override if specified, otherwise delete will try to delete wrong id
      String id = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());

      // if an index override field has been specified, use its value as the index to send to opensearch,
      final String indexToSend = Optional.ofNullable(getIndexOverride(doc)).orElse(index);

      Pair<String, String> indexAndId = Pair.of(indexToSend, id);

      if (!isMarkedForDeletion(doc)) {
        // send any buffered delete-by-queries now, so they apply before this write rather than after it
        sendDeleteByQuerySegment(termsToDeleteByQuery);
        idsToDelete.remove(indexAndId);
        documentsToUpload.put(indexAndId, doc);
      } else {
        documentsToUpload.remove(indexAndId);
        if (!hasDeleteByFieldValues(doc)) {
          // same as above: buffered delete-by-queries go before this delete by id
          sendDeleteByQuerySegment(termsToDeleteByQuery);
          idsToDelete.put(indexAndId, doc);
        } else {
          // this doc is a delete-by-query, so flush the writes buffered before it first: send the pending uploads and
          // deletes by id, clear both maps, and collect their per-doc failures
          failedDocs.addAll(sendWriteSegment(documentsToUpload, idsToDelete));
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

    // at most one of these has anything left to send
    failedDocs.addAll(sendWriteSegment(documentsToUpload, idsToDelete));
    sendDeleteByQuerySegment(termsToDeleteByQuery);

    return failedDocs;
  }

  // Sends and clears the pending uploads and deletes by id. They never share an (index, id), so their order between
  // each other doesn't matter.
  private Set<Pair<Document, Exception>> sendWriteSegment(Map<Pair<String, String>, Document> documentsToUpload,
      Map<Pair<String, String>, Document> idsToDelete) throws IndexerException {
    Set<Pair<Document, Exception>> failedDocs = uploadDocuments(documentsToUpload);
    deleteById(idsToDelete);
    documentsToUpload.clear();
    idsToDelete.clear();
    return failedDocs;
  }

  private void sendDeleteByQuerySegment(Map<Pair<String, String>, List<String>> termsToDeleteByQuery) throws IndexerException {
    deleteByQuery(termsToDeleteByQuery);
    termsToDeleteByQuery.clear();
  }

  private void deleteById(Map<Pair<String, String>, Document> idsToDelete) throws IndexerException {
    if (idsToDelete.isEmpty()) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();
    // whether each delete, in request order, was sent with a version; the response items come back in the same order
    List<Boolean> versioned = new ArrayList<>();
    for (Map.Entry<Pair<String, String>, Document> entry : idsToDelete.entrySet()) {
      Pair<String, String> indexAndId = entry.getKey();
      Long versionNum = getDeleteVersionNum(entry.getValue());
      versioned.add(versionNum != null);
      br.operations(op -> op
          .delete(d -> {
            d.index(indexAndId.getLeft()).id(indexAndId.getRight());
            // a versioned delete is rejected (409) when the index holds a newer version, and leaves a tombstone that
            // rejects an older upload arriving after it, for as long as the index keeps tombstones (index.gc_deletes)
            if (versionNum != null) {
              d.versionType(versionType).version(versionNum);
            }
            return d;
          })
      );
    }

    BulkResponse response;
    try {
      response = client.bulk(br.build());
    } catch (OpenSearchException e) {
      throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      throw new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
    }

    if (response.errors()) {
      boolean failed = false;
      List<BulkResponseItem> items = response.items();
      for (int i = 0; i < items.size(); i++) {
        BulkResponseItem item = items.get(i);
        if (item.error() == null) {
          continue;
        }
        // a 409 on a versioned delete means the index already holds this version or a newer one: either this delete
        // was applied on an earlier attempt of the batch (a retry resends it), or a newer document superseded it
        if (item.status() == 409 && i < versioned.size() && versioned.get(i)) {
          log.debug("Versioned delete of id: {} already applied or superseded: {}", item.id(), item.error().reason());
          continue;
        }
        log.debug("Error while deleting id: {} because: {}", item.id(), item.error().reason());
        failed = true;
      }
      if (failed) {
        throw new IndexerException("encountered errors while deleting documents");
      }
    }
  }

  private void deleteByQuery(Map<Pair<String, String>, List<String>> termsToDeleteByQuery) throws IndexerException {
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
        // a delete-by-query only matches what the index has refreshed, so refresh first: otherwise it misses documents
        // uploaded just before it, in this batch or an earlier one, and can fail on a document deleted but not yet refreshed
        client.indices().refresh(r -> r.index(entry.getKey()));
        response = client.deleteByQuery(deleteByQueryRequest);
      } catch (OpenSearchException e) {
        throw new IndexerRetryableException(e.status(), "OpenSearch returned HTTP " + e.status(), e);
      } catch (IOException e) {
        throw new IndexerRetryableException("Transport failure communicating with OpenSearch", e);
      }

      if (!response.failures().isEmpty()) {
        for (BulkByScrollFailure failure : response.failures()) {
          if (failure.cause() == null) {
            log.debug("Error while deleting by query, cause was null");
          } else {
            log.debug("Error while deleting by query: {}, because of: {}", failure.cause().reason(), failure.cause());
          }

        }
        throw new IndexerException("encountered errors while deleting by query");
      }
    }
  }

  private Set<Pair<Document, Exception>> uploadDocuments(Map<Pair<String, String>, Document> documentsToUpload) throws IndexerException {

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
      Map<String, Object> indexerDoc = getRawIndexerDoc(doc);

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

      if (doc.hasChildren()) {
        addChildren(doc, indexerDoc);
      }

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
              // doc_as_upsert: an update to an id not yet in the index creates it instead of failing with
              // document_missing_exception
              return up.document(indexerDoc).docAsUpsert(true);
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
    } catch (OpenSearchException e) {
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

  /** Only call on documents that have children. */
  private void addChildren(Document doc, Map<String, Object> indexerDoc) {
    if (childDocumentsField == null) {
      log.warn("Document {} has children but opensearch.childDocumentsField is not configured. They will not be indexed.", doc.getId());
      return;
    }

    List<Map<String, Object>> childDocMaps = new ArrayList<>();
    List<Document> children = doc.getChildren();

    for (Document child : children) {
      // calling getRawIndexerDoc applies black/whitelist and drops nested children, which we don't support
      childDocMaps.add(getRawIndexerDoc(child));
    }
    indexerDoc.put(childDocumentsField, childDocMaps);
  }

  // Accepts both the enum constant name (External, ExternalGte) and the JSON spelling (external, external_gte).
  private static VersionType parseVersionType(String name) {
    for (VersionType type : VersionType.values()) {
      if (type.name().equalsIgnoreCase(name) || type.jsonValue().equalsIgnoreCase(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown indexer.versionType \"" + name + "\"; expected one of "
        + java.util.Arrays.toString(VersionType.values()));
  }

  private Long getVersionNum(Document doc) {
    if (versionField != null && doc.has(versionField)) {
      return doc.getLong(versionField);
    }
    if (doc instanceof KafkaDocument kafkaDoc) {
      return kafkaDoc.getOffset();
    }
    return null;
  }

  // The version to send with a delete by id, or null for an unversioned delete. Unlike getVersionNum, this never falls
  // back to the Kafka offset: the offset of a delete is not comparable to a version taken from indexer.versionField, so
  // only a document that carries indexer.versionField gets a versioned delete.
  private Long getDeleteVersionNum(Document doc) {
    if ((versionType == VersionType.External || versionType == VersionType.ExternalGte)
        && versionField != null && doc.has(versionField)) {
      return doc.getLong(versionField);
    }
    return null;
  }
}
