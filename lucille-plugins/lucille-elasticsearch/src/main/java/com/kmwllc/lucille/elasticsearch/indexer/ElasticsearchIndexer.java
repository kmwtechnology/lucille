package com.kmwllc.lucille.elasticsearch.indexer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.BulkIndexByScrollFailure;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.elasticsearch.util.ElasticsearchUtils;
import com.kmwllc.lucille.indexer.IndexerType;
import com.kmwllc.lucille.util.FieldFilter;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// TODO: upgrade the ElasticsearchIndexer to use the Elasticsearch Java API Client

/**
 * Indexes documents to Elasticsearch using the Java Client.
 * Additional parameters are made available by the {@link com.kmwllc.lucille.core.Indexer} abstract class.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>index (String, Required) : Target Elasticsearch index name.</li>
 *   <li>url (String or List&lt;String&gt;, Required) : Elasticsearch HTTP endpoint (e.g., https://localhost:9200), or a list of
 *   endpoints for the same cluster. With a list, the client round-robins requests across the hosts and fails over between them.</li>
 *   <li>update (Boolean, Optional) : Use partial update API instead of index/replace, creating the document if it does not exist. Defaults to false.</li>
 *   <li>acceptInvalidCert (Boolean, Optional) : Allow invalid TLS certificates. Defaults to false.</li>
 *   <li>useCompression (Boolean, Optional) : Whether to use compression in the underlying Elasticsearch HTTP client. Defaults to false.</li>
 *   <li>connectTimeoutMs (Long, Optional) : Maximum time to establish a connection. Defaults to 5000.</li>
 *   <li>socketTimeoutMs (Long, Optional) : Maximum time to wait for data on an open connection, including a bulk or
 *   delete-by-query response. A timeout is a transport failure, retried when indexer.maxRetries is set. Defaults to
 *   30000; raise it for long deletes, or set it to -1 for no limit.</li>
 *   <li>connectionTimeToLiveMs (Long, Optional) : Maximum lifetime of a pooled connection, so long-running indexers
 *   behind a load balancer or Kubernetes Service reconnect and reach new nodes. Defaults to 300000 (5 minutes).</li>
 *   <li>childDocumentsField (String, Optional) : Field name to place attached child documents in the
 *   indexed document. If not set, child documents are not indexed. For child queries to work correctly, this field should be mapped
 *   as type "nested" in the index mapping.</li>
 *   <li>routingField (String, Optional) : Document field that supplies the routing key.</li>
 *   <li>versionType (String, Optional) : Versioning type when using external versions. With External or ExternalGte,
 *   an upload carries the document's versionField value, or else its Kafka offset.</li>
 *   <li>versionField (String, Optional) : Document field holding the external version. Requires
 *   versionType. With External or ExternalGte, a delete by id carries this field's value too; a delete without it
 *   is unversioned, and never uses the Kafka offset.</li>
 *   <li>elasticsearch.join.joinFieldName (String, Optional) : Name of the join field mapped in the index.</li>
 *   <li>elasticsearch.join.isChild (Boolean, Optional) : Whether documents are children in the join relation. Defaults to false.</li>
 *   <li>elasticsearch.join.childName (String, Optional) : Child relation name when isChild is true.</li>
 *   <li>elasticsearch.join.parentDocumentIdSource (String, Optional) : Source field for the parent document id when isChild is true.</li>
 *   <li>elasticsearch.join.parentName (String, Optional) : Parent relation name when isChild is false.</li>
 * </ul>
 */
@AutoService(Indexer.class)
@IndexerType("Elasticsearch")
public class ElasticsearchIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("index")
      .requiredStringOrList("url")
      .optionalBoolean("update", "acceptInvalidCert", "useCompression")
      .optionalNumber("connectTimeoutMs", "socketTimeoutMs", "connectionTimeToLiveMs")
      .optionalString("childDocumentsField", "routingField", "versionType", "versionField")
      .optionalNumber("maxConnectionsPerRoute", "maxConnectionsTotal")
      .optionalParent("join", new TypeReference<Map<String, String>>() {}).build();

  private static final Logger log = LoggerFactory.getLogger(ElasticsearchIndexer.class);

  // not final: the standard constructor builds it only after validation, see there
  private ElasticsearchClient client;
  private final String index;

  //flag for using partial update API when sending documents to elastic
  private final boolean update;
  private final ElasticJoinData joinData;
  private final String routingField;
  private final VersionType versionType;
  private final String childDocumentsField;
  private final String versionField;

  public ElasticsearchIndexer(Config config, IndexerContext context, ElasticsearchClient client) {
    super(config, context);

    if (getIndexOverrideField() != null) {
      throw new IllegalArgumentException(
          "Cannot create ElasticsearchIndexer. Config setting 'indexer.indexOverrideField' is not supported by ElasticsearchIndexer.");
    }

    this.client = client;
    this.index = ElasticsearchUtils.getElasticsearchIndex(config);
    this.update = ConfigUtils.getBooleanOrDefault(config, "elasticsearch.update", false);

    joinData = ElasticJoinData.fromConfig(config);
    this.routingField = ConfigUtils.getStringOrDefault(config, "elasticsearch.routingField", null);
    this.versionType = config.hasPath("elasticsearch.versionType") ? parseVersionType(config.getString("elasticsearch.versionType")) : null;
    this.versionField = ConfigUtils.getStringOrDefault(config, "elasticsearch.versionField", null);
    if (this.versionField != null && this.versionType == null) {
      throw new IllegalArgumentException("elasticsearch.versionType must be set if elasticsearch.versionField is set");
    }
    this.childDocumentsField = ConfigUtils.getStringOrDefault(config, "elasticsearch.childDocumentsField", null);
  }

  // Accepts both the enum constant name (External, ExternalGte) and the JSON spelling (external, external_gte).
  private static VersionType parseVersionType(String name) {
    for (VersionType type : VersionType.values()) {
      if (type.name().equalsIgnoreCase(name) || type.jsonValue().equalsIgnoreCase(name)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown elasticsearch.versionType \"" + name + "\"; expected one of "
        + java.util.Arrays.toString(VersionType.values()));
  }

  // The version to send with a document: the configured versionField if present, else the Kafka offset for a
  // document read from Kafka, else none.
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
  // back to the Kafka offset: the offset of a delete is not comparable to a version taken from elasticsearch.versionField, so
  // only a document that carries elasticsearch.versionField gets a versioned delete.
  private Long getDeleteVersionNum(Document doc) {
    if ((versionType == VersionType.External || versionType == VersionType.ExternalGte)
        && versionField != null && doc.has(versionField)) {
      return doc.getLong(versionField);
    }
    return null;
  }

  public ElasticsearchIndexer(Config config, IndexerContext context) throws IndexerException {
    this(config, context, null);
    // built after super() and this class's config checks, so a constructor that throws can't leak the client's
    // connection pool and threads; nothing after this line can throw
    this.client = getClient(config, isBypass());
  }

  @Override
  protected String getIndexerConfigKey() { return "elasticsearch"; }

  // The Elasticsearch Java client is thread-safe over a shared connection pool, so concurrent sends through the single
  // shared client are safe. The pool is sized for the configured concurrency by AsyncConnectionPoolUtils.
  @Override
  protected boolean supportsConcurrentSends() {
    return true;
  }

  private static ElasticsearchClient getClient(Config config, boolean bypass) throws IndexerException{
    try {
      return bypass ? null : ElasticsearchUtils.getElasticsearchOfficialClient(config);
    } catch (Exception e) {
      throw new IndexerException("Couldn't create Elasticsearch client", e);
    }
  }

  @Override
  protected boolean checkConnection() {
    BooleanResponse response;
    try {
      response = client.ping();
    } catch (Exception e) {
      log.error("Couldn't ping Elasticsearch ", e);
      return false;
    }
    if (response == null || !response.value()) {
      log.error("Non true response when pinging Elasticsearch: {}", response);
      return false;
    }
    return true;
  }

  @Override
  protected List<IndexerFailure> sendToIndex(List<Document> documents) throws IndexerException {
    Map<String, Document> documentsToUpload = new LinkedHashMap<>();
    // the document that marked each id for deletion, so deleteById can send its version
    Map<String, Document> idsToDelete = new LinkedHashMap<>();
    Map<String, List<String>> termsToDeleteByQuery = new LinkedHashMap<>();
    List<IndexerFailure> failedDocs = new ArrayList<>();

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

      if (!isMarkedForDeletion(doc)) {
        // send any buffered delete-by-queries now, so they apply before this write rather than after it
        sendDeleteByQuerySegment(termsToDeleteByQuery);
        idsToDelete.remove(id);
        documentsToUpload.put(id, doc);
      } else {
        documentsToUpload.remove(id);
        if (!hasDeleteByFieldValues(doc)) {
          // same as above: buffered delete-by-queries go before this delete by id
          sendDeleteByQuerySegment(termsToDeleteByQuery);
          idsToDelete.put(id, doc);
        } else {
          // this doc is a delete-by-query, so flush the writes buffered before it first: send the pending uploads and
          // deletes by id, clear both maps, and collect their per-doc failures
          failedDocs.addAll(sendWriteSegment(documentsToUpload, idsToDelete));
          //indexer.deleteByFieldField gives you the field in the document that holds which field whose value is queried for
          // deletion, so we need to do doc.getString(deleteByFieldField).
          String deleteField = doc.getString(getDeleteByFieldField());
          if (!termsToDeleteByQuery.containsKey(deleteField)) {
            termsToDeleteByQuery.put(deleteField, new ArrayList<>());
          }
          termsToDeleteByQuery.get(deleteField).add(doc.getString(getDeleteByFieldValue()));
        }
      }
    }

    // at most one of these has anything left to send
    failedDocs.addAll(sendWriteSegment(documentsToUpload, idsToDelete));
    sendDeleteByQuerySegment(termsToDeleteByQuery);

    return failedDocs;
  }

  // Sends and clears the pending uploads and deletes by id. They never share an id, so their order between each other
  // doesn't matter.
  private List<IndexerFailure> sendWriteSegment(Map<String, Document> documentsToUpload,
      Map<String, Document> idsToDelete) throws IndexerException {
    List<IndexerFailure> failedDocs = uploadDocuments(documentsToUpload.values());
    deleteById(idsToDelete);
    documentsToUpload.clear();
    idsToDelete.clear();
    return failedDocs;
  }

  private void sendDeleteByQuerySegment(Map<String, List<String>> termsToDeleteByQuery) throws IndexerException {
    deleteByQuery(termsToDeleteByQuery);
    termsToDeleteByQuery.clear();
  }

  private void deleteById(Map<String, Document> idsToDelete) throws IndexerException {
    if (idsToDelete.isEmpty()) {
      return;
    }

    BulkRequest.Builder br = new BulkRequest.Builder();
    // whether each delete, in request order, was sent with a version; the response items come back in the same order
    List<Boolean> versioned = new ArrayList<>();
    for (Map.Entry<String, Document> entry : idsToDelete.entrySet()) {
      String id = entry.getKey();
      Long versionNum = getDeleteVersionNum(entry.getValue());
      versioned.add(versionNum != null);
      br.operations(op -> op
          .delete(d -> {
            d.index(index).id(id);
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
    } catch (ElasticsearchException e) {
      throw new IndexerRetryableException(e.status(), "ElasticSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      throw new IndexerRetryableException("Transport failure communicating with ElasticSearch", e);
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

  private void deleteByQuery(Map<String, List<String>> termsToDeleteByQuery) throws IndexerException {
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

    DeleteByQueryResponse response;
    try {
      // a delete-by-query only matches what the index has refreshed, so refresh first: otherwise it misses documents
      // uploaded just before it, in this batch or an earlier one, and can fail on a document deleted but not yet refreshed
      client.indices().refresh(r -> r.index(index));
      response = client.deleteByQuery(deleteByQueryRequest);
    } catch (ElasticsearchException e) {
      throw new IndexerRetryableException(e.status(), "ElasticSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      throw new IndexerRetryableException("Transport failure communicating with ElasticSearch", e);
    }

    if (!response.failures().isEmpty()) {
      for (BulkIndexByScrollFailure failure : response.failures()) {
        log.debug("Error while deleting by query: {}, because of: {}", failure.cause().reason(), failure.cause());
      }
      throw new IndexerException("encountered errors while deleting by query");
    }
  }

  private List<IndexerFailure> uploadDocuments(Collection<Document> documentsToUpload) throws IndexerException {
    if (documentsToUpload.isEmpty()) {
      return List.of();
    }

    // building a map so we can quickly retrieve documents later on, if they fail during indexing.
    Map<String, Document> documentsUploaded = new LinkedHashMap<>();
    BulkRequest.Builder br = new BulkRequest.Builder();

    for (Document doc : documentsToUpload) {
      // removing fields in the blacklist or not in the whitelist in configurations
      Map<String, Object> indexerDoc = getRawIndexerDoc(doc);

      // if a doc id override value exists, make sure it is used instead of pre-existing doc id
      String docId = Optional.ofNullable(getDocIdOverride(doc)).orElse(doc.getId());
      documentsUploaded.put(docId, doc);

      // written to the request, not the Document, so a retried batch doesn't find it already set
      joinData.populateJoinData(doc, indexerDoc, getFieldFilter());

      // only add id if our fieldFilter allows it (based on our blacklist and whitelist)
      // - Case 1: id and idOverride filtered out -> idOverride used by Indexer, both removed from Document (tested in testBlacklistWithOverride)
      // - Case 2: id is filtered, idOverride exists -> idOverride used by Indexer, only id field removed from Document (tested in testBlacklistWithOverride2)
      // - Case 3: id is filtered, idOverride null -> id used by Indexer, id also removed from Document (tested in testRouting)
      // - Case 4: both unfiltered, idOverride exists -> idOverride used by Indexer, id and idOverride field exist in Document (tested in testOverride)
      // - Case 5: both unfiltered, idOverride null -> document id remains and used by Indexer (Default case & tested)
      if (getFieldFilter().shouldInclude(Document.ID_FIELD)) {
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
                  // doc_as_upsert: an update to an id not yet in the index creates it instead of failing with
                  // document_missing_exception
                  .action(upx -> upx
                      .doc(indexerDoc)
                      .docAsUpsert(true)
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

    List<IndexerFailure> failedDocs = new ArrayList<>();

    BulkResponse response;
    try {
      response = client.bulk(br.build());
    } catch (ElasticsearchException e) {
      // HTTP-level error with a known status code — wrap so the base Indexer can apply retry policy
      throw new IndexerRetryableException(e.status(), "ElasticSearch returned HTTP " + e.status(), e);
    } catch (IOException e) {
      // Transport-level failure (connection refused, timeout, etc.) — no status code available
      throw new IndexerRetryableException("Transport failure communicating with ElasticSearch", e);
    }

    if (response != null) {
      for (BulkResponseItem item : response.items()) {
        if (item.error() != null) {
          // For the error - if the id is a document's id, then it failed, and we add it to the set.
          // If not, we don't know what the error is, and opt to throw an actual IndexerException instead.
          if (documentsUploaded.containsKey(item.id())) {
            Document failedDoc = documentsUploaded.get(item.id());
            // item.status() is always an HTTP status code. The Elasticsearch bulk API defines the per-item
            // status field as the HTTP status of that individual operation (e.g. 200, 201, 400, 409, 429,
            // 503). Wrapping it as an IndexerRetryableException lets the base Indexer apply the retry policy
            // per-document, retrying only those whose status code is in the configured retryable set.
            failedDocs.add(new IndexerFailure(failedDoc, new IndexerRetryableException(item.status(),
                "Elasticsearch bulk item error (status " + item.status() + "): " + item.error().reason(), null)));
          } else {
            throw new IndexerException(item.error().reason());
          }
        }
      }
    }

    return failedDocs;
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

  /** Only call on documents that have children. */
  private void addChildren(Document doc, Map<String, Object> indexerDoc) {
    if (childDocumentsField == null) {
      log.warn("Document {} has children but elasticsearch.childDocumentsField is not configured. They will not be indexed.", doc.getId());
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

  public static class ElasticJoinData {

    private String joinFieldName;
    private boolean isChild;
    private String parentName;
    private String childName;
    private String parentDocumentIdSource;

    public void populateJoinData(Document doc, Map<String, Object> indexerDoc, FieldFilter fieldFilter) {

      // no need to do a join
      if (joinFieldName == null) {
        return;
      }

      // check that join field doesn't already exist
      if (doc.has(joinFieldName)) {
        throw new IllegalStateException("Document already has join field: " + joinFieldName);
      }

      // the join field follows indexer.whitelist and indexer.blacklist like any other field
      if (!fieldFilter.shouldInclude(joinFieldName)) {
        return;
      }

      // set a necessary join field
      if (isChild) {
        String parentId = doc.getString(parentDocumentIdSource);
        indexerDoc.put(joinFieldName, getChildNode(parentId));
      } else {
        indexerDoc.put(joinFieldName, parentName);
      }
    }

    private Map<String, Object> getChildNode(String parentId) {
      Map<String, Object> node = new LinkedHashMap<>();
      node.put("name", childName);
      node.put("parent", parentId);
      return node;
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

}
